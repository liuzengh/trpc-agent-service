package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/artifact"
	artifactinmemory "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	memory "trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	ttool "trpc.group/trpc-go/trpc-agent-go/tool"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

// fakeKnowledge is a knowledge.Knowledge stub for assembly wiring.
type fakeKnowledge struct{}

func (fakeKnowledge) Search(context.Context, *knowledge.SearchRequest) (*knowledge.SearchResult, error) {
	return &knowledge.SearchResult{}, nil
}

// fakeMemory is a memory.Service stub whose only job is to contribute its
// tools to the assembled agent.
type fakeMemory struct{}

func (fakeMemory) ReadMemories(context.Context, memory.UserKey, int) ([]*memory.Entry, error) {
	return nil, nil
}
func (fakeMemory) SearchMemories(context.Context, memory.UserKey, string, ...memory.SearchOption) ([]*memory.Entry, error) {
	return nil, nil
}
func (fakeMemory) AddMemory(context.Context, memory.UserKey, string, []string, ...memory.AddOption) error {
	return nil
}
func (fakeMemory) UpdateMemory(context.Context, memory.Key, string, []string, ...memory.UpdateOption) error {
	return nil
}
func (fakeMemory) DeleteMemory(context.Context, memory.Key) error { return nil }
func (fakeMemory) ClearMemories(context.Context, memory.UserKey) error {
	return nil
}
func (fakeMemory) Tools() []ttool.Tool { return nil }
func (fakeMemory) EnqueueAutoMemoryJob(context.Context, *session.Session) error {
	return nil
}
func (fakeMemory) Close() error { return nil }

var _ memory.Service = fakeMemory{}

// unusedArtifact keeps the artifact import tied to the interface shape.
var _ artifact.Service = artifactinmemory.NewService()

// newRunnerProcessor falls back to the platform defaults for a non-positive
// timeout and a negative retry count.
func TestNewRunnerProcessorDefaults(t *testing.T) {
	p := newRunnerProcessor(&fakeRunner{}, "test-model", 0, -1)
	if p.timeout != DefaultRunTimeout {
		t.Fatalf("timeout = %v, want DefaultRunTimeout", p.timeout)
	}
	if p.retries != 0 {
		t.Fatalf("retries = %d, want 0", p.retries)
	}
}

// NewRunnerProcessor wires the optional memory / knowledge / artifact
// services and tool callbacks onto the assembled agent.
func TestNewRunnerProcessorWiresOptionalServices(t *testing.T) {
	temp := 0.5
	cfg := RunnerConfig{
		AppName:        "app",
		BaseURL:        "https://api.deepseek.com",
		APIKey:         "sk-test",
		ModelName:      "test-model",
		Instruction:    "be helpful",
		Temperature:    &temp,
		SessionService: nil, // not exercised by construction
		Timeout:        time.Second,
		Retries:        1,
		ToolCallbacks:  ttool.NewCallbacks(),
		MemoryService:  fakeMemory{},
		Knowledge:      fakeKnowledge{},
		KnowledgeFilter: map[string]any{
			"tenant_id": "t1",
			"app_id":    "a1",
		},
		ArtifactService: artifactinmemory.NewService(),
	}
	p := NewRunnerProcessor(cfg)
	if p == nil {
		t.Fatal("processor must be built with the optional services wired")
	}
	if p.timeout != time.Second || p.retries != 1 || p.model != "test-model" {
		t.Fatalf("unexpected processor fields: %+v", p)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// A markdown reply is marked as such so channels can render it.
func TestRunnerProcessorMarksMarkdownReply(t *testing.T) {
	p := newRunnerProcessor(&fakeRunner{run: scriptedEvents(finalEvent("## 标题\n- 列表"))},
		"test-model", time.Second, 0)
	out, err := p.Process(context.Background(), testMsg("hi"))
	if err != nil {
		t.Fatal(err)
	}
	if out.TextType != channels.TextTypeMarkdown {
		t.Fatalf("markdown reply must be marked, got text type %q", out.TextType)
	}
}

// A plain reply stays unmarked.
func TestRunnerProcessorPlainTextStaysUnmarked(t *testing.T) {
	p := newRunnerProcessor(&fakeRunner{run: scriptedEvents(finalEvent("你好"))},
		"test-model", time.Second, 0)
	out, err := p.Process(context.Background(), testMsg("hi"))
	if err != nil {
		t.Fatal(err)
	}
	if out.TextType != "" {
		t.Fatalf("plain reply must not be marked, got %q", out.TextType)
	}
}

// Cancellation during the inter-attempt backoff aborts the retry loop with
// the raw cancel error (drain/shutdown path).
func TestRunnerProcessorCancelDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &fakeRunner{run: func(ctx context.Context, _ int) (<-chan *event.Event, error) {
		// First attempt fails with a plain model error; the cancel fires
		// while the retry backoff is sleeping.
		time.AfterFunc(80*time.Millisecond, cancel)
		return scriptedEvents(errorEvent("boom"))(ctx, 0)
	}}
	p := newRunnerProcessor(r, "test-model", time.Second, 3)
	_, err := p.Process(ctx, channels.InboundMessage{Channel: "mock", SessionKey: "s", UserID: "u", Text: "hi"})
	if err == nil {
		t.Fatal("canceled retry loop must return an error")
	}
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want the raw cancel error, got %v", err)
	}
}

// The token usage observed on the event stream is accumulated in the outbound
// message across retries.
func TestRunnerProcessorAccumulatesTokenUsage(t *testing.T) {
	r := &fakeRunner{run: func(ctx context.Context, call int) (<-chan *event.Event, error) {
		if call == 1 {
			return scriptedEvents(errorEvent("rate limited"))(ctx, call)
		}
		return scriptedEvents(finalEvent("ok"))(ctx, call)
	}}
	p := newRunnerProcessor(r, "test-model", time.Second, 1)
	out, err := p.Process(context.Background(), testMsg("hi"))
	if err != nil {
		t.Fatal(err)
	}
	if out.PromptTokens != 10 || out.CompletionTokens != 5 {
		t.Fatalf("token usage must accumulate, got %+v", out)
	}
	if r.callCount() != 2 {
		t.Fatalf("want 2 attempts, got %d", r.callCount())
	}
}
