package worker

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/llm"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

// countingModel answers with a fixed text and counts the generations, so a test
// can tell "the turn ran again" from "the turn was resumed".
type countingModel struct {
	mu    sync.Mutex
	calls int
	text  string
}

func (m *countingModel) Info() model.Info { return model.Info{Name: "counting"} }

func (m *countingModel) GenerateContent(context.Context, *model.Request) (<-chan *model.Response, error) {
	m.mu.Lock()
	m.calls++
	m.mu.Unlock()
	ch := make(chan *model.Response, 1)
	ch <- finalAssistant(m.text)
	close(ch)
	return ch, nil
}

func (m *countingModel) generations() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

// sessionEventFixture builds a worker whose session backend is the shared
// in-memory service, so a test can inspect what a turn persisted.
func sessionEventFixture(t *testing.T) (*Worker, *storage.Router, *countingModel, *agent.Manager) {
	t.Helper()
	ctx := context.Background()
	mdl := &countingModel{text: "记住了"}
	reg := llm.NewRegistry(func(context.Context, llm.Endpoint) (model.Model, error) { return mdl, nil })
	if err := reg.Create(ctx, llm.Endpoint{
		ID: "e-evt", Scope: llm.ScopeTenant, TenantID: "t-evt", Name: "main",
		Provider: "openai", BaseURL: "http://localhost", ModelName: "m",
	}); err != nil {
		t.Fatalf("endpoint: %v", err)
	}
	agents := agent.NewManager(reg)
	if err := agents.Create(ctx, agent.Agent{ID: "a-evt", TenantID: "t-evt", Name: "helper"}); err != nil {
		t.Fatalf("agent: %v", err)
	}
	if _, err := agents.Publish(ctx, "a-evt", agent.RuntimeProfile{
		SystemPrompt: "be helpful", EndpointID: "e-evt",
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	router := storage.NewRouter(tenant.NewManager(),
		storage.SessionConfig{Backend: storage.BackendInMemory},
		storage.MemoryConfig{Backend: storage.BackendInMemory},
	)
	return New(nil, agents, nil, nil, router, nil, nil, nil, nil), router, mdl, agents
}

// countUserTurns reports how many persisted events of the session carry the
// turn's user text.
func countUserTurns(t *testing.T, router *storage.Router, tenantID, userID, sessionID, text string) int {
	t.Helper()
	s, err := router.Sessions(context.Background(), tenantID)
	if err != nil {
		t.Fatalf("sessions: %v", err)
	}
	sess, err := s.Get(context.Background(), tenantID, userID, sessionID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	n := 0
	for i := range sess.Events {
		ev := sess.Events[i]
		if ev.Response == nil {
			continue
		}
		for _, ch := range ev.Response.Choices {
			if strings.Contains(ch.Message.Content, text) {
				n++
			}
		}
	}
	return n
}

// TestTurnRequestIDMakesTheSessionLogReplayAware pins the event-level
// idempotency contract: the run carries the inbound message id as its
// RequestID, so a replayed turn is recognisable in the session log instead of
// looking like a brand-new user message. Two runs of the same message must not
// produce two user turns for the model to answer.
func TestTurnRequestIDMakesTheSessionLogReplayAware(t *testing.T) {
	ctx := context.Background()
	w, router, mdl, _ := sessionEventFixture(t)

	const userText = "帮我记一下偏好"
	msg := inbound("m-evt-1", "t-evt", "a-evt", "s-evt-1", "u-1", userText)
	if _, _, _, err := w.run(ctx, "a-evt", msg, "lock-1"); err != nil {
		t.Fatalf("first turn: %v", err)
	}
	if got := countUserTurns(t, router, "t-evt", "u-1", "s-evt-1", userText); got != 1 {
		t.Fatalf("after the first turn the session has %d user turns, want 1", got)
	}
	if mdl.generations() != 1 {
		t.Fatalf("model generations = %d after one turn, want 1", mdl.generations())
	}

	// A replay of the same inbound message (the crash-recovery path).
	reply, _, _, err := w.run(ctx, "a-evt", msg, "lock-1")
	if err != nil {
		t.Fatalf("replayed turn: %v", err)
	}
	got := countUserTurns(t, router, "t-evt", "u-1", "s-evt-1", userText)
	if got != 1 {
		t.Errorf("after the replay the session has %d user turns, want 1: RequestID did not identify the already-persisted turn", got)
	}
	if mdl.generations() != 1 {
		t.Errorf("model generations = %d, want 1: the replay re-ran a turn whose answer was already recorded", mdl.generations())
	}
	if reply == nil || reply.Content == nil || reply.Content.Content != "记住了" {
		t.Errorf("resumed reply = %+v, want the text recorded by the first attempt", reply)
	}
}

// TestRecordedAssistantTextIgnoresPartialAnswers pins the resume rule: a
// still-streaming answer must not be delivered as if it were complete.
func TestRecordedAssistantTextIgnoresPartialAnswers(t *testing.T) {
	final := func(text string) event.Event {
		return event.Event{
			RequestID: "m-1",
			Response: &model.Response{Choices: []model.Choice{{
				Message: model.Message{Role: model.RoleAssistant, Content: text},
			}}},
		}
	}
	partial := final("half an ans")
	partial.Response.IsPartial = true
	partial.Response.Choices[0].Delta = model.Message{Role: model.RoleAssistant, Content: "half an ans"}

	if got := recordedAssistantText([]event.Event{partial}, "m-1"); got != "" {
		t.Errorf("recordedAssistantText(partial) = %q, want empty (re-run rather than deliver a truncated reply)", got)
	}
	if got := recordedAssistantText([]event.Event{partial, final("complete")}, "m-1"); got != "complete" {
		t.Errorf("recordedAssistantText = %q, want complete", got)
	}
	// Another message's events must never be mistaken for this one's answer.
	other := final("someone else's answer")
	other.RequestID = "m-2"
	if got := recordedAssistantText([]event.Event{other}, "m-1"); got != "" {
		t.Errorf("recordedAssistantText(other request) = %q, want empty", got)
	}
	if got := recordedAssistantText(nil, "m-1"); got != "" {
		t.Errorf("recordedAssistantText(nil) = %q, want empty", got)
	}
}

// countEventsWithRequestID counts the session events stamped with a request id:
// the platform sets it to the inbound message id, which is what makes a turn
// traceable back to the message that caused it.
func countEventsWithRequestID(t *testing.T, router *storage.Router, tenantID, userID, sessionID, requestID string) int {
	t.Helper()
	s, err := router.Sessions(context.Background(), tenantID)
	if err != nil {
		t.Fatalf("sessions: %v", err)
	}
	sess, err := s.Get(context.Background(), tenantID, userID, sessionID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	n := 0
	for i := range sess.Events {
		if sess.Events[i].RequestID == requestID {
			n++
		}
	}
	return n
}

func TestTurnPersistsEventsUnderTheInboundMessageID(t *testing.T) {
	ctx := context.Background()
	w, router, _, _ := sessionEventFixture(t)

	msg := inbound("m-evt-id", "t-evt", "a-evt", "s-evt-id", "u-1", "hello")
	if _, _, _, err := w.run(ctx, "a-evt", msg, "lock-1"); err != nil {
		t.Fatalf("turn: %v", err)
	}
	if got := countEventsWithRequestID(t, router, "t-evt", "u-1", "s-evt-id", msg.ID); got == 0 {
		t.Error("no session event carries the inbound message id as its request id")
	}
}
