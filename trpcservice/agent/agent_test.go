package agent

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	// runBudget bounds one test run. A regression that reopens the unbounded
	// call loop makes the run hit this deadline instead of hanging the suite,
	// and the hit count in the failure says how far it got.
	runBudget = 5 * time.Second

	// emptyStream is the degenerate upstream: a well-formed 200 stream that
	// carries no content and no finish_reason. It never satisfies the flow's
	// exit condition, which is exactly what makes it dangerous.
	emptyStream = "data: [DONE]\n\n"

	// oneReply is the smallest stream that reads as a complete answer: a
	// content chunk, a finish chunk, then [DONE]. It is the shape
	// cmd/fake-model emits, so the framework's client parses it identically.
	oneReply = `data: {"id":"chatcmpl-fake","object":"chat.completion.chunk","created":1,` +
		`"model":"fake-model","choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]}` +
		"\n\n" +
		`data: {"id":"chatcmpl-fake","object":"chat.completion.chunk","created":1,` +
		`"model":"fake-model","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}` +
		"\n\n" +
		"data: [DONE]\n\n"
)

// sessionSeq hands every run its own session, so one test never inherits
// another's history — and so a run that ended on the call cap cannot influence
// the run that is supposed to prove the cap.
var sessionSeq atomic.Int64

// countingModel serves body for every completion and counts the calls. The
// count is the whole point: it is what turns "the platform coped" into a
// number an operator can reason about.
func countingModel(t *testing.T, body string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("test server cannot stream")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, body)
		flusher.Flush()
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func demoTenant(baseURL string) *tenant.Context {
	return &tenant.Context{
		ID:    "demo",
		Model: tenant.ModelConfig{Name: "m", APIKey: "sk-test", BaseURL: baseURL},
	}
}

func configWith(baseURL string, calls int) *config.Config {
	return &config.Config{
		DefaultTenant: "demo",
		Agent: config.AgentConfig{
			MessageTimeout: config.DefaultMessageTimeout,
			MaxLLMCalls:    calls,
		},
		Tenants: map[string]*tenant.Context{"demo": demoTenant(baseURL)},
	}
}

// runOne sends one message and returns everything the run surfaced: the error
// runner.Run reported, or the text of every error event it emitted. Which of
// the two carries a flow error is the framework's choice rather than ours, so
// the assertions look at the union.
func runOne(t *testing.T, r runner.Runner, text string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), runBudget)
	defer cancel()

	sid := fmt.Sprintf("demo:webchat:u%d", sessionSeq.Add(1))
	events, err := r.Run(ctx, "u1", sid, model.NewUserMessage(text))
	if err != nil {
		return err.Error()
	}
	var b strings.Builder
	for ev := range events {
		if ev.IsError() && ev.Response != nil && ev.Response.Error != nil {
			b.WriteString(ev.Response.Error.Message)
			b.WriteString("; ")
		}
	}
	return b.String()
}

// TestEmptyUpstreamIsBoundedByTheCallCap is the regression guard for what the
// fault drill found: against an upstream that answers 200 with an empty stream,
// the framework's LLM cycle never reaches a final response and calls the model
// again immediately. With no cap configured that ran at roughly 8.3k upstream
// calls per second for as long as the message budget lasted — 16,588 calls for
// one message on a 2s budget, and the default budget is two minutes
// (docs/spec-deployment-fault-drill.md §4.5). One user message became an
// amplification attack on the upstream. The cap makes that cost a constant the
// operator chose, and names it in the error.
func TestEmptyUpstreamIsBoundedByTheCallCap(t *testing.T) {
	const ceiling = 3
	srv, hits := countingModel(t, emptyStream)

	reg, err := NewRegistry(configWith(srv.URL, ceiling), inmemory.NewSessionService())
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	r, ok := reg.Runner("demo")
	if !ok {
		t.Fatal("no runner for demo")
	}

	surfaced := runOne(t, r, "hello")

	if got := hits.Load(); got != ceiling {
		t.Errorf("upstream calls = %d, want exactly %d: one message must cost a bounded number of calls", got, ceiling)
	}
	want := fmt.Sprintf("max LLM calls (%d) exceeded", ceiling)
	if !strings.Contains(surfaced, want) {
		t.Errorf("the run ended without naming the cap %q; surfaced: %q", want, surfaced)
	}
}

// TestNonPositiveCapIsCorrectedNotTrusted covers the other way the hole
// reopens: a caller that passes 0 or a negative cap. The framework reads both
// as "no limit", so NewRunner substitutes the default instead of forwarding
// them — a hand-built config, or a copy that loses the field, must not be able
// to silently disable the bound.
func TestNonPositiveCapIsCorrectedNotTrusted(t *testing.T) {
	for _, asked := range []int{0, -1} {
		t.Run(fmt.Sprintf("asked=%d", asked), func(t *testing.T) {
			srv, hits := countingModel(t, emptyStream)

			r, err := NewRunner(demoTenant(srv.URL), inmemory.NewSessionService(), asked, nil)
			if err != nil {
				t.Fatalf("runner: %v", err)
			}
			runOne(t, r, "hello")

			if got := hits.Load(); got != int32(config.DefaultMaxLLMCalls) {
				t.Errorf("upstream calls = %d, want the default cap %d: a non-positive cap must be corrected, not trusted",
					got, config.DefaultMaxLLMCalls)
			}
		})
	}
}

// TestBehavingUpstreamCostsOneCall pins the side that must not regress: the cap
// is a ceiling, not a tax. A complete reply ends the run on the first call.
func TestBehavingUpstreamCostsOneCall(t *testing.T) {
	srv, hits := countingModel(t, oneReply)

	reg, err := NewRegistry(configWith(srv.URL, config.DefaultMaxLLMCalls), inmemory.NewSessionService())
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	r, ok := reg.Runner("demo")
	if !ok {
		t.Fatal("no runner for demo")
	}
	runOne(t, r, "hello")

	if got := hits.Load(); got != 1 {
		t.Errorf("upstream calls = %d, want 1: a complete reply must end the run", got)
	}
}

// TestApplyPicksUpANewCap is the agent-side half of the /admin/settings hot
// update. Unlike the message budget, which the gateway re-reads per dispatch,
// the cap is baked into the runner at construction — so changing it only takes
// effect if Apply rebuilds the runners with the new envelope. This is the test
// that would catch a commit path that saves the config but forgets to rebuild.
func TestApplyPicksUpANewCap(t *testing.T) {
	srv, hits := countingModel(t, emptyStream)

	reg, err := NewRegistry(configWith(srv.URL, 2), inmemory.NewSessionService())
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	r, ok := reg.Runner("demo")
	if !ok {
		t.Fatal("no runner for demo")
	}
	runOne(t, r, "hello")
	if got := hits.Load(); got != 2 {
		t.Fatalf("before the change: upstream calls = %d, want 2", got)
	}

	hits.Store(0)
	if err := reg.Apply(configWith(srv.URL, 5)); err != nil {
		t.Fatalf("apply: %v", err)
	}
	r, ok = reg.Runner("demo")
	if !ok {
		t.Fatal("no runner for demo after apply")
	}
	runOne(t, r, "hello")

	if got := hits.Load(); got != 5 {
		t.Errorf("after the change: upstream calls = %d, want 5 — Apply did not rebuild the runners with the new cap", got)
	}
}

func TestNewRunnerRequiresAnAPIKey(t *testing.T) {
	_, err := NewRunner(&tenant.Context{ID: "demo"}, inmemory.NewSessionService(), 3, nil)
	if err == nil {
		t.Fatal("expected an error for a tenant without an api key")
	}
	if !strings.Contains(err.Error(), "api key is required") {
		t.Errorf("error = %q, want it to name the missing api key", err)
	}
}
