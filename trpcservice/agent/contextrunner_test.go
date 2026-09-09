package agent

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/stretchr/testify/require"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func testRunContext() identity.RunContext {
	return identity.RunContext{
		RequestID:   "request-1",
		TenantID:    "tenant-a",
		AppID:       "assistant",
		PrincipalID: "principal-1",
		SessionID:   "conversation-1",
		RevisionID:  "revision-1",
	}
}

func testContextRunner(inner *recordingRunner) *contextRunner {
	return &contextRunner{
		inner:      inner,
		tenantID:   "tenant-a",
		appID:      "assistant",
		revisionID: "revision-1",
	}
}

// A request that never passed platform authentication carries no scope, so the
// wrapper must refuse it instead of inventing one.
func TestContextRunnerRejectsRunWithoutTrustedScope(t *testing.T) {
	inner := &recordingRunner{}
	wrapper := testContextRunner(inner)

	events, err := wrapper.Run(
		context.Background(),
		"attacker",
		"attacker-session",
		model.NewUserMessage("hello"),
	)
	require.ErrorIs(t, err, ErrUntrustedRun)
	require.ErrorIs(t, err, identity.ErrNoRunContext)
	require.Nil(t, events)
	require.Empty(t, inner.recorded())

	unconfigured := &contextRunner{}
	events, err = unconfigured.Run(
		context.Background(), "attacker", "attacker-session", model.NewUserMessage("hello"),
	)
	require.ErrorIs(t, err, ErrUntrustedRun)
	require.Nil(t, events)
}

// A scope that belongs to another tenant, app or revision must not execute on
// this Runtime, even though it is a well-formed authenticated scope.
func TestContextRunnerRejectsScopeFromAnotherRuntime(t *testing.T) {
	for name, mutate := range map[string]func(*identity.RunContext){
		"tenant":   func(c *identity.RunContext) { c.TenantID = "tenant-b" },
		"app":      func(c *identity.RunContext) { c.AppID = "reporter" },
		"revision": func(c *identity.RunContext) { c.RevisionID = "revision-2" },
	} {
		t.Run(name, func(t *testing.T) {
			inner := &recordingRunner{}
			wrapper := testContextRunner(inner)
			scope := testRunContext()
			mutate(&scope)
			ctx, err := identity.WithRunContext(context.Background(), scope)
			require.NoError(t, err)

			events, err := wrapper.Run(ctx, "", "", model.NewUserMessage("hello"))
			require.ErrorIs(t, err, ErrUntrustedRun)
			require.ErrorContains(t, err, "does not match runtime")
			require.Nil(t, events)
			require.Empty(t, inner.recorded())
		})
	}
}

// The protocol adapter derives userID from the request body and sessionID from
// a request header. Both arguments have to be dropped.
func TestContextRunnerIgnoresProtocolSuppliedIdentity(t *testing.T) {
	inner := &recordingRunner{}
	wrapper := testContextRunner(inner)
	ctx, err := identity.WithRunContext(context.Background(), testRunContext())
	require.NoError(t, err)

	events, err := wrapper.Run(
		ctx,
		"attacker",
		"attacker-session",
		model.NewUserMessage("hello"),
		trpcagent.WithRequestID("request-1"),
	)
	require.NoError(t, err)
	require.NotNil(t, events)
	for range events {
	}

	require.Equal(t, []recordedRun{{
		userID:    "u/principal-1",
		sessionID: "conversation-1",
		content:   "hello",
	}}, inner.recorded())
}

// Whatever run options arrive, the run ends up labelled with the platform's id.
//
// Two upstream facts make this the wrapper's job. The OpenAI adapter does not
// mint a request id at all: buildRunOptions (server/openai/run_input.go) only
// assembles history, the tool-result rewriter and external tools. The Runner
// then generates one itself — `ro := agent.RunOptions{RequestID: uuid.NewString()}`
// before it applies the options, and a second uuid afterwards if an option left
// it empty (runner/runner.go:546-552). So without this wrapper every run would
// carry a fresh random id that nothing outside the framework has ever seen.
//
// The mechanism is positional: options are applied in order and the last write
// wins, so appending the platform id after everything the caller passed
// overwrites both the Runner's seed and any id a caller supplied. The
// "caller's own" cases below are not something the adapter does today — they
// are the property being relied on, asserted directly.
func TestContextRunnerForcesThePlatformRequestID(t *testing.T) {
	for name, options := range map[string][]trpcagent.RunOption{
		"no options":     nil,
		"caller's own":   {trpcagent.WithRequestID("caller-minted")},
		"empty override": {trpcagent.WithRequestID("")},
		"last word": {
			trpcagent.WithRequestID("first"),
			trpcagent.WithRequestID("last"),
		},
	} {
		t.Run(name, func(t *testing.T) {
			inner := &recordingRunner{}
			wrapper := testContextRunner(inner)
			ctx, err := identity.WithRunContext(context.Background(), testRunContext())
			require.NoError(t, err)

			events, err := wrapper.Run(ctx, "", "", model.NewUserMessage("hello"), options...)
			require.NoError(t, err)
			for range events {
			}
			require.Equal(t, []string{"request-1"}, inner.requestIDs())
		})
	}
}

// The caller's own option slice must come back unchanged. Appending to it would
// write into an array the caller still owns whenever it had spare capacity, so
// the next run through the same slice would carry this run's id.
func TestTrustedRunOptionsDoesNotWriteIntoTheCallersSlice(t *testing.T) {
	callers := make([]trpcagent.RunOption, 1, 4)
	callers[0] = trpcagent.WithRequestID("caller-minted")

	trusted := TrustedRunOptions(testRunContext(), callers)
	require.Len(t, callers, 1)
	require.Len(t, trusted, 2)
	require.Equal(t, "request-1", resolvedRequestID(trusted))
	// The caller's slice still resolves to the caller's id: nothing was written
	// past its length.
	require.Equal(t, "caller-minted", resolvedRequestID(callers))
	// And the spare capacity was not used as scratch space. This has to read the
	// backing array directly: re-slicing back to length 1 would only re-check
	// the element already checked above, and the element that an append into the
	// caller's array would have overwritten is the one at index 1.
	require.Nil(t, callers[:cap(callers)][1])
}

// A trusted scope always carries a request id — identity.RunContext validates
// it — so this is the whole of the option's contract.
func resolvedRequestID(options []trpcagent.RunOption) string {
	var resolved trpcagent.RunOptions
	for _, option := range options {
		option(&resolved)
	}
	return resolved.RequestID
}

// The platform request id has to reach the framework Events, not merely the
// RunOptions. The Events are where the id becomes observable — they are what a
// caller quoting an X-Request-ID is quoting, and the same scope is what the tool
// audit trail reads its request id from. Nothing beyond those two consumes it
// yet; request logging and traces are separate work.
func TestContextRunnerRequestIDReachesTheFrameworkEvents(t *testing.T) {
	shared := sessioninmemory.NewSessionService()
	t.Cleanup(func() { require.NoError(t, shared.Close()) })
	revision := publishedRevision("revision-1", "echo-v1")
	runtime, err := NewRuntimeFromRevision(revision, shared, entitling(t, revision))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	scope := testRunContext()
	scope.AppID = runtime.AgentAppID
	scope.TenantID = runtime.TenantID
	scope.RevisionID = runtime.RevisionID
	ctx, err := identity.WithRunContext(context.Background(), scope)
	require.NoError(t, err)

	// Untrusted identity arguments, as a protocol adapter supplies them, plus a
	// request id the adapter itself would never send — the strongest input,
	// since it is the one the wrapper has to overwrite rather than merely fill.
	events, err := runtime.protocolRunner.Run(
		ctx,
		"attacker",
		"attacker-session",
		model.NewUserMessage("hello"),
		trpcagent.WithRequestID("caller-minted"),
	)
	require.NoError(t, err)

	seen := 0
	for received := range events {
		seen++
		require.Equal(t, scope.RequestID, received.RequestID)
	}
	require.NotZero(t, seen, "the run produced no events to correlate")
}

// The Runtime owns the real Runner. An adapter that closed its injected Runner
// must not reach it.
func TestContextRunnerCloseDoesNotCloseInnerRunner(t *testing.T) {
	inner := &recordingRunner{}
	wrapper := testContextRunner(inner)

	require.NoError(t, wrapper.Close())
	require.NoError(t, wrapper.Close())
	require.Zero(t, inner.closes())
}

func TestRuntimeGivesProtocolAdapterTheContextRunner(t *testing.T) {
	shared := sessioninmemory.NewSessionService()
	t.Cleanup(func() { require.NoError(t, shared.Close()) })
	revision := publishedRevision("revision-1", "echo-v1")
	runtime, err := NewRuntimeFromRevision(revision, shared, entitling(t, revision))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	wrapper, ok := runtime.protocolRunner.(*contextRunner)
	require.True(t, ok)
	require.NotSame(t, runtime.Runner, runtime.protocolRunner)
	require.Same(t, runtime.Runner, wrapper.inner)
	require.Equal(t, runtime.TenantID, wrapper.tenantID)
	require.Equal(t, runtime.AgentAppID, wrapper.appID)
	require.Equal(t, runtime.RevisionID, wrapper.revisionID)
}

// Close must reach the real Runner exactly once: the wrapper adds a second
// reference to it, not a second owner.
func TestRuntimeCloseClosesRealRunnerExactlyOnce(t *testing.T) {
	recorder := &closeRecorder{}
	runtime := recordingRuntime(recorder, nil, nil, nil)
	runtime.protocolRunner = &contextRunner{inner: runtime.Runner}

	require.NoError(t, runtime.Close())
	require.NoError(t, runtime.protocolRunner.Close())
	require.NoError(t, runtime.Close())
	require.Equal(t, []string{"adapter", "runner", "session"}, recorder.order())
}

// Without a trusted scope the adapter still answers, but the agent never runs.
func TestRuntimeOpenAIHandlerFailsClosedWithoutRunContext(t *testing.T) {
	runtime := NewDemoRuntime()
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	handler, err := runtime.OpenAIHandler()
	require.NoError(t, err)
	response := serveChatCompletionWithContext(
		t,
		context.Background(),
		handler,
		`{"model":"deterministic-echo","user":"attacker","messages":[
			{"role":"user","content":"hello"}
		]}`,
	)
	require.NotEqual(t, http.StatusOK, response.Code)
	require.NotContains(t, response.Body.String(), "echo: hello")
}

type recordedRun struct {
	userID    string
	sessionID string
	content   string
}

// recordingRunner stands in for the real Runner so a test can read back the
// identity arguments and the run options the wrapper produced.
type recordingRunner struct {
	mu         sync.Mutex
	runs       []recordedRun
	requests   []string
	closeCalls int
}

func (r *recordingRunner) Run(
	_ context.Context,
	userID string,
	sessionID string,
	message model.Message,
	options ...trpcagent.RunOption,
) (<-chan *event.Event, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.runs = append(r.runs, recordedRun{
		userID:    userID,
		sessionID: sessionID,
		content:   message.Content,
	})
	// Resolved the way the framework resolves it — in order, last write wins —
	// because the order is the whole of what this wrapper controls.
	r.requests = append(r.requests, resolvedRequestID(options))
	events := make(chan *event.Event)
	close(events)
	return events, nil
}

func (r *recordingRunner) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closeCalls++
	return nil
}

func (r *recordingRunner) recorded() []recordedRun {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedRun(nil), r.runs...)
}

// requestIDs is the request id each run resolved to, in order.
func (r *recordingRunner) requestIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.requests...)
}

func (r *recordingRunner) closes() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closeCalls
}
