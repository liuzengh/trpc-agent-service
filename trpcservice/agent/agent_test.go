package agent

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/security"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storagebundle"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/stretchr/testify/require"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestDemoRuntimePersistsMultipleTurns(t *testing.T) {
	runtime := NewDemoRuntime()
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	for _, input := range []string{"first", "second"} {
		events, err := runtime.Runner.Run(
			context.Background(),
			"u/test-user",
			"c/test-session",
			model.NewUserMessage(input),
			trpcagent.WithRequestID("request-"+input),
		)
		require.NoError(t, err)
		var completionSeen bool
		for evt := range events {
			completionSeen = completionSeen || evt.IsRunnerCompletion()
		}
		require.True(t, completionSeen)
	}

	sess, err := runtime.SessionService.GetSession(context.Background(), session.Key{
		AppName:   DemoAppName,
		UserID:    "u/test-user",
		SessionID: "c/test-session",
	})
	require.NoError(t, err)
	require.NotNil(t, sess)
	require.GreaterOrEqual(t, sess.GetEventCount(), 4)
}

func TestRuntimeCloseIsIdempotent(t *testing.T) {
	runtime := NewDemoRuntime()
	require.NoError(t, runtime.Close())
	require.NoError(t, runtime.Close())
}

func TestRuntimeOwnsExactlyOneOpenAIHandler(t *testing.T) {
	runtime := NewDemoRuntime()
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	require.NotNil(t, runtime.openAI)
	handler, err := runtime.OpenAIHandler()
	require.NoError(t, err)
	require.NotNil(t, handler)
	again, err := runtime.OpenAIHandler()
	require.NoError(t, err)
	require.Same(t, handler, again)

	response := serveChatCompletion(t, runtime, `{
		"model":"deterministic-echo","messages":[{"role":"user","content":"hello"}]
	}`)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "echo: hello")
	require.Contains(t, response.Body.String(), DemoModelName)
}

func TestRuntimeOpenAIHandlerRejectsIncompleteRuntime(t *testing.T) {
	handler, err := (&Runtime{}).OpenAIHandler()
	require.ErrorContains(t, err, "runtime identity is incomplete")
	require.Nil(t, handler)

	handler, err = (&Runtime{
		TenantID: "tenant-a", AgentAppID: "assistant", RevisionID: "revision-1",
	}).OpenAIHandler()
	require.ErrorContains(t, err, "runtime execution unit is incomplete")
	require.Nil(t, handler)
}

func TestRuntimeCloseOrderAndErrorRetention(t *testing.T) {
	adapterErr := errors.New("adapter close failure")
	runnerErr := errors.New("runner close failure")
	sessionErr := errors.New("session close failure")
	recorder := &closeRecorder{}
	runtime := recordingRuntime(recorder, adapterErr, runnerErr, sessionErr)

	firstErr := runtime.Close()
	require.ErrorIs(t, firstErr, adapterErr)
	require.ErrorIs(t, firstErr, runnerErr)
	require.ErrorIs(t, firstErr, sessionErr)
	require.Equal(t, []string{"adapter", "runner", "session"}, recorder.order())

	secondErr := runtime.Close()
	require.ErrorIs(t, secondErr, adapterErr)
	require.ErrorIs(t, secondErr, runnerErr)
	require.ErrorIs(t, secondErr, sessionErr)
	require.Equal(t, []string{"adapter", "runner", "session"}, recorder.order())
}

func TestRuntimeCloseIsConcurrentSafe(t *testing.T) {
	adapterErr := errors.New("adapter close failure")
	recorder := &closeRecorder{}
	runtime := recordingRuntime(recorder, adapterErr, nil, nil)

	const callers = 16
	results := make(chan error, callers)
	var waitGroup sync.WaitGroup
	for caller := 0; caller < callers; caller++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			results <- runtime.Close()
		}()
	}
	waitGroup.Wait()
	close(results)

	for closeErr := range results {
		require.ErrorIs(t, closeErr, adapterErr)
	}
	require.Equal(t, []string{"adapter", "runner", "session"}, recorder.order())
}

// A Runtime that shares the process Session service must never close it: the
// resolver closes runtimes independently, and the remaining runtimes keep
// serving from the same conversation state.
func TestRuntimeCloseKeepsSharedSessionService(t *testing.T) {
	shared := sessioninmemory.NewSessionService()
	t.Cleanup(func() { require.NoError(t, shared.Close()) })

	firstRevision := publishedRevision("revision-1", "echo-v1")
	first, err := NewRuntimeFromRevision(firstRevision, shared, entitling(t, firstRevision))
	require.NoError(t, err)
	secondRevision := publishedRevision("revision-2", "echo-v2")
	second, err := NewRuntimeFromRevision(secondRevision, shared, entitling(t, secondRevision))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Close()) })

	require.NoError(t, first.Close())

	// The body user is attacker-controlled input, so the surviving Runtime must
	// still write the conversation under the authenticated principal.
	response := serveChatCompletion(t, second, `{
		"model":"echo-v2","user":"user-1","messages":[{"role":"user","content":"after close"}]
	}`)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "echo: after close")

	sess, err := shared.GetSession(context.Background(), session.Key{
		AppName:   second.AppName,
		UserID:    "u/principal-1",
		SessionID: "shared-session",
	})
	require.NoError(t, err)
	require.NotNil(t, sess)
	require.GreaterOrEqual(t, sess.GetEventCount(), 2)
}

// publishedRevision builds a revision the way the repository would have: with a
// ConfigDigest computed from the config it carries.
//
// The digest is not decoration here. A Runtime re-derives it and refuses a
// revision whose stored digest does not match, so a helper that left it empty
// would produce revisions no test could build — and, worse, a helper that
// hard-coded one would keep passing after the config beside it changed.
//
// Callers that mutate Config afterwards must re-seal it.
func publishedRevision(revisionID string, modelName string) tenant.AgentRevision {
	return sealed(tenant.AgentRevision{
		ID:         revisionID,
		TenantID:   "tenant-a",
		AgentAppID: "assistant",
		Status:     tenant.RevisionStatusPublished,
		Config: tenant.RevisionConfig{
			AgentName:   "test-agent",
			Instruction: "Answer through the deterministic model.",
			Model:       tenant.ModelConfig{Provider: "deterministic", Name: modelName},
		},
	})
}

// sealed returns revision with its digest recomputed over its current config.
func sealed(revision tenant.AgentRevision) tenant.AgentRevision {
	digest, err := revision.Config.Digest()
	if err != nil {
		panic("agent test: revision config is not digestible: " + err.Error())
	}
	revision.ConfigDigest = digest
	return revision
}

// entitling grants revision's tenant exactly the capabilities revision names.
//
// Every test that builds a Runtime states its entitlement, because the platform
// requires one and a test that got a permissive default would be testing a build
// path the process does not have. This helper is the "yes, this tenant may"
// answer; tests about refusal pass security.DenyCapabilities() or a narrower
// grant instead.
//
// It deduplicates before granting so that a revision deliberately repeating a
// ref still reaches the tool registry, which is what owns that complaint.
func entitling(t *testing.T, revision tenant.AgentRevision) security.RevisionAuthorizer {
	t.Helper()
	grant := security.Grant{
		TenantID:   revision.TenantID,
		PolicyRefs: unique(revision.Config.PolicyRefs),
	}
	if ref := revision.Config.Model.SecretRef; ref != "" {
		grant.SecretRefs = []string{ref}
	}
	entitlements, err := security.NewEntitlements(grant)
	require.NoError(t, err)
	return entitlements
}

func unique(refs []string) []string {
	seen := make(map[string]struct{}, len(refs))
	result := make([]string, 0, len(refs))
	for _, ref := range refs {
		if _, repeated := seen[ref]; repeated {
			continue
		}
		seen[ref] = struct{}{}
		result = append(result, ref)
	}
	return result
}

// serveChatCompletion calls a Runtime adapter the way the platform does: with
// the trusted scope already attached to the request context.
func serveChatCompletion(
	t *testing.T,
	runtime *Runtime,
	body string,
) *httptest.ResponseRecorder {
	t.Helper()
	handler, err := runtime.OpenAIHandler()
	require.NoError(t, err)
	ctx, err := identity.WithRunContext(context.Background(), identity.RunContext{
		RequestID:   "request-1",
		TenantID:    runtime.TenantID,
		AppID:       runtime.AgentAppID,
		PrincipalID: "principal-1",
		SessionID:   "shared-session",
		RevisionID:  runtime.RevisionID,
	})
	require.NoError(t, err)
	return serveChatCompletionWithContext(t, ctx, handler, body)
}

func serveChatCompletionWithContext(
	t *testing.T,
	ctx context.Context,
	handler http.Handler,
	body string,
) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(
		http.MethodPost, "/v1/chat/completions", strings.NewReader(body),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Session-ID", "shared-session")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request.WithContext(ctx))
	return response
}

// recordingRuntime builds a Runtime that passes validation and reports the order
// in which its owned resources are closed.
//
// Its store is an owning lease, so "session" appears in the recorded order at
// the point a Runtime that owns its store would close it. A Runtime over a
// borrowed or Router-issued lease records nothing there, which is the property
// TestRuntimeCloseKeepsSharedSessionService covers from the other side.
func recordingRuntime(
	recorder *closeRecorder,
	adapterErr error,
	runnerErr error,
	sessionErr error,
) *Runtime {
	sessions := &closeRecordingSessionService{recorder: recorder, closeErr: sessionErr}
	return &Runtime{
		TenantID:   "tenant-a",
		AgentAppID: "assistant",
		RevisionID: "revision-1",
		AppName:    "t/tenant-a/a/assistant",
		ModelName:  "echo-test",
		Agent:      &stubAgent{},
		Runner: &closeRecordingRunner{
			recorder: recorder,
			closeErr: runnerErr,
		},
		SessionService: sessions,
		openAI: &closeRecordingAdapter{
			recorder: recorder,
			closeErr: adapterErr,
		},
		openAIHandler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
		store:         storagebundle.Own(storagebundle.Bundle{Session: sessions}),
	}
}

type closeRecorder struct {
	mu       sync.Mutex
	recorded []string
}

func (c *closeRecorder) record(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recorded = append(c.recorded, name)
}

func (c *closeRecorder) order() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.recorded...)
}

type stubAgent struct {
	trpcagent.Agent
}

type closeRecordingAdapter struct {
	recorder *closeRecorder
	closeErr error
}

func (a *closeRecordingAdapter) Handler() http.Handler {
	return http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
}

func (a *closeRecordingAdapter) Close() error {
	a.recorder.record("adapter")
	return a.closeErr
}

type closeRecordingRunner struct {
	recorder *closeRecorder
	closeErr error
}

func (r *closeRecordingRunner) Run(
	context.Context,
	string,
	string,
	model.Message,
	...trpcagent.RunOption,
) (<-chan *event.Event, error) {
	return nil, errors.New("unexpected run")
}

func (r *closeRecordingRunner) Close() error {
	r.recorder.record("runner")
	return r.closeErr
}

type closeRecordingSessionService struct {
	session.Service
	recorder *closeRecorder
	closeErr error
}

func (s *closeRecordingSessionService) Close() error {
	s.recorder.record("session")
	return s.closeErr
}
