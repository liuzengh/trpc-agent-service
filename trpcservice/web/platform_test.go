package web

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	platformagent "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/security"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionlease"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionrun"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storagebundle"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/stretchr/testify/require"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

const (
	keyTenantA   = "chat-key-tenant-a-0123456789"
	keyTenantB   = "chat-key-tenant-b-0123456789"
	keyUnknown   = "chat-key-unknown-0123456789"
	principalA   = "principal-a"
	principalB   = "principal-b"
	appAssistant = "assistant"
)

// Admin credentials for these tests. They are longer than the chat keys because
// the admin authenticator holds them to a 32-character minimum, and the point of
// that minimum is lost if the tests quietly work below it.
const (
	adminKeyPlatform  = "admin-key-platform-0123456789abcdef"
	adminKeyTenantA   = "admin-key-tenant-a-0123456789abcdef"
	adminKeyTenantB   = "admin-key-tenant-b-0123456789abcdef"
	adminKeyUnknown   = "admin-key-unknown--0123456789abcdef"
	principalPlatform = "platform-admin"
	principalAdminA   = "tenant-a-admin"
	principalAdminB   = "tenant-b-admin"
)

// The pin makes a published revision invisible to sessions that already
// started, and a rollback leaves those sessions alone as well.
func TestPlatformPinsSessionToFirstRevision(t *testing.T) {
	platform := newPlatformTestServer(t)
	seedTenantAppRevision(t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1")

	headers := chatHeaders(keyTenantA, appAssistant)
	headers[HeaderSessionID] = "conversation-1"
	first := requireStatus(t, platform.handler, http.MethodPost, chatPath, `{
		"model":"ignored","messages":[{"role":"user","content":"first"}]
	}`, headers, http.StatusOK)
	require.Contains(t, first.Body.String(), `"model":"echo-v1"`)
	require.Contains(t, first.Body.String(), "echo: first")
	require.Equal(t, "revision-1", first.Header().Get(HeaderAgentRevisionID))
	require.Equal(t, "conversation-1", first.Header().Get(HeaderSessionID))

	createRevisionThroughAPI(t, platform.handler, "tenant-a", appAssistant, "revision-2", 2, "echo-v2")
	publishRevisionThroughAPI(t, platform.handler, "tenant-a", appAssistant, "revision-2")

	// The pinned session keeps serving revision-1 even though revision-2 is now
	// the default route.
	pinned := requireStatus(t, platform.handler, http.MethodPost, chatPath, `{
		"model":"ignored","messages":[{"role":"user","content":"second"}]
	}`, headers, http.StatusOK)
	require.Contains(t, pinned.Body.String(), `"model":"echo-v1"`)
	require.Equal(t, "revision-1", pinned.Header().Get(HeaderAgentRevisionID))

	// A new session takes the current default.
	fresh := cloneHeaders(headers)
	fresh[HeaderSessionID] = "conversation-2"
	current := requireStatus(t, platform.handler, http.MethodPost, chatPath, `{
		"model":"ignored","messages":[{"role":"user","content":"fresh"}]
	}`, fresh, http.StatusOK)
	require.Contains(t, current.Body.String(), `"model":"echo-v2"`)
	require.Equal(t, "revision-2", current.Header().Get(HeaderAgentRevisionID))

	// Rolling back does not move either pin.
	rollback := publishRevisionThroughAPI(t, platform.handler, "tenant-a", appAssistant, "revision-1")
	var rollbackResponse publishRevisionResponse
	require.NoError(t, json.Unmarshal(rollback.Body.Bytes(), &rollbackResponse))
	require.Equal(t, "revision-1", rollbackResponse.App.RoutingPolicy.DefaultRevisionID)
	require.Equal(t, uint64(3), rollbackResponse.App.RoutingVersion)

	afterRollback := requireStatus(t, platform.handler, http.MethodPost, chatPath, `{
		"model":"ignored","messages":[{"role":"user","content":"rollback"}]
	}`, fresh, http.StatusOK)
	require.Contains(t, afterRollback.Body.String(), `"model":"echo-v2"`)
	require.Equal(t, "revision-2", afterRollback.Header().Get(HeaderAgentRevisionID))
	require.Equal(t, 2, platform.directory.Size())
	require.Equal(t, 2, platform.resolver.CacheSize())

	stored, err := platform.sessions.GetSession(context.Background(), session.Key{
		AppName:   "t/tenant-a/a/" + appAssistant,
		UserID:    "u/" + principalA,
		SessionID: "conversation-1",
	})
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.GreaterOrEqual(t, stored.GetEventCount(), 4)
}

// X-Agent-Revision-ID selects the revision of a first run only. On a pinned
// session it may confirm the pin, never change it.
func TestPlatformRevisionHintCannotMovePinnedSession(t *testing.T) {
	platform := newPlatformTestServer(t)
	seedTenantAppRevision(t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1")
	createRevisionThroughAPI(t, platform.handler, "tenant-a", appAssistant, "revision-2", 2, "echo-v2")
	publishRevisionThroughAPI(t, platform.handler, "tenant-a", appAssistant, "revision-2")

	// A hint on a fresh session picks the older revision instead of the default.
	headers := chatHeaders(keyTenantA, appAssistant)
	headers[HeaderSessionID] = "conversation-1"
	headers[HeaderAgentRevisionID] = "revision-1"
	hinted := requireStatus(t, platform.handler, http.MethodPost, chatPath, `{
		"model":"ignored","messages":[{"role":"user","content":"hinted"}]
	}`, headers, http.StatusOK)
	require.Contains(t, hinted.Body.String(), `"model":"echo-v1"`)
	require.Equal(t, "revision-1", hinted.Header().Get(HeaderAgentRevisionID))

	// Repeating the same hint is consistent with the pin, so it is allowed.
	repeated := requireStatus(t, platform.handler, http.MethodPost, chatPath, `{
		"model":"ignored","messages":[{"role":"user","content":"repeated"}]
	}`, headers, http.StatusOK)
	require.Contains(t, repeated.Body.String(), `"model":"echo-v1"`)

	// A different hint is a conflict, not a silent switch.
	conflicting := cloneHeaders(headers)
	conflicting[HeaderAgentRevisionID] = "revision-2"
	conflict := requireStatus(t, platform.handler, http.MethodPost, chatPath, `{
		"model":"ignored","messages":[{"role":"user","content":"conflicting"}]
	}`, conflicting, http.StatusConflict)
	require.Contains(t, conflict.Body.String(), `"code":"pin_conflict"`)
	require.Contains(t, conflict.Body.String(), "revision-1")

	// Dropping the hint keeps serving the pin, not the default revision.
	unhinted := cloneHeaders(headers)
	delete(unhinted, HeaderAgentRevisionID)
	stillPinned := requireStatus(t, platform.handler, http.MethodPost, chatPath, `{
		"model":"ignored","messages":[{"role":"user","content":"still pinned"}]
	}`, unhinted, http.StatusOK)
	require.Contains(t, stillPinned.Body.String(), `"model":"echo-v1"`)

	malformed := cloneHeaders(headers)
	malformed[HeaderAgentRevisionID] = "revision 1"
	invalid := requireStatus(t, platform.handler, http.MethodPost, chatPath, `{
		"model":"ignored","messages":[{"role":"user","content":"invalid"}]
	}`, malformed, http.StatusBadRequest)
	require.Contains(t, invalid.Body.String(), `"code":"invalid_argument"`)
}

// The conversation identity comes from the credential. A request body that
// claims another user must not reach the Session key space.
func TestPlatformChatIgnoresRequestSuppliedUser(t *testing.T) {
	platform := newPlatformTestServer(t)
	seedTenantAppRevision(t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1")

	headers := chatHeaders(keyTenantA, appAssistant)
	headers[HeaderSessionID] = "conversation-1"
	for _, content := range []string{"first", "second"} {
		requireStatus(t, platform.handler, http.MethodPost, chatPath, fmt.Sprintf(`{
			"model":"ignored","user":"attacker","messages":[{"role":"user","content":%q}]
		}`, content), headers, http.StatusOK)
	}

	appName := "t/tenant-a/a/" + appAssistant
	trusted, err := platform.sessions.GetSession(context.Background(), session.Key{
		AppName: appName, UserID: "u/" + principalA, SessionID: "conversation-1",
	})
	require.NoError(t, err)
	require.NotNil(t, trusted)
	require.GreaterOrEqual(t, trusted.GetEventCount(), 4)

	for _, claimed := range []string{"attacker", "u/attacker"} {
		spoofed, sessionErr := platform.sessions.GetSession(context.Background(), session.Key{
			AppName: appName, UserID: claimed, SessionID: "conversation-1",
		})
		require.NoError(t, sessionErr)
		require.Nil(t, spoofed, "request body user %q reached the session store", claimed)
	}
}

func TestPlatformChatAuthenticationMatrix(t *testing.T) {
	platform := newPlatformTestServer(t)
	seedTenantAppRevision(t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1")
	body := `{"model":"ignored","messages":[{"role":"user","content":"hello"}]}`

	unauthenticated := map[string]map[string]string{
		"missing credential": {HeaderAgentAppID: appAssistant},
		"unknown key": {
			HeaderAuthorization: "Bearer " + keyUnknown,
			HeaderAgentAppID:    appAssistant,
		},
		"wrong scheme": {
			HeaderAuthorization: "Basic " + keyTenantA,
			HeaderAgentAppID:    appAssistant,
		},
		"empty token": {
			HeaderAuthorization: "Bearer ",
			HeaderAgentAppID:    appAssistant,
		},
		"raw key": {
			HeaderAuthorization: keyTenantA,
			HeaderAgentAppID:    appAssistant,
		},
	}
	for name, headers := range unauthenticated {
		t.Run(name, func(t *testing.T) {
			response := requireStatus(
				t, platform.handler, http.MethodPost, chatPath, body, headers,
				http.StatusUnauthorized,
			)
			require.Contains(t, response.Body.String(), `"code":"unauthenticated"`)
			require.Equal(t, "Bearer", response.Header().Get("WWW-Authenticate"))
		})
	}

	forbidden := map[string]map[string]string{
		"tenant assertion mismatch": {
			HeaderAuthorization: "Bearer " + keyTenantA,
			HeaderTenantID:      "tenant-b",
			HeaderAgentAppID:    appAssistant,
		},
		"app outside the grant": {
			HeaderAuthorization: "Bearer " + keyTenantA,
			HeaderAgentAppID:    "reporter",
		},
	}
	for name, headers := range forbidden {
		t.Run(name, func(t *testing.T) {
			response := requireStatus(
				t, platform.handler, http.MethodPost, chatPath, body, headers,
				http.StatusForbidden,
			)
			require.Contains(t, response.Body.String(), `"code":"forbidden"`)
		})
	}

	// A matching assertion is redundant but accepted.
	asserted := chatHeaders(keyTenantA, appAssistant)
	asserted[HeaderTenantID] = "tenant-a"
	requireStatus(t, platform.handler, http.MethodPost, chatPath, body, asserted, http.StatusOK)

	missingApp := requireStatus(
		t, platform.handler, http.MethodPost, chatPath, body,
		map[string]string{HeaderAuthorization: "Bearer " + keyTenantA},
		http.StatusBadRequest,
	)
	require.Contains(t, missingApp.Body.String(), `"code":"missing_route"`)

	// A browser cannot attach a credential to a preflight request.
	preflight := requireStatus(
		t, platform.handler, http.MethodOptions, chatPath, "", nil, http.StatusNoContent,
	)
	require.Contains(t, preflight.Header().Get("Access-Control-Allow-Headers"), HeaderAuthorization)
}

// A client that lets the platform mint the session id has to get it back, or it
// cannot continue the conversation.
func TestPlatformReturnsGeneratedSessionID(t *testing.T) {
	platform := newPlatformTestServer(t)
	seedTenantAppRevision(t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1")
	headers := chatHeaders(keyTenantA, appAssistant)

	first := requireStatus(t, platform.handler, http.MethodPost, chatPath, `{
		"model":"ignored","messages":[{"role":"user","content":"first"}]
	}`, headers, http.StatusOK)
	generated := first.Header().Get(HeaderSessionID)
	require.NoError(t, tenant.ValidateResourceID("session id", generated))
	require.Equal(t, "revision-1", first.Header().Get(HeaderAgentRevisionID))
	require.Contains(
		t, first.Header().Get("Access-Control-Expose-Headers"), HeaderSessionID,
	)

	second := requireStatus(t, platform.handler, http.MethodPost, chatPath, `{
		"model":"ignored","messages":[{"role":"user","content":"second"}]
	}`, headers, http.StatusOK)
	require.NotEqual(t, generated, second.Header().Get(HeaderSessionID))

	// Reusing the returned id continues the same conversation.
	resumed := cloneHeaders(headers)
	resumed[HeaderSessionID] = generated
	requireStatus(t, platform.handler, http.MethodPost, chatPath, `{
		"model":"ignored","messages":[{"role":"user","content":"resumed"}]
	}`, resumed, http.StatusOK)

	stored, err := platform.sessions.GetSession(context.Background(), session.Key{
		AppName:   "t/tenant-a/a/" + appAssistant,
		UserID:    "u/" + principalA,
		SessionID: generated,
	})
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.GreaterOrEqual(t, stored.GetEventCount(), 4)

	streamed := requireStatus(t, platform.handler, http.MethodPost, chatPath, `{
		"model":"ignored","stream":true,"messages":[{"role":"user","content":"stream"}]
	}`, headers, http.StatusOK)
	require.Equal(t, "text/event-stream", streamed.Header().Get("Content-Type"))
	require.NoError(t, tenant.ValidateResourceID(
		"session id", streamed.Header().Get(HeaderSessionID),
	))
	require.Equal(t, "revision-1", streamed.Header().Get(HeaderAgentRevisionID))
}

func TestPlatformRejectsInvalidSessionID(t *testing.T) {
	platform := newPlatformTestServer(t)
	seedTenantAppRevision(t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1")

	for _, sessionID := range []string{"conversation 1", "conversation/1", "-leading-dash"} {
		headers := chatHeaders(keyTenantA, appAssistant)
		headers[HeaderSessionID] = sessionID
		response := requireStatus(t, platform.handler, http.MethodPost, chatPath, `{
			"model":"ignored","messages":[{"role":"user","content":"hello"}]
		}`, headers, http.StatusBadRequest)
		require.Contains(t, response.Body.String(), `"code":"invalid_session_id"`)
	}
	require.Zero(t, platform.directory.Size())
}

// The adapter owns the status line, so the platform has to publish the session
// and revision headers before handing the request over. A body the adapter
// rejects still pins the session: that is the current, documented semantics.
func TestPlatformPublishesHeadersBeforeAdapterErrors(t *testing.T) {
	platform := newPlatformTestServer(t)
	seedTenantAppRevision(t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1")

	response := doChat(t, platform.handler, `{"model":`, chatHeaders(keyTenantA, appAssistant))
	require.GreaterOrEqual(t, response.Code, http.StatusBadRequest, response.Body.String())
	require.NoError(t, tenant.ValidateResourceID(
		"session id", response.Header().Get(HeaderSessionID),
	))
	require.Equal(t, "revision-1", response.Header().Get(HeaderAgentRevisionID))
	require.Equal(t, 1, platform.directory.Size())
}

// Two first runs of one session used to reach the directory together and race
// for the pin; the run lease now serializes them before either resolves a
// candidate. Both still end up on the single winning revision, and the refused
// one has to leave no runtime lease behind.
//
// The directory's own guarantee for genuinely simultaneous EnsurePin callers is
// not weakened by this, it just stops being reachable through one process's
// chat endpoint. It is proven directly, against every implementation, by
// sessiondirtest.EnsurePinConcurrently.
func TestPlatformSerializesConcurrentRunsOfOneSession(t *testing.T) {
	barrier := &barrierDirectory{
		inner:   sessiondir.NewMemoryDirectory(),
		arrived: make(chan struct{}),
		release: make(chan struct{}),
	}
	platform := newPlatformTestServerWithDirectory(t, barrier)
	seedTenantAppRevision(t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1")
	createRevisionThroughAPI(t, platform.handler, "tenant-a", appAssistant, "revision-2", 2, "echo-v2")
	publishRevisionThroughAPI(t, platform.handler, "tenant-a", appAssistant, "revision-2")

	// One request pins through the development hint, the other through the
	// current default route.
	hinted := chatHeaders(keyTenantA, appAssistant)
	hinted[HeaderSessionID] = "conversation-1"
	hinted[HeaderAgentRevisionID] = "revision-1"
	defaulted := chatHeaders(keyTenantA, appAssistant)
	defaulted[HeaderSessionID] = "conversation-1"

	body := `{"model":"ignored","messages":[{"role":"user","content":"concurrent"}]}`
	firstCh := make(chan *httptest.ResponseRecorder, 1)
	go func() { firstCh <- doChat(t, platform.handler, body, hinted) }()

	// The first request holds the lease and is parked in EnsurePin. The second
	// arrives while it is there, so it is refused without resolving anything.
	barrier.awaitArrival(t)
	refused := doChat(t, platform.handler, body, defaulted)
	require.Equal(t, http.StatusConflict, refused.Code, refused.Body.String())
	require.Contains(t, refused.Body.String(), `"code":"session_busy"`)
	require.Equal(t, "2", refused.Header().Get(HeaderRetryAfter))

	barrier.releaseAll()
	first := <-firstCh
	require.Equal(t, http.StatusOK, first.Code, first.Body.String())
	winner := first.Header().Get(HeaderAgentRevisionID)
	require.Equal(t, "revision-1", winner)

	// Retrying after the first run finished works, and lands on the same pin.
	second := doChat(t, platform.handler, body, defaulted)
	require.Equal(t, http.StatusOK, second.Code, second.Body.String())
	require.Equal(t, winner, second.Header().Get(HeaderAgentRevisionID))
	require.Contains(t, first.Body.String(), `"model":"echo-v1"`)
	require.Contains(t, second.Body.String(), `"model":"echo-v1"`)

	pinned, found, err := barrier.inner.GetPin(context.Background(), sessiondir.Key{
		TenantID:    "tenant-a",
		AppID:       appAssistant,
		PrincipalID: principalA,
		SessionID:   "conversation-1",
	})
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, winner, pinned)

	// Close only returns once every lease is back, so a refusal that kept one
	// would hang here instead of reporting a leak.
	closed := make(chan error, 1)
	go func() { closed <- platform.resolver.Close() }()
	select {
	case closeErr := <-closed:
		require.NoError(t, closeErr)
	case <-time.After(10 * time.Second):
		t.Fatal("resolver did not close: a runtime lease leaked")
	}
}

func TestPlatformTenantIsolationAndErrors(t *testing.T) {
	platform := newPlatformTestServer(t)
	for _, tenantID := range []string{"tenant-a", "tenant-b"} {
		seedTenantAppRevision(
			t, platform.handler, tenantID, appAssistant, "revision-1", 1, "model-"+tenantID,
		)
	}

	// Each credential reaches its own tenant only; the tenant is never taken
	// from a request header.
	for apiKey, tenantID := range map[string]string{
		keyTenantA: "tenant-a",
		keyTenantB: "tenant-b",
	} {
		headers := chatHeaders(apiKey, appAssistant)
		headers[HeaderSessionID] = "conversation-shared"
		response := requireStatus(t, platform.handler, http.MethodPost, chatPath, `{
			"model":"ignored","messages":[{"role":"user","content":"isolated"}]
		}`, headers, http.StatusOK)
		require.Contains(t, response.Body.String(), `"model":"model-`+tenantID+`"`)
	}
	require.Equal(t, 2, platform.resolver.CacheSize())
	// The same session id under two tenants is two conversations.
	require.Equal(t, 2, platform.directory.Size())

	unknownRevision := requireStatus(
		t, platform.handler, http.MethodGet,
		"/admin/v1/tenants/tenant-b/apps/assistant/revisions/private-a",
		"", adminHeaders(adminKeyPlatform), http.StatusNotFound,
	)
	require.JSONEq(t, `{
		"error":{"code":"not_found","message":"resource not found"}
	}`, unknownRevision.Body.String())

	invalidJSON := requireStatus(
		t, platform.handler, http.MethodPost, "/admin/v1/tenants",
		`{"id":"tenant-c","slug":"tenant-c","name":"Tenant C","unknown":true}`,
		adminHeaders(adminKeyPlatform), http.StatusBadRequest,
	)
	require.Contains(t, invalidJSON.Body.String(), `"code":"invalid_json"`)

	duplicate := requireStatus(t, platform.handler, http.MethodPost, "/admin/v1/tenants", `{
		"id":"tenant-a","slug":"tenant-a","name":"Duplicate"
	}`, adminHeaders(adminKeyPlatform), http.StatusConflict)
	require.Contains(t, duplicate.Body.String(), `"code":"already_exists"`)
}

func TestPlatformDynamicStreaming(t *testing.T) {
	platform := newPlatformTestServer(t)
	seedTenantAppRevision(t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1")

	response := requireStatus(t, platform.handler, http.MethodPost, chatPath, `{
		"model":"ignored","stream":true,"messages":[{"role":"user","content":"stream"}]
	}`, chatHeaders(keyTenantA, appAssistant), http.StatusOK)
	require.Equal(t, "text/event-stream", response.Header().Get("Content-Type"))
	require.Contains(t, response.Body.String(), "echo: ")
	require.Contains(t, response.Body.String(), "stream")
	require.Contains(t, response.Body.String(), "data: [DONE]")
}

// The runtime lease must cover the whole SSE handler, otherwise the resolver
// could close the Runner and its adapter while chunks are still being written.
func TestPlatformStreamingHoldsRuntimeLeaseUntilHandlerReturns(t *testing.T) {
	platform := newPlatformTestServer(t)
	seedTenantAppRevision(t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1")

	writer := newBlockingResponseWriter()
	request := httptest.NewRequest(http.MethodPost, chatPath, bytes.NewBufferString(`{
		"model":"ignored","stream":true,"messages":[{"role":"user","content":"stream"}]
	}`))
	request.Header.Set("Content-Type", "application/json")
	for name, value := range chatHeaders(keyTenantA, appAssistant) {
		request.Header.Set(name, value)
	}
	served := make(chan struct{})
	go func() {
		defer close(served)
		platform.handler.ServeHTTP(writer, request)
	}()
	// Every failure below must still unblock the handler: while it is parked in
	// Write it holds a lease, and the resolver.Close from the server cleanup
	// would wait for that lease forever instead of reporting the failure.
	t.Cleanup(func() {
		writer.release()
		<-served
	})

	select {
	case <-writer.started:
	case <-served:
		t.Fatalf("handler returned without streaming: %s", writer.bodyString())
	}

	closeResult := make(chan error, 1)
	go func() { closeResult <- platform.resolver.Close() }()
	scope := tenant.TenantContext{TenantID: "tenant-a"}
	require.Eventually(t, func() bool {
		// Close marks the resolver asynchronously, so a probe can still win the
		// race and take a real lease. Returning it keeps closeAll from waiting on
		// a lease this test would otherwise never release.
		resolved, resolveErr := platform.resolver.Resolve(
			context.Background(), scope, appAssistant, "",
		)
		if resolveErr == nil {
			resolved.Release()
			return false
		}
		return errors.Is(resolveErr, platformagent.ErrResolverClosed)
	}, time.Second, time.Millisecond)
	select {
	case <-closeResult:
		t.Fatal("resolver closed while a streaming handler still held the lease")
	default:
	}

	writer.release()
	<-served
	require.NoError(t, <-closeResult)
	require.Equal(t, "text/event-stream", writer.Header().Get("Content-Type"))
	require.NotEmpty(t, writer.Header().Get(HeaderSessionID))
	require.Contains(t, writer.bodyString(), "data: [DONE]")

	// The platform keeps no runtime state of its own, so traffic stops as soon
	// as the resolver is closed.
	unavailable := requireStatus(t, platform.handler, http.MethodPost, chatPath, `{
		"model":"ignored","messages":[{"role":"user","content":"after close"}]
	}`, chatHeaders(keyTenantA, appAssistant), http.StatusServiceUnavailable)
	require.Contains(t, unavailable.Body.String(), `"code":"runtime_unavailable"`)
}

func TestPlatformRejectsRuntimeWithoutOpenAIHandler(t *testing.T) {
	repository := tenant.NewMemoryRepository()
	runs, err := sessionrun.NewService(
		resolverFunc(func(
			context.Context,
			tenant.TenantContext,
			string,
			string,
		) (platformagent.ResolvedRuntime, error) {
			return platformagent.ResolvedRuntime{
				Runtime: &platformagent.Runtime{},
				Revision: tenant.AgentRevision{
					ID: "revision-1", TenantID: "tenant-a", AgentAppID: appAssistant,
				},
			}, nil
		}),
		sessiondir.NewMemoryDirectory(),
		newTestCoordinator(t),
	)
	require.NoError(t, err)
	server, err := NewPlatformServer(
		repository,
		newTestProfiles(t, repository),
		runs,
		newTestAuthenticator(t),
		newTestAdminAuthenticator(t),
		security.DenyCapabilities(),
	)
	require.NoError(t, err)

	response := requireStatus(t, server.Handler(), http.MethodPost, chatPath, `{
		"model":"ignored","messages":[{"role":"user","content":"broken"}]
	}`, chatHeaders(keyTenantA, appAssistant), http.StatusInternalServerError)
	require.Contains(t, response.Body.String(), `"code":"internal_error"`)
}

func TestNewPlatformServerRequiresEveryDependency(t *testing.T) {
	repository := tenant.NewMemoryRepository()
	runs, err := sessionrun.NewService(
		resolverFunc(func(
			context.Context,
			tenant.TenantContext,
			string,
			string,
		) (platformagent.ResolvedRuntime, error) {
			return platformagent.ResolvedRuntime{}, nil
		}),
		sessiondir.NewMemoryDirectory(),
		newTestCoordinator(t),
	)
	require.NoError(t, err)
	authenticator := newTestAuthenticator(t)
	admin := newTestAdminAuthenticator(t)
	revisions := security.DenyCapabilities()
	profiles := newTestProfiles(t, repository)

	_, err = NewPlatformServer(nil, profiles, runs, authenticator, admin, revisions)
	require.ErrorContains(t, err, "tenant repository is required")
	// A control plane with a backend-profile route and nowhere to put a profile
	// would accept a storage arrangement and lose it.
	_, err = NewPlatformServer(repository, nil, runs, authenticator, admin, revisions)
	require.ErrorContains(t, err, "backend profile repository is required")
	// The lease, the pin and the Runtime now live behind one service, so this is
	// the single check that stands where three used to. The dependencies it
	// holds are checked where they are consumed; see
	// TestNewServiceRequiresEveryDependency in the sessionrun package.
	_, err = NewPlatformServer(repository, profiles, nil, authenticator, admin, revisions)
	require.ErrorContains(t, err, "run service is required")
	_, err = NewPlatformServer(repository, profiles, runs, nil, admin, revisions)
	require.ErrorContains(t, err, "chat authenticator is required")
	// A control plane with no way to authenticate is a control plane whose only
	// protection is that nobody has found the port yet.
	_, err = NewPlatformServer(repository, profiles, runs, authenticator, nil, revisions)
	require.ErrorContains(t, err, "admin authenticator is required")
	// And one with no opinion on which revisions may run would let every tenant
	// name every capability the process can reach.
	_, err = NewPlatformServer(repository, profiles, runs, authenticator, admin, nil)
	require.ErrorContains(t, err, "revision authorizer is required")

	server, err := NewPlatformServer(repository, profiles, runs, authenticator, admin, revisions)
	require.NoError(t, err)
	require.NotNil(t, server.Handler())
}

func TestPlatformHTTPMethodAndCORS(t *testing.T) {
	platform := newPlatformTestServer(t)
	health := requireStatus(t, platform.handler, http.MethodGet, "/healthz", "", nil, http.StatusOK)
	require.JSONEq(t, `{"status":"ok"}`, health.Body.String())

	// The wrong method on an admin route is a 405 only once the caller has
	// proved who they are. Unauthenticated, the method is not information the
	// server owes anybody — see TestAdminAuthenticatesBeforeRouting.
	wrongMethod := requireStatus(
		t, platform.handler, http.MethodGet, "/admin/v1/tenants",
		"", adminHeaders(adminKeyPlatform), http.StatusMethodNotAllowed,
	)
	require.Equal(t, http.MethodPost, wrongMethod.Header().Get("Allow"))
	require.Empty(t, wrongMethod.Header().Values("Access-Control-Allow-Origin"))

	chatMethod := requireStatus(
		t, platform.handler, http.MethodGet, chatPath, "", nil, http.StatusMethodNotAllowed,
	)
	require.Equal(t, "POST, OPTIONS", chatMethod.Header().Get("Allow"))
	require.Equal(t, "*", chatMethod.Header().Get("Access-Control-Allow-Origin"))

	preflight := requireStatus(
		t, platform.handler, http.MethodOptions, chatPath, "", nil, http.StatusNoContent,
	)
	require.Equal(t, "*", preflight.Header().Get("Access-Control-Allow-Origin"))
	allowed := preflight.Header().Get("Access-Control-Allow-Headers")
	for _, header := range []string{
		HeaderAuthorization, HeaderTenantID, HeaderAgentAppID, HeaderAgentRevisionID, HeaderSessionID,
	} {
		require.Contains(t, allowed, header)
	}
	exposed := preflight.Header().Get("Access-Control-Expose-Headers")
	require.Contains(t, exposed, HeaderSessionID)
	require.Contains(t, exposed, HeaderAgentRevisionID)
	// Retry-After is the only actionable part of a 409 session_busy, and a
	// browser cannot read it unless it is exposed.
	require.Contains(t, exposed, HeaderRetryAfter)
	// X-Request-ID is the only part of a 500 a browser client can usefully
	// quote back to an operator, and it is unreadable unless it is exposed.
	require.Contains(t, exposed, HeaderRequestID)
	require.Equal(t, http.MethodPost, preflight.Header().Get("Access-Control-Allow-Methods"))
}

// The id on the answer and the id on the run are one value, proved end to end:
// the header comes off a real /v1/chat/completions response and the comparison
// is against the Events the run actually left in the session service.
//
// Three upstream facts make this worth asserting rather than assuming. The
// OpenAI adapter contributes no request id — buildRunOptions
// (server/openai/run_input.go) assembles history, the tool-result rewriter and
// external tools, nothing else. The Runner mints one of its own before applying
// any option, and mints a second afterwards if an option left the field empty
// (runner/runner.go:546 and :550). So a run always ends up labelled, and without
// the platform wrapper appending last it would be labelled with a uuid that
// never left the framework — the header would be a number nothing else in the
// system had ever recorded, and it would look exactly as correct as this does.
//
// The second turn is what makes it a request id rather than a session id: same
// session, same principal, same revision, different value, and the session ends
// up holding events under both.
func TestPlatformRequestIDOnTheAnswerLabelsTheRunsEvents(t *testing.T) {
	platform := newPlatformTestServer(t)
	seedTenantAppRevision(t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1")

	headers := chatHeaders(keyTenantA, appAssistant)
	headers[HeaderSessionID] = "conversation-1"
	body := `{"model":"ignored","messages":[{"role":"user","content":"hello"}]}`

	published := make([]string, 0, 2)
	for range 2 {
		answer := requireStatus(
			t, platform.handler, http.MethodPost, chatPath, body, headers, http.StatusOK,
		)
		requestID := answer.Header().Get(HeaderRequestID)
		require.NotEmpty(t, requestID)
		published = append(published, requestID)
	}
	require.NotEqual(t, published[0], published[1],
		"two requests were given one id: it identifies the session, not the request")

	stored, err := platform.sessions.GetSession(context.Background(), session.Key{
		AppName:   "t/tenant-a/a/" + appAssistant,
		UserID:    "u/" + principalA,
		SessionID: "conversation-1",
	})
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.NotEmpty(t, stored.Events)

	// Every event belongs to one of the two answers, and both answers are
	// represented. An unlabelled event would mean a path that reached the session
	// without the platform's id; a third value would mean an id was minted
	// somewhere this platform does not control.
	labels := make(map[string]int, len(published))
	for _, recorded := range stored.Events {
		require.Contains(t, published, recorded.RequestID,
			"session event carries request id %q, which no answer published",
			recorded.RequestID)
		labels[recorded.RequestID]++
	}
	require.Len(t, labels, 2)
	for _, requestID := range published {
		require.Positive(t, labels[requestID],
			"answer %q left no event behind", requestID)
	}
}

// Every chat answer carries the id this platform gave the run, including the
// ones that refuse before anything is started: a caller reporting a 401, a 409
// or a 500 has something to quote, which is the whole point of a correlation
// id. The preflight is the exception — nothing ran, so there is nothing to
// correlate.
func TestPlatformPublishesARequestIDOnEveryAnswer(t *testing.T) {
	platform := newPlatformTestServer(t)
	seedTenantAppRevision(t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1")
	body := `{"model":"ignored","messages":[{"role":"user","content":"hello"}]}`

	pinned := chatHeaders(keyTenantA, appAssistant)
	pinned[HeaderSessionID] = "conversation-1"
	conflicting := cloneHeaders(pinned)
	conflicting[HeaderAgentRevisionID] = "revision-2"

	answers := []struct {
		name           string
		headers        map[string]string
		expectedStatus int
	}{
		{name: "served", headers: pinned, expectedStatus: http.StatusOK},
		{
			name:           "unauthenticated",
			headers:        map[string]string{HeaderAgentAppID: appAssistant},
			expectedStatus: http.StatusUnauthorized,
		},
		{
			name:           "missing route",
			headers:        map[string]string{HeaderAuthorization: "Bearer " + keyTenantA},
			expectedStatus: http.StatusBadRequest,
		},
		{name: "pin conflict", headers: conflicting, expectedStatus: http.StatusConflict},
	}
	seen := make(map[string]string, len(answers))
	for _, answer := range answers {
		t.Run(answer.name, func(t *testing.T) {
			response := requireStatus(
				t, platform.handler, http.MethodPost, chatPath, body, answer.headers,
				answer.expectedStatus,
			)
			requestID := response.Header().Get(HeaderRequestID)
			require.NotEmpty(t, requestID)
			// Ids are per request, not per session or per process: two runs of
			// one conversation that shared an id would be indistinguishable in
			// the logs, which is the failure this header exists to prevent.
			require.NotContains(t, seen, requestID)
			seen[requestID] = answer.name
		})
	}

	// A preflight carries no id. Nothing was run, and minting one would put a
	// correlation id on a request there is nothing to correlate it with.
	preflight := requireStatus(
		t, platform.handler, http.MethodOptions, chatPath, "", nil, http.StatusNoContent,
	)
	require.Empty(t, preflight.Header().Get(HeaderRequestID))
}

// The request id is the platform's, never the caller's. A client that could
// choose it could file its traffic under another tenant's identifier, or send
// an empty one and file it under none.
func TestPlatformNeverTrustsAClientSuppliedRequestID(t *testing.T) {
	platform := newPlatformTestServer(t)
	seedTenantAppRevision(t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1")
	body := `{"model":"ignored","messages":[{"role":"user","content":"hello"}]}`

	for sessionID, supplied := range map[string]string{
		"conversation-borrowed":  "another-tenants-request-id",
		"conversation-empty":     "",
		"conversation-malformed": "not a request id",
	} {
		t.Run(sessionID, func(t *testing.T) {
			headers := chatHeaders(keyTenantA, appAssistant)
			headers[HeaderSessionID] = sessionID
			headers[HeaderRequestID] = supplied

			response := requireStatus(
				t, platform.handler, http.MethodPost, chatPath, body, headers, http.StatusOK,
			)
			minted := response.Header().Get(HeaderRequestID)
			require.NotEmpty(t, minted)
			require.NotEqual(t, supplied, minted)
		})
	}
}

// A browser reads an error body only when the actual response carries the CORS
// headers. The refusals below all return before routing, which is exactly where
// they used to be dropped, leaving the caller with an opaque CORS failure
// instead of the documented JSON error.
func TestPlatformPublishesCORSHeadersOnEarlyRefusals(t *testing.T) {
	platform := newPlatformTestServer(t)
	seedTenantAppRevision(t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1")
	body := `{"model":"ignored","messages":[{"role":"user","content":"hello"}]}`

	pinned := chatHeaders(keyTenantA, appAssistant)
	pinned[HeaderSessionID] = "conversation-1"
	requireStatus(t, platform.handler, http.MethodPost, chatPath, body, pinned, http.StatusOK)
	conflicting := cloneHeaders(pinned)
	conflicting[HeaderAgentRevisionID] = "revision-2"

	refusals := []struct {
		name           string
		headers        map[string]string
		expectedStatus int
		expectedCode   string
	}{
		{
			name:           "unauthenticated",
			headers:        map[string]string{HeaderAgentAppID: appAssistant},
			expectedStatus: http.StatusUnauthorized,
			expectedCode:   "unauthenticated",
		},
		{
			name: "forbidden app",
			headers: map[string]string{
				HeaderAuthorization: "Bearer " + keyTenantA,
				HeaderAgentAppID:    "reporter",
			},
			expectedStatus: http.StatusForbidden,
			expectedCode:   "forbidden",
		},
		{
			name:           "missing route",
			headers:        map[string]string{HeaderAuthorization: "Bearer " + keyTenantA},
			expectedStatus: http.StatusBadRequest,
			expectedCode:   "missing_route",
		},
		{
			name:           "pin conflict",
			headers:        conflicting,
			expectedStatus: http.StatusConflict,
			expectedCode:   "pin_conflict",
		},
	}
	for _, refusal := range refusals {
		t.Run(refusal.name, func(t *testing.T) {
			response := requireStatus(
				t, platform.handler, http.MethodPost, chatPath, body, refusal.headers,
				refusal.expectedStatus,
			)
			require.Contains(t, response.Body.String(), `"code":"`+refusal.expectedCode+`"`)
			require.Equal(t, "*", response.Header().Get("Access-Control-Allow-Origin"))
			exposed := response.Header().Get("Access-Control-Expose-Headers")
			require.Contains(t, exposed, HeaderSessionID)
			require.Contains(t, exposed, HeaderAgentRevisionID)
			require.Contains(t, exposed, HeaderRetryAfter)
			require.Contains(t, exposed, HeaderRequestID)
		})
	}

	// A refusal names no revision, so the run scope headers stay absent: the
	// session was never pinned and there is nothing to report.
	unauthenticated := requireStatus(
		t, platform.handler, http.MethodPost, chatPath, body,
		map[string]string{HeaderAgentAppID: appAssistant}, http.StatusUnauthorized,
	)
	require.Empty(t, unauthenticated.Header().Get(HeaderAgentRevisionID))
	require.Empty(t, unauthenticated.Header().Get(HeaderSessionID))
}

const chatPath = "/v1/chat/completions"

type platformTestServer struct {
	handler    http.Handler
	repository tenant.Repository
	profiles   storagebundle.ProfileRepository
	resolver   *platformagent.RuntimeResolver
	sessions   session.Service
	directory  *sessiondir.MemoryDirectory

	// leases is what the platform coordinates through, and leaseStore is the
	// state behind it. A test that needs a second Worker builds another
	// coordinator over the same store; see peerCoordinator.
	leases     sessionlease.Coordinator
	leaseStore *sessionlease.MemoryStore
}

// platformTestOptions replaces what used to be a growing parameter list. Every
// field is optional and the zero value is the ordinary platform.
type platformTestOptions struct {
	// directory wraps the pin store, so a test can park or observe a run at the
	// point where it has the lease but has not yet resolved a revision.
	directory chatDirectory

	// coordinator replaces the in-memory one entirely, for the failure modes a
	// working coordinator cannot be made to produce.
	coordinator sessionlease.Coordinator

	// lease tunes the timings of the default coordinator. The zero value is the
	// production 15s TTL, which is what most tests want: a lease that cannot
	// expire underneath them.
	lease sessionlease.Config

	// revisions is the entitlement table. It is nil for the tests whose
	// revisions name no capability at all, which then get the deny-everything
	// authorizer rather than a permissive one.
	revisions security.CapabilityAuthorizer

	// profiles replaces the control plane's backend-profile storage. It is nil
	// for the tests that never touch one, which then get a repository gated by
	// the same tenant table the rest of the platform runs on — the same wiring
	// the binary uses under the inmemory profile.
	profiles storagebundle.ProfileRepository

	// repository replaces the in-memory control plane. It exists so a test can
	// prove that a refusal happened *before* the repository, by handing the
	// platform one that fails the test if it is called at all.
	repository tenant.Repository

	// stores is the storage resolver runtimes are built against. It is nil for
	// the tests whose revisions name no backend profile, which then get the
	// compatibility path over the one process session service — the same wiring
	// as before this field existed.
	//
	// A test that sets it is asking to be wired the way the binary is: through a
	// Router, where a profile reference is resolved rather than refused outright.
	// It owns the Router and must register its Close before calling in here, so
	// that the resolver's Cleanup — registered later, and therefore run first —
	// has already let go of every lease by the time the Router waits for them.
	stores storagebundle.Resolver
}

// chatDirectory is what the platform is handed: a directory, plus whatever a
// test wrapper needs from it.
type chatDirectory interface {
	sessiondir.Directory
	// inner returns the real directory the wrapper delegates to, so assertions
	// read the pins rather than the wrapper.
	unwrap() *sessiondir.MemoryDirectory
	// cleanup releases anything parked in the wrapper. It runs before the
	// resolver is closed, so a failed test never leaves a request waiting while
	// Close waits for its lease.
	cleanup()
}

func newPlatformTestServer(t *testing.T) *platformTestServer {
	t.Helper()
	return newPlatformTestServerWith(t, platformTestOptions{})
}

// newPlatformTestServerWithDirectory lets a test observe or delay the pin
// without changing how the platform is wired.
func newPlatformTestServerWithDirectory(
	t *testing.T,
	wrapper *barrierDirectory,
) *platformTestServer {
	t.Helper()
	if wrapper == nil {
		return newPlatformTestServerWith(t, platformTestOptions{})
	}
	return newPlatformTestServerWith(t, platformTestOptions{directory: wrapper})
}

func newPlatformTestServerWith(
	t *testing.T,
	opts platformTestOptions,
) *platformTestServer {
	t.Helper()
	var repository tenant.Repository = tenant.NewMemoryRepository()
	if opts.repository != nil {
		repository = opts.repository
	}
	sessionService := sessioninmemory.NewSessionService()
	t.Cleanup(func() { require.NoError(t, sessionService.Close()) })
	resolver, err := platformagent.NewRuntimeResolver(
		repository,
		func(
			ctx context.Context,
			revision tenant.AgentRevision,
		) (*platformagent.Runtime, error) {
			// Explicitly capability-denying: every revision these tests publish
			// names no secret and no policy, so this is both the strictest
			// authorizer and an accurate one. Tests that need a capability build
			// their own server with a grant.
			authorizer := opts.revisions
			if authorizer == nil {
				authorizer = security.DenyCapabilities()
			}
			if opts.stores != nil {
				return platformagent.NewRuntime(ctx, revision, opts.stores, authorizer)
			}
			return platformagent.NewRuntimeFromRevision(revision, sessionService, authorizer)
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resolver.Close()) })

	directory := sessiondir.NewMemoryDirectory()
	var chat sessiondir.Directory = directory
	if opts.directory != nil {
		directory = opts.directory.unwrap()
		chat = opts.directory
		t.Cleanup(opts.directory.cleanup)
	}

	store := sessionlease.NewMemoryStore()
	coordinator := opts.coordinator
	if coordinator == nil {
		concrete, coordErr := sessionlease.NewMemoryCoordinator(store, opts.lease)
		require.NoError(t, coordErr)
		coordinator = concrete
	}
	// Before the resolver's cleanup, so a run that is only still alive because
	// it holds a lease is cut loose before Close waits for it.
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })

	revisions := opts.revisions
	if revisions == nil {
		revisions = security.DenyCapabilities()
	}
	profiles := opts.profiles
	if profiles == nil {
		profiles = newTestProfiles(t, repository)
	}
	runs, err := sessionrun.NewService(resolver, chat, coordinator)
	require.NoError(t, err)
	server, err := NewPlatformServer(
		repository, profiles, runs, newTestAuthenticator(t), newTestAdminAuthenticator(t),
		revisions,
	)
	require.NoError(t, err)
	return &platformTestServer{
		handler:    server.Handler(),
		repository: repository,
		profiles:   profiles,
		resolver:   resolver,
		sessions:   sessionService,
		directory:  directory,
		leases:     coordinator,
		leaseStore: store,
	}
}

// newTestProfiles is the profile storage the platform runs on when a test does
// not supply its own: an in-memory repository gated by the same tenant table,
// which is exactly how the binary wires the inmemory profile.
//
// It is a function rather than a field default so that every server gets its
// own, and a test that seeds a profile cannot leak it into the next one.
func newTestProfiles(
	t *testing.T,
	tenants storagebundle.TenantGate,
) storagebundle.ProfileRepository {
	t.Helper()
	profiles, err := storagebundle.NewMemoryProfileRepository(tenants)
	require.NoError(t, err)
	return profiles
}

// newTestAdminAuthenticator grants the three control-plane roles these tests
// exercise: one platform admin, and one tenant admin for each of the two
// tenants, so a cross-tenant attempt is a real credential reaching for something
// that is not its own rather than an unknown key being turned away.
func newTestAdminAuthenticator(t *testing.T) identity.AdminAuthenticator {
	t.Helper()
	authenticator, err := identity.NewStaticAdminAPIKeyAuthenticator(
		map[string]identity.AdminIdentity{
			adminKeyPlatform: {
				Role:        identity.RolePlatformAdmin,
				PrincipalID: principalPlatform,
			},
			adminKeyTenantA: {
				Role:        identity.RoleTenantAdmin,
				PrincipalID: principalAdminA,
				TenantID:    "tenant-a",
			},
			adminKeyTenantB: {
				Role:        identity.RoleTenantAdmin,
				PrincipalID: principalAdminB,
				TenantID:    "tenant-b",
			},
		})
	require.NoError(t, err)
	return authenticator
}

// adminHeaders is what every admin request carries: the credential, and the
// media type that keeps the request outside the browser's simple-request set.
func adminHeaders(apiKey string) map[string]string {
	return map[string]string{
		HeaderAuthorization: "Bearer " + apiKey,
		"Content-Type":      "application/json",
	}
}

// newTestCoordinator is the coordinator for tests that only need the platform
// to have one, and never contend for a session.
func newTestCoordinator(t *testing.T) sessionlease.Coordinator {
	t.Helper()
	coordinator, err := sessionlease.NewMemoryCoordinator(
		sessionlease.NewMemoryStore(), sessionlease.Config{},
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })
	return coordinator
}

// peerCoordinator is a second Worker over the same lease state: what the Redis
// backend gives two processes, the memory backend gives two coordinators over
// one store. It is how these tests both hold a session against the platform and
// check whether the platform gave one back.
func (p *platformTestServer) peerCoordinator(t *testing.T) sessionlease.Coordinator {
	t.Helper()
	peer, err := sessionlease.NewMemoryCoordinator(p.leaseStore, sessionlease.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, peer.Close()) })
	return peer
}

func newTestAuthenticator(t *testing.T) identity.Authenticator {
	t.Helper()
	authenticator, err := identity.NewStaticAPIKeyAuthenticator(map[string]identity.Identity{
		keyTenantA: {
			TenantID:      "tenant-a",
			PrincipalID:   principalA,
			AllowedAppIDs: []string{appAssistant},
		},
		keyTenantB: {
			TenantID:      "tenant-b",
			PrincipalID:   principalB,
			AllowedAppIDs: []string{appAssistant},
		},
	})
	require.NoError(t, err)
	return authenticator
}

func chatHeaders(apiKey string, appID string) map[string]string {
	return map[string]string{
		HeaderAuthorization: "Bearer " + apiKey,
		HeaderAgentAppID:    appID,
	}
}

// barrierDirectory parks every first run inside EnsurePin, so a test can line
// up two requests that each already resolved a different candidate.
type barrierDirectory struct {
	inner       *sessiondir.MemoryDirectory
	arrived     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
}

func (d *barrierDirectory) GetPin(
	ctx context.Context,
	key sessiondir.Key,
) (string, bool, error) {
	return d.inner.GetPin(ctx, key)
}

func (d *barrierDirectory) EnsurePin(
	ctx context.Context,
	key sessiondir.Key,
	candidateRevisionID string,
) (string, error) {
	d.arrived <- struct{}{}
	<-d.release
	return d.inner.EnsurePin(ctx, key, candidateRevisionID)
}

// awaitArrival fails fast instead of hanging when a request stops before the
// barrier, which would otherwise look like a deadlock rather than a rejection.
func (d *barrierDirectory) awaitArrival(t *testing.T) {
	t.Helper()
	select {
	case <-d.arrived:
	case <-time.After(10 * time.Second):
		t.Fatal("a chat request never reached EnsurePin")
	}
}

func (d *barrierDirectory) releaseAll() {
	d.releaseOnce.Do(func() { close(d.release) })
}

func (d *barrierDirectory) unwrap() *sessiondir.MemoryDirectory { return d.inner }

func (d *barrierDirectory) cleanup() { d.releaseAll() }

// contextDirectory parks a run the same way barrierDirectory does, but hands
// the test the run's context. That context is the one the platform derived from
// the lease, so it is what a test asserts on when it wants to know whether
// losing a lease actually stops the run that held it.
type contextDirectory struct {
	inner       *sessiondir.MemoryDirectory
	arrived     chan context.Context
	release     chan struct{}
	releaseOnce sync.Once
}

func newContextDirectory() *contextDirectory {
	return &contextDirectory{
		inner:   sessiondir.NewMemoryDirectory(),
		arrived: make(chan context.Context, 1),
		release: make(chan struct{}),
	}
}

func (d *contextDirectory) GetPin(
	ctx context.Context,
	key sessiondir.Key,
) (string, bool, error) {
	return d.inner.GetPin(ctx, key)
}

func (d *contextDirectory) EnsurePin(
	ctx context.Context,
	key sessiondir.Key,
	candidateRevisionID string,
) (string, error) {
	select {
	case d.arrived <- ctx:
	default:
	}
	<-d.release
	return d.inner.EnsurePin(ctx, key, candidateRevisionID)
}

// awaitRunContext returns the context the parked run is executing under.
func (d *contextDirectory) awaitRunContext(t *testing.T) context.Context {
	t.Helper()
	select {
	case ctx := <-d.arrived:
		return ctx
	case <-time.After(10 * time.Second):
		t.Fatal("a chat request never reached EnsurePin")
		return nil
	}
}

func (d *contextDirectory) releaseAll() {
	d.releaseOnce.Do(func() { close(d.release) })
}

func (d *contextDirectory) unwrap() *sessiondir.MemoryDirectory { return d.inner }

func (d *contextDirectory) cleanup() { d.releaseAll() }

func seedTenantAppRevision(
	t *testing.T,
	handler http.Handler,
	tenantID string,
	appID string,
	revisionID string,
	revisionNo uint64,
	modelName string,
) {
	t.Helper()
	requireStatus(t, handler, http.MethodPost, "/admin/v1/tenants", fmt.Sprintf(`{
		"id":%q,"slug":%q,"name":%q
	}`, tenantID, tenantID, "Tenant "+tenantID),
		adminHeaders(adminKeyPlatform), http.StatusCreated)
	requireStatus(
		t, handler, http.MethodPost, "/admin/v1/tenants/"+tenantID+"/apps",
		fmt.Sprintf(`{"id":%q,"name":"Assistant"}`, appID),
		adminHeaders(adminKeyPlatform), http.StatusCreated,
	)
	createRevisionThroughAPI(t, handler, tenantID, appID, revisionID, revisionNo, modelName)
	publishRevisionThroughAPI(t, handler, tenantID, appID, revisionID)
}

type resolverFunc func(
	context.Context,
	tenant.TenantContext,
	string,
	string,
) (platformagent.ResolvedRuntime, error)

func (f resolverFunc) Resolve(
	ctx context.Context,
	scope tenant.TenantContext,
	appID string,
	pinnedRevisionID string,
) (platformagent.ResolvedRuntime, error) {
	return f(ctx, scope, appID, pinnedRevisionID)
}

// blockingResponseWriter holds the handler inside its first write so the test
// can observe the lease while the SSE response is still in flight.
type blockingResponseWriter struct {
	header      http.Header
	started     chan struct{}
	released    chan struct{}
	startedOnce sync.Once
	releaseOnce sync.Once

	mu     sync.Mutex
	body   bytes.Buffer
	status int
}

func newBlockingResponseWriter() *blockingResponseWriter {
	return &blockingResponseWriter{
		header:   make(http.Header),
		started:  make(chan struct{}),
		released: make(chan struct{}),
	}
}

func (w *blockingResponseWriter) Header() http.Header {
	return w.header
}

func (w *blockingResponseWriter) WriteHeader(status int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.status = status
}

func (w *blockingResponseWriter) Write(chunk []byte) (int, error) {
	w.startedOnce.Do(func() { close(w.started) })
	<-w.released
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.body.Write(chunk)
}

func (w *blockingResponseWriter) Flush() {}

func (w *blockingResponseWriter) release() {
	w.releaseOnce.Do(func() { close(w.released) })
}

func (w *blockingResponseWriter) bodyString() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.body.String()
}

func createRevisionThroughAPI(
	t *testing.T,
	handler http.Handler,
	tenantID string,
	appID string,
	revisionID string,
	revisionNo uint64,
	modelName string,
) *httptest.ResponseRecorder {
	t.Helper()
	// No created_by: the request cannot state authorship, and a body carrying
	// the field is a 400. The stored value comes from the credential.
	body := fmt.Sprintf(`{
		"id":%q,
		"revision_no":%d,
		"config":{
			"agent_name":"test-agent",
			"instruction":"Answer through the deterministic model.",
			"model":{"provider":"deterministic","name":%q}
		}
	}`, revisionID, revisionNo, modelName)
	return requireStatus(
		t,
		handler,
		http.MethodPost,
		fmt.Sprintf("/admin/v1/tenants/%s/apps/%s/revisions", tenantID, appID),
		body,
		adminHeaders(adminKeyPlatform),
		http.StatusCreated,
	)
}

func publishRevisionThroughAPI(
	t *testing.T,
	handler http.Handler,
	tenantID string,
	appID string,
	revisionID string,
) *httptest.ResponseRecorder {
	t.Helper()
	return requireStatus(
		t,
		handler,
		http.MethodPost,
		fmt.Sprintf(
			"/admin/v1/tenants/%s/apps/%s/revisions/%s/publish",
			tenantID,
			appID,
			revisionID,
		),
		"",
		// Content-Type is required even though publish sends no body: the rule
		// is about what a browser may send unasked, not about parsing.
		adminHeaders(adminKeyPlatform),
		http.StatusOK,
	)
}

func doChat(
	t *testing.T,
	handler http.Handler,
	body string,
	headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()
	return serve(handler, http.MethodPost, chatPath, body, headers)
}

func requireStatus(
	t *testing.T,
	handler http.Handler,
	method string,
	path string,
	body string,
	headers map[string]string,
	expectedStatus int,
) *httptest.ResponseRecorder {
	t.Helper()
	response := serve(handler, method, path, body, headers)
	require.Equal(t, expectedStatus, response.Code, response.Body.String())
	return response
}

func serve(
	handler http.Handler,
	method string,
	path string,
	body string,
	headers map[string]string,
) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func cloneHeaders(headers map[string]string) map[string]string {
	cloned := make(map[string]string, len(headers)+1)
	for name, value := range headers {
		cloned[name] = value
	}
	return cloned
}
