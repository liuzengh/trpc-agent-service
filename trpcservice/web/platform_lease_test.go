package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	platformagent "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionlease"
	"github.com/stretchr/testify/require"
)

// runKey is the lease scope the platform derives from an authenticated request:
// the full session identity, never a session id on its own.
func runKey(tenantID, principalID, sessionID string) sessiondir.Key {
	return sessiondir.Key{
		TenantID:    tenantID,
		AppID:       appAssistant,
		PrincipalID: principalID,
		SessionID:   sessionID,
	}
}

// A session another Worker is already running is refused, and the refusal says
// how long to wait. This is the whole user-visible contract of the run lease.
func TestPlatformRefusesASessionAnotherWorkerHolds(t *testing.T) {
	platform := newPlatformTestServer(t)
	seedTenantAppRevision(
		t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1",
	)

	peer := platform.peerCoordinator(t)
	held, err := peer.Acquire(
		context.Background(), runKey("tenant-a", principalA, "conversation-1"),
	)
	require.NoError(t, err)

	headers := chatHeaders(keyTenantA, appAssistant)
	headers[HeaderSessionID] = "conversation-1"
	body := `{"model":"ignored","messages":[{"role":"user","content":"hello"}]}`

	busy := requireStatus(
		t, platform.handler, http.MethodPost, chatPath, body, headers,
		http.StatusConflict,
	)
	require.Contains(t, busy.Body.String(), `"code":"session_busy"`)
	require.Contains(t, busy.Body.String(), "conversation-1")
	require.Equal(t, "2", busy.Header().Get(HeaderRetryAfter))
	// A refusal is not a run: nothing was pinned and no runtime was built. A
	// refused request that had already pinned would decide the revision of a
	// session it never got to serve.
	require.Equal(t, 0, platform.directory.Size())
	require.Equal(t, 0, platform.resolver.CacheSize())

	// Once the other Worker is finished, the same request works.
	require.NoError(t, held.Release(context.Background()))
	ok := requireStatus(
		t, platform.handler, http.MethodPost, chatPath, body, headers, http.StatusOK,
	)
	require.Equal(t, "revision-1", ok.Header().Get(HeaderAgentRevisionID))
	require.Equal(t, 1, platform.directory.Size())
}

// The lease is scoped to the whole session identity, so holding one session
// blocks that session and nothing else.
func TestPlatformBusySessionDoesNotBlockOtherSessions(t *testing.T) {
	platform := newPlatformTestServer(t)
	for _, tenantID := range []string{"tenant-a", "tenant-b"} {
		seedTenantAppRevision(
			t, platform.handler, tenantID, appAssistant, "revision-1", 1, "echo-"+tenantID,
		)
	}

	peer := platform.peerCoordinator(t)
	_, err := peer.Acquire(
		context.Background(), runKey("tenant-a", principalA, "conversation-1"),
	)
	require.NoError(t, err)

	body := `{"model":"ignored","messages":[{"role":"user","content":"hello"}]}`
	unaffected := []struct {
		name      string
		apiKey    string
		sessionID string
	}{
		// Same credential, a different conversation.
		{name: "other session", apiKey: keyTenantA, sessionID: "conversation-2"},
		// Another tenant's principal, colliding on the session id alone. The key
		// digest is built from the full identity, so this must not collide.
		{name: "other tenant", apiKey: keyTenantB, sessionID: "conversation-1"},
	}
	for _, run := range unaffected {
		t.Run(run.name, func(t *testing.T) {
			headers := chatHeaders(run.apiKey, appAssistant)
			headers[HeaderSessionID] = run.sessionID
			requireStatus(
				t, platform.handler, http.MethodPost, chatPath, body, headers,
				http.StatusOK,
			)
		})
	}

	// And the held one is still held.
	headers := chatHeaders(keyTenantA, appAssistant)
	headers[HeaderSessionID] = "conversation-1"
	requireStatus(
		t, platform.handler, http.MethodPost, chatPath, body, headers,
		http.StatusConflict,
	)
}

// A run that finishes normally gives the lease back, so the next turn of the
// same conversation is not refused. Without this the lease would serialize a
// conversation at its TTL rather than at its runs.
func TestPlatformReleasesTheLeaseOnACleanFinish(t *testing.T) {
	platform := newPlatformTestServer(t)
	seedTenantAppRevision(
		t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1",
	)
	headers := chatHeaders(keyTenantA, appAssistant)
	headers[HeaderSessionID] = "conversation-1"
	body := `{"model":"ignored","messages":[{"role":"user","content":"turn"}]}`

	for turn := 0; turn < 3; turn++ {
		requireStatus(
			t, platform.handler, http.MethodPost, chatPath, body, headers,
			http.StatusOK,
		)
	}

	// Observed from outside: another Worker can take the session immediately.
	peer := platform.peerCoordinator(t)
	lease, err := peer.Acquire(
		context.Background(), runKey("tenant-a", principalA, "conversation-1"),
	)
	require.NoError(t, err)
	require.NoError(t, lease.Release(context.Background()))
}

// A refusal that never started a run releases nothing but must not leave the
// lease held either. The pin conflict below is refused after the lease is
// taken, which is the path that would strand it.
func TestPlatformReleasesTheLeaseOnARefusalAfterAcquiring(t *testing.T) {
	platform := newPlatformTestServer(t)
	seedTenantAppRevision(
		t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1",
	)
	headers := chatHeaders(keyTenantA, appAssistant)
	headers[HeaderSessionID] = "conversation-1"
	body := `{"model":"ignored","messages":[{"role":"user","content":"hello"}]}`
	requireStatus(
		t, platform.handler, http.MethodPost, chatPath, body, headers, http.StatusOK,
	)

	conflicting := cloneHeaders(headers)
	conflicting[HeaderAgentRevisionID] = "revision-2"
	conflict := requireStatus(
		t, platform.handler, http.MethodPost, chatPath, body, conflicting,
		http.StatusConflict,
	)
	require.Contains(t, conflict.Body.String(), `"code":"pin_conflict"`)
	// A pin conflict is the client's mistake, not a busy session: it must not
	// leave the conversation locked until the TTL expires.
	require.Empty(t, conflict.Header().Get(HeaderRetryAfter))
	requireStatus(
		t, platform.handler, http.MethodPost, chatPath, body, headers, http.StatusOK,
	)
}

// Coordination that cannot answer is not permission to run. Every reply that is
// not a definite "busy" has to reach the client as 503, including one this
// build cannot classify.
func TestPlatformRefusesWhenCoordinationCannotAnswer(t *testing.T) {
	failures := map[string]error{
		"unavailable": sessionlease.ErrUnavailable,
		"closed":      sessionlease.ErrClosed,
		"unknown":     errUnrecognisedByThisBuild,
	}
	for name, failure := range failures {
		t.Run(name, func(t *testing.T) {
			platform := newPlatformTestServerWith(t, platformTestOptions{
				coordinator: &stubCoordinator{err: failure},
			})
			seedTenantAppRevision(
				t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1",
			)
			headers := chatHeaders(keyTenantA, appAssistant)
			headers[HeaderSessionID] = "conversation-1"

			response := requireStatus(t, platform.handler, http.MethodPost, chatPath, `{
				"model":"ignored","messages":[{"role":"user","content":"hello"}]
			}`, headers, http.StatusServiceUnavailable)
			require.Contains(t, response.Body.String(), `"code":"coordination_unavailable"`)
			require.Contains(t, response.Body.String(), "was not started")
			// Not a retry hint: nobody knows when coordination comes back, and
			// suggesting two seconds would turn an outage into a stampede.
			require.Empty(t, response.Header().Get(HeaderRetryAfter))
			require.Equal(t, 0, platform.directory.Size())
			require.Equal(t, 0, platform.resolver.CacheSize())
		})
	}
}

// Losing the lease mid-run cancels the run. That is cooperative cancellation
// and nothing more: it stops this Worker from carrying on, it does not stop the
// writes already in flight. See the sessionlease package documentation.
func TestPlatformCancelsARunThatLostItsLease(t *testing.T) {
	directory := newContextDirectory()
	platform := newPlatformTestServerWith(t, platformTestOptions{directory: directory})
	seedTenantAppRevision(
		t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1",
	)
	headers := chatHeaders(keyTenantA, appAssistant)
	headers[HeaderSessionID] = "conversation-1"

	served := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		served <- doChat(t, platform.handler, `{
			"model":"ignored","messages":[{"role":"user","content":"long"}]
		}`, headers)
	}()

	// The parked run holds the lease and is executing under the derived run
	// context, which is still live.
	runCtx := directory.awaitRunContext(t)
	require.NoError(t, runCtx.Err())

	// Closing the coordinator is how this process loses every lease it holds at
	// once, which is what a shutdown or a takeover looks like from inside a run.
	require.NoError(t, platform.leases.Close())
	select {
	case <-runCtx.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("losing the lease did not cancel the run it belonged to")
	}
	require.ErrorIs(t, runCtx.Err(), context.Canceled)

	directory.releaseAll()
	<-served
}

// A run that ended because the client went away does not release. The Runner
// keeps writing terminal events for about a second after cancellation, on a
// context that cannot be cancelled, so handing the session to the next Worker
// at that moment would hand it to one writing against those tail writes. The
// TTL covers the gap instead, and the price is that the session stays refused
// for the remainder of it.
func TestPlatformDoesNotReleaseWhenTheClientDisconnects(t *testing.T) {
	directory := newContextDirectory()
	platform := newPlatformTestServerWith(t, platformTestOptions{directory: directory})
	seedTenantAppRevision(
		t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1",
	)
	headers := chatHeaders(keyTenantA, appAssistant)
	headers[HeaderSessionID] = "conversation-1"

	clientCtx, disconnect := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() {
		defer close(served)
		request := httptest.NewRequest(http.MethodPost, chatPath, strings.NewReader(`{
			"model":"ignored","messages":[{"role":"user","content":"hello"}]
		}`)).WithContext(clientCtx)
		request.Header.Set("Content-Type", "application/json")
		for name, value := range headers {
			request.Header.Set(name, value)
		}
		platform.handler.ServeHTTP(httptest.NewRecorder(), request)
	}()

	directory.awaitRunContext(t)
	disconnect()
	directory.releaseAll()
	<-served

	// The lock is still held, by nobody, until it expires.
	peer := platform.peerCoordinator(t)
	_, err := peer.Acquire(
		context.Background(), runKey("tenant-a", principalA, "conversation-1"),
	)
	require.ErrorIs(t, err, sessionlease.ErrSessionBusy)
}

func TestPlatformKeepsTheSessionWhenTheClientLeavesMidRun(t *testing.T) {
	upstream := newBlockingUpstream(t)
	platform := newPlatformTestServerWith(t, platformTestOptions{
		lease: sessionlease.Config{
			TTL:           2 * time.Second,
			RenewInterval: 400 * time.Millisecond,
			SafetyMargin:  400 * time.Millisecond,
		},
	})
	seedTenantAppRevision(
		t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1",
	)
	seedUpstreamRevision(
		t, platform.handler, "tenant-a", appAssistant, "revision-2", 2, upstream.server.URL,
	)

	clientCtx, disconnect := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() {
		defer close(served)
		request := httptest.NewRequest(http.MethodPost, chatPath, strings.NewReader(`{
			"model":"ignored","messages":[{"role":"user","content":"hello"}]
		}`)).WithContext(clientCtx)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set(HeaderAuthorization, "Bearer "+keyTenantA)
		request.Header.Set(HeaderAgentAppID, appAssistant)
		request.Header.Set(HeaderSessionID, "conversation-1")
		platform.handler.ServeHTTP(httptest.NewRecorder(), request)
	}()
	// Drain the request before the platform's cleanup waits for its Runtime.
	t.Cleanup(func() {
		disconnect()
		upstream.answer()
		select {
		case <-served:
		case <-time.After(10 * time.Second):
			t.Error("chat request did not finish during cleanup")
		}
	})

	upstream.awaitEntered(t)
	disconnect()
	upstream.awaitGone(t)
	select {
	case <-served:
	case <-time.After(10 * time.Second):
		t.Fatal("a disconnected chat request never returned from ServeHTTP")
	}

	peer := platform.peerCoordinator(t)
	key := runKey("tenant-a", principalA, "conversation-1")
	_, err := peer.Acquire(context.Background(), key)
	require.ErrorIs(t, err, sessionlease.ErrSessionBusy,
		"an interrupted run released its session before the lease expired")

	var taken sessionlease.Lease
	require.Eventually(t, func() bool {
		lease, acquireErr := peer.Acquire(context.Background(), key)
		if acquireErr != nil {
			return false
		}
		taken = lease
		return true
	}, 10*time.Second, 10*time.Millisecond, "the interrupted run stranded the conversation")
	require.NoError(t, taken.Release(context.Background()))
}

func seedUpstreamRevision(
	t *testing.T,
	handler http.Handler,
	tenantID, appID, revisionID string,
	revisionNo uint64,
	baseURL string,
) {
	t.Helper()
	body := fmt.Sprintf(`{
		"id":%q,
		"revision_no":%d,
		"config":{
			"agent_name":"test-agent",
			"instruction":"Answer through the upstream endpoint.",
			"model":{"provider":%q,"name":"test-model","base_url":%q}
		}
	}`, revisionID, revisionNo, platformagent.ProviderOpenAICompatible, baseURL+"/v1")
	requireStatus(
		t, handler, http.MethodPost,
		fmt.Sprintf("/admin/v1/tenants/%s/apps/%s/revisions", tenantID, appID),
		body, adminHeaders(adminKeyPlatform), http.StatusCreated,
	)
	publishRevisionThroughAPI(t, handler, tenantID, appID, revisionID)
}

type blockingUpstream struct {
	server      *httptest.Server
	entered     chan struct{}
	gone        chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	goneOnce    sync.Once
	releaseOnce sync.Once
}

func newBlockingUpstream(t *testing.T) *blockingUpstream {
	t.Helper()
	upstream := &blockingUpstream{
		entered: make(chan struct{}),
		gone:    make(chan struct{}),
		release: make(chan struct{}),
	}
	upstream.server = httptest.NewServer(http.HandlerFunc(
		func(writer http.ResponseWriter, request *http.Request) {
			if err := json.NewDecoder(request.Body).Decode(&map[string]any{}); err != nil {
				return
			}
			upstream.enteredOnce.Do(func() { close(upstream.entered) })
			select {
			case <-upstream.release:
			case <-request.Context().Done():
				upstream.goneOnce.Do(func() { close(upstream.gone) })
				return
			}
			writer.Header().Set("Content-Type", "text/event-stream")
			_, _ = writer.Write([]byte("data: [DONE]\n\n"))
		}))
	t.Cleanup(func() {
		upstream.answer()
		upstream.server.Close()
	})
	return upstream
}

func (u *blockingUpstream) answer() {
	u.releaseOnce.Do(func() { close(u.release) })
}

func (u *blockingUpstream) awaitEntered(t *testing.T) {
	t.Helper()
	select {
	case <-u.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the model call never reached the upstream endpoint")
	}
}

func (u *blockingUpstream) awaitGone(t *testing.T) {
	t.Helper()
	select {
	case <-u.gone:
	case <-time.After(10 * time.Second):
		t.Fatal("the disconnect never reached the model call")
	}
}

// A lease that was already lost is not released either. Releasing is
// owner-matched so it could not disturb the new owner, but asking at all would
// be this build claiming an authority over the session that it no longer has.
func TestPlatformDoesNotReleaseALeaseItAlreadyLost(t *testing.T) {
	directory := newContextDirectory()
	// A TTL short enough for another Worker to take the session over while this
	// one is parked, which is what a stalled Worker looks like from outside.
	platform := newPlatformTestServerWith(t, platformTestOptions{
		directory: directory,
		lease: sessionlease.Config{
			TTL:           300 * time.Millisecond,
			RenewInterval: 50 * time.Millisecond,
			SafetyMargin:  50 * time.Millisecond,
		},
	})
	seedTenantAppRevision(
		t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1",
	)
	headers := chatHeaders(keyTenantA, appAssistant)
	headers[HeaderSessionID] = "conversation-1"

	served := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		served <- doChat(t, platform.handler, `{
			"model":"ignored","messages":[{"role":"user","content":"hello"}]
		}`, headers)
	}()
	runCtx := directory.awaitRunContext(t)

	// Take the session over from outside. Renewal keeps the lock alive while the
	// run is parked, so the takeover has to be forced the way a real one is: the
	// holder stops renewing. Closing the coordinator is that, from this side.
	require.NoError(t, platform.leases.Close())
	select {
	case <-runCtx.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the parked run was never told it lost the lease")
	}

	peer := platform.peerCoordinator(t)
	var taken sessionlease.Lease
	require.Eventually(t, func() bool {
		lease, err := peer.Acquire(
			context.Background(), runKey("tenant-a", principalA, "conversation-1"),
		)
		if err != nil {
			return false
		}
		taken = lease
		return true
	}, 10*time.Second, 10*time.Millisecond, "the abandoned lock never expired")

	// The stalled run now finishes. It must not take the session away from the
	// Worker that inherited it.
	directory.releaseAll()
	<-served
	require.NoError(t, taken.Release(context.Background()))
}

// The fence is an observation handle, not an admission token. Publishing it
// would invite a client to treat it as a guarantee that this build does not
// make, so it appears in no response, on any path.
func TestPlatformNeverPublishesTheFence(t *testing.T) {
	platform := newPlatformTestServer(t)
	seedTenantAppRevision(
		t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1",
	)
	headers := chatHeaders(keyTenantA, appAssistant)
	headers[HeaderSessionID] = "conversation-1"
	body := `{"model":"ignored","messages":[{"role":"user","content":"hello"}]}`

	ok := requireStatus(
		t, platform.handler, http.MethodPost, chatPath, body, headers, http.StatusOK,
	)

	peer := platform.peerCoordinator(t)
	held, err := peer.Acquire(
		context.Background(), runKey("tenant-a", principalA, "conversation-1"),
	)
	require.NoError(t, err)
	require.NotZero(t, held.Fence(), "the fence exists, it is just not published")
	busy := requireStatus(
		t, platform.handler, http.MethodPost, chatPath, body, headers,
		http.StatusConflict,
	)

	for _, response := range []*httptest.ResponseRecorder{ok, busy} {
		for name, values := range response.Header() {
			for _, value := range values {
				require.NotContains(t, name, "Fence")
				require.NotContains(t, name, "fence")
				require.NotContains(t, value, "fence")
			}
		}
		require.NotContains(t, response.Body.String(), "fence")
	}
}

// Releasing happens after the response has been written, and a streamed one
// cannot be taken back. A release that fails is therefore a lock that expires
// on its TTL instead of immediately, and nothing else: it must not turn a
// delivered answer into an error.
func TestPlatformKeepsTheResponseWhenReleasingFails(t *testing.T) {
	released := make(chan struct{})
	coordinator := &stubCoordinator{lease: &stubLease{
		done:       make(chan struct{}),
		releaseErr: errUnrecognisedByThisBuild,
		onRelease:  func() { close(released) },
	}}
	platform := newPlatformTestServerWith(t, platformTestOptions{
		coordinator: coordinator,
	})
	seedTenantAppRevision(
		t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1",
	)
	headers := chatHeaders(keyTenantA, appAssistant)
	headers[HeaderSessionID] = "conversation-1"

	response := requireStatus(t, platform.handler, http.MethodPost, chatPath, `{
		"model":"ignored","messages":[{"role":"user","content":"hello"}]
	}`, headers, http.StatusOK)
	select {
	case <-released:
	default:
		t.Fatal("a clean finish did not try to release the lease")
	}
	require.Equal(t, "conversation-1", response.Header().Get(HeaderSessionID))
	require.Equal(t, "revision-1", response.Header().Get(HeaderAgentRevisionID))
	require.Contains(t, response.Body.String(), `"model":"echo-v1"`)
	require.NotContains(t, response.Body.String(), `"code":`)
}

// errUnrecognisedByThisBuild stands for a coordination failure that is neither
// busy nor one of the documented sentinels.
var errUnrecognisedByThisBuild = errStub("coordination said something unexpected")

type errStub string

func (e errStub) Error() string { return string(e) }

// stubCoordinator produces the Acquire results a working coordinator cannot be
// asked for.
type stubCoordinator struct {
	lease sessionlease.Lease
	err   error
}

func (c *stubCoordinator) Acquire(
	context.Context,
	sessiondir.Key,
) (sessionlease.Lease, error) {
	return c.lease, c.err
}

func (c *stubCoordinator) Close() error { return nil }

type stubLease struct {
	done       chan struct{}
	releaseErr error
	onRelease  func()
}

func (l *stubLease) Fence() uint64 { return 1 }

func (l *stubLease) Done() <-chan struct{} { return l.done }

func (l *stubLease) Release(context.Context) error {
	if l.onRelease != nil {
		l.onRelease()
	}
	return l.releaseErr
}
