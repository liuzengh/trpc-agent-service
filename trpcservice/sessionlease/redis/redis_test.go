package redis_test

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionlease"
	redislease "github.com/liuzengh/trpc-agent-service/trpcservice/sessionlease/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionlease/sessionleasetest"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Nothing in this file needs a Redis. Most tests close their client first, and
// go-redis refuses a command on a closed client before it dials; the two that
// are about a backend which accepts and then says nothing use a local listener
// that does exactly that.

func TestNewRejectsUnusableConfiguration(t *testing.T) {
	t.Parallel()

	t.Run("client", func(t *testing.T) {
		t.Parallel()
		_, err := redislease.New(nil, redislease.Options{})
		require.ErrorIs(t, err, sessionlease.ErrInvalidConfig)
	})

	t.Run("prefix characters", func(t *testing.T) {
		t.Parallel()
		// Braces would carry a hash tag of their own and split the lock and the
		// fence of one lease across two cluster slots.
		for _, prefix := range []string{"has space", "has{brace}", "has/slash", "has\n"} {
			_, err := redislease.New(newClosedClient(t), redislease.Options{KeyPrefix: prefix})
			require.ErrorIs(t, err, sessionlease.ErrInvalidConfig, "prefix %q", prefix)
		}
	})

	t.Run("prefix length", func(t *testing.T) {
		t.Parallel()
		_, err := redislease.New(newClosedClient(t), redislease.Options{
			KeyPrefix: strings.Repeat("a", 65),
		})
		require.ErrorIs(t, err, sessionlease.ErrInvalidConfig)
	})

	t.Run("lease timings", func(t *testing.T) {
		t.Parallel()
		_, err := redislease.New(newClosedClient(t), redislease.Options{
			Lease: sessionlease.Config{TTL: time.Second, RenewInterval: 2 * time.Second},
		})
		require.ErrorIs(t, err, sessionlease.ErrInvalidConfig)
	})

	t.Run("ttl precision", func(t *testing.T) {
		t.Parallel()
		for _, ttl := range []time.Duration{
			500 * time.Microsecond,
			1500 * time.Microsecond,
			time.Second + time.Nanosecond,
		} {
			_, err := redislease.New(newClosedClient(t), redislease.Options{
				Lease: sessionlease.Config{
					TTL: ttl, RenewInterval: ttl / 4, SafetyMargin: ttl / 4,
				},
			})
			require.ErrorIs(t, err, sessionlease.ErrInvalidConfig, "ttl %s", ttl)
		}
	})

	t.Run("whole millisecond ttl", func(t *testing.T) {
		t.Parallel()
		for _, config := range []sessionlease.Config{
			{},
			{TTL: 300 * time.Millisecond, RenewInterval: 50 * time.Millisecond, SafetyMargin: 50 * time.Millisecond},
			{TTL: time.Millisecond, RenewInterval: 200 * time.Microsecond, SafetyMargin: 300 * time.Microsecond},
		} {
			coordinator, err := redislease.New(newClosedClient(t), redislease.Options{Lease: config})
			require.NoError(t, err, "config %+v", config)
			require.NoError(t, coordinator.Close())
		}
	})
}

func TestAcquireValidatesBeforeItTouchesRedis(t *testing.T) {
	t.Parallel()

	coordinator, err := redislease.New(newClosedClient(t), redislease.Options{})
	require.NoError(t, err)

	key := sessionleasetest.Key()
	key.SessionID = "not a session id"

	// A closed client would report ErrUnavailable, so getting the argument
	// error back proves the check ran first.
	_, err = coordinator.Acquire(t.Context(), key)
	require.ErrorIs(t, err, tenant.ErrInvalidArgument)
	require.NotErrorIs(t, err, sessionlease.ErrUnavailable)
}

func TestAcquireFailsClosedWhenTheClientCannotRun(t *testing.T) {
	t.Parallel()

	coordinator, err := redislease.New(newClosedClient(t), redislease.Options{})
	require.NoError(t, err)

	_, err = coordinator.Acquire(t.Context(), sessionleasetest.Key())
	require.ErrorIs(t, err, sessionlease.ErrUnavailable,
		"a backend that cannot answer must not let the caller run")
	require.NotErrorIs(t, err, sessionlease.ErrSessionBusy,
		"a coordination outage answers 503, not 409")
}

func TestCloseDoesNotCloseTheBorrowedClient(t *testing.T) {
	t.Parallel()

	client := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = client.Close() })

	coordinator, err := redislease.New(client, redislease.Options{})
	require.NoError(t, err)
	require.NoError(t, coordinator.Close())
	require.NoError(t, coordinator.Close(), "Close is idempotent")

	// The client is the caller's, and a caller is free to keep using it or to
	// have handed the same one to something else, so closing the coordinator
	// must leave it usable. A second Close on an already-closed client returns
	// an error, so a nil error here says the coordinator did not close it.
	require.NoError(t, client.Close())
}

func TestAcquireRefusedAfterClose(t *testing.T) {
	t.Parallel()

	coordinator, err := redislease.New(newClosedClient(t), redislease.Options{})
	require.NoError(t, err)
	require.NoError(t, coordinator.Close())

	_, err = coordinator.Acquire(t.Context(), sessionleasetest.Key())
	require.ErrorIs(t, err, sessionlease.ErrClosed)
}

// A backend that has gone quiet is the case the tests below are about, and it
// is not the same as one that refuses: the connection is up, the command is
// sent, and nothing comes back. That is the only situation in which how long
// Close can be held up is in question, and a listener that accepts and then
// stays silent reproduces it without a Redis.
//
// How long it is held up turns out to depend on the client the process handed
// over, so both regimes are pinned here. go-redis v9 passes context.Background
// to the socket read unless it was built with ContextTimeoutEnabled, so on a
// default client no deadline this package sets can reach a command already on
// the wire. Both tests assert a bound; only one of them is this package's.

// silentReadTimeout is the read timeout given to a client that ignores
// contexts. It is what bounds a command on the wire for such a client, so it is
// kept short enough to wait for.
const silentReadTimeout = 300 * time.Millisecond

func TestAcquireGivesUpOnItsOwnBudgetWhenTheClientRespectsContexts(t *testing.T) {
	t.Parallel()

	cfg := sessionleasetest.Timings()
	budget := cfg.TTL - cfg.SafetyMargin - cfg.RenewInterval
	// A read timeout far longer than the budget, so a bound observed below is
	// this package's and not the client's.
	client := newSilentClient(t, goredis.Options{
		ContextTimeoutEnabled: true,
		ReadTimeout:           time.Minute,
	})
	coordinator, err := redislease.New(client, redislease.Options{Lease: cfg})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })

	started := time.Now()
	_, err = coordinator.Acquire(t.Context(), sessionleasetest.Key())
	elapsed := time.Since(started)

	require.ErrorIs(t, err, sessionlease.ErrUnavailable,
		"a backend that never answers fails closed; the caller must not run")
	require.NotErrorIs(t, err, sessionlease.ErrSessionBusy,
		"a backend that will not answer is not a busy session")
	require.NotErrorIs(t, err, context.DeadlineExceeded,
		"the caller's own context is fine, so it must not be handed back a "+
			"deadline it never set")
	require.Less(t, elapsed, 10*budget,
		"an acquisition has to give up on the budget its own lease timings imply "+
			"(%s), not on the minute this client would have waited", budget)
}

func TestCloseIsStillBoundedWhenTheClientIgnoresContexts(t *testing.T) {
	t.Parallel()

	cfg := sessionleasetest.Timings()
	// The default client: it ignores contexts, so what bounds a command already
	// sent is its own read timeout and retry count rather than anything this
	// package can set. Close waits for that command — that part is pinned
	// deterministically in the parent package, where the wait can be held open
	// on purpose — and what is asserted here is the other half: waiting for it
	// still terminates.
	client := newSilentClient(t, goredis.Options{ReadTimeout: silentReadTimeout})
	coordinator, err := redislease.New(client, redislease.Options{Lease: cfg})
	require.NoError(t, err)

	running := make(chan struct{})
	acquired := make(chan error, 1)
	go func() {
		close(running)
		_, err := coordinator.Acquire(context.Background(), sessionleasetest.Key())
		acquired <- err
	}()
	<-running
	// Long enough that the command is on the wire rather than still queued for
	// a connection, which is the case cancelling cannot get Close out of.
	time.Sleep(2 * silentReadTimeout)

	started := time.Now()
	require.NoError(t, coordinator.Close())
	require.Less(t, time.Since(started), time.Minute,
		"a shutdown that waits for a command against a backend which never "+
			"answers still has to end")

	select {
	case err := <-acquired:
		require.Error(t, err, "an acquisition nothing answered cannot have succeeded")
	case <-time.After(time.Minute):
		t.Fatal("the acquisition was still running long after Close returned")
	}
}

// newClosedClient returns a client that refuses every command without dialing,
// which is how this package's failure paths are exercised offline.
func newClosedClient(t *testing.T) goredis.UniversalClient {
	t.Helper()
	client := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})
	require.NoError(t, client.Close())
	return client
}

// newSilentClient returns a client, configured as opts asks, pointed at a
// listener that accepts connections and never answers a command.
func newSilentClient(t *testing.T, opts goredis.Options) goredis.UniversalClient {
	t.Helper()
	opts.Addr = newSilentServer(t)
	client := goredis.NewClient(&opts)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func newSilentServer(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var mu sync.Mutex
	var accepted []net.Conn
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			// Held open and never read from or written to.
			mu.Lock()
			accepted = append(accepted, conn)
			mu.Unlock()
		}
	}()

	t.Cleanup(func() {
		require.NoError(t, listener.Close())
		<-stopped
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range accepted {
			_ = conn.Close()
		}
	})
	return listener.Addr().String()
}
