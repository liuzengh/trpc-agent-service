package sessionlease_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionlease"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionlease/sessionleasetest"
)

func TestMemoryCoordinatorSatisfiesTheContract(t *testing.T) {
	t.Parallel()

	sessionleasetest.RunCoordinatorSuite(t, func(t *testing.T) sessionleasetest.Backend {
		// One store per subtest is what "isolated backend" means here; the
		// coordinators over it are the Workers.
		store := sessionlease.NewMemoryStore()
		return sessionleasetest.Backend{
			New: func(t *testing.T, cfg sessionlease.Config) sessionlease.Coordinator {
				coordinator, err := sessionlease.NewMemoryCoordinator(store, cfg)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, coordinator.Close()) })
				return coordinator
			},
			// A map cannot become unreachable, so the suite's
			// unreachable-backend subtests are skipped rather than faked.
			Break: nil,
		}
	})
}

func TestNewMemoryCoordinatorRejectsUnusableConfiguration(t *testing.T) {
	t.Parallel()

	t.Run("store", func(t *testing.T) {
		t.Parallel()
		_, err := sessionlease.NewMemoryCoordinator(nil, sessionlease.Config{})
		require.ErrorIs(t, err, sessionlease.ErrInvalidConfig)
	})

	t.Run("timings", func(t *testing.T) {
		t.Parallel()
		_, err := sessionlease.NewMemoryCoordinator(sessionlease.NewMemoryStore(), sessionlease.Config{
			TTL:           time.Second,
			RenewInterval: 2 * time.Second,
		})
		require.ErrorIs(t, err, sessionlease.ErrInvalidConfig)
	})
}

func TestConfigDefaultsLeaveRoomForARetry(t *testing.T) {
	t.Parallel()

	defaults := sessionlease.Config{}.WithDefaults()
	require.Equal(t, sessionlease.DefaultTTL, defaults.TTL)
	require.Equal(t, sessionlease.DefaultRenewInterval, defaults.RenewInterval)
	require.Equal(t, sessionlease.DefaultSafetyMargin, defaults.SafetyMargin)
	require.NoError(t, sessionlease.Config{}.Validate())
}

func TestConfigValidateRejectsTimingsWithoutASafetyBudget(t *testing.T) {
	t.Parallel()

	for name, cfg := range map[string]sessionlease.Config{
		"renew not shorter than ttl": {
			TTL: time.Second, RenewInterval: time.Second, SafetyMargin: 100 * time.Millisecond,
		},
		"margin not shorter than ttl": {
			TTL: time.Second, RenewInterval: 100 * time.Millisecond, SafetyMargin: time.Second,
		},
		"no room for one retry": {
			TTL: time.Second, RenewInterval: 600 * time.Millisecond, SafetyMargin: 500 * time.Millisecond,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.ErrorIs(t, cfg.Validate(), sessionlease.ErrInvalidConfig)
		})
	}
}

func TestKeyDigestIsStableAndOpaque(t *testing.T) {
	t.Parallel()

	key := sessionleasetest.Key()
	digest := sessionlease.KeyDigest(key)

	require.Regexp(t, `^[0-9a-f]{64}$`, digest,
		"a lease scope is named by a fixed-width hex digest, so no tenant, "+
			"principal or session identifier can survive into a keyspace an "+
			"operator, a scan or a slow-log line can read")
	require.Equal(t, digest, sessionlease.KeyDigest(key), "the digest of a key does not change")
}

func TestKeyDigestSeparatesEveryField(t *testing.T) {
	t.Parallel()

	base := sessionlease.KeyDigest(sessionleasetest.Key())
	for field, key := range sessionleasetest.MutateKey() {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			require.NotEqual(t, base, sessionlease.KeyDigest(key),
				"a lease is scoped to the whole key, so %s has to change the digest", field)
		})
	}
}

func TestKeyDigestCannotBeForgedByMovingAFieldBoundary(t *testing.T) {
	t.Parallel()

	// "tenant-ab" + "c" and "tenant-a" + "bc" would collide under naive
	// concatenation. Length prefixes are what stop one tenant from naming a
	// session that lands on another tenant's lock.
	left := sessiondir.Key{TenantID: "tenant-ab", AppID: "c", PrincipalID: "p", SessionID: "s", Epoch: 1}
	right := sessiondir.Key{TenantID: "tenant-a", AppID: "bc", PrincipalID: "p", SessionID: "s", Epoch: 1}
	require.NotEqual(t, sessionlease.KeyDigest(left), sessionlease.KeyDigest(right))
}

// scriptedHolder is a backend that answers however a test tells it to, so the
// shared renewal loop can be driven through outcomes no real backend produces
// on demand.
type scriptedHolder struct {
	mu       sync.Mutex
	renewals int
	releases int
	answer   func(attempt int) (bool, error)
}

func (h *scriptedHolder) Renew(context.Context) (bool, error) {
	h.mu.Lock()
	h.renewals++
	attempt := h.renewals
	h.mu.Unlock()
	return h.answer(attempt)
}

func (h *scriptedHolder) Release(context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.releases++
	return nil
}

func (h *scriptedHolder) counts() (renewals, releases int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.renewals, h.releases
}

func fastTimings() sessionlease.Config {
	return sessionlease.Config{
		TTL:           600 * time.Millisecond,
		RenewInterval: 60 * time.Millisecond,
		SafetyMargin:  200 * time.Millisecond,
	}
}

// newLease starts a lease the way a backend does, with the acquisition stamped
// at the moment the lock was taken.
func newLease(
	t *testing.T,
	cfg sessionlease.Config,
	fence uint64,
	holder sessionlease.Holder,
) sessionlease.Lease {
	t.Helper()
	return sessionlease.NewLease(sessionlease.LeaseParams{
		RunCtx:     t.Context(),
		CoordCtx:   context.Background(),
		Config:     cfg,
		Fence:      fence,
		Holder:     holder,
		AcquiredAt: time.Now(),
	})
}

func TestLeaseRetriesAnUnconfirmedRenewalAndThenGivesUp(t *testing.T) {
	t.Parallel()

	boom := errors.New("backend did not answer")
	holder := &scriptedHolder{answer: func(int) (bool, error) { return false, boom }}
	cfg := fastTimings()

	started := time.Now()
	lease := newLease(t, cfg, 7, holder)

	select {
	case <-lease.Done():
	case <-time.After(cfg.TTL + time.Second):
		t.Fatal("a holder that never confirms a renewal has to give up")
	}
	elapsed := time.Since(started)

	// The holder must stop claiming the lease before the lock could expire, or
	// two Workers would believe they hold it at the same time.
	require.Less(t, elapsed, cfg.TTL, "give up before the lock expires, not after")

	renewals, releases := holder.counts()
	require.Greater(t, renewals, 1,
		"an unconfirmed renewal is retried inside the safety budget, not given up on")
	require.Zero(t, releases, "a lost lease is abandoned to its TTL, never deleted")

	require.ErrorIs(t, lease.Release(t.Context()), sessionlease.ErrLeaseLost)
	_, releases = holder.counts()
	require.Zero(t, releases, "releasing a lost lease must not delete another owner's lock")
}

func TestLeaseTreatsADefinitiveRefusalAsLoss(t *testing.T) {
	t.Parallel()

	holder := &scriptedHolder{answer: func(int) (bool, error) { return false, nil }}
	lease := newLease(t, fastTimings(), 1, holder)

	select {
	case <-lease.Done():
	case <-time.After(time.Second):
		t.Fatal("a holder told it no longer owns the lock has to stop claiming it")
	}

	renewals, _ := holder.counts()
	require.Equal(t, 1, renewals, "a definitive refusal is not retried")
}

func TestLeaseStopsRenewingOnceReleased(t *testing.T) {
	t.Parallel()

	holder := &scriptedHolder{answer: func(int) (bool, error) { return true, nil }}
	cfg := fastTimings()
	lease := newLease(t, cfg, 1, holder)

	time.Sleep(2 * cfg.RenewInterval)
	require.NoError(t, lease.Release(t.Context()))

	renewals, releases := holder.counts()
	require.Equal(t, 1, releases)
	time.Sleep(4 * cfg.RenewInterval)
	after, _ := holder.counts()
	require.Equal(t, renewals, after, "a released lease is not renewed any more")
}

func TestLeaseStopsRenewingWhenTheRunEnds(t *testing.T) {
	t.Parallel()

	holder := &scriptedHolder{answer: func(int) (bool, error) { return true, nil }}
	cfg := fastTimings()
	runCtx, endRun := context.WithCancel(context.Background())
	lease := sessionlease.NewLease(sessionlease.LeaseParams{
		RunCtx: runCtx, CoordCtx: context.Background(),
		Config: cfg, Fence: 1, Holder: holder, AcquiredAt: time.Now(),
	})

	endRun()
	select {
	case <-lease.Done():
	case <-time.After(time.Second):
		t.Fatal("the end of the run is the end of the lease")
	}

	renewals, releases := holder.counts()
	time.Sleep(4 * cfg.RenewInterval)
	after, _ := holder.counts()
	require.Equal(t, renewals, after,
		"renewing a lock whose run is over pushes its expiry out by another TTL "+
			"and holds the session against the next Worker for nothing")
	require.Zero(t, releases,
		"an ended run abandons its lock to the TTL, which is what covers the "+
			"tail writes of the Runner it just cancelled")
}

func TestLeaseStopsRenewingWhenTheCoordinatorCloses(t *testing.T) {
	t.Parallel()

	holder := &scriptedHolder{answer: func(int) (bool, error) { return true, nil }}
	cfg := fastTimings()
	coordCtx, closeCoordinator := context.WithCancel(context.Background())
	lease := sessionlease.NewLease(sessionlease.LeaseParams{
		RunCtx: t.Context(), CoordCtx: coordCtx,
		Config: cfg, Fence: 1, Holder: holder, AcquiredAt: time.Now(),
	})

	closeCoordinator()
	select {
	case <-lease.Done():
	case <-time.After(time.Second):
		t.Fatal("closing the coordinator ends the leases it handed out")
	}

	_, releases := holder.counts()
	require.Zero(t, releases,
		"shutdown is exactly when the TTL has to cover a cancelled run's tail writes")
}

func TestLeaseFenceIsWhateverTheBackendAssigned(t *testing.T) {
	t.Parallel()

	holder := &scriptedHolder{answer: func(int) (bool, error) { return true, nil }}
	lease := newLease(t, fastTimings(), 42, holder)
	require.Equal(t, uint64(42), lease.Fence())
	require.NoError(t, lease.Release(t.Context()))
}
