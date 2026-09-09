package sessionlease

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
)

// These assertions are below the Coordinator interface on purpose. A managed
// lease refuses to touch a backend once it knows it lost the lock, so the
// owner-matching the store itself has to do — the last line of defence when a
// holder does not know yet — is only reachable from inside the package. The
// Redis backend asserts the same properties against its Lua scripts.

func storeKey() sessiondir.Key {
	return sessiondir.Key{
		TenantID:    "tenant-a",
		AppID:       "assistant",
		PrincipalID: "principal-a",
		SessionID:   "session-a",
		Epoch:       1,
	}
}

func TestMemoryStoreExcludesASecondOwner(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	fence, ok := store.acquire(storeKey(), "owner-a", time.Minute)
	require.True(t, ok)
	require.Equal(t, uint64(1), fence)

	_, ok = store.acquire(storeKey(), "owner-b", time.Minute)
	require.False(t, ok)
}

func TestMemoryStoreReleaseOnlyRemovesItsOwnLock(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	_, ok := store.acquire(storeKey(), "owner-a", time.Nanosecond)
	require.True(t, ok)
	time.Sleep(time.Millisecond)

	_, ok = store.acquire(storeKey(), "owner-b", time.Minute)
	require.True(t, ok, "an expired lock can be taken over")

	store.release(storeKey(), "owner-a")

	_, ok = store.acquire(storeKey(), "owner-c", time.Minute)
	require.False(t, ok, "a stale release must not hand the lock to a third party")
	require.True(t, store.renew(storeKey(), "owner-b", time.Minute),
		"the new owner still holds the lock after the stale release")
}

func TestMemoryStoreRenewOnlyExtendsItsOwnLock(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	_, ok := store.acquire(storeKey(), "owner-a", time.Minute)
	require.True(t, ok)

	require.False(t, store.renew(storeKey(), "owner-b", time.Minute),
		"a renewal from a holder that does not own the lock is refused")
	require.True(t, store.renew(storeKey(), "owner-a", time.Minute))
}

func TestMemoryStoreRenewRefusesAnExpiredLock(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	_, ok := store.acquire(storeKey(), "owner-a", time.Nanosecond)
	require.True(t, ok)
	time.Sleep(time.Millisecond)

	require.False(t, store.renew(storeKey(), "owner-a", time.Minute),
		"an expired lock cannot be renewed back to life: another Worker may "+
			"already have taken it")
}

func TestMemoryStoreFenceOutlivesEveryLock(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	key := storeKey()

	first, ok := store.acquire(key, "owner-a", time.Minute)
	require.True(t, ok)
	store.release(key, "owner-a")
	second, ok := store.acquire(key, "owner-b", time.Minute)
	require.True(t, ok)

	require.Greater(t, second, first, "the fence advances on every acquisition")
	require.Len(t, store.fences, 1)
	require.Len(t, store.locks, 1)

	store.release(key, "owner-b")
	require.Empty(t, store.locks, "a released lock is gone")
	require.Len(t, store.fences, 1,
		"the fence counter is kept so it stays monotonic across acquisitions; "+
			"this is the same unbounded growth the Redis backend documents")
}

func TestMemoryStoreSeparatesKeys(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	other := storeKey()
	other.Epoch = 2

	_, ok := store.acquire(storeKey(), "owner-a", time.Minute)
	require.True(t, ok)
	_, ok = store.acquire(other, "owner-b", time.Minute)
	require.True(t, ok, "a different epoch is a different lease scope")
}
