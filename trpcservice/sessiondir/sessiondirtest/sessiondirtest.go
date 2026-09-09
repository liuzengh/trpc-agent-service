// Package sessiondirtest holds the behaviour contract that every
// sessiondir.Directory implementation has to satisfy.
//
// The suite lives in its own package rather than in a _test.go file beside
// MemoryDirectory because a second implementation, in a second package, has to
// run exactly the same assertions. A conformance suite that is copied instead
// of shared stops being a contract the moment one copy is fixed.
//
// RunDirectorySuite takes a factory rather than a directory because an
// implementation backed by a real database needs a fresh, isolated store for
// each subtest. The factory is called once per subtest and is handed that
// subtest's *testing.T, so an implementation can register its own cleanup.
//
// The suite asserts only behaviour the Directory interface promises. It never
// reaches behind the interface, so it says nothing about how a store is
// namespaced, migrated or connected, and nothing about MemoryDirectory.Size.
// Those belong to each implementation's own tests.
package sessiondirtest

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/stretchr/testify/require"
)

// suiteTimeout bounds every subtest. An in-memory directory never reaches it
// and a reachable database answers in milliseconds, so this only stops an
// unreachable one from hanging until the package timeout.
const suiteTimeout = 30 * time.Second

// Callers is how many concurrent first runs the contract is proven against.
// Exported because an implementation's own tests contend the same way against
// several connections, and the two numbers should not drift.
const Callers = 32

// NewDirectory builds the directory a single subtest runs against. It must
// return a store that is empty and isolated from every other subtest: the
// suite reuses fixed ids such as "tenant-a" across subtests, so a shared store
// would collide.
type NewDirectory func(t *testing.T) sessiondir.Directory

// RunDirectorySuite runs the whole contract against newDirectory.
func RunDirectorySuite(t *testing.T, newDirectory NewDirectory) {
	t.Helper()

	t.Run("PinsFirstCandidate", func(t *testing.T) {
		assertPinsFirstCandidate(t, newDirectory(t))
	})
	t.Run("EnsurePinAgreesOnOneWinner", func(t *testing.T) {
		assertEnsurePinAgreesOnOneWinner(t, newDirectory(t))
	})
	t.Run("IsolatesKeyFields", func(t *testing.T) {
		assertIsolatesKeyFields(t, newDirectory(t))
	})
	t.Run("StoresEveryEpochValue", func(t *testing.T) {
		assertStoresEveryEpochValue(t, newDirectory(t))
	})
	t.Run("RejectsInvalidInput", func(t *testing.T) {
		assertRejectsInvalidInput(t, newDirectory(t))
	})
	t.Run("RejectsUnusableContext", func(t *testing.T) {
		assertRejectsUnusableContext(t, newDirectory(t))
	})
}

// Context returns the context a subtest and its fixtures should use. It is
// derived from t.Context, so it is cancelled when the subtest ends.
//
// A cleanup that has to undo a write must not use this context: t.Context is
// cancelled before cleanups run, so an implementation's teardown needs a fresh
// context of its own.
func Context(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), suiteTimeout)
	t.Cleanup(cancel)
	return ctx
}

// Key is the conversation every assertion here pins, and the fixture an
// implementation's own tests should start from.
func Key() sessiondir.Key {
	return sessiondir.Key{
		TenantID:    "tenant-a",
		AppID:       "assistant",
		PrincipalID: "principal-1",
		SessionID:   "conversation-1",
	}
}

// Candidate names the revision caller i arrives with. Every concurrent caller
// carries a different one, which is what makes a disagreement visible: if the
// callers all offered the same revision, an implementation that let each of
// them keep its own candidate would be indistinguishable from one that agreed.
func Candidate(caller int) string {
	return fmt.Sprintf("revision-%d", caller)
}

// MutateKey holds the field mutations that each have to address a different
// conversation. Exported so an implementation can prove the same separation at
// its own storage layer, against the same list.
func MutateKey() map[string]func(*sessiondir.Key) {
	return map[string]func(*sessiondir.Key){
		"tenant":    func(k *sessiondir.Key) { k.TenantID = "tenant-b" },
		"app":       func(k *sessiondir.Key) { k.AppID = "reporter" },
		"principal": func(k *sessiondir.Key) { k.PrincipalID = "principal-2" },
		"session":   func(k *sessiondir.Key) { k.SessionID = "conversation-2" },
		"epoch":     func(k *sessiondir.Key) { k.Epoch = 1 },
	}
}

// EnsurePinCall is one concurrent first run. Directory is per call so a test
// can spread the callers over several instances, which is what proves the
// agreement comes from the store rather than from a lock inside one object.
type EnsurePinCall struct {
	Directory           sessiondir.Directory
	Key                 sessiondir.Key
	CandidateRevisionID string
}

// EnsurePinResult is what one call returned.
type EnsurePinResult struct {
	Winner string
	Err    error
}

// EnsurePinConcurrently releases every call at the same moment and waits for
// all of them. The barrier matters: without it the goroutines start far enough
// apart that the later ones usually find the pin already committed, which is
// the easy path rather than the contended one.
//
// It asserts nothing, so a caller can use it both to require agreement and to
// prove that a deliberately non-atomic implementation fails to agree.
func EnsurePinConcurrently(
	t *testing.T,
	ctx context.Context,
	calls []EnsurePinCall,
) []EnsurePinResult {
	t.Helper()
	results := make([]EnsurePinResult, len(calls))
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(len(calls))
	done.Add(len(calls))
	for i, call := range calls {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			winner, err := call.Directory.EnsurePin(ctx, call.Key, call.CandidateRevisionID)
			results[i] = EnsurePinResult{Winner: winner, Err: err}
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()
	return results
}

// ContendOneKey builds Callers first runs of one conversation, each carrying
// its own candidate, spread round-robin over directories.
func ContendOneKey(key sessiondir.Key, directories ...sessiondir.Directory) []EnsurePinCall {
	calls := make([]EnsurePinCall, 0, Callers)
	for caller := 0; caller < Callers; caller++ {
		calls = append(calls, EnsurePinCall{
			Directory:           directories[caller%len(directories)],
			Key:                 key,
			CandidateRevisionID: Candidate(caller),
		})
	}
	return calls
}

// AgreementError is the property the contended case exists for, as a value:
// every caller succeeded, they were all told the same revision, that revision
// is one of the candidates actually offered, and it is what the store reports
// afterwards. It returns the agreed pin and a nil error when all of that holds.
//
// It is a predicate rather than a set of assertions so that a test can also
// require it to fail. Proving that an implementation without an atomic first
// write would be rejected needs to run this same check against that
// implementation and see it complain; a copy of the check written for that
// purpose would prove only that the copy still works.
func AgreementError(
	ctx context.Context,
	directory sessiondir.Directory,
	key sessiondir.Key,
	results []EnsurePinResult,
) (string, error) {
	for i, result := range results {
		if result.Err != nil {
			return "", fmt.Errorf("caller %d failed: %w", i, result.Err)
		}
	}
	pinned, found, err := directory.GetPin(ctx, key)
	if err != nil {
		return "", fmt.Errorf("read back the pin: %w", err)
	}
	if !found {
		return "", fmt.Errorf("a contended first run left the session unpinned")
	}

	offered := false
	for caller := range results {
		if Candidate(caller) == pinned {
			offered = true
			break
		}
	}
	if !offered {
		return "", fmt.Errorf("the pin %q is not one of the candidates that was offered", pinned)
	}
	for i, result := range results {
		if result.Winner != pinned {
			return "", fmt.Errorf(
				"caller %d was told %q while the session is pinned to %q",
				i, result.Winner, pinned)
		}
	}
	return pinned, nil
}

// RequireOneWinner is AgreementError asserted.
func RequireOneWinner(
	t *testing.T,
	ctx context.Context,
	directory sessiondir.Directory,
	key sessiondir.Key,
	results []EnsurePinResult,
) string {
	t.Helper()
	pinned, err := AgreementError(ctx, directory, key, results)
	require.NoError(t, err, "concurrent first runs of one session must agree on one winner")
	return pinned
}

func assertPinsFirstCandidate(t *testing.T, directory sessiondir.Directory) {
	t.Helper()
	ctx := Context(t)
	key := Key()

	pinned, found, err := directory.GetPin(ctx, key)
	require.NoError(t, err)
	require.False(t, found)
	require.Empty(t, pinned)

	winner, err := directory.EnsurePin(ctx, key, "revision-1")
	require.NoError(t, err)
	require.Equal(t, "revision-1", winner)

	// A later run of the same session keeps the original revision even when it
	// arrives with a newer candidate.
	winner, err = directory.EnsurePin(ctx, key, "revision-2")
	require.NoError(t, err)
	require.Equal(t, "revision-1", winner)

	pinned, found, err = directory.GetPin(ctx, key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "revision-1", pinned)
}

// Concurrent first runs each carry their own candidate. Exactly one of them may
// become the pin, and every caller has to be told which one.
func assertEnsurePinAgreesOnOneWinner(t *testing.T, directory sessiondir.Directory) {
	t.Helper()
	ctx := Context(t)
	key := Key()

	results := EnsurePinConcurrently(t, ctx, ContendOneKey(key, directory))
	winner := RequireOneWinner(t, ctx, directory, key, results)

	// The pin still holds after the contention is over: a caller arriving late
	// with yet another candidate is told the same winner.
	late, err := directory.EnsurePin(ctx, key, "revision-late")
	require.NoError(t, err)
	require.Equal(t, winner, late)
}

// The same session id under a different tenant, app, principal or epoch is a
// different conversation and must not inherit a pin.
func assertIsolatesKeyFields(t *testing.T, directory sessiondir.Directory) {
	t.Helper()
	ctx := Context(t)
	base := Key()
	winner, err := directory.EnsurePin(ctx, base, "revision-1")
	require.NoError(t, err)
	require.Equal(t, "revision-1", winner)

	for name, mutate := range MutateKey() {
		t.Run(name, func(t *testing.T) {
			other := base
			mutate(&other)
			pinned, found, pinErr := directory.GetPin(ctx, other)
			require.NoError(t, pinErr)
			require.False(t, found, "a different %s must not inherit the pin", name)
			require.Empty(t, pinned)

			pinned, pinErr = directory.EnsurePin(ctx, other, "revision-2")
			require.NoError(t, pinErr)
			require.Equal(t, "revision-2", pinned)
		})
	}

	pinned, found, err := directory.GetPin(ctx, base)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "revision-1", pinned,
		"pinning the neighbouring conversations must not have moved the original")
}

// Epoch is a uint32, so the whole range has to survive a round trip. A store
// that narrowed it to a signed 32-bit column would accept the small values
// every other test uses and fail only here.
func assertStoresEveryEpochValue(t *testing.T, directory sessiondir.Directory) {
	t.Helper()
	ctx := Context(t)

	epochs := []uint32{0, 1, math.MaxInt32, math.MaxInt32 + 1, math.MaxUint32 - 1, math.MaxUint32}
	for i, epoch := range epochs {
		key := Key()
		key.Epoch = epoch
		candidate := Candidate(i)
		winner, err := directory.EnsurePin(ctx, key, candidate)
		require.NoErrorf(t, err, "epoch %d could not be pinned", epoch)
		require.Equal(t, candidate, winner)
	}
	// Read back afterwards rather than inside the loop, so a store that mapped
	// two epochs onto one row is caught by the second one overwriting the first
	// rather than hidden by reading each row before the collision happens.
	for i, epoch := range epochs {
		key := Key()
		key.Epoch = epoch
		pinned, found, err := directory.GetPin(ctx, key)
		require.NoErrorf(t, err, "epoch %d could not be read back", epoch)
		require.Truef(t, found, "epoch %d lost its pin", epoch)
		require.Equalf(t, Candidate(i), pinned, "epoch %d came back with the wrong pin", epoch)
	}
}

func assertRejectsInvalidInput(t *testing.T, directory sessiondir.Directory) {
	t.Helper()
	ctx := Context(t)

	for name, mutate := range map[string]func(*sessiondir.Key){
		"tenant":    func(k *sessiondir.Key) { k.TenantID = "" },
		"app":       func(k *sessiondir.Key) { k.AppID = "" },
		"principal": func(k *sessiondir.Key) { k.PrincipalID = "not a principal" },
		"session":   func(k *sessiondir.Key) { k.SessionID = "conversation/1" },
	} {
		t.Run(name, func(t *testing.T) {
			key := Key()
			mutate(&key)
			_, _, err := directory.GetPin(ctx, key)
			require.ErrorIs(t, err, tenant.ErrInvalidArgument)
			_, err = directory.EnsurePin(ctx, key, "revision-1")
			require.ErrorIs(t, err, tenant.ErrInvalidArgument)
		})
	}

	_, err := directory.EnsurePin(ctx, Key(), "")
	require.ErrorIs(t, err, tenant.ErrInvalidArgument)
	_, err = directory.EnsurePin(ctx, Key(), "revision 1")
	require.ErrorIs(t, err, tenant.ErrInvalidArgument)

	// A rejected call must not have written anything.
	_, found, err := directory.GetPin(ctx, Key())
	require.NoError(t, err)
	require.False(t, found)
}

func assertRejectsUnusableContext(t *testing.T, directory sessiondir.Directory) {
	t.Helper()

	var missingContext context.Context
	_, _, err := directory.GetPin(missingContext, Key())
	require.ErrorIs(t, err, tenant.ErrInvalidArgument)
	_, err = directory.EnsurePin(missingContext, Key(), "revision-1")
	require.ErrorIs(t, err, tenant.ErrInvalidArgument)

	cancelled, cancel := context.WithCancel(Context(t))
	cancel()
	_, _, err = directory.GetPin(cancelled, Key())
	require.ErrorIs(t, err, context.Canceled)
	_, err = directory.EnsurePin(cancelled, Key(), "revision-1")
	require.ErrorIs(t, err, context.Canceled)

	// A caller that had already given up must not have pinned anything.
	_, found, err := directory.GetPin(Context(t), Key())
	require.NoError(t, err)
	require.False(t, found)
}
