package redis

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionlease"
)

// hashTag captures what Redis Cluster uses to pick a slot.
var hashTag = regexp.MustCompile(`\{([^{}]+)\}`)

func TestKeysOfOneLeaseShareASlot(t *testing.T) {
	t.Parallel()

	coordinator := &Coordinator{prefix: DefaultKeyPrefix}
	digest := "0123456789abcdef"

	lock := coordinator.lockKey(digest)
	fence := coordinator.fenceKey(digest)

	require.Equal(t, DefaultKeyPrefix+":{0123456789abcdef}:lock", lock)
	require.Equal(t, DefaultKeyPrefix+":{0123456789abcdef}:fence", fence)

	lockTag := hashTag.FindStringSubmatch(lock)
	fenceTag := hashTag.FindStringSubmatch(fence)
	require.Len(t, lockTag, 2)
	require.Len(t, fenceTag, 2)
	require.Equal(t, lockTag[1], fenceTag[1],
		"the acquire script touches both keys in one call, which a Redis Cluster "+
			"only allows inside a single slot")
	require.Equal(t, digest, lockTag[1], "the hash tag is the scope digest, not a fixed string")
}

func TestKeysOfDifferentScopesDoNotShareASlot(t *testing.T) {
	t.Parallel()

	coordinator := &Coordinator{prefix: DefaultKeyPrefix}
	require.NotEqual(t, coordinator.lockKey("aaaa"), coordinator.lockKey("bbbb"))
}

func TestParseAcquireReadsTheScriptReply(t *testing.T) {
	t.Parallel()

	t.Run("taken", func(t *testing.T) {
		t.Parallel()
		taken, fence, err := parseAcquire([]any{int64(1), int64(7)})
		require.NoError(t, err)
		require.True(t, taken)
		require.Equal(t, uint64(7), fence)
	})

	t.Run("busy", func(t *testing.T) {
		t.Parallel()
		taken, fence, err := parseAcquire([]any{int64(0), int64(0)})
		require.NoError(t, err)
		require.False(t, taken)
		require.Zero(t, fence)
	})
}

func TestParseAcquireFailsClosedOnAnythingElse(t *testing.T) {
	t.Parallel()

	// A reply this build does not recognise may come from a Redis running a
	// different version of the script. Guessing which of "taken" or "busy" it
	// meant is exactly the mistake that would let two Workers run at once, so
	// every one of these has to be an error and none may be ErrSessionBusy.
	for name, reply := range map[string]any{
		"nil":            nil,
		"string":         "OK",
		"integer":        int64(1),
		"short list":     []any{int64(1)},
		"long list":      []any{int64(1), int64(2), int64(3)},
		"string taken":   []any{"1", int64(2)},
		"string fence":   []any{int64(1), "2"},
		"unknown taken":  []any{int64(2), int64(1)},
		"negative taken": []any{int64(-1), int64(1)},
		"zero fence":     []any{int64(1), int64(0)},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			taken, _, err := parseAcquire(reply)
			require.ErrorIs(t, err, sessionlease.ErrUnavailable)
			require.NotErrorIs(t, err, sessionlease.ErrSessionBusy)
			require.False(t, taken)
		})
	}
}

func TestClassifyKeepsContextErrorsIntact(t *testing.T) {
	t.Parallel()

	for name, err := range map[string]error{
		"canceled": context.Canceled,
		"deadline": context.DeadlineExceeded,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := classify("acquire", err)
			require.ErrorIs(t, got, err,
				"a caller that went away must not be reported as a coordination outage")
			require.NotErrorIs(t, got, sessionlease.ErrUnavailable)
		})
	}
}

func TestClassifyReportsEverythingElseAsUnavailable(t *testing.T) {
	t.Parallel()

	cause := errors.New("connection refused")
	got := classify("renew", cause)
	require.ErrorIs(t, got, sessionlease.ErrUnavailable)
	require.ErrorIs(t, got, cause, "the cause stays in the chain for diagnosis")
	require.NotErrorIs(t, got, sessionlease.ErrSessionBusy)
}

func TestClassifyPassesNilThrough(t *testing.T) {
	t.Parallel()
	require.NoError(t, classify("acquire", nil))
}
