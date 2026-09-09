package sessiondir

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/stretchr/testify/require"
)

// The Directory contract itself lives in sessiondirtest and runs against this
// implementation from conformance_test.go. What is left here is what only
// MemoryDirectory promises: Size, and the behaviour of a value that was never
// built by NewMemoryDirectory.

func testKey() Key {
	return Key{
		TenantID:    "tenant-a",
		AppID:       "assistant",
		PrincipalID: "principal-1",
		SessionID:   "conversation-1",
	}
}

// Size is what makes the unbounded-growth risk in the package documentation
// observable: one credential can add a row per session id it invents.
func TestMemoryDirectorySizeCountsPinnedSessions(t *testing.T) {
	directory := NewMemoryDirectory()
	ctx := context.Background()
	require.Zero(t, directory.Size())

	base := testKey()
	_, err := directory.EnsurePin(ctx, base, "revision-1")
	require.NoError(t, err)
	require.Equal(t, 1, directory.Size())

	// Re-pinning an existing session stores nothing new.
	_, err = directory.EnsurePin(ctx, base, "revision-2")
	require.NoError(t, err)
	require.Equal(t, 1, directory.Size())

	// A neighbouring conversation is a second entry.
	other := base
	other.SessionID = "conversation-2"
	_, err = directory.EnsurePin(ctx, other, "revision-2")
	require.NoError(t, err)
	require.Equal(t, 2, directory.Size())

	// A rejected call must not grow the map.
	invalid := base
	invalid.SessionID = "conversation/3"
	_, err = directory.EnsurePin(ctx, invalid, "revision-3")
	require.ErrorIs(t, err, tenant.ErrInvalidArgument)
	_, err = directory.EnsurePin(ctx, base, "revision 3")
	require.ErrorIs(t, err, tenant.ErrInvalidArgument)
	require.Equal(t, 2, directory.Size())
}

// A MemoryDirectory that never went through NewMemoryDirectory has a nil map,
// so every entry point has to refuse rather than panic on a nil dereference.
func TestMemoryDirectoryRejectsUninitialisedReceiver(t *testing.T) {
	for name, directory := range map[string]*MemoryDirectory{
		"nil":        nil,
		"zero value": {},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := directory.GetPin(context.Background(), testKey())
			require.ErrorIs(t, err, tenant.ErrInvalidArgument)
			_, err = directory.EnsurePin(context.Background(), testKey(), "revision-1")
			require.ErrorIs(t, err, tenant.ErrInvalidArgument)
			require.Zero(t, directory.Size())
		})
	}
}
