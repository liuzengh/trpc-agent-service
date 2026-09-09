package postgres_test

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir/sessiondirtest"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/stretchr/testify/require"
)

// These tests need no database. Everything that does is in integration_test.go
// behind an environment gate.

func TestNewRejectsNilPool(t *testing.T) {
	directory, err := postgres.New(nil)
	require.ErrorIs(t, err, postgres.ErrInvalidConfig)
	require.Nil(t, directory)
}

func TestMigrateRejectsNilPool(t *testing.T) {
	err := postgres.Migrate(context.Background(), nil)
	require.ErrorIs(t, err, postgres.ErrInvalidConfig)
}

// TestMigrateHonorsCanceledContext pins that the context is checked before
// anything else, so a caller that has already given up never opens a
// transaction it will only roll back.
func TestMigrateHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, postgres.Migrate(ctx, nil), context.Canceled)
}

// TestUninitialisedDirectoryIsRefused covers the value New never returns. A
// nil *Directory satisfies the interface, so a wiring mistake reaches these
// methods as an ordinary call; refusing it beats dereferencing a nil pool in
// the middle of a chat request.
func TestUninitialisedDirectoryIsRefused(t *testing.T) {
	var directory *postgres.Directory
	require.Implements(t, (*sessiondir.Directory)(nil), directory)

	_, _, err := directory.GetPin(context.Background(), sessiondirtest.Key())
	require.ErrorIs(t, err, postgres.ErrInvalidConfig)
	_, err = directory.EnsurePin(context.Background(), sessiondirtest.Key(), "revision-1")
	require.ErrorIs(t, err, postgres.ErrInvalidConfig)
}

// TestStorageErrorIsNotADomainError pins the separation the chat API depends
// on. A session whose pin could not be read must not fall through to "this
// session has no pin", which is what an infrastructure failure matching a
// tenant sentinel would look like to a caller: the next run would adopt
// whatever revision is current instead of the one the conversation started on.
func TestStorageErrorIsNotADomainError(t *testing.T) {
	for _, sentinel := range []error{
		tenant.ErrInvalidArgument,
		tenant.ErrTenantScope,
		tenant.ErrNotFound,
		tenant.ErrAlreadyExists,
		tenant.ErrTenantInactive,
		tenant.ErrNoPublishedRevision,
		tenant.ErrRevisionNotPublished,
		tenant.ErrConfigIntegrity,
	} {
		require.NotErrorIs(t, postgres.ErrStorage, sentinel)
		require.NotErrorIs(t, postgres.ErrInvalidConfig, sentinel)
	}
}
