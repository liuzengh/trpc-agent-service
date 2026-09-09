package postgres_test

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant/postgres"
	"github.com/stretchr/testify/require"
)

// These tests need no database. Everything that does is in integration_test.go
// behind an environment gate.

func TestNewRejectsNilPool(t *testing.T) {
	repository, err := postgres.New(nil)
	require.ErrorIs(t, err, postgres.ErrInvalidConfig)
	require.Nil(t, repository)
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

// TestStorageErrorIsNotADomainError pins the separation the admin API depends
// on. It echoes the text of a domain error back to the client and answers
// anything else with a generic 500, so an infrastructure failure that matched
// a tenant sentinel would both return the wrong status and put raw driver text
// into a response body.
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
