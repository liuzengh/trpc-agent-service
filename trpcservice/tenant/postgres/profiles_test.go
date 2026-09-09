package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storagebundle"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storagebundle/profiletest"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant/postgres"
	"github.com/stretchr/testify/require"
)

// These tests need no database either. What they can still pin is the order of
// the checks: everything decidable from the request alone is decided before the
// pool is touched, so a repository pointed at an unreachable server must refuse
// bad input with a domain error rather than with a connection failure.

func TestNewProfileRepositoryRejectsNilPool(t *testing.T) {
	repository, err := postgres.NewProfileRepository(nil)
	require.ErrorIs(t, err, postgres.ErrInvalidConfig)
	require.Nil(t, repository)
}

// unreachableProfileRepository returns a repository whose pool can never
// connect. pgxpool does not dial until a query needs a connection, so building
// it offline is safe — and any check that failed to run before the database
// would show up here as a connection error instead of the domain error asserted.
func unreachableProfileRepository(t *testing.T) *postgres.ProfileRepository {
	t.Helper()
	pool, err := pgxpool.New(
		context.Background(), "postgres://offline:offline@127.0.0.1:1/none?connect_timeout=1")
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	repository, err := postgres.NewProfileRepository(pool)
	require.NoError(t, err)
	return repository
}

// offlineContext bounds a call that must not reach the database. It is short:
// if one of these calls really did try to connect, the test should fail quickly
// rather than wait out a dial timeout.
func offlineContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestProfileRepositoryHonorsACanceledContextBeforeConnecting(t *testing.T) {
	repository := unreachableProfileRepository(t)
	scope := tenant.TenantContext{TenantID: "tenant-a"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := repository.CreateProfile(
		ctx, scope, profiletest.PostgresProfile(scope.TenantID, "p1"), "user-admin")
	require.ErrorIs(t, err, context.Canceled)

	_, err = repository.GetProfile(ctx, scope, "p1")
	require.ErrorIs(t, err, context.Canceled)

	_, err = repository.ResolveProfile(ctx, scope, "p1")
	require.ErrorIs(t, err, context.Canceled)

	_, err = repository.ListProfiles(ctx, scope)
	require.ErrorIs(t, err, context.Canceled)

	// A nil context is a bad request rather than a panic, which is the
	// convention every repository in this project follows.
	//nolint:staticcheck // a nil context is exactly what is under test here.
	_, err = repository.GetProfile(nil, scope, "p1")
	require.ErrorIs(t, err, tenant.ErrInvalidArgument)
}

// The request is judged before the database is consulted. This matters for more
// than latency: a profile that could never be built must be refused with the
// reason, and a caller writing outside its scope must be refused whether or not
// the database is reachable at all.
func TestProfileRepositoryRefusesBadRequestsWithoutTheDatabase(t *testing.T) {
	repository := unreachableProfileRepository(t)
	ctx := offlineContext(t)
	scope := tenant.TenantContext{TenantID: "tenant-a"}

	t.Run("an unusable scope", func(t *testing.T) {
		_, err := repository.CreateProfile(
			ctx, tenant.TenantContext{}, profiletest.PostgresProfile("tenant-a", "p1"), "user-admin")
		require.ErrorIs(t, err, tenant.ErrInvalidArgument)
		_, err = repository.GetProfile(ctx, tenant.TenantContext{}, "p1")
		require.ErrorIs(t, err, tenant.ErrInvalidArgument)
		_, err = repository.ResolveProfile(ctx, tenant.TenantContext{}, "p1")
		require.ErrorIs(t, err, tenant.ErrInvalidArgument)
		_, err = repository.ListProfiles(ctx, tenant.TenantContext{})
		require.ErrorIs(t, err, tenant.ErrInvalidArgument)
	})

	t.Run("another tenant's profile", func(t *testing.T) {
		_, err := repository.CreateProfile(
			ctx, scope, profiletest.PostgresProfile("tenant-b", "p1"), "user-admin")
		require.ErrorIs(t, err, tenant.ErrTenantScope)
	})

	t.Run("no creator", func(t *testing.T) {
		_, err := repository.CreateProfile(
			ctx, scope, profiletest.PostgresProfile(scope.TenantID, "p1"), "")
		require.ErrorIs(t, err, tenant.ErrInvalidArgument)
	})

	t.Run("a profile that could never be built", func(t *testing.T) {
		_, err := repository.CreateProfile(
			ctx, scope, storagebundle.Profile{TenantID: scope.TenantID, ID: "p1"}, "user-admin")
		require.ErrorIs(t, err, storagebundle.ErrInvalidProfile)
	})

	t.Run("an id no profile could have", func(t *testing.T) {
		_, err := repository.GetProfile(ctx, scope, "not a valid id")
		require.ErrorIs(t, err, tenant.ErrInvalidArgument)
		_, err = repository.ResolveProfile(ctx, scope, "")
		require.ErrorIs(t, err, tenant.ErrInvalidArgument)
	})
}

// The profile repository is a ProfileSource, which is what lets the production
// Router read tenant profiles out of the control plane's own database rather
// than out of a second store that would have to be kept in step with it.
func TestProfileRepositoryIsAProfileSource(t *testing.T) {
	repository := unreachableProfileRepository(t)

	var source storagebundle.ProfileSource = repository
	require.NotNil(t, source)

	var full storagebundle.ProfileRepository = repository
	require.NotNil(t, full)
}
