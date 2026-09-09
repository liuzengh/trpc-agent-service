package postgres_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storagebundle"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storagebundle/profiletest"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant/postgres"
	"github.com/stretchr/testify/require"
)

// These tests run against a real server behind the same gate as the rest of
// this package's integration tests; see the comment at the top of
// integration_test.go for how to start one. `go test ./...` on a machine with
// no postgres stays green.

func newProfileRepository(t *testing.T, pool *pgxpool.Pool) *postgres.ProfileRepository {
	t.Helper()
	repository, err := postgres.NewProfileRepository(pool)
	require.NoError(t, err)
	return repository
}

// newIsolatedProfileStore is the one-pool, one-schema case the suite wants. The
// tenant repository and the profile repository share the pool, which is the
// arrangement production uses and the only one where the foreign key means
// anything.
func newIsolatedProfileStore(t *testing.T, dsn string) profiletest.Store {
	t.Helper()
	pool := openPool(t, dsn, newSchema(t, dsn))
	return profiletest.Store{
		Profiles: newProfileRepository(t, pool),
		Tenants:  newRepository(t, pool),
	}
}

// TestIntegrationProfileRepositoryConformance runs the same contract the
// in-memory reference runs, against PostgreSQL. A difference between the two is
// a bug in one of them, not a property of the backend — which is the whole
// reason the limit and the single-winner race are asserted by a shared suite
// rather than by two tests that happen to agree.
func TestIntegrationProfileRepositoryConformance(t *testing.T) {
	dsn := requireDSN(t)
	profiletest.RunProfileRepositorySuite(t, func(t *testing.T) profiletest.Store {
		return newIsolatedProfileStore(t, dsn)
	})
}

// A profile is what a published revision points at, so it has to outlive the
// process that created it. This is the assertion the in-memory implementation
// cannot make.
func TestIntegrationProfileSurvivesARestart(t *testing.T) {
	dsn := requireDSN(t)
	schema := newSchema(t, dsn)
	scope := tenant.TenantContext{TenantID: "tenant-a"}
	profile := profiletest.PostgresProfile(scope.TenantID, "p1")

	ctx := integrationContext(t)

	// One process creates it.
	first := openPool(t, dsn, schema)
	profiletest.SeedTenant(t, ctx, newRepository(t, first), scope.TenantID, tenant.StatusActive)
	created, err := newProfileRepository(t, first).CreateProfile(ctx, scope, profile, "user-admin")
	require.NoError(t, err)
	first.Close()

	// Another one reads it, with nothing in common but the database.
	second := newProfileRepository(t, openPool(t, dsn, schema))
	got, err := second.GetProfile(ctx, scope, "p1")
	require.NoError(t, err)
	require.Equal(t, created, got, "a restart must not change what was stored")

	resolved, err := second.ResolveProfile(ctx, scope, "p1")
	require.NoError(t, err)
	require.Equal(t, profile, resolved)

	// And the id is still taken, which is what makes it a version.
	_, err = second.CreateProfile(ctx, scope, profile, "user-admin")
	require.ErrorIs(t, err, tenant.ErrAlreadyExists)
}

// The stored fingerprint exists for the row that something other than this
// repository wrote. Staging that needs SQL, which is why it is here rather than
// in the shared suite.
func TestIntegrationProfileRowEditedOutOfBandIsRefused(t *testing.T) {
	dsn := requireDSN(t)
	ctx := integrationContext(t)

	t.Run("spec no longer matches its fingerprint", func(t *testing.T) {
		pool := openPool(t, dsn, newSchema(t, dsn))
		scope := tenant.TenantContext{TenantID: "tenant-a"}
		profiles := newProfileRepository(t, pool)
		profiletest.SeedTenant(t, ctx, newRepository(t, pool), scope.TenantID, tenant.StatusActive)

		_, err := profiles.CreateProfile(
			ctx, scope, profiletest.PostgresProfile(scope.TenantID, "p1"), "user-admin")
		require.NoError(t, err)

		// The kind of edit an operator makes with a psql session open: point the
		// profile at a different credential and leave the fingerprint alone.
		_, err = pool.Exec(ctx, `UPDATE backend_profiles
			SET spec = jsonb_set(spec, '{session,postgres,dsn_ref}', '"env:SOMEBODY_ELSES_DSN"')
			WHERE tenant_id = $1 AND id = $2`, scope.TenantID, "p1")
		require.NoError(t, err)

		_, err = profiles.GetProfile(ctx, scope, "p1")
		require.ErrorIs(t, err, tenant.ErrConfigIntegrity)
		_, err = profiles.ResolveProfile(ctx, scope, "p1")
		require.ErrorIs(t, err, tenant.ErrConfigIntegrity)
		_, err = profiles.ListProfiles(ctx, scope)
		require.ErrorIs(t, err, tenant.ErrConfigIntegrity)
	})

	t.Run("spec claims another row", func(t *testing.T) {
		pool := openPool(t, dsn, newSchema(t, dsn))
		scope := tenant.TenantContext{TenantID: "tenant-a"}
		other := tenant.TenantContext{TenantID: "tenant-b"}
		tenants := newRepository(t, pool)
		profiles := newProfileRepository(t, pool)
		profiletest.SeedTenant(t, ctx, tenants, scope.TenantID, tenant.StatusActive)
		profiletest.SeedTenant(t, ctx, tenants, other.TenantID, tenant.StatusActive)

		created, err := profiles.CreateProfile(
			ctx, scope, profiletest.PostgresProfile(scope.TenantID, "p1"), "user-admin")
		require.NoError(t, err)

		// A restore into the wrong row: tenant-b's row carrying tenant-a's
		// content, fingerprint and all. Only the identity check catches this,
		// and without it tenant-b would be handed tenant-a's storage.
		_, err = pool.Exec(ctx, `INSERT INTO backend_profiles
			(tenant_id, id, spec, fingerprint, created_by, created_at)
			SELECT $1, id, spec, fingerprint, created_by, created_at
			FROM backend_profiles WHERE tenant_id = $2 AND id = $3`,
			other.TenantID, scope.TenantID, "p1")
		require.NoError(t, err)

		_, err = profiles.GetProfile(ctx, other, "p1")
		require.ErrorIs(t, err, tenant.ErrConfigIntegrity)
		_, err = profiles.ResolveProfile(ctx, other, "p1")
		require.ErrorIs(t, err, tenant.ErrConfigIntegrity)

		// The tenant it really belongs to still reads normally.
		got, err := profiles.GetProfile(ctx, scope, "p1")
		require.NoError(t, err)
		require.Equal(t, created, got)
	})
}

// The tenant foreign key is what makes "a profile without a tenant" impossible
// rather than merely refused. The Go layer checks it first, so this is what
// happens when something goes around the Go layer.
func TestIntegrationProfileRowRequiresItsTenant(t *testing.T) {
	dsn := requireDSN(t)
	ctx := integrationContext(t)
	pool := openPool(t, dsn, newSchema(t, dsn))

	_, err := pool.Exec(ctx, `INSERT INTO backend_profiles
		(tenant_id, id, spec, fingerprint, created_by, created_at)
		VALUES ($1, $2, '{}'::jsonb, '', 'user-admin', now())`, "tenant-missing", "p1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "backend_profiles_tenant_fkey")
}

// A profile create takes a row lock on the tenant, so this pins that the lock it
// takes does not block the writes that share that row. FOR UPDATE here would
// serialise every app and revision insert in the tenant behind every profile
// create, which is a performance bug that no unit test would ever show.
func TestIntegrationProfileCreateDoesNotBlockRevisionWrites(t *testing.T) {
	dsn := requireDSN(t)
	ctx := integrationContext(t)
	pool := openPool(t, dsn, newSchema(t, dsn))
	scope := tenant.TenantContext{TenantID: "tenant-a"}

	tenants := newRepository(t, pool)
	profiles := newProfileRepository(t, pool)
	profiletest.SeedTenant(t, ctx, tenants, scope.TenantID, tenant.StatusActive)

	// Hold the lock a profile create takes, from a transaction of our own.
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	var status string
	require.NoError(t, tx.QueryRow(ctx,
		`SELECT status FROM tenants WHERE id = $1 FOR NO KEY UPDATE`, scope.TenantID,
	).Scan(&status))

	// An app insert takes FOR KEY SHARE on the same row for its foreign key. It
	// must not wait for the lock above.
	created, err := tenants.CreateAgentApp(ctx, scope, tenant.AgentApp{
		ID:       "assistant",
		TenantID: scope.TenantID,
		Name:     "Test App",
	})
	require.NoError(t, err)
	require.Equal(t, "assistant", created.ID)

	require.NoError(t, tx.Rollback(ctx))

	// And the profile create still works once nothing holds the row.
	_, err = profiles.CreateProfile(
		ctx, scope, profiletest.PostgresProfile(scope.TenantID, "p1"), "user-admin")
	require.NoError(t, err)
}

// The limit is a lifetime budget, so it has to be counted from storage rather
// than from anything a process remembers. Two independent repositories over one
// database must agree on how full a tenant is.
func TestIntegrationProfileLimitIsCountedFromStorage(t *testing.T) {
	dsn := requireDSN(t)
	ctx := integrationContext(t)
	schema := newSchema(t, dsn)
	scope := tenant.TenantContext{TenantID: "tenant-a"}

	first := newProfileRepository(t, openPool(t, dsn, schema))
	profiletest.SeedTenant(
		t, ctx, newRepository(t, openPool(t, dsn, schema)), scope.TenantID, tenant.StatusActive)

	for i := range storagebundle.MaxProfilesPerTenant {
		_, err := first.CreateProfile(ctx, scope,
			profiletest.InMemoryProfile(scope.TenantID, profileID(i)), "user-admin")
		require.NoError(t, err)
	}

	// A second process, which has counted nothing itself.
	second := newProfileRepository(t, openPool(t, dsn, schema))
	_, err := second.CreateProfile(
		ctx, scope, profiletest.InMemoryProfile(scope.TenantID, "p-late"), "user-admin")
	require.ErrorIs(t, err, storagebundle.ErrProfileLimit)

	listed, err := second.ListProfiles(ctx, scope)
	require.NoError(t, err)
	require.Len(t, listed, storagebundle.MaxProfilesPerTenant)
}

func profileID(i int) string {
	return "p-" + string(rune('a'+i/26)) + string(rune('a'+i%26))
}
