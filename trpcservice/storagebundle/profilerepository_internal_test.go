package storagebundle

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionbackend"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/stretchr/testify/require"
)

// newTamperableRepository returns a repository whose stored map this test can
// reach.
//
// Reaching into it is the point. The integrity check exists for a row that
// something other than the repository changed — a partial restore, a manual
// UPDATE, a half-applied migration — and there is no way to stage that through
// the interface. The PostgreSQL implementation can stage it with SQL; in memory,
// this is the equivalent.
func newTamperableRepository(t *testing.T) (*MemoryProfileRepository, tenant.TenantContext) {
	t.Helper()
	tenants := tenant.NewMemoryRepository()
	scope := tenant.TenantContext{TenantID: "tenant-a"}
	_, err := tenants.CreateTenant(context.Background(), tenant.Tenant{
		ID:     scope.TenantID,
		Slug:   scope.TenantID,
		Name:   "Test Tenant",
		Status: tenant.StatusActive,
	})
	require.NoError(t, err)

	profiles, err := NewMemoryProfileRepository(tenants)
	require.NoError(t, err)
	return profiles, scope
}

func seedProfile(
	t *testing.T, repository *MemoryProfileRepository, scope tenant.TenantContext, id string,
) ProfileRecord {
	t.Helper()
	record, err := repository.CreateProfile(
		context.Background(), scope, postgresProfile(scope.TenantID, id), "user-admin")
	require.NoError(t, err)
	return record
}

// A record whose content no longer matches its recorded fingerprint is not
// served. It fails closed rather than open: a Runtime built from storage nobody
// published is worse than a request that fails, because the sessions pinned to
// it keep being served from wherever it points.
func TestStoredProfileThatDriftedFromItsFingerprintIsRefused(t *testing.T) {
	repository, scope := newTamperableRepository(t)
	seedProfile(t, repository, scope, "p1")

	key := profileKey{tenantID: scope.TenantID, profileID: "p1"}
	tampered := repository.profiles[key]
	tampered.Session.Postgres.DSNRef = "env:SOMEBODY_ELSES_DSN"
	repository.profiles[key] = tampered

	_, err := repository.GetProfile(context.Background(), scope, "p1")
	require.ErrorIs(t, err, tenant.ErrConfigIntegrity)

	// Every reader, not just the one the control plane uses: ResolveProfile is
	// the one the data plane calls, and it is the one that would otherwise build
	// against the edited content.
	_, err = repository.ResolveProfile(context.Background(), scope, "p1")
	require.ErrorIs(t, err, tenant.ErrConfigIntegrity)

	// And a list refuses as a whole rather than returning the intact records: a
	// list missing one profile reads as "that profile was never created".
	_, err = repository.ListProfiles(context.Background(), scope)
	require.ErrorIs(t, err, tenant.ErrConfigIntegrity)
}

// The same for a record that was moved: content found under one identity that
// claims another is content that was restored into the wrong row. Serving it
// would hand one tenant the storage arrangement of another.
func TestStoredProfileFiledUnderTheWrongIdentityIsRefused(t *testing.T) {
	t.Run("wrong id", func(t *testing.T) {
		repository, scope := newTamperableRepository(t)
		record := seedProfile(t, repository, scope, "p1")

		// Filed under p2, still saying p1 — and with the fingerprint it really
		// has, so only the identity check can catch it.
		repository.profiles[profileKey{tenantID: scope.TenantID, profileID: "p2"}] = record

		_, err := repository.GetProfile(context.Background(), scope, "p2")
		require.ErrorIs(t, err, tenant.ErrConfigIntegrity)
		_, err = repository.ListProfiles(context.Background(), scope)
		require.ErrorIs(t, err, tenant.ErrConfigIntegrity)
	})

	t.Run("wrong tenant", func(t *testing.T) {
		repository, scope := newTamperableRepository(t)
		record := seedProfile(t, repository, scope, "p1")

		other := tenant.TenantContext{TenantID: "tenant-b"}
		repository.profiles[profileKey{tenantID: other.TenantID, profileID: "p1"}] = record

		_, err := repository.GetProfile(context.Background(), other, "p1")
		require.ErrorIs(t, err, tenant.ErrConfigIntegrity)
		_, err = repository.ResolveProfile(context.Background(), other, "p1")
		require.ErrorIs(t, err, tenant.ErrConfigIntegrity)

		// The tenant it really belongs to is unaffected.
		_, err = repository.GetProfile(context.Background(), scope, "p1")
		require.NoError(t, err)
	})
}

// A record that is not a valid profile at all — content that predates a rule, or
// a decode that produced something Validate refuses — is the same fault. It must
// not be reported as a merely invalid argument, because nobody supplied it.
func TestStoredProfileThatIsNotValidIsAnIntegrityFault(t *testing.T) {
	repository, scope := newTamperableRepository(t)
	seedProfile(t, repository, scope, "p1")

	key := profileKey{tenantID: scope.TenantID, profileID: "p1"}
	tampered := repository.profiles[key]
	tampered.Session.Backend = "cassandra"
	repository.profiles[key] = tampered

	_, err := repository.GetProfile(context.Background(), scope, "p1")
	require.ErrorIs(t, err, tenant.ErrConfigIntegrity)
	require.NotErrorIs(t, err, ErrInvalidProfile)
}

// Verify is the check both repositories share, so what it accepts is asserted
// directly rather than only through a tampered store.
func TestProfileRecordVerifyAcceptsWhatItStored(t *testing.T) {
	profile := postgresProfile("tenant-a", "p1")
	fingerprint, err := profile.Fingerprint()
	require.NoError(t, err)

	record := ProfileRecord{
		Profile:     profile,
		Fingerprint: fingerprint,
		CreatedBy:   "user-admin",
		CreatedAt:   time.Now().UTC(),
	}
	require.NoError(t, record.Verify("tenant-a", "p1"))

	// Provenance is not fingerprinted: it is recorded beside the content, not
	// derived from it, so changing it is not a content fault. It is also not
	// something the admin API lets anyone change.
	record.CreatedBy = "user-other"
	record.CreatedAt = time.Time{}
	require.NoError(t, record.Verify("tenant-a", "p1"))

	require.Error(t, record.Verify("tenant-b", "p1"))
	require.Error(t, record.Verify("tenant-a", "p2"))
}

// The clock is truncated to microseconds so a record read back from PostgreSQL
// equals the record that was written: timestamptz keeps microseconds, and a Go
// time that kept nanoseconds would come back different — which the conformance
// suite compares for equality.
func TestStoredNowIsUTCAndMicrosecondPrecision(t *testing.T) {
	now := storedNow()
	require.Equal(t, time.UTC, now.Location())
	require.Zero(t, now.Nanosecond()%1000)
	require.WithinDuration(t, time.Now(), now, time.Minute)
}

// The limit is a package constant because two implementations and the admin API
// all have to mean the same number by it.
func TestMaxProfilesPerTenantIsTheDocumentedBound(t *testing.T) {
	require.Equal(t, 32, MaxProfilesPerTenant)
}

// The in-memory repository shares its map with nothing, but it does share the
// tenant table: a tenant that the control plane never created cannot own a
// profile, and the gate is the control plane's own repository rather than a
// second table that could disagree with it.
func TestMemoryProfileRepositoryConsultsTheTenantTableItWasGivenLive(t *testing.T) {
	tenants := tenant.NewMemoryRepository()
	profiles, err := NewMemoryProfileRepository(tenants)
	require.NoError(t, err)
	scope := tenant.TenantContext{TenantID: "tenant-a"}

	// Before the tenant exists.
	_, err = profiles.CreateProfile(
		context.Background(), scope, inMemoryProfile(scope.TenantID, "p1"), "user-admin")
	require.ErrorIs(t, err, tenant.ErrNotFound)

	// After it is created in the table this repository was handed, without
	// re-constructing anything.
	_, err = tenants.CreateTenant(context.Background(), tenant.Tenant{
		ID:     scope.TenantID,
		Slug:   scope.TenantID,
		Name:   "Test Tenant",
		Status: tenant.StatusActive,
	})
	require.NoError(t, err)

	record, err := profiles.CreateProfile(
		context.Background(), scope, inMemoryProfile(scope.TenantID, "p1"), "user-admin")
	require.NoError(t, err)
	require.Equal(t, sessionbackend.BackendInMemory, record.Session.Backend)
}
