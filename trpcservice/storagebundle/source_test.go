package storagebundle

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/stretchr/testify/require"
)

// NoProfiles is a real answer rather than a placeholder: a process with no
// profile storage cannot honour a profile reference, so every lookup is a
// refusal — including the ids that look like they might be special.
func TestNoProfilesRefusesEveryLookup(t *testing.T) {
	source := NoProfiles()

	for _, profileID := range []string{"p1", "", "../../etc/passwd", "default"} {
		profile, err := source.ResolveProfile(
			context.Background(), testScope("tenant-a"), profileID)
		require.ErrorIs(t, err, ErrProfileNotFound)
		require.Equal(t, Profile{}, profile)
	}
}

// The scope is checked even though nothing will be found. A source that
// answered "not found" to a caller with no tenant would let a missing scope
// look like a missing profile, which is the wrong refusal.
func TestNoProfilesValidatesTheScopeFirst(t *testing.T) {
	profile, err := NoProfiles().ResolveProfile(context.Background(), tenant.TenantContext{}, "p1")
	require.ErrorIs(t, err, tenant.ErrInvalidArgument)
	require.NotErrorIs(t, err, ErrProfileNotFound)
	require.Equal(t, Profile{}, profile)
}

func TestMemoryProfileSourceStoresAndResolves(t *testing.T) {
	source := NewMemoryProfileSource()
	stored := postgresProfile("tenant-a", "p1")
	require.NoError(t, source.Put(stored))

	resolved, err := source.ResolveProfile(context.Background(), testScope("tenant-a"), "p1")
	require.NoError(t, err)
	require.Equal(t, stored, resolved)

	_, err = source.ResolveProfile(context.Background(), testScope("tenant-a"), "p2")
	require.ErrorIs(t, err, ErrProfileNotFound)
}

// The id is the version, so "replace p1" is never a legal operation — not even
// with content identical to what is already stored. Allowing the identical case
// would make the rule a comparison rather than a contract, and the next edit
// that differs by a byte would be the one that got through.
func TestMemoryProfileSourcePutRefusesToOverwrite(t *testing.T) {
	source := NewMemoryProfileSource()
	original := postgresProfile("tenant-a", "p1")
	require.NoError(t, source.Put(original))

	err := source.Put(inMemoryProfile("tenant-a", "p1"))
	require.ErrorIs(t, err, tenant.ErrAlreadyExists)

	require.ErrorIs(t, source.Put(original), tenant.ErrAlreadyExists)

	// And the stored content is the original, not the rejected one.
	resolved, err := source.ResolveProfile(context.Background(), testScope("tenant-a"), "p1")
	require.NoError(t, err)
	require.Equal(t, original, resolved)
}

func TestMemoryProfileSourcePutRefusesAnInvalidProfile(t *testing.T) {
	source := NewMemoryProfileSource()

	require.ErrorIs(t, source.Put(Profile{ID: "p1"}), ErrInvalidProfile)
	require.ErrorIs(t, source.Put(Profile{TenantID: "tenant-a"}), ErrInvalidProfile)
	require.ErrorIs(
		t,
		source.Put(Profile{
			TenantID: "tenant-a",
			ID:       "p1",
			Session:  SessionSpec{Backend: "cassandra"},
		}),
		ErrInvalidProfile,
	)

	_, err := source.ResolveProfile(context.Background(), testScope("tenant-a"), "p1")
	require.ErrorIs(t, err, ErrProfileNotFound)
}

// Two tenants may use the same profile id, and neither may reach the other's.
// The refusal is ErrProfileNotFound and not a scope error: telling them apart
// would answer "does tenant B have a profile called p1", which is not a
// question tenant A may ask.
func TestMemoryProfileSourceIsolatesTenantsUnderTheSameID(t *testing.T) {
	source := NewMemoryProfileSource()
	tenantA := postgresProfile("tenant-a", "p1")
	tenantB := redisProfile("tenant-b", "p1")
	require.NoError(t, source.Put(tenantA))
	require.NoError(t, source.Put(tenantB), "the same id under another tenant is a new profile")

	resolvedA, err := source.ResolveProfile(context.Background(), testScope("tenant-a"), "p1")
	require.NoError(t, err)
	require.Equal(t, tenantA, resolvedA)

	resolvedB, err := source.ResolveProfile(context.Background(), testScope("tenant-b"), "p1")
	require.NoError(t, err)
	require.Equal(t, tenantB, resolvedB)

	// A third tenant sees neither, and sees the same refusal it would get for
	// an id nobody ever stored.
	_, err = source.ResolveProfile(context.Background(), testScope("tenant-c"), "p1")
	require.ErrorIs(t, err, ErrProfileNotFound)
	unknown, unknownErr := source.ResolveProfile(
		context.Background(), testScope("tenant-c"), "never-stored")
	require.ErrorIs(t, unknownErr, ErrProfileNotFound)
	require.Equal(t, Profile{}, unknown)
}

func TestMemoryProfileSourceValidatesTheScope(t *testing.T) {
	source := NewMemoryProfileSource()
	require.NoError(t, source.Put(inMemoryProfile("tenant-a", "p1")))

	_, err := source.ResolveProfile(context.Background(), tenant.TenantContext{}, "p1")
	require.ErrorIs(t, err, tenant.ErrInvalidArgument)
	require.NotErrorIs(t, err, ErrProfileNotFound)
}

// Refusing to overwrite is only half of "the id is the version".
//
// SessionSpec keeps its backend settings behind pointers, so storing the value
// Put was handed would store the caller's pointers. A caller that kept its copy
// could then edit stored content without calling Put at all — the one operation
// this source exists to refuse.
func TestMemoryProfileSourcePutStoresACopy(t *testing.T) {
	source := NewMemoryProfileSource()
	postgres := postgresProfile("tenant-a", "p1")
	require.NoError(t, source.Put(postgres))

	postgres.Session.Postgres.DSNRef = "env:SOMEWHERE_ELSE"
	postgres.Session.Postgres.Schema = "someone_elses_schema"

	resolved, err := source.ResolveProfile(context.Background(), testScope("tenant-a"), "p1")
	require.NoError(t, err)
	require.Equal(t, postgresProfile("tenant-a", "p1"), resolved)

	// Redis settings sit behind a pointer of their own, so the copy has to reach
	// both arms rather than the one a test happened to pick.
	redis := redisProfile("tenant-b", "p1")
	require.NoError(t, source.Put(redis))
	redis.Session.Redis.URLRef = "env:SOMEWHERE_ELSE"
	redis.Session.Redis.KeyPrefix = "someone_elses_prefix"

	resolvedRedis, err := source.ResolveProfile(context.Background(), testScope("tenant-b"), "p1")
	require.NoError(t, err)
	require.Equal(t, redisProfile("tenant-b", "p1"), resolvedRedis)
}

// And the other direction: what ResolveProfile hands out is a copy too.
//
// Router calls this on every Resolve and fingerprints what comes back, so a
// returned pointer into stored content is a way to change a profile's
// fingerprint without publishing a new id — which Router reports as
// ErrProfileChanged, permanently, for an edit the source never accepted.
func TestMemoryProfileSourceResolveReturnsACopy(t *testing.T) {
	source := NewMemoryProfileSource()
	require.NoError(t, source.Put(postgresProfile("tenant-a", "p1")))

	first, err := source.ResolveProfile(context.Background(), testScope("tenant-a"), "p1")
	require.NoError(t, err)
	firstPrint, err := first.Fingerprint()
	require.NoError(t, err)

	first.Session.Postgres.Schema = "someone_elses_schema"
	first.Session.Postgres.TablePrefix = "hijacked_"

	second, err := source.ResolveProfile(context.Background(), testScope("tenant-a"), "p1")
	require.NoError(t, err)
	require.Equal(t, postgresProfile("tenant-a", "p1"), second)
	require.NotSame(t, first.Session.Postgres, second.Session.Postgres,
		"two resolutions sharing one pointer is the same aliasing by another route")

	// The fingerprint is the level the contract is actually enforced at, so it
	// is the level this has to hold at.
	secondPrint, err := second.Fingerprint()
	require.NoError(t, err)
	require.Equal(t, firstPrint, secondPrint)
}

// The singleflight and cache key is (tenant, id) rendered with a separator no
// id may contain, so no pair of tenants and ids can collide by concatenation.
func TestProfileKeyCannotCollideByConcatenation(t *testing.T) {
	require.NotEqual(
		t,
		profileKey{tenantID: "a", profileID: "b-c"}.String(),
		profileKey{tenantID: "a-b", profileID: "c"}.String(),
	)
	require.NotEqual(
		t,
		profileKey{tenantID: "a", profileID: "bc"}.String(),
		profileKey{tenantID: "ab", profileID: "c"}.String(),
	)

	// The separator is excluded from both halves by the id rules, so it cannot
	// be smuggled into one of them to forge the other.
	require.Error(t, tenant.ValidateResourceID("backend profile id", "a\x00b"))
}
