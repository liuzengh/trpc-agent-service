// Package profiletest holds the behaviour contract that every
// storagebundle.ProfileRepository implementation has to satisfy.
//
// It lives in its own package for the reason tenanttest does: a second
// implementation, backed by a real database in a second package, has to run
// exactly the same assertions, and a conformance suite that is copied instead
// of shared stops being a contract the moment one copy is fixed.
//
// The suite asserts only what the interface promises. It never reaches behind
// it, so it says nothing about how a store is keyed, migrated or locked — an
// implementation's own tests own that, including how it detects a row that
// something other than the repository changed.
package profiletest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionbackend"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storagebundle"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/stretchr/testify/require"
)

// suiteTimeout bounds every subtest, for the reason tenanttest's does: an
// in-memory repository never reaches it, a reachable database answers in
// milliseconds, and an unreachable one should fail rather than hang until the
// package timeout.
//
// It is generous because two subtests are concurrent: the limit test creates
// MaxProfilesPerTenant profiles from many goroutines at once, and against a
// real database each of those is a row lock plus a round trip.
const suiteTimeout = 60 * time.Second

// Store is what one subtest runs against: the profile repository under test and
// the tenant table it is gated by.
//
// The two arrive together because a profile cannot exist without a tenant, and
// they must be the same control plane — a suite that seeded tenants into one
// table while the repository consulted another would test nothing about the
// gate. For the PostgreSQL implementation that means one pool; for the in-memory
// one, one MemoryRepository.
type Store struct {
	Profiles storagebundle.ProfileRepository
	Tenants  tenant.Repository
}

// NewStore builds the Store a single subtest runs against. It must return an
// empty store isolated from every other subtest: the suite reuses fixed ids such
// as "tenant-a" across subtests, so a shared store would collide.
type NewStore func(t *testing.T) Store

// RunProfileRepositorySuite runs the whole contract against newStore.
func RunProfileRepositorySuite(t *testing.T, newStore NewStore) {
	t.Helper()

	t.Run("CreateGetListAndResolve", func(t *testing.T) {
		assertCreateGetListAndResolve(t, newStore(t))
	})
	t.Run("RefusesToReplaceAnID", func(t *testing.T) {
		assertRefusesToReplaceAnID(t, newStore(t))
	})
	t.Run("EnforcesTenantIsolation", func(t *testing.T) {
		assertEnforcesTenantIsolation(t, newStore(t))
	})
	t.Run("RequiresAnActiveTenant", func(t *testing.T) {
		assertRequiresAnActiveTenant(t, newStore(t))
	})
	t.Run("RejectsInvalidInput", func(t *testing.T) {
		assertRejectsInvalidInput(t, newStore(t))
	})
	t.Run("SharesNoMemoryWithItsCallers", func(t *testing.T) {
		assertSharesNoMemoryWithItsCallers(t, newStore(t))
	})
	t.Run("OrdersListByID", func(t *testing.T) {
		assertOrdersListByID(t, newStore(t))
	})
	t.Run("ConcurrentCreateOfOneIDHasOneWinner", func(t *testing.T) {
		assertConcurrentCreateOfOneIDHasOneWinner(t, newStore(t))
	})
	t.Run("EnforcesTheProfileLimit", func(t *testing.T) {
		assertEnforcesTheProfileLimit(t, newStore(t))
	})
	t.Run("RefusesADuplicateIDEvenWhenFull", func(t *testing.T) {
		assertRefusesADuplicateIDEvenWhenFull(t, newStore(t))
	})
	t.Run("HonorsACanceledContext", func(t *testing.T) {
		assertHonorsACanceledContext(t, newStore(t))
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

// PostgresProfile is the fixture the suite creates with, exported so an
// implementation's own tests reuse it rather than inventing a second one that
// drifts. It names a credential reference, never a credential.
func PostgresProfile(tenantID string, profileID string) storagebundle.Profile {
	return storagebundle.Profile{
		TenantID: tenantID,
		ID:       profileID,
		Session: storagebundle.SessionSpec{
			Backend: sessionbackend.BackendPostgres,
			Postgres: &storagebundle.PostgresSpec{
				DSNRef:      "env:TENANT_SESSION_DSN",
				Schema:      "sessions",
				TablePrefix: profileID,
			},
		},
	}
}

// RedisProfile is the second fixture, so a test that needs two different
// contents under two ids does not have to invent one.
func RedisProfile(tenantID string, profileID string) storagebundle.Profile {
	return storagebundle.Profile{
		TenantID: tenantID,
		ID:       profileID,
		Session: storagebundle.SessionSpec{
			Backend: sessionbackend.BackendRedis,
			Redis: &storagebundle.RedisSpec{
				URLRef:    "env:TENANT_SESSION_URL",
				KeyPrefix: profileID,
			},
		},
	}
}

// InMemoryProfile is the third, for a test that wants content with no
// credential reference at all.
func InMemoryProfile(tenantID string, profileID string) storagebundle.Profile {
	return storagebundle.Profile{
		TenantID: tenantID,
		ID:       profileID,
		Session:  storagebundle.SessionSpec{Backend: sessionbackend.BackendInMemory},
	}
}

// SeedTenant creates one tenant and fails the test if it cannot.
func SeedTenant(
	t *testing.T,
	ctx context.Context,
	repository tenant.Repository,
	tenantID string,
	status tenant.Status,
) tenant.Tenant {
	t.Helper()
	created, err := repository.CreateTenant(ctx, tenant.Tenant{
		ID:     tenantID,
		Slug:   tenantID,
		Name:   "Test Tenant " + tenantID,
		Status: status,
	})
	require.NoError(t, err)
	return created
}

func assertCreateGetListAndResolve(t *testing.T, store Store) {
	t.Helper()
	ctx := Context(t)
	scope := tenant.TenantContext{TenantID: "tenant-a"}
	SeedTenant(t, ctx, store.Tenants, scope.TenantID, tenant.StatusActive)

	input := PostgresProfile(scope.TenantID, "p1")
	created, err := store.Profiles.CreateProfile(ctx, scope, input, "user-admin")
	require.NoError(t, err)

	// The content comes back as it went in, and the provenance is the
	// repository's: a caller does not get to choose who created a record or
	// when, so these are asserted against what was recorded, not what was sent.
	require.Equal(t, input, created.Profile)
	require.Equal(t, "user-admin", created.CreatedBy)
	require.False(t, created.CreatedAt.IsZero(), "a stored record is dated")
	require.WithinDuration(t, time.Now(), created.CreatedAt, time.Hour)

	fingerprint, err := input.Fingerprint()
	require.NoError(t, err)
	require.Equal(t, fingerprint, created.Fingerprint)

	got, err := store.Profiles.GetProfile(ctx, scope, "p1")
	require.NoError(t, err)
	require.Equal(t, created, got)

	// ResolveProfile is the data plane's view: the content, without the
	// provenance the data plane has no use for.
	resolved, err := store.Profiles.ResolveProfile(ctx, scope, "p1")
	require.NoError(t, err)
	require.Equal(t, input, resolved)

	listed, err := store.Profiles.ListProfiles(ctx, scope)
	require.NoError(t, err)
	require.Equal(t, []storagebundle.ProfileRecord{created}, listed)

	// An id that was never created is not found, and so is one that could never
	// be created — both without saying which.
	_, err = store.Profiles.GetProfile(ctx, scope, "p2")
	require.ErrorIs(t, err, storagebundle.ErrProfileNotFound)
	_, err = store.Profiles.ResolveProfile(ctx, scope, "p2")
	require.ErrorIs(t, err, storagebundle.ErrProfileNotFound)
}

// The id is the version. This is the assertion that makes that a property of
// the storage rather than a convention: there is no Update in the interface, and
// Create is not one in disguise.
func assertRefusesToReplaceAnID(t *testing.T, store Store) {
	t.Helper()
	ctx := Context(t)
	scope := tenant.TenantContext{TenantID: "tenant-a"}
	SeedTenant(t, ctx, store.Tenants, scope.TenantID, tenant.StatusActive)

	original := PostgresProfile(scope.TenantID, "p1")
	created, err := store.Profiles.CreateProfile(ctx, scope, original, "user-admin")
	require.NoError(t, err)

	// Different content under a taken id is refused.
	_, err = store.Profiles.CreateProfile(
		ctx, scope, RedisProfile(scope.TenantID, "p1"), "user-admin")
	require.ErrorIs(t, err, tenant.ErrAlreadyExists)

	// So is identical content, which is the case an implementation is tempted to
	// treat as idempotent. It must not: "the same content" is not something a
	// caller can be relied on to have checked, and a create that sometimes
	// succeeds on a taken id is a create whose result cannot be trusted.
	_, err = store.Profiles.CreateProfile(ctx, scope, original, "user-admin")
	require.ErrorIs(t, err, tenant.ErrAlreadyExists)

	// A different creator does not make it a different profile either.
	_, err = store.Profiles.CreateProfile(ctx, scope, original, "user-other")
	require.ErrorIs(t, err, tenant.ErrAlreadyExists)

	// And nothing above changed what is stored.
	got, err := store.Profiles.GetProfile(ctx, scope, "p1")
	require.NoError(t, err)
	require.Equal(t, created, got)

	listed, err := store.Profiles.ListProfiles(ctx, scope)
	require.NoError(t, err)
	require.Len(t, listed, 1)
}

func assertEnforcesTenantIsolation(t *testing.T, store Store) {
	t.Helper()
	ctx := Context(t)
	tenantA := tenant.TenantContext{TenantID: "tenant-a"}
	tenantB := tenant.TenantContext{TenantID: "tenant-b"}
	SeedTenant(t, ctx, store.Tenants, tenantA.TenantID, tenant.StatusActive)
	SeedTenant(t, ctx, store.Tenants, tenantB.TenantID, tenant.StatusActive)

	_, err := store.Profiles.CreateProfile(
		ctx, tenantA, PostgresProfile(tenantA.TenantID, "p1"), "user-admin")
	require.NoError(t, err)

	// Another tenant's profile is not found, not filtered: the same answer an id
	// that never existed gets. Telling the two apart would answer "does tenant A
	// have a profile called p1", which is not a question tenant B may ask.
	_, err = store.Profiles.GetProfile(ctx, tenantB, "p1")
	require.ErrorIs(t, err, storagebundle.ErrProfileNotFound)
	_, err = store.Profiles.ResolveProfile(ctx, tenantB, "p1")
	require.ErrorIs(t, err, storagebundle.ErrProfileNotFound)

	listed, err := store.Profiles.ListProfiles(ctx, tenantB)
	require.NoError(t, err)
	require.Empty(t, listed)

	// The same id is free in another tenant, and creating it there does not
	// disturb the first.
	createdB, err := store.Profiles.CreateProfile(
		ctx, tenantB, RedisProfile(tenantB.TenantID, "p1"), "user-admin")
	require.NoError(t, err)
	require.Equal(t, tenantB.TenantID, createdB.TenantID)

	gotA, err := store.Profiles.GetProfile(ctx, tenantA, "p1")
	require.NoError(t, err)
	require.Equal(t, sessionbackend.BackendPostgres, gotA.Session.Backend)

	// A profile whose content claims another tenant is refused even when the
	// scope is legitimate: the scope is the authority, and a repository that
	// stored the body's tenant would let an authorized caller write into a
	// tenant it was never scoped to.
	_, err = store.Profiles.CreateProfile(
		ctx, tenantA, PostgresProfile(tenantB.TenantID, "p9"), "user-admin")
	require.ErrorIs(t, err, tenant.ErrTenantScope)

	_, err = store.Profiles.GetProfile(ctx, tenantB, "p9")
	require.ErrorIs(t, err, storagebundle.ErrProfileNotFound)
}

func assertRequiresAnActiveTenant(t *testing.T, store Store) {
	t.Helper()
	ctx := Context(t)

	// A tenant the control plane never created cannot own a profile. This is
	// what the PostgreSQL foreign key makes unrepresentable, and an in-memory
	// implementation that allowed it would not be the same interface.
	missing := tenant.TenantContext{TenantID: "tenant-missing"}
	_, err := store.Profiles.CreateProfile(
		ctx, missing, PostgresProfile(missing.TenantID, "p1"), "user-admin")
	require.Error(t, err)
	require.ErrorIs(t, err, tenant.ErrNotFound)

	// Reading that tenant is not an error, though. It answers exactly what
	// reading another tenant's profile answers — nothing found, an empty list —
	// because the read path is keyed by the caller's tenant and never asks
	// whether that tenant exists. That is the ProfileSource rule one level up: a
	// caller who may not see a profile may not learn whether its tenant is real
	// either, and an implementation that returned ErrNotFound here would turn
	// every list into a tenant-existence oracle. Writes are where the tenant has
	// to exist, and they check it under a lock they need anyway.
	_, err = store.Profiles.GetProfile(ctx, missing, "p1")
	require.ErrorIs(t, err, storagebundle.ErrProfileNotFound)
	require.NotErrorIs(t, err, tenant.ErrNotFound)
	_, err = store.Profiles.ResolveProfile(ctx, missing, "p1")
	require.ErrorIs(t, err, storagebundle.ErrProfileNotFound)

	missingList, err := store.Profiles.ListProfiles(ctx, missing)
	require.NoError(t, err)
	require.Empty(t, missingList)

	// Neither can a tenant that is not active. A suspended tenant's existing
	// profiles keep resolving — the sessions pinned to them are still being
	// served — but nothing new is written into it.
	for _, status := range []tenant.Status{tenant.StatusSuspended, tenant.StatusDeleting} {
		tenantID := "tenant-" + string(status)
		scope := tenant.TenantContext{TenantID: tenantID}
		SeedTenant(t, ctx, store.Tenants, tenantID, status)

		_, err := store.Profiles.CreateProfile(
			ctx, scope, PostgresProfile(tenantID, "p1"), "user-admin")
		require.ErrorIs(t, err, tenant.ErrTenantInactive)

		listed, err := store.Profiles.ListProfiles(ctx, scope)
		require.NoError(t, err, "reads of an inactive tenant still work")
		require.Empty(t, listed)
	}
}

func assertRejectsInvalidInput(t *testing.T, store Store) {
	t.Helper()
	ctx := Context(t)
	scope := tenant.TenantContext{TenantID: "tenant-a"}
	SeedTenant(t, ctx, store.Tenants, scope.TenantID, tenant.StatusActive)

	valid := PostgresProfile(scope.TenantID, "p1")

	t.Run("a profile that could never be built", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			profile storagebundle.Profile
		}{
			{"no id", func() storagebundle.Profile {
				p := PostgresProfile(scope.TenantID, "p1")
				p.ID = ""
				return p
			}()},
			{"no backend", storagebundle.Profile{TenantID: scope.TenantID, ID: "p2"}},
			{"unknown backend", storagebundle.Profile{
				TenantID: scope.TenantID,
				ID:       "p3",
				Session:  storagebundle.SessionSpec{Backend: "cassandra"},
			}},
			{"settings for another backend", func() storagebundle.Profile {
				p := PostgresProfile(scope.TenantID, "p4")
				p.Session.Redis = &storagebundle.RedisSpec{URLRef: "env:OTHER"}
				return p
			}()},
			{"a connection string where a reference belongs", func() storagebundle.Profile {
				p := PostgresProfile(scope.TenantID, "p5")
				p.Session.Postgres.DSNRef = "postgres://user:pw@host:5432/db"
				return p
			}()},
		} {
			_, err := store.Profiles.CreateProfile(ctx, scope, tc.profile, "user-admin")
			require.ErrorIs(t, err, storagebundle.ErrInvalidProfile, tc.name)
		}
	})

	t.Run("an unusable scope", func(t *testing.T) {
		_, err := store.Profiles.CreateProfile(
			ctx, tenant.TenantContext{}, valid, "user-admin")
		require.ErrorIs(t, err, tenant.ErrInvalidArgument)
		_, err = store.Profiles.GetProfile(ctx, tenant.TenantContext{}, "p1")
		require.ErrorIs(t, err, tenant.ErrInvalidArgument)
		_, err = store.Profiles.ResolveProfile(ctx, tenant.TenantContext{}, "p1")
		require.ErrorIs(t, err, tenant.ErrInvalidArgument)
		_, err = store.Profiles.ListProfiles(ctx, tenant.TenantContext{})
		require.ErrorIs(t, err, tenant.ErrInvalidArgument)
	})

	t.Run("no creator", func(t *testing.T) {
		// CreatedBy comes from the authenticated principal, so an empty one is a
		// caller that lost track of who is acting, not a record with an unknown
		// author.
		_, err := store.Profiles.CreateProfile(ctx, scope, valid, "")
		require.ErrorIs(t, err, tenant.ErrInvalidArgument)
	})

	t.Run("an id no profile could have", func(t *testing.T) {
		_, err := store.Profiles.GetProfile(ctx, scope, "not a valid id")
		require.ErrorIs(t, err, tenant.ErrInvalidArgument)
		_, err = store.Profiles.ResolveProfile(ctx, scope, "")
		require.ErrorIs(t, err, tenant.ErrInvalidArgument)
	})

	// Nothing above was stored.
	listed, err := store.Profiles.ListProfiles(ctx, scope)
	require.NoError(t, err)
	require.Empty(t, listed)
}

// A Profile keeps its backend settings behind pointers, so copying one by
// assignment shares them. Every boundary that stores a profile or hands one out
// has to break that sharing: the id is the version, and a caller still holding
// one of those pointers could otherwise change stored content without creating
// a new id — after which Router reports that profile as ErrProfileChanged
// forever, for an edit nobody made through the repository.
func assertSharesNoMemoryWithItsCallers(t *testing.T, store Store) {
	t.Helper()
	ctx := Context(t)
	scope := tenant.TenantContext{TenantID: "tenant-a"}
	SeedTenant(t, ctx, store.Tenants, scope.TenantID, tenant.StatusActive)

	input := PostgresProfile(scope.TenantID, "p1")
	created, err := store.Profiles.CreateProfile(ctx, scope, input, "user-admin")
	require.NoError(t, err)

	// The caller's value is still the caller's, and writing through it must not
	// reach the store.
	input.Session.Postgres.DSNRef = "env:SOMEBODY_ELSES_DSN"
	input.Session.Postgres.TablePrefix = "mutated"

	// So is the value the repository handed back.
	created.Session.Postgres.Schema = "mutated"

	got, err := store.Profiles.GetProfile(ctx, scope, "p1")
	require.NoError(t, err)
	require.Equal(t, "env:TENANT_SESSION_DSN", got.Session.Postgres.DSNRef)
	require.Equal(t, "p1", got.Session.Postgres.TablePrefix)
	require.Equal(t, "sessions", got.Session.Postgres.Schema)

	// And every read is its own copy, so one reader cannot spoil another's.
	got.Session.Postgres.Schema = "mutated again"
	again, err := store.Profiles.GetProfile(ctx, scope, "p1")
	require.NoError(t, err)
	require.Equal(t, "sessions", again.Session.Postgres.Schema)

	resolved, err := store.Profiles.ResolveProfile(ctx, scope, "p1")
	require.NoError(t, err)
	resolved.Session.Postgres.Schema = "mutated too"
	resolvedAgain, err := store.Profiles.ResolveProfile(ctx, scope, "p1")
	require.NoError(t, err)
	require.Equal(t, "sessions", resolvedAgain.Session.Postgres.Schema)

	listed, err := store.Profiles.ListProfiles(ctx, scope)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	listed[0].Session.Postgres.Schema = "mutated in the list"
	listedAgain, err := store.Profiles.ListProfiles(ctx, scope)
	require.NoError(t, err)
	require.Equal(t, "sessions", listedAgain[0].Session.Postgres.Schema)
}

// The order is part of the interface, not of an implementation. A list that came
// back in insertion order from one repository and in index order from another
// would make the admin API's output depend on which one a deployment runs.
func assertOrdersListByID(t *testing.T, store Store) {
	t.Helper()
	ctx := Context(t)
	scope := tenant.TenantContext{TenantID: "tenant-a"}
	SeedTenant(t, ctx, store.Tenants, scope.TenantID, tenant.StatusActive)

	for _, id := range []string{"p-zulu", "p-alpha", "p-mike", "p-0", "p-Alpha"} {
		_, err := store.Profiles.CreateProfile(
			ctx, scope, InMemoryProfile(scope.TenantID, id), "user-admin")
		require.NoError(t, err, id)
	}

	listed, err := store.Profiles.ListProfiles(ctx, scope)
	require.NoError(t, err)

	ids := make([]string, 0, len(listed))
	for _, record := range listed {
		ids = append(ids, record.ID)
	}
	// Byte order, which is what both implementations can promise: a database
	// collation that sorted case-insensitively would not match Go's <, so the
	// PostgreSQL implementation has to order by a collation that does.
	require.Equal(t, []string{"p-0", "p-Alpha", "p-alpha", "p-mike", "p-zulu"}, ids)
}

// Two administrators creating the same id at the same moment is one winner and
// one ErrAlreadyExists. A repository that checked existence and then inserted
// without holding anything would let both through, and the loser's content would
// silently replace the winner's — which is the immutability contract broken by a
// race rather than by an API.
func assertConcurrentCreateOfOneIDHasOneWinner(t *testing.T, store Store) {
	t.Helper()
	ctx := Context(t)
	scope := tenant.TenantContext{TenantID: "tenant-a"}
	SeedTenant(t, ctx, store.Tenants, scope.TenantID, tenant.StatusActive)

	const racers = 8
	var (
		start   = make(chan struct{})
		wait    sync.WaitGroup
		mu      sync.Mutex
		winners []storagebundle.ProfileRecord
		losers  []error
	)
	for i := range racers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			// Each racer sends different content under the same id, so the
			// winner is identifiable afterwards.
			profile := PostgresProfile(scope.TenantID, "p1")
			profile.Session.Postgres.TablePrefix = fmt.Sprintf("racer_%d", i)
			<-start
			record, err := store.Profiles.CreateProfile(
				ctx, scope, profile, fmt.Sprintf("user-%d", i))
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				losers = append(losers, err)
				return
			}
			winners = append(winners, record)
		}()
	}
	close(start)
	wait.Wait()

	require.Len(t, winners, 1, "exactly one create may succeed")
	require.Len(t, losers, racers-1)
	for _, err := range losers {
		require.ErrorIs(t, err, tenant.ErrAlreadyExists)
	}

	// What is stored is what the winner was told it stored.
	got, err := store.Profiles.GetProfile(ctx, scope, "p1")
	require.NoError(t, err)
	require.Equal(t, winners[0], got)

	listed, err := store.Profiles.ListProfiles(ctx, scope)
	require.NoError(t, err)
	require.Len(t, listed, 1)
}

// The limit is a resource bound with a hard edge: every profile a tenant
// resolves becomes a connection pool that lives for the life of the process,
// because Router builds each one once and never evicts it. A limit that could be
// exceeded by racing past it would not be a bound at all, so it is asserted
// under concurrency rather than in sequence.
func assertEnforcesTheProfileLimit(t *testing.T, store Store) {
	t.Helper()
	ctx := Context(t)
	scope := tenant.TenantContext{TenantID: "tenant-a"}
	SeedTenant(t, ctx, store.Tenants, scope.TenantID, tenant.StatusActive)

	// Twice the limit, all at once, all with distinct ids: nothing here can be
	// refused as a duplicate, so every refusal is the limit doing its job.
	const attempts = 2 * storagebundle.MaxProfilesPerTenant
	var (
		start    = make(chan struct{})
		wait     sync.WaitGroup
		mu       sync.Mutex
		accepted int
		refused  []error
	)
	for i := range attempts {
		wait.Add(1)
		go func() {
			defer wait.Done()
			profile := InMemoryProfile(scope.TenantID, fmt.Sprintf("p-%02d", i))
			<-start
			_, err := store.Profiles.CreateProfile(ctx, scope, profile, "user-admin")
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				refused = append(refused, err)
				return
			}
			accepted++
		}()
	}
	close(start)
	wait.Wait()

	require.Equal(t, storagebundle.MaxProfilesPerTenant, accepted)
	require.Len(t, refused, attempts-storagebundle.MaxProfilesPerTenant)
	for _, err := range refused {
		require.ErrorIs(t, err, storagebundle.ErrProfileLimit)
	}

	// Not "about the limit": exactly the limit, counted from storage.
	listed, err := store.Profiles.ListProfiles(ctx, scope)
	require.NoError(t, err)
	require.Len(t, listed, storagebundle.MaxProfilesPerTenant)

	// And it stays enforced afterwards, including for an id that would otherwise
	// be free.
	_, err = store.Profiles.CreateProfile(
		ctx, scope, InMemoryProfile(scope.TenantID, "p-late"), "user-admin")
	require.ErrorIs(t, err, storagebundle.ErrProfileLimit)

	// The refusal does not leak into a tenant that has room.
	SeedTenant(t, ctx, store.Tenants, "tenant-b", tenant.StatusActive)
	_, err = store.Profiles.CreateProfile(
		ctx,
		tenant.TenantContext{TenantID: "tenant-b"},
		InMemoryProfile("tenant-b", "p-late"),
		"user-admin",
	)
	require.NoError(t, err)
}

// A taken id is a conflict whether or not the tenant has room for another
// profile. The two refusals answer different questions — "that id already
// exists" is about the request, "this tenant is full" is about the tenant — and
// only the first one the caller can act on: profiles cannot be deleted, so an
// administrator told a full tenant has nothing to do, while one told the id is
// taken picks another id. An implementation that counted first would hand out
// the useless answer for a request that was never about the limit, and would
// also report the same create differently depending on how full the tenant
// happened to be.
func assertRefusesADuplicateIDEvenWhenFull(t *testing.T, store Store) {
	t.Helper()
	ctx := Context(t)
	scope := tenant.TenantContext{TenantID: "tenant-a"}
	SeedTenant(t, ctx, store.Tenants, scope.TenantID, tenant.StatusActive)

	for i := range storagebundle.MaxProfilesPerTenant {
		_, err := store.Profiles.CreateProfile(
			ctx, scope, InMemoryProfile(scope.TenantID, fmt.Sprintf("p-%02d", i)), "user-admin")
		require.NoError(t, err)
	}

	// A free id is refused for the reason it should be.
	_, err := store.Profiles.CreateProfile(
		ctx, scope, InMemoryProfile(scope.TenantID, "p-new"), "user-admin")
	require.ErrorIs(t, err, storagebundle.ErrProfileLimit)

	// A taken id is refused for the other reason, whatever it carries.
	for _, profile := range []storagebundle.Profile{
		InMemoryProfile(scope.TenantID, "p-00"),
		RedisProfile(scope.TenantID, "p-00"),
	} {
		_, err := store.Profiles.CreateProfile(ctx, scope, profile, "user-admin")
		require.ErrorIs(t, err, tenant.ErrAlreadyExists)
		require.NotErrorIs(t, err, storagebundle.ErrProfileLimit)
	}

	// Neither refusal wrote anything, and the profile that was already there is
	// the one that is still there.
	listed, err := store.Profiles.ListProfiles(ctx, scope)
	require.NoError(t, err)
	require.Len(t, listed, storagebundle.MaxProfilesPerTenant)

	got, err := store.Profiles.GetProfile(ctx, scope, "p-00")
	require.NoError(t, err)
	require.Equal(t, sessionbackend.BackendInMemory, got.Session.Backend)
}

func assertHonorsACanceledContext(t *testing.T, store Store) {
	t.Helper()
	scope := tenant.TenantContext{TenantID: "tenant-a"}
	SeedTenant(t, Context(t), store.Tenants, scope.TenantID, tenant.StatusActive)

	_, err := store.Profiles.CreateProfile(
		Context(t), scope, PostgresProfile(scope.TenantID, "p1"), "user-admin")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(Context(t))
	cancel()

	_, err = store.Profiles.CreateProfile(
		ctx, scope, PostgresProfile(scope.TenantID, "p2"), "user-admin")
	require.True(t, errors.Is(err, context.Canceled), "got %v", err)

	_, err = store.Profiles.GetProfile(ctx, scope, "p1")
	require.True(t, errors.Is(err, context.Canceled), "got %v", err)

	_, err = store.Profiles.ResolveProfile(ctx, scope, "p1")
	require.True(t, errors.Is(err, context.Canceled), "got %v", err)

	_, err = store.Profiles.ListProfiles(ctx, scope)
	require.True(t, errors.Is(err, context.Canceled), "got %v", err)

	// The write that was refused did not happen.
	listed, err := store.Profiles.ListProfiles(Context(t), scope)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, "p1", listed[0].ID)
}

// AssertNoSecretInError is exported so an implementation's own tests can hold
// the line the suite holds: a repository error may name a profile, a tenant and
// a field, and nothing that came out of a credential.
func AssertNoSecretInError(t *testing.T, err error, secrets ...string) {
	t.Helper()
	require.Error(t, err)
	for _, secret := range secrets {
		require.False(t, strings.Contains(err.Error(), secret),
			"error must not carry %q: %v", secret, err)
	}
}
