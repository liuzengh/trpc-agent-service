// Package tenanttest holds the behaviour contract that every
// tenant.Repository implementation has to satisfy.
//
// The suite lives in its own package rather than in a _test.go file beside
// MemoryRepository because a second implementation, in a second package, has
// to run exactly the same assertions. A conformance suite that is copied
// instead of shared stops being a contract the moment one copy is fixed.
//
// RunRepositorySuite takes a factory rather than a repository because an
// implementation backed by a real database needs a fresh, isolated store for
// each subtest. The factory is called once per subtest and is handed that
// subtest's *testing.T, so an implementation can register its own cleanup.
//
// The suite asserts only behaviour the Repository interface promises. It never
// reaches behind the interface, so it says nothing about how a store is
// namespaced, migrated or connected; that is each implementation's own test.
package tenanttest

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/stretchr/testify/require"
)

// suiteTimeout bounds every subtest. An in-memory repository never reaches it
// and a reachable database answers in milliseconds, so this only stops an
// unreachable one from hanging until the package timeout.
const suiteTimeout = 30 * time.Second

// NewRepository builds the repository a single subtest runs against. It must
// return a store that is empty and isolated from every other subtest: the
// suite reuses fixed resource IDs such as "tenant-a" across subtests, so a
// shared store would collide.
type NewRepository func(t *testing.T) tenant.Repository

// RunRepositorySuite runs the whole contract against newRepository.
func RunRepositorySuite(t *testing.T, newRepository NewRepository) {
	t.Helper()

	t.Run("PublishResolveAndRevisionImmutability", func(t *testing.T) {
		assertPublishResolveAndRevisionImmutability(t, newRepository(t))
	})
	t.Run("DefaultAndPinnedRevisionRouting", func(t *testing.T) {
		assertDefaultAndPinnedRevisionRouting(t, newRepository(t))
	})
	t.Run("EnforcesTenantIsolation", func(t *testing.T) {
		assertEnforcesTenantIsolation(t, newRepository(t))
	})
	t.Run("RejectsInvalidAndDuplicateConfiguration", func(t *testing.T) {
		assertRejectsInvalidAndDuplicateConfiguration(t, newRepository(t))
	})
	t.Run("HonorsCanceledContext", func(t *testing.T) {
		assertHonorsCanceledContext(t, newRepository(t))
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

func assertPublishResolveAndRevisionImmutability(t *testing.T, repository tenant.Repository) {
	t.Helper()
	ctx := Context(t)
	scope := tenant.TenantContext{TenantID: "tenant-a"}
	SeedTenantAndApp(t, ctx, repository, scope, "assistant")

	temperature := 0.4
	revisionInput := tenant.AgentRevision{
		ID:         "revision-1",
		TenantID:   scope.TenantID,
		AgentAppID: "assistant",
		RevisionNo: 1,
		CreatedBy:  "user-admin",
		Config: tenant.RevisionConfig{
			AgentName:   "support-agent",
			Instruction: "Help the user.",
			Model: tenant.ModelConfig{
				Provider:    "deterministic",
				Name:        "echo-v1",
				Temperature: &temperature,
			},
			ToolRefs: []string{"orders.read"},
		},
	}
	created, err := repository.CreateRevision(ctx, scope, revisionInput)
	require.NoError(t, err)
	require.Len(t, created.ConfigDigest, 64)
	require.Equal(t, tenant.RevisionStatusDraft, created.Status)

	_, err = repository.ResolveRevision(ctx, scope, "assistant", "")
	require.ErrorIs(t, err, tenant.ErrNoPublishedRevision)

	// Mutating the input after the call must not reach the store: a repository
	// that kept the caller's slice would hand the edit back on the next read.
	revisionInput.Config.ToolRefs[0] = "orders.delete"
	temperature = 1.8
	stored, err := repository.GetRevision(ctx, scope, "assistant", "revision-1")
	require.NoError(t, err)
	require.Equal(t, []string{"orders.read"}, stored.Config.ToolRefs)
	require.Equal(t, 0.4, *stored.Config.Model.Temperature)

	// The same holds for a value a caller was handed: it is theirs to mutate,
	// and the mutation must not travel back into the store.
	stored.Config.ToolRefs[0] = "mutated.after.read"
	*stored.Config.Model.Temperature = 2
	storedAgain, err := repository.GetRevision(ctx, scope, "assistant", "revision-1")
	require.NoError(t, err)
	require.Equal(t, []string{"orders.read"}, storedAgain.Config.ToolRefs)
	require.Equal(t, 0.4, *storedAgain.Config.Model.Temperature)

	app, published, err := repository.PublishRevision(ctx, scope, "assistant", "revision-1")
	require.NoError(t, err)
	require.Equal(t, tenant.RevisionStatusPublished, published.Status)
	require.NotNil(t, published.PublishedAt)
	require.Equal(t, uint64(1), app.RoutingVersion)
	require.Equal(t, "revision-1", app.RoutingPolicy.DefaultRevisionID)
	require.Equal(t, uint32(10000), app.RoutingPolicy.Routes[0].Weight)

	// Publishing must be durable, not just reflected in the returned value.
	reloaded, err := repository.GetRevision(ctx, scope, "assistant", "revision-1")
	require.NoError(t, err)
	require.Equal(t, tenant.RevisionStatusPublished, reloaded.Status)
	require.NotNil(t, reloaded.PublishedAt)
	require.Equal(t, created.ConfigDigest, reloaded.ConfigDigest,
		"the config digest is immutable across a publish")

	resolved, err := repository.ResolveRevision(ctx, scope, "assistant", "")
	require.NoError(t, err)
	require.Equal(t, "revision-1", resolved.ID)

	app, _, err = repository.PublishRevision(ctx, scope, "assistant", "revision-1")
	require.NoError(t, err)
	require.Equal(t, uint64(1), app.RoutingVersion, "idempotent publish must not advance routing")
}

func assertDefaultAndPinnedRevisionRouting(t *testing.T, repository tenant.Repository) {
	t.Helper()
	ctx := Context(t)
	scope := tenant.TenantContext{TenantID: "tenant-a"}
	SeedTenantAndApp(t, ctx, repository, scope, "assistant")
	SeedPublishedRevision(t, ctx, repository, scope, "assistant", "revision-1", 1)
	SeedPublishedRevision(t, ctx, repository, scope, "assistant", "revision-2", 2)

	defaultRevision, err := repository.ResolveRevision(ctx, scope, "assistant", "")
	require.NoError(t, err)
	require.Equal(t, "revision-2", defaultRevision.ID)

	pinnedRevision, err := repository.ResolveRevision(ctx, scope, "assistant", "revision-1")
	require.NoError(t, err)
	require.Equal(t, "revision-1", pinnedRevision.ID)

	// Re-publishing an already published, non-default revision is the rollback
	// path: it moves the default back and advances routing so caches invalidate.
	app, rolledBackRevision, err := repository.PublishRevision(ctx, scope, "assistant", "revision-1")
	require.NoError(t, err)
	require.Equal(t, "revision-1", rolledBackRevision.ID)
	require.Equal(t, "revision-1", app.RoutingPolicy.DefaultRevisionID)
	require.Equal(t, uint64(3), app.RoutingVersion)

	rolledBackDefault, err := repository.ResolveRevision(ctx, scope, "assistant", "")
	require.NoError(t, err)
	require.Equal(t, "revision-1", rolledBackDefault.ID)
}

func assertEnforcesTenantIsolation(t *testing.T, repository tenant.Repository) {
	t.Helper()
	ctx := Context(t)
	tenantA := tenant.TenantContext{TenantID: "tenant-a"}
	tenantB := tenant.TenantContext{TenantID: "tenant-b"}
	SeedTenantAndApp(t, ctx, repository, tenantA, "shared-app-id")
	SeedTenantAndApp(t, ctx, repository, tenantB, "shared-app-id")
	SeedPublishedRevision(t, ctx, repository, tenantA, "shared-app-id", "revision-1", 1)
	SeedPublishedRevision(t, ctx, repository, tenantB, "shared-app-id", "revision-1", 1)

	// The same app id and the same revision id under two tenants address two
	// different rows.
	revisionA, err := repository.ResolveRevision(ctx, tenantA, "shared-app-id", "")
	require.NoError(t, err)
	revisionB, err := repository.ResolveRevision(ctx, tenantB, "shared-app-id", "")
	require.NoError(t, err)
	require.Equal(t, tenantA.TenantID, revisionA.TenantID)
	require.Equal(t, tenantB.TenantID, revisionB.TenantID)

	_, err = repository.CreateAgentApp(ctx, tenantA, tenant.AgentApp{
		ID:       "wrong-scope",
		TenantID: tenantB.TenantID,
		Name:     "Wrong Scope",
	})
	require.ErrorIs(t, err, tenant.ErrTenantScope)

	_, err = repository.GetRevision(ctx, tenantB, "missing-app", "revision-1")
	require.ErrorIs(t, err, tenant.ErrNotFound)
}

func assertRejectsInvalidAndDuplicateConfiguration(t *testing.T, repository tenant.Repository) {
	t.Helper()
	ctx := Context(t)
	_, err := repository.CreateTenant(ctx, tenant.Tenant{
		ID:   "tenant/unsafe",
		Slug: "unsafe",
		Name: "Unsafe",
	})
	require.ErrorIs(t, err, tenant.ErrInvalidArgument)

	SeedTenant(t, ctx, repository, "tenant-a", tenant.StatusActive)
	_, err = repository.CreateTenant(ctx, tenant.Tenant{
		ID:   "tenant-b",
		Slug: "tenant-a",
		Name: "Duplicate Slug",
	})
	require.ErrorIs(t, err, tenant.ErrAlreadyExists)

	// A duplicate id and a duplicate slug are two separate constraints. Both
	// report ErrAlreadyExists, so a storage-backed implementation has to
	// recognise each one rather than mapping whichever it happens to hit first.
	_, err = repository.CreateTenant(ctx, tenant.Tenant{
		ID:   "tenant-a",
		Slug: "tenant-a-second",
		Name: "Duplicate ID",
	})
	require.ErrorIs(t, err, tenant.ErrAlreadyExists)

	suspendedScope := tenant.TenantContext{TenantID: "tenant-suspended"}
	SeedTenant(t, ctx, repository, suspendedScope.TenantID, tenant.StatusSuspended)
	_, err = repository.CreateAgentApp(ctx, suspendedScope, tenant.AgentApp{
		ID:       "assistant",
		TenantID: suspendedScope.TenantID,
		Name:     "Assistant",
	})
	require.ErrorIs(t, err, tenant.ErrTenantInactive)

	// Routing is repository-owned state. A caller that supplies it is ignored,
	// not rejected, so an admin API can echo a whole app object back.
	scope := tenant.TenantContext{TenantID: "tenant-a"}
	_, err = repository.CreateAgentApp(ctx, scope, tenant.AgentApp{
		ID:       "assistant",
		TenantID: scope.TenantID,
		Name:     "Assistant",
		RoutingPolicy: tenant.RoutingPolicy{
			DefaultRevisionID: "caller-controlled",
		},
		RoutingVersion: 99,
	})
	require.NoError(t, err)
	storedApp, err := repository.GetAgentApp(ctx, scope, "assistant")
	require.NoError(t, err)
	require.Zero(t, storedApp.RoutingVersion)
	require.Empty(t, storedApp.RoutingPolicy.DefaultRevisionID)

	_, err = repository.CreateAgentApp(ctx, scope, tenant.AgentApp{
		ID:       "assistant",
		TenantID: scope.TenantID,
		Name:     "Duplicate App",
	})
	require.ErrorIs(t, err, tenant.ErrAlreadyExists)

	SeedPublishedRevision(t, ctx, repository, scope, "assistant", "revision-1", 1)
	_, err = repository.CreateRevision(ctx, scope, tenant.AgentRevision{
		ID:         "revision-other",
		TenantID:   scope.TenantID,
		AgentAppID: "assistant",
		RevisionNo: 1,
		CreatedBy:  "test-admin",
		Config: tenant.RevisionConfig{
			AgentName: "duplicate-number",
			Model:     tenant.ModelConfig{Provider: "deterministic", Name: "echo-test"},
		},
	})
	require.ErrorIs(t, err, tenant.ErrAlreadyExists)

	_, err = repository.CreateRevision(ctx, scope, tenant.AgentRevision{
		ID:         "revision-1",
		TenantID:   scope.TenantID,
		AgentAppID: "assistant",
		RevisionNo: 99,
		CreatedBy:  "test-admin",
		Config: tenant.RevisionConfig{
			AgentName: "duplicate-id",
			Model:     tenant.ModelConfig{Provider: "deterministic", Name: "echo-test"},
		},
	})
	require.ErrorIs(t, err, tenant.ErrAlreadyExists)

	_, err = repository.CreateRevision(ctx, scope, tenant.AgentRevision{
		ID:         "revision-orphan",
		TenantID:   scope.TenantID,
		AgentAppID: "missing-app",
		RevisionNo: 1,
		CreatedBy:  "test-admin",
		Config: tenant.RevisionConfig{
			AgentName: "orphan",
			Model:     tenant.ModelConfig{Provider: "deterministic", Name: "echo-test"},
		},
	})
	require.ErrorIs(t, err, tenant.ErrNotFound)

	// A revision number has to survive a signed 64-bit column. The bound is a
	// domain rule rather than one implementation's private limit, so a value
	// the reference implementation accepts is never one a storage-backed
	// implementation has to refuse.
	_, err = repository.CreateRevision(ctx, scope, tenant.AgentRevision{
		ID:         "revision-overflow",
		TenantID:   scope.TenantID,
		AgentAppID: "assistant",
		RevisionNo: math.MaxUint64,
		CreatedBy:  "test-admin",
		Config: tenant.RevisionConfig{
			AgentName: "overflow",
			Model:     tenant.ModelConfig{Provider: "deterministic", Name: "echo-test"},
		},
	})
	require.ErrorIs(t, err, tenant.ErrInvalidArgument)

	created, err := repository.CreateRevision(ctx, scope, tenant.AgentRevision{
		ID:         "revision-draft",
		TenantID:   scope.TenantID,
		AgentAppID: "assistant",
		RevisionNo: 2,
		CreatedBy:  "test-admin",
		Config: tenant.RevisionConfig{
			AgentName: "draft-agent",
			Model:     tenant.ModelConfig{Provider: "deterministic", Name: "echo-test"},
		},
	})
	require.NoError(t, err)
	require.Equal(t, tenant.RevisionStatusDraft, created.Status)
	require.Nil(t, created.PublishedAt)

	// A pinned revision is only routable once it is published.
	_, err = repository.ResolveRevision(ctx, scope, "assistant", "revision-draft")
	require.ErrorIs(t, err, tenant.ErrRevisionNotPublished)
}

func assertHonorsCanceledContext(t *testing.T, repository tenant.Repository) {
	t.Helper()
	ctx, cancel := context.WithCancel(Context(t))
	cancel()

	// Every entry point checks the context before it validates or reads, so a
	// caller that has already given up never reaches storage.
	_, err := repository.GetTenant(ctx, "tenant-a")
	require.True(t, errors.Is(err, context.Canceled))

	_, err = repository.CreateTenant(ctx, tenant.Tenant{
		ID:   "tenant-a",
		Slug: "tenant-a",
		Name: "Canceled",
	})
	require.True(t, errors.Is(err, context.Canceled))

	scope := tenant.TenantContext{TenantID: "tenant-a"}
	_, err = repository.CreateAgentApp(ctx, scope, tenant.AgentApp{
		ID:       "assistant",
		TenantID: scope.TenantID,
		Name:     "Canceled",
	})
	require.True(t, errors.Is(err, context.Canceled))

	_, err = repository.GetAgentApp(ctx, scope, "assistant")
	require.True(t, errors.Is(err, context.Canceled))

	_, err = repository.CreateRevision(
		ctx, scope, DraftRevisionInput(scope, "assistant", "revision-1", 1),
	)
	require.True(t, errors.Is(err, context.Canceled))

	_, err = repository.GetRevision(ctx, scope, "assistant", "revision-1")
	require.True(t, errors.Is(err, context.Canceled))

	_, _, err = repository.PublishRevision(ctx, scope, "assistant", "revision-1")
	require.True(t, errors.Is(err, context.Canceled))

	_, err = repository.ResolveRevision(ctx, scope, "assistant", "")
	require.True(t, errors.Is(err, context.Canceled))
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

// SeedTenantAndApp creates an active tenant and one active app inside it.
func SeedTenantAndApp(
	t *testing.T,
	ctx context.Context,
	repository tenant.Repository,
	scope tenant.TenantContext,
	appID string,
) tenant.AgentApp {
	t.Helper()
	SeedTenant(t, ctx, repository, scope.TenantID, tenant.StatusActive)
	created, err := repository.CreateAgentApp(ctx, scope, tenant.AgentApp{
		ID:       appID,
		TenantID: scope.TenantID,
		Name:     "Test App",
	})
	require.NoError(t, err)
	return created
}

// DraftRevisionInput builds a valid CreateRevision argument. It is exported so
// an implementation's own tests reuse the fixture the suite uses rather than
// inventing a second one that drifts.
func DraftRevisionInput(
	scope tenant.TenantContext,
	appID string,
	revisionID string,
	revisionNo uint64,
) tenant.AgentRevision {
	return tenant.AgentRevision{
		ID:         revisionID,
		TenantID:   scope.TenantID,
		AgentAppID: appID,
		RevisionNo: revisionNo,
		CreatedBy:  "test-admin",
		Config: tenant.RevisionConfig{
			AgentName: "test-agent",
			Model: tenant.ModelConfig{
				Provider: "deterministic",
				Name:     "echo-test",
			},
		},
	}
}

// SeedDraftRevision creates one draft revision and leaves it unpublished.
func SeedDraftRevision(
	t *testing.T,
	ctx context.Context,
	repository tenant.Repository,
	scope tenant.TenantContext,
	appID string,
	revisionID string,
	revisionNo uint64,
) tenant.AgentRevision {
	t.Helper()
	created, err := repository.CreateRevision(
		ctx, scope, DraftRevisionInput(scope, appID, revisionID, revisionNo),
	)
	require.NoError(t, err)
	return created
}

// SeedPublishedRevision creates a draft revision and publishes it, which also
// makes it the app's default.
func SeedPublishedRevision(
	t *testing.T,
	ctx context.Context,
	repository tenant.Repository,
	scope tenant.TenantContext,
	appID string,
	revisionID string,
	revisionNo uint64,
) tenant.AgentRevision {
	t.Helper()
	created := SeedDraftRevision(t, ctx, repository, scope, appID, revisionID, revisionNo)
	_, published, err := repository.PublishRevision(ctx, scope, appID, created.ID)
	require.NoError(t, err)
	return published
}
