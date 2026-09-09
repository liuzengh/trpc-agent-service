package config

import (
	"context"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/stretchr/testify/require"
)

func TestSeedDemoIsIdempotentAndPublished(t *testing.T) {
	repository := tenant.NewMemoryRepository()
	require.NoError(t, SeedDemo(context.Background(), repository))
	require.NoError(t, SeedDemo(context.Background(), repository))

	revision, err := repository.ResolveRevision(
		context.Background(),
		tenant.TenantContext{TenantID: DemoTenantID},
		DemoAgentAppID,
		"",
	)
	require.NoError(t, err)
	require.Equal(t, DemoRevisionID, revision.ID)
	require.Equal(t, tenant.RevisionStatusPublished, revision.Status)
	require.Equal(t, "deterministic", revision.Config.Model.Provider)
}

// A restart must not undo a deployment. With a persistent repository the second
// SeedDemo runs against a store that already holds whatever the operator
// published, so re-publishing echo-v1 unconditionally would move production
// traffic back a revision on nothing but a process restart.
func TestSeedDemoDoesNotClobberAPublishedRevision(t *testing.T) {
	ctx := context.Background()
	repository := tenant.NewMemoryRepository()
	require.NoError(t, SeedDemo(ctx, repository))

	scope := tenant.TenantContext{TenantID: DemoTenantID}
	publishRevision(t, ctx, repository, scope, "echo-v2", 2)

	// The restart.
	require.NoError(t, SeedDemo(ctx, repository))

	revision, err := repository.ResolveRevision(ctx, scope, DemoAgentAppID, "")
	require.NoError(t, err)
	require.Equal(t, "echo-v2", revision.ID, "seeding again moved the default back")

	app, err := repository.GetAgentApp(ctx, scope, DemoAgentAppID)
	require.NoError(t, err)
	require.Equal(t, "echo-v2", app.RoutingPolicy.DefaultRevisionID)
}

// The complement of the test above: skipping the publish is conditional on the
// app already routing somewhere, not on the app merely existing. A process that
// died between creating the revision and publishing it leaves exactly that
// state, and the next boot has to finish the job or the demo never serves.
func TestSeedDemoPublishesWhenTheAppHasNoDefault(t *testing.T) {
	ctx := context.Background()
	repository := tenant.NewMemoryRepository()
	scope := tenant.TenantContext{TenantID: DemoTenantID}

	_, err := repository.CreateTenant(ctx, tenant.Tenant{
		ID: DemoTenantID, Slug: DemoTenantID, Name: "Demo Tenant",
	})
	require.NoError(t, err)
	_, err = repository.CreateAgentApp(ctx, scope, tenant.AgentApp{
		ID: DemoAgentAppID, TenantID: DemoTenantID, Name: "Echo Assistant",
	})
	require.NoError(t, err)

	app, err := repository.GetAgentApp(ctx, scope, DemoAgentAppID)
	require.NoError(t, err)
	require.Empty(t, app.RoutingPolicy.DefaultRevisionID, "fixture must start unrouted")

	require.NoError(t, SeedDemo(ctx, repository))

	revision, err := repository.ResolveRevision(ctx, scope, DemoAgentAppID, "")
	require.NoError(t, err)
	require.Equal(t, DemoRevisionID, revision.ID)
}

// Several workers boot against one shared repository at once. Every create
// tolerates ErrAlreadyExists and a goroutine only reaches the publish once the
// revision exists, so the race is between identical publishes of echo-v1 —
// which is the same write twice, not a conflict.
func TestSeedDemoIsSafeUnderConcurrentBoots(t *testing.T) {
	ctx := context.Background()
	repository := tenant.NewMemoryRepository()

	const boots = 8
	errs := make([]error, boots)
	var wg sync.WaitGroup
	wg.Add(boots)
	for i := range boots {
		go func() {
			defer wg.Done()
			errs[i] = SeedDemo(ctx, repository)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		require.NoErrorf(t, err, "boot %d", i)
	}

	revision, err := repository.ResolveRevision(
		ctx, tenant.TenantContext{TenantID: DemoTenantID}, DemoAgentAppID, "")
	require.NoError(t, err)
	require.Equal(t, DemoRevisionID, revision.ID)
}

// publishRevision stands in for an operator deploying a new revision through
// the Admin API.
func publishRevision(
	t *testing.T,
	ctx context.Context,
	repository tenant.Repository,
	scope tenant.TenantContext,
	revisionID string,
	revisionNo uint64,
) {
	t.Helper()
	_, err := repository.CreateRevision(ctx, scope, tenant.AgentRevision{
		ID:         revisionID,
		TenantID:   DemoTenantID,
		AgentAppID: DemoAgentAppID,
		RevisionNo: revisionNo,
		CreatedBy:  "operator",
		Config: tenant.RevisionConfig{
			AgentName:   "echo-assistant",
			Description: "Operator revision",
			Instruction: "Return the model response.",
			Model: tenant.ModelConfig{
				Provider: "deterministic",
				Name:     "deterministic-echo",
			},
		},
	})
	require.NoError(t, err)
	_, _, err = repository.PublishRevision(ctx, scope, DemoAgentAppID, revisionID)
	require.NoError(t, err)
}
