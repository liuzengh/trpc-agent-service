package tenant

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// This file is in-package on purpose. Everything that can be reached through
// the Repository interface belongs in the shared conformance suite instead; the
// one test here covers a branch the interface gives no way to produce.

// TestMemoryRepositoryPublishRejectsCorruptStoredConfig pins how the reference
// implementation classifies a stored config that no longer matches its digest.
//
// The conformance suite cannot cover this. A digest mismatch means the stored
// config changed after it was created, and no sequence of Repository calls can
// do that — which is the whole point of the check. The PostgreSQL
// implementation is tested against a real out-of-band UPDATE; the equivalent
// here is writing through the map, so this test has to see the unexported
// state.
//
// What matters is that both implementations agree it is not the caller's
// fault. ErrInvalidArgument would make the admin API answer 400 and echo the
// message, for a fault no request can cause and no request can fix.
func TestMemoryRepositoryPublishRejectsCorruptStoredConfig(t *testing.T) {
	ctx := t.Context()
	repository := NewMemoryRepository()
	scope := TenantContext{TenantID: "tenant-a"}

	_, err := repository.CreateTenant(ctx, Tenant{
		ID:   "tenant-a",
		Slug: "tenant-a",
		Name: "Test Tenant",
	})
	require.NoError(t, err)
	_, err = repository.CreateAgentApp(ctx, scope, AgentApp{
		ID:       "assistant",
		TenantID: scope.TenantID,
		Name:     "Test App",
	})
	require.NoError(t, err)
	_, err = repository.CreateRevision(ctx, scope, AgentRevision{
		ID:         "revision-1",
		TenantID:   scope.TenantID,
		AgentAppID: "assistant",
		RevisionNo: 1,
		CreatedBy:  "test-admin",
		Config: RevisionConfig{
			AgentName: "test-agent",
			Model:     ModelConfig{Provider: "deterministic", Name: "echo-test"},
		},
	})
	require.NoError(t, err)

	// Change the stored config and leave the digest alone, the way a partial
	// restore or a stray write would.
	key := revisionKey{tenantID: scope.TenantID, appID: "assistant", revisionID: "revision-1"}
	corrupted := repository.revisions[key]
	corrupted.Config.Instruction = "do something else"
	repository.revisions[key] = corrupted

	_, _, err = repository.PublishRevision(ctx, scope, "assistant", "revision-1")
	require.ErrorIs(t, err, ErrConfigIntegrity)
	require.NotErrorIs(t, err, ErrInvalidArgument,
		"a corrupt stored row is a server fault, not a bad request")

	// Nothing half-applied: the revision is still a draft and the app never
	// moved, which is what the PostgreSQL transaction guarantees separately.
	revision, err := repository.GetRevision(ctx, scope, "assistant", "revision-1")
	require.NoError(t, err)
	require.Equal(t, RevisionStatusDraft, revision.Status)
	require.Nil(t, revision.PublishedAt)

	app, err := repository.GetAgentApp(ctx, scope, "assistant")
	require.NoError(t, err)
	require.Zero(t, app.RoutingVersion)
	require.Empty(t, app.RoutingPolicy.DefaultRevisionID)
}
