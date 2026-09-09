package storagebundle_test

import (
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storagebundle"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storagebundle/profiletest"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/stretchr/testify/require"
)

// The in-memory repository runs the same contract the PostgreSQL one does. It
// is not the easy half of that pair: it is the implementation every test in this
// repository that is not gated on a database runs against, so a behaviour the
// suite pins here is a behaviour those tests can rely on.
func TestMemoryProfileRepositoryConformsToTheContract(t *testing.T) {
	profiletest.RunProfileRepositorySuite(t, func(t *testing.T) profiletest.Store {
		tenants := tenant.NewMemoryRepository()
		profiles, err := storagebundle.NewMemoryProfileRepository(tenants)
		require.NoError(t, err)
		return profiletest.Store{Profiles: profiles, Tenants: tenants}
	})
}

// The gate is required, and it is required at construction: a repository that
// accepted a nil one would fail at the first create, in a process that had
// already reported itself started.
func TestNewMemoryProfileRepositoryRequiresATenantSource(t *testing.T) {
	profiles, err := storagebundle.NewMemoryProfileRepository(nil)
	require.ErrorContains(t, err, "tenant source")
	require.Nil(t, profiles)
}

// The repository is a ProfileSource, which is what lets the production Router
// read tenant profiles out of the control plane's own storage rather than out of
// a second table that would have to be kept in step with it.
func TestMemoryProfileRepositoryIsAProfileSource(t *testing.T) {
	tenants := tenant.NewMemoryRepository()
	profiles, err := storagebundle.NewMemoryProfileRepository(tenants)
	require.NoError(t, err)

	var source storagebundle.ProfileSource = profiles
	require.NotNil(t, source)

	var repository storagebundle.ProfileRepository = profiles
	require.NotNil(t, repository)
}
