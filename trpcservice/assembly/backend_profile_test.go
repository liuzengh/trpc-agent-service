package assembly

import (
	"context"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

type testBackendProfileResolver map[string]config.BackendConfig

func (r testBackendProfileResolver) ResolveTenantBackend(_ context.Context, _, _, profileID string) (config.BackendConfig, error) {
	backend, ok := r[profileID]
	if !ok {
		return config.BackendConfig{}, fmt.Errorf("test backend profile %q not found", profileID)
	}
	return backend, nil
}
