package gateway

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// SnapshotResolver loads the exact immutable configuration pinned at ingress.
type SnapshotResolver interface {
	Resolve(context.Context, string, string, tenant.ConfigVersion) (config.RuntimeSnapshot, error)
}

// FileSnapshotResolver resolves snapshots from a validated static config file.
// It exists for offline development and tests; production resolves snapshots
// from the control-plane store via StoreSnapshotResolver.
type FileSnapshotResolver struct{ File *config.File }

// Resolve rejects a request if the published version no longer matches its pin.
func (resolver FileSnapshotResolver) Resolve(_ context.Context, tenantID, appID string, version tenant.ConfigVersion) (config.RuntimeSnapshot, error) {
	snapshot, err := resolver.File.Snapshot(tenantID, appID)
	if err != nil {
		return config.RuntimeSnapshot{}, err
	}
	if snapshot.Version() != version {
		return config.RuntimeSnapshot{}, fmt.Errorf("gateway: pinned config version %d is unavailable (current %d)", version, snapshot.Version())
	}
	return snapshot, nil
}

// StoreSnapshotResolver resolves the immutable published version pinned at
// ingress from the control-plane store, so in-flight requests keep their
// original Bundle while new requests pick up freshly published versions.
type StoreSnapshotResolver struct {
	Published *config.PublishedCache
}

// Resolve loads the pinned version and derives the immutable runtime view.
func (resolver StoreSnapshotResolver) Resolve(ctx context.Context, tenantID, appID string, version tenant.ConfigVersion) (config.RuntimeSnapshot, error) {
	if resolver.Published == nil {
		return config.RuntimeSnapshot{}, errors.New("gateway: nil published config cache")
	}
	file, err := resolver.Published.Version(ctx, tenantID, version)
	if err != nil {
		return config.RuntimeSnapshot{}, err
	}
	snapshot, err := file.Snapshot(tenantID, appID)
	if err != nil {
		return config.RuntimeSnapshot{}, err
	}
	if snapshot.Version() != version {
		return config.RuntimeSnapshot{}, fmt.Errorf("gateway: pinned config version %d is unavailable (snapshot %d)", version, snapshot.Version())
	}
	return snapshot, nil
}
