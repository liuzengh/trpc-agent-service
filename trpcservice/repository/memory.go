package repository

import (
	"context"
	"sort"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// MemoryStore is a concurrency-safe Store for tests and single-process demos.
type MemoryStore struct {
	mu      sync.RWMutex
	records map[string][]ConfigRecord
}

// NewMemoryStore creates an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{records: make(map[string][]ConfigRecord)}
}

// PublishConfig atomically appends a version when expected matches the head.
func (store *MemoryStore) PublishConfig(_ context.Context, record ConfigRecord, expected tenant.ConfigVersion) (ConfigRecord, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	versions := store.records[record.TenantID]
	var current tenant.ConfigVersion
	if len(versions) > 0 {
		current = versions[len(versions)-1].Version
	}
	if current != expected {
		return ConfigRecord{}, ErrVersionConflict
	}
	record.Version = current + 1
	record = cloneRecord(record)
	store.records[record.TenantID] = append(versions, record)
	return cloneRecord(record), nil
}

// ListConfigVersions lists only versions owned by tenantID, newest first.
func (store *MemoryStore) ListConfigVersions(_ context.Context, tenantID string) ([]ConfigRecord, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	versions := store.records[tenantID]
	result := make([]ConfigRecord, len(versions))
	for i := range versions {
		result[len(versions)-1-i] = cloneRecord(versions[i])
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].Version > result[j].Version })
	return result, nil
}

// GetConfigVersion returns one tenant-scoped immutable version.
func (store *MemoryStore) GetConfigVersion(_ context.Context, tenantID string, version tenant.ConfigVersion) (ConfigRecord, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	for _, record := range store.records[tenantID] {
		if record.Version == version {
			return cloneRecord(record), nil
		}
	}
	return ConfigRecord{}, ErrNotFound
}

// GetCurrentConfig returns the tenant's newest published version.
func (store *MemoryStore) GetCurrentConfig(_ context.Context, tenantID string) (ConfigRecord, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	versions := store.records[tenantID]
	if len(versions) == 0 {
		return ConfigRecord{}, ErrNotFound
	}
	return cloneRecord(versions[len(versions)-1]), nil
}
