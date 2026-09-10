package postgres

import (
	"context"
	"database/sql"
	"sync"

	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
)

// MultiTenantStore routes audit writes to tenant-bound Store instances while
// sharing one database pool. It is used by multi-tenant bootstrap; each child
// Store still applies PostgreSQL app.tenant_id RLS scoping.
type MultiTenantStore struct {
	db     *sql.DB
	mu     sync.Mutex
	stores map[string]*Store
}

// NewMultiTenant creates a lazy tenant-routed audit writer.
func NewMultiTenant(db *sql.DB) *MultiTenantStore {
	return &MultiTenantStore{db: db, stores: make(map[string]*Store)}
}

// Append routes one validated event to its tenant-bound writer.
func (store *MultiTenantStore) Append(ctx context.Context, event audit.Event) (audit.AppendResult, error) {
	if store == nil || store.db == nil || event.TenantID == "" {
		return audit.AppendResult{}, ErrStorage
	}
	store.mu.Lock()
	child := store.stores[event.TenantID]
	if child == nil {
		var err error
		child, err = New(store.db, event.TenantID)
		if err != nil {
			store.mu.Unlock()
			return audit.AppendResult{}, err
		}
		store.stores[event.TenantID] = child
	}
	store.mu.Unlock()
	return child.Append(ctx, event)
}

var _ audit.Writer = (*MultiTenantStore)(nil)
