package storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// appTenantCacheTTL bounds how long one app → tenant mapping lives in the
// per-process cache. Long enough to keep the hot path off PG; short enough
// that a reassignment made through the Admin API converges within it. The
// admin invalidation broadcast does not reach this layer (it has no redis
// handle), so a TTL is the staleness bound.
const appTenantCacheTTL = 30 * time.Second

// appTenantResolver resolves a session/memory row's tenant_id from its
// agent_app id, cached per process.
type appTenantResolver struct {
	pool *pgxpool.Pool

	mu    sync.Mutex
	cache map[string]appTenantEntry
	now   func() time.Time // stubbed by tests
}

type appTenantEntry struct {
	tenantID  string
	expiresAt time.Time
}

func newAppTenantResolver(pool *pgxpool.Pool) *appTenantResolver {
	return &appTenantResolver{pool: pool, cache: make(map[string]appTenantEntry), now: time.Now}
}

func (r *appTenantResolver) resolve(ctx context.Context, appID string) (string, error) {
	now := r.now()
	r.mu.Lock()
	e, ok := r.cache[appID]
	r.mu.Unlock()
	if ok && now.Before(e.expiresAt) {
		return e.tenantID, nil
	}
	var tenantID string
	if err := r.pool.QueryRow(ctx,
		`SELECT tenant_id FROM agent_app WHERE id = $1`, appID).Scan(&tenantID); err != nil {
		return "", fmt.Errorf("resolve tenant for app %s: %w", appID, err)
	}
	r.mu.Lock()
	r.cache[appID] = appTenantEntry{tenantID: tenantID, expiresAt: now.Add(appTenantCacheTTL)}
	r.mu.Unlock()
	return tenantID, nil
}
