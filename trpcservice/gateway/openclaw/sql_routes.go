package openclaw

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// ExpectedCredential returns the server-owned credential expected for one
// enabled binding at one published version. It must never return client
// input; returning an error simply means the candidate row does not match.
type ExpectedCredential func(ctx context.Context, tenantID, bindingID string, version tenant.ConfigVersion) (string, error)

// SQLRoutes resolves gateway credentials against the control-plane database,
// so publishing or disabling a tenant, app, or binding takes effect on the
// next request without restarting the service. Disabled tenants and apps
// resolve no routes, which keeps new requests out of the Runtime.
type SQLRoutes struct {
	DB           *sql.DB
	Expected     ExpectedCredential
	QueryTimeout time.Duration
}

// NewSQLRoutes validates a database-backed route resolver.
func NewSQLRoutes(db *sql.DB, expected ExpectedCredential) (*SQLRoutes, error) {
	if db == nil || expected == nil {
		return nil, errors.New("openclaw: database and credential resolver are required")
	}
	return &SQLRoutes{DB: db, Expected: expected}, nil
}

// Resolve finds enabled bindings owned by enabled tenants and apps, then
// performs a constant-time credential comparison per candidate.
func (routes *SQLRoutes) Resolve(bindingID, credential string) (Route, error) {
	if routes == nil || routes.DB == nil || routes.Expected == nil || bindingID == "" || credential == "" {
		return Route{}, errors.New("openclaw: invalid binding credential")
	}
	timeout := routes.QueryTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	rows, err := routes.DB.QueryContext(ctx, `SELECT cb.tenant_id, cb.app_id, cb.channel_type, t.current_config_version
		FROM channel_bindings cb
		JOIN tenants t ON t.tenant_id = cb.tenant_id
		JOIN agent_apps a ON a.tenant_id = cb.tenant_id AND a.app_id = cb.app_id
		WHERE cb.binding_id = $1 AND cb.enabled AND a.enabled AND t.enabled`, bindingID)
	if err != nil {
		return Route{}, fmt.Errorf("openclaw: resolve binding: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var route Route
		if err := rows.Scan(&route.TenantID, &route.AppID, &route.ChannelType, &route.ConfigVersion); err != nil {
			return Route{}, fmt.Errorf("openclaw: scan binding: %w", err)
		}
		route.BindingID = bindingID
		expected, err := routes.Expected(ctx, route.TenantID, bindingID, route.ConfigVersion)
		if err != nil || expected == "" {
			continue
		}
		if len(expected) == len(credential) && subtle.ConstantTimeCompare([]byte(expected), []byte(credential)) == 1 {
			route.Credential = credential
			return route, nil
		}
	}
	if err := rows.Err(); err != nil {
		return Route{}, fmt.Errorf("openclaw: resolve binding: %w", err)
	}
	return Route{}, errors.New("openclaw: invalid binding credential")
}
