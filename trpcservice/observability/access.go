package observability

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrTenantAccessDenied reports that an actor cannot query a tenant aggregate.
	ErrTenantAccessDenied = errors.New("tenant observability access denied")
	// ErrInvalidTenantScope reports an empty or malformed tenant scope.
	ErrInvalidTenantScope = errors.New("invalid tenant observability scope")
)

// TenantAuthorizer is the narrow boundary used by dashboards and aggregate
// readers. Implementations must derive actor identity from trusted auth.
type TenantAuthorizer interface {
	AllowTenant(context.Context, string, string) bool
}

// PlatformAuthorizer is an optional extension for process-level dashboards.
// Implementations should derive the platform role from trusted auth rather
// than from a caller-provided label or tenant scope.
type PlatformAuthorizer interface {
	AllowPlatform(context.Context, string) bool
}

// AuthorizeTenant validates an authorized actor/tenant pair before a dashboard
// or aggregate query is constructed. It never builds a query expression.
func AuthorizeTenant(ctx context.Context, authorizer TenantAuthorizer, actorID, tenantID string) error {
	if ctx == nil || strings.TrimSpace(actorID) == "" || strings.TrimSpace(tenantID) == "" || strings.ContainsAny(actorID+tenantID, "\r\n") {
		return ErrInvalidTenantScope
	}
	if authorizer == nil || !authorizer.AllowTenant(ctx, actorID, tenantID) {
		return ErrTenantAccessDenied
	}
	return nil
}

// StaticTenantAuthorizer is a small immutable authorizer for process-local
// dashboard adapters and tests. The map is copied at construction.
type StaticTenantAuthorizer struct {
	allowed  map[string]map[string]struct{}
	platform map[string]struct{}
}

// NewStaticTenantAuthorizer creates an actor-to-tenant allowlist.
func NewStaticTenantAuthorizer(allowed map[string][]string) StaticTenantAuthorizer {
	copyAllowed := make(map[string]map[string]struct{}, len(allowed))
	for actor, tenants := range allowed {
		set := make(map[string]struct{}, len(tenants))
		for _, tenant := range tenants {
			if strings.TrimSpace(tenant) != "" {
				set[tenant] = struct{}{}
			}
		}
		copyAllowed[actor] = set
	}
	return StaticTenantAuthorizer{allowed: copyAllowed, platform: map[string]struct{}{}}
}

// NewStaticTenantAuthorizerWithPlatformActors creates the process-local test
// authorizer and explicitly marks which actors may read platform telemetry.
func NewStaticTenantAuthorizerWithPlatformActors(allowed map[string][]string, platformActors []string) StaticTenantAuthorizer {
	authorizer := NewStaticTenantAuthorizer(allowed)
	for _, actor := range platformActors {
		if strings.TrimSpace(actor) != "" {
			authorizer.platform[actor] = struct{}{}
		}
	}
	return authorizer
}

// AllowTenant reports whether actorID may read tenantID aggregates.
func (a StaticTenantAuthorizer) AllowTenant(_ context.Context, actorID, tenantID string) bool {
	_, ok := a.allowed[actorID][tenantID]
	return ok
}

// AllowPlatform permits process-level dashboards only for the conventional
// process-local operator identities. Production adapters should replace this
// test policy with an explicit role/claim check.
func (a StaticTenantAuthorizer) AllowPlatform(_ context.Context, actorID string) bool {
	_, ok := a.platform[actorID]
	return ok
}

// ValidateTenantQueryScope rejects unbounded dashboard scope requests.
func ValidateTenantQueryScope(tenantIDs []string) error {
	if len(tenantIDs) == 0 {
		return fmt.Errorf("%w: at least one tenant is required", ErrInvalidTenantScope)
	}
	if len(tenantIDs) > maxDashboardTenantScope {
		return fmt.Errorf("%w: tenant scope exceeds limit", ErrInvalidTenantScope)
	}
	seen := make(map[string]struct{}, len(tenantIDs))
	for _, tenantID := range tenantIDs {
		if strings.TrimSpace(tenantID) == "" || strings.ContainsAny(tenantID, "\r\n") {
			return ErrInvalidTenantScope
		}
		if _, duplicate := seen[tenantID]; duplicate {
			return fmt.Errorf("%w: duplicate tenant", ErrInvalidTenantScope)
		}
		seen[tenantID] = struct{}{}
	}
	return nil
}

// DashboardView identifies the two intentionally separate observability
// surfaces. Platform telemetry is process scoped; tenant usage is sourced
// from the authorized audit aggregate rather than Prometheus labels.
type DashboardView string

const maxDashboardTenantScope = 64

const (
	// DashboardViewPlatform exposes process-scoped telemetry to platform operators.
	DashboardViewPlatform DashboardView = "platform"
	// DashboardViewTenant exposes authorized audit-backed tenant usage aggregates.
	DashboardViewTenant DashboardView = "tenant"
)

// DashboardQuery is the fixed query plan returned by the authorization-first
// adapter. TenantHashes never contain raw tenant identifiers.
type DashboardQuery struct {
	View         DashboardView
	Panel        string
	PromQL       string
	TenantHashes []string
}

var dashboardQueries = map[string]string{
	"request_rate":  "sum(rate(trpcservice_requests_total{component=\"http\",operation=\"http.request\",status=~\"complete|success|error|failure|canceled|timeout\"}[5m]))",
	"error_rate":    "sum(rate(trpcservice_requests_total{component=\"http\",operation=\"http.request\",status=~\"error|failure|canceled|timeout\"}[5m])) / clamp_min(sum(rate(trpcservice_requests_total{component=\"http\",operation=\"http.request\",status=~\"complete|success|error|failure|canceled|timeout\"}[5m])), 1e-9)",
	"latency_p95":   "histogram_quantile(0.95, sum by (le, operation) (rate(trpcservice_operation_duration_ms_bucket[5m])))",
	"delivery_rate": "sum(rate(trpcservice_channel_deliveries_total{status=~\"retry|failure|dead_letter\"}[5m])) / clamp_min(sum(rate(trpcservice_channel_deliveries_total{status=~\"success|retry|failure|dead_letter\"}[5m])), 1e-9)",
}

// BuildDashboardQuery authorizes a dashboard view and returns one of the
// fixed, non-user-supplied query templates. Tenant views intentionally expose
// only the audit-backed usage panel; process telemetry is platform-only.
func BuildDashboardQuery(ctx context.Context, authorizer TenantAuthorizer, actorID string, view DashboardView, tenantIDs []string, panel string) (DashboardQuery, error) {
	if view != DashboardViewPlatform && view != DashboardViewTenant {
		return DashboardQuery{}, ErrInvalidTenantScope
	}
	if strings.TrimSpace(panel) == "" {
		return DashboardQuery{}, ErrInvalidTenantScope
	}
	if view == DashboardViewPlatform {
		if strings.TrimSpace(actorID) == "" {
			return DashboardQuery{}, ErrInvalidTenantScope
		}
		platformAuthorizer, ok := authorizer.(PlatformAuthorizer)
		if !ok || !platformAuthorizer.AllowPlatform(ctx, actorID) {
			return DashboardQuery{}, ErrTenantAccessDenied
		}
		query, ok := dashboardQueries[panel]
		if !ok {
			return DashboardQuery{}, fmt.Errorf("%w: unsupported dashboard panel", ErrInvalidTenantScope)
		}
		return DashboardQuery{View: view, Panel: panel, PromQL: query}, nil
	}
	if panel != "usage" {
		return DashboardQuery{}, fmt.Errorf("%w: tenant view only supports usage", ErrTenantAccessDenied)
	}
	if err := ValidateTenantQueryScope(tenantIDs); err != nil {
		return DashboardQuery{}, err
	}
	hashes := make([]string, 0, len(tenantIDs))
	for _, tenantID := range tenantIDs {
		if err := AuthorizeTenant(ctx, authorizer, actorID, tenantID); err != nil {
			return DashboardQuery{}, err
		}
		hashes = append(hashes, HashIdentifier(tenantID))
	}
	return DashboardQuery{View: view, Panel: panel, TenantHashes: hashes}, nil
}

// HashIdentifier returns the stable short identifier used by authorized
// dashboard scopes and telemetry attributes. It is deliberately not a metric
// label and cannot be reversed into the source identifier.
func HashIdentifier(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:16]
}
