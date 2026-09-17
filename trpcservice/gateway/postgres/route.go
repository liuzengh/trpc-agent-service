package postgres

import (
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// HTTPRouteResolver resolves the canonical API app/session against the
// current tenant row and immutable configuration snapshot. Request body
// aliases are accepted only when they match the signed principal claims.
type HTTPRouteResolver struct {
	Tenants tenant.Repository
	Configs config.Repository
}

func (r HTTPRouteResolver) ResolveRunRoute(request *http.Request, principal gateway.Principal, appRoute, requestedSessionID string) (gateway.RunRoute, error) {
	if request == nil || r.Tenants == nil || r.Configs == nil {
		return gateway.RunRoute{}, runtime.ErrCapabilityUnsupported
	}
	if !principal.Authenticated || principal.TenantID == "" || principal.TenantVersion < 1 || principal.SubjectID == "" ||
		principal.UserID == "" || principal.AgentAppID == "" || principal.SessionID == "" || strings.TrimSpace(appRoute) != appRoute || appRoute == "" {
		return gateway.RunRoute{}, gateway.ErrUnauthenticated
	}
	if appRoute != principal.AgentAppID || (requestedSessionID != "" && requestedSessionID != principal.SessionID) {
		return gateway.RunRoute{}, gateway.ErrForbidden
	}
	current, err := r.Tenants.Get(request.Context(), principal.TenantID)
	if err != nil {
		return gateway.RunRoute{}, err
	}
	if current.Status != tenant.StatusActive || current.Version != principal.TenantVersion {
		return gateway.RunRoute{}, runtime.ErrTenantScope
	}
	tc := tenant.Context{TenantID: current.TenantID, TenantVersion: current.Version,
		AgentAppID: principal.AgentAppID, SubjectID: principal.SubjectID,
		Channel: "http", TrustedSource: "authenticated_api"}
	if _, err := r.Configs.ResolveExecutionBinding(request.Context(), tc); err != nil {
		return gateway.RunRoute{}, err
	}
	return gateway.RunRoute{Tenant: tc, UserID: principal.UserID, SessionID: principal.SessionID}, nil
}

var _ gateway.RunRouteResolver = HTTPRouteResolver{}
