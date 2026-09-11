package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
)

type tenantToolPolicyResponse struct {
	TenantID string                     `json:"tenant_id"`
	Tools    []identity.TenantToolGrant `json:"tools"`
	Catalog  []ToolInfo                 `json:"catalog,omitempty"`
}

func (c *consoleAPI) tenantToolPolicy(writer http.ResponseWriter, request *http.Request) {
	if c.dependencies.Identities == nil {
		writeJSON(writer, http.StatusServiceUnavailable, map[string]any{"error": "identity store is not configured"})
		return
	}
	tenantID := resolveTenantParam(request)
	if tenantID == "" {
		badRequest(writer, "tenant is required")
		return
	}
	switch request.Method {
	case http.MethodGet:
		user, ok := sessionUser(request)
		if !ok {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		if !user.IsSystemAdmin && !requireTenantWrite(writer, request, tenantID) {
			return
		}
		c.writeTenantToolPolicy(writer, request.Context(), tenantID)
	case http.MethodPut:
		if !requireSystemAdmin(writer, request) {
			return
		}
		var body tenantToolPolicyResponse
		if err := decodeJSONBody(request, &body); err != nil {
			badRequest(writer, err.Error())
			return
		}
		if strings.TrimSpace(body.TenantID) != "" && strings.TrimSpace(body.TenantID) != tenantID {
			badRequest(writer, "tenant_id does not match selected tenant")
			return
		}
		if err := c.validateToolGrantsAgainstCatalog(body.Tools); err != nil {
			badRequest(writer, err.Error())
			return
		}
		if err := c.dependencies.Identities.ReplaceTenantToolGrants(request.Context(), tenantID, body.Tools); err != nil {
			c.writeTenantToolPolicyError(writer, err)
			return
		}
		c.writeTenantToolPolicy(writer, request.Context(), tenantID)
	default:
		methodNotAllowed(writer, http.MethodGet+", "+http.MethodPut)
	}
}

func (c *consoleAPI) writeTenantToolPolicy(writer http.ResponseWriter, ctx context.Context, tenantID string) {
	grants, err := c.dependencies.Identities.ListTenantToolGrants(ctx, tenantID)
	if err != nil {
		c.writeTenantToolPolicyError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, tenantToolPolicyResponse{
		TenantID: tenantID,
		Tools:    filterToolGrantsByCatalog(grants, c.dependencies.ToolCatalog),
		Catalog:  append([]ToolInfo(nil), c.dependencies.ToolCatalog...),
	})
}

func filterToolGrantsByCatalog(grants []identity.TenantToolGrant, catalog []ToolInfo) []identity.TenantToolGrant {
	available := make(map[string]struct{}, len(catalog))
	for _, tool := range catalog {
		if name := strings.TrimSpace(tool.Name); name != "" {
			available[name] = struct{}{}
		}
	}
	visible := make([]identity.TenantToolGrant, 0, len(grants))
	for _, grant := range grants {
		if _, ok := available[strings.TrimSpace(grant.ToolName)]; ok {
			visible = append(visible, grant)
		}
	}
	return visible
}

func (c *consoleAPI) validateToolGrantsAgainstCatalog(grants []identity.TenantToolGrant) error {
	available := make(map[string]struct{}, len(c.dependencies.ToolCatalog))
	for _, tool := range c.dependencies.ToolCatalog {
		available[strings.TrimSpace(tool.Name)] = struct{}{}
	}
	seen := make(map[string]struct{}, len(grants))
	for _, grant := range grants {
		name := strings.TrimSpace(grant.ToolName)
		if name == "" {
			return errors.New("tool grant name is required")
		}
		if _, ok := available[name]; !ok {
			return fmt.Errorf("tool %q is not present in the platform tool catalog", name)
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("duplicate tool grant %q", name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

func (c *consoleAPI) tenantTools(ctx context.Context, tenantID string) ([]ToolInfo, error) {
	if c.dependencies.Identities == nil {
		return nil, errors.New("identity store is not configured")
	}
	grants, err := c.dependencies.Identities.ListTenantToolGrants(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	allowed := make(map[string]struct{}, len(grants))
	for _, grant := range grants {
		allowed[grant.ToolName] = struct{}{}
	}
	result := make([]ToolInfo, 0, len(grants))
	for _, tool := range c.dependencies.ToolCatalog {
		if _, ok := allowed[tool.Name]; ok {
			result = append(result, tool)
		}
	}
	return result, nil
}

func (c *consoleAPI) validateApplicationToolPolicy(ctx context.Context, tenantConfig config.TenantConfig) error {
	if len(tenantConfig.Tools.Allowed) == 0 {
		return nil
	}
	if c.dependencies.Identities == nil {
		return errors.New("identity store is not configured")
	}
	platformTools := make(map[string]struct{}, len(c.dependencies.ToolCatalog))
	for _, tool := range c.dependencies.ToolCatalog {
		platformTools[tool.Name] = struct{}{}
	}
	grants, err := c.dependencies.Identities.ListTenantToolGrants(ctx, tenantConfig.TenantID)
	if err != nil {
		return fmt.Errorf("read tenant tool policy: %w", err)
	}
	allowed := make(map[string]struct{}, len(grants))
	for _, grant := range grants {
		allowed[grant.ToolName] = struct{}{}
	}
	for _, name := range tenantConfig.Tools.Allowed {
		if _, platformManaged := platformTools[name]; !platformManaged {
			continue
		}
		if _, ok := allowed[name]; !ok {
			return fmt.Errorf("application tool %q is not authorized for tenant %q", name, tenantConfig.TenantID)
		}
	}
	return nil
}

func (c *consoleAPI) writeTenantToolPolicyError(writer http.ResponseWriter, err error) {
	if errors.Is(err, identity.ErrTenantNotFound) {
		notFound(writer, "tenant does not exist")
		return
	}
	serverError(writer, "tenant tool policy", err)
}
