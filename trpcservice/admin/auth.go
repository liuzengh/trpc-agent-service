package admin

import (
	"context"
	"crypto/subtle"
	"errors"
	"strings"
)

const (
	RoleSuperAdmin  = "superadmin"
	RoleTenantAdmin = "tenant_admin"
	RoleOperator    = "operator"
	RoleAuditor     = "auditor"
)

type Principal struct {
	Name      string
	Token     string
	Role      string
	TenantIDs []string
}

type Permission string

const (
	PermissionTenantCreate Permission = "tenant_create"
	PermissionWrite        Permission = "write"
	PermissionOperate      Permission = "operate"
	PermissionRead         Permission = "read"
	PermissionDebug        Permission = "debug_execute"
)

func (p Principal) Allows(permission Permission, tenantID string) bool {
	if p.Role == RoleSuperAdmin {
		return true
	}
	if !p.hasTenant(tenantID) {
		return false
	}
	switch p.Role {
	case RoleTenantAdmin:
		return permission == PermissionWrite || permission == PermissionOperate ||
			permission == PermissionRead || permission == PermissionDebug
	case RoleOperator:
		return permission == PermissionOperate || permission == PermissionRead || permission == PermissionDebug
	case RoleAuditor:
		return permission == PermissionRead
	default:
		return false
	}
}

func (p Principal) hasTenant(tenantID string) bool {
	for _, allowed := range p.TenantIDs {
		if allowed == "*" || allowed == tenantID {
			return true
		}
	}
	return false
}

func ValidatePrincipals(principals []Principal) error {
	if len(principals) == 0 {
		return errors.New("at least one Admin principal is required")
	}
	names := make(map[string]struct{}, len(principals))
	for _, principal := range principals {
		if strings.TrimSpace(principal.Name) == "" || len(principal.Token) < 24 {
			return errors.New("admin principal name and strong token are required")
		}
		switch principal.Role {
		case RoleSuperAdmin:
		case RoleTenantAdmin, RoleOperator, RoleAuditor:
			if len(principal.TenantIDs) == 0 {
				return errors.New("tenant-scoped Admin principal requires tenant_ids")
			}
		default:
			return errors.New("unknown Admin principal role")
		}
		if _, exists := names[principal.Name]; exists {
			return errors.New("admin principal name is duplicated")
		}
		names[principal.Name] = struct{}{}
	}
	return nil
}

func authenticate(principals []Principal, authorization string) (Principal, bool) {
	const prefix = "Bearer "
	if !strings.HasPrefix(authorization, prefix) {
		return Principal{}, false
	}
	provided := strings.TrimSpace(strings.TrimPrefix(authorization, prefix))
	var matched Principal
	found := 0
	for _, principal := range principals {
		equal := subtle.ConstantTimeCompare([]byte(provided), []byte(principal.Token))
		if equal == 1 {
			matched = principal
		}
		found |= equal
	}
	return matched, found == 1
}

type principalContextKey struct{}

func contextWithPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, principalContextKey{}, principal)
}

func PrincipalName(ctx context.Context) string {
	principal, _ := ctx.Value(principalContextKey{}).(Principal)
	if principal.Name == "" {
		return "admin"
	}
	return principal.Name
}
