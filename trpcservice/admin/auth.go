package admin

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	platformaudit "github.com/liuzengh/trpc-agent-service/trpcservice/audit"
)

// ErrForbidden means an authenticated control-plane principal is outside the
// requested tenant scope or lacks the operation permission.
var ErrForbidden = errors.New("admin authorization denied")

// AdminRole is the small control-plane role set. Authentication credentials
// are configured by the operator; callers cannot choose a role in a request.
type AdminRole string

const (
	RoleSystemAdmin AdminRole = "system_admin"
	RoleOperator    AdminRole = "operator"
	RoleAuditor     AdminRole = "auditor"
)

// AdminAuthConfig configures independent bearer credentials for the three
// control-plane roles. Operator and Auditor credentials must carry an
// explicit tenant allowlist. The legacy NewHTTPHandler constructor creates a
// System Admin credential for local development.
type AdminAuthConfig struct {
	SystemAdminToken  string
	OperatorToken     string
	OperatorTenantIDs []string
	AuditorToken      string
	AuditorTenantIDs  []string
}

// Validate checks that configured control-plane credentials have distinct
// tokens and explicit tenant scopes where required.
func (c AdminAuthConfig) Validate() error {
	_, err := c.credentials()
	return err
}

// AdminPrincipal is derived from a configured credential. TenantIDs is copied
// at authentication time and is never accepted from an HTTP header.
type AdminPrincipal struct {
	Role      AdminRole
	ActorID   string
	TenantIDs []string
}

func (p AdminPrincipal) AllowsTenant(tenantID string) bool {
	if strings.TrimSpace(tenantID) == "" {
		return false
	}
	if p.Role == RoleSystemAdmin {
		return true
	}
	for _, allowed := range p.TenantIDs {
		if allowed == tenantID {
			return true
		}
	}
	return false
}

func (p AdminPrincipal) CanMutate() bool {
	return p.Role == RoleSystemAdmin || p.Role == RoleOperator
}

func (p AdminPrincipal) CanProvision() bool {
	return p.Role == RoleSystemAdmin
}

type adminCredential struct {
	token     string
	principal AdminPrincipal
}

func (c AdminAuthConfig) credentials() ([]adminCredential, error) {
	if strings.TrimSpace(c.SystemAdminToken) == "" {
		return nil, errors.New("system admin token is required")
	}
	if err := validateTenantIDs(c.OperatorTenantIDs, c.OperatorToken != ""); err != nil {
		return nil, fmt.Errorf("operator tenant scope: %w", err)
	}
	if err := validateTenantIDs(c.AuditorTenantIDs, c.AuditorToken != ""); err != nil {
		return nil, fmt.Errorf("auditor tenant scope: %w", err)
	}
	credentials := []adminCredential{{
		token:     strings.TrimSpace(c.SystemAdminToken),
		principal: AdminPrincipal{Role: RoleSystemAdmin, ActorID: "admin:system"},
	}}
	if strings.TrimSpace(c.OperatorToken) != "" {
		credentials = append(credentials, adminCredential{
			token: strings.TrimSpace(c.OperatorToken),
			principal: AdminPrincipal{
				Role: RoleOperator, ActorID: "admin:operator",
				TenantIDs: slices.Clone(c.OperatorTenantIDs),
			},
		})
	}
	if strings.TrimSpace(c.AuditorToken) != "" {
		credentials = append(credentials, adminCredential{
			token: strings.TrimSpace(c.AuditorToken),
			principal: AdminPrincipal{
				Role: RoleAuditor, ActorID: "admin:auditor",
				TenantIDs: slices.Clone(c.AuditorTenantIDs),
			},
		})
	}
	for i := range credentials {
		for j := i + 1; j < len(credentials); j++ {
			if credentials[i].token == credentials[j].token {
				return nil, errors.New("admin role tokens must be distinct")
			}
		}
	}
	return credentials, nil
}

func validateTenantIDs(values []string, required bool) error {
	if !required && len(values) == 0 {
		return nil
	}
	if required && len(values) == 0 {
		return errors.New("tenant allowlist is required when the role token is configured")
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return errors.New("tenant allowlist contains an empty tenant")
		}
		if _, ok := seen[value]; ok {
			return fmt.Errorf("tenant %q is duplicated", value)
		}
		seen[value] = struct{}{}
	}
	return nil
}

type principalContextKey struct{}

func withPrincipal(ctx context.Context, principal AdminPrincipal) context.Context {
	ctx = context.WithValue(ctx, principalContextKey{}, principal)
	return platformaudit.WithControlPlaneActor(ctx, principal.ActorID, string(principal.Role))
}

// PrincipalFromContext returns the authenticated control-plane principal.
func PrincipalFromContext(ctx context.Context) (AdminPrincipal, bool) {
	if ctx == nil {
		return AdminPrincipal{}, false
	}
	principal, ok := ctx.Value(principalContextKey{}).(AdminPrincipal)
	return principal, ok
}
