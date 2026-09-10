package observability

import (
	"context"
	"errors"
	"strconv"
	"testing"
)

func TestAuthorizeTenantEnforcesTrustedScope(t *testing.T) {
	authorizer := NewStaticTenantAuthorizer(map[string][]string{"admin": {"tenant-a"}, "user": {"tenant-b"}})
	if err := AuthorizeTenant(context.Background(), authorizer, "admin", "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(AuthorizeTenant(context.Background(), authorizer, "admin", "tenant-b"), ErrTenantAccessDenied) {
		t.Fatal("cross-tenant query must be denied")
	}
	if !errors.Is(AuthorizeTenant(context.Background(), authorizer, "", "tenant-a"), ErrInvalidTenantScope) {
		t.Fatal("empty actor must be rejected")
	}
}

func TestValidateTenantQueryScopeRejectsUnboundedInput(t *testing.T) {
	if err := ValidateTenantQueryScope([]string{"tenant-a", "tenant-b"}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateTenantQueryScope(nil); err == nil {
		t.Fatal("empty scope must be rejected")
	}
	if err := ValidateTenantQueryScope([]string{"tenant-a", "tenant-a"}); err == nil {
		t.Fatal("duplicate scope must be rejected")
	}
	scope := make([]string, maxDashboardTenantScope+1)
	for index := range scope {
		scope[index] = "tenant-" + strconv.Itoa(index)
	}
	if err := ValidateTenantQueryScope(scope); !errors.Is(err, ErrInvalidTenantScope) {
		t.Fatalf("oversized scope error = %v", err)
	}
}

func TestBuildDashboardQuerySeparatesPlatformAndTenantViews(t *testing.T) {
	authorizer := NewStaticTenantAuthorizerWithPlatformActors(map[string][]string{"operator": {"tenant-a"}, "tenant-user": {"tenant-a"}}, []string{"operator"})
	platform, err := BuildDashboardQuery(context.Background(), authorizer, "operator", DashboardViewPlatform, nil, "request_rate")
	if err != nil || platform.PromQL == "" || platform.TenantHashes != nil {
		t.Fatalf("platform query = %#v, err=%v", platform, err)
	}
	if _, err := BuildDashboardQuery(context.Background(), authorizer, "tenant-user", DashboardViewPlatform, nil, "request_rate"); !errors.Is(err, ErrTenantAccessDenied) {
		t.Fatalf("non-platform actor error = %v", err)
	}
	tenant, err := BuildDashboardQuery(context.Background(), authorizer, "tenant-user", DashboardViewTenant, []string{"tenant-a"}, "usage")
	if err != nil || len(tenant.TenantHashes) != 1 || tenant.TenantHashes[0] == "tenant-a" || tenant.PromQL != "" {
		t.Fatalf("tenant query = %#v, err=%v", tenant, err)
	}
	if _, err := BuildDashboardQuery(context.Background(), authorizer, "tenant-user", DashboardViewTenant, []string{"tenant-a"}, "error_rate"); !errors.Is(err, ErrTenantAccessDenied) {
		t.Fatalf("tenant process panel error = %v", err)
	}
}
