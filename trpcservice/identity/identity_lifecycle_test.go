package identity

import (
	"context"
	"errors"
	"testing"
)

func TestMemoryIdentityStoreProtectsAdministrativeContinuity(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryIdentityStore()
	if err := store.UpsertLoginProvider(ctx, ProviderDescriptor{ProviderID: "mock", Type: ProviderMock, DisplayName: "Mock"}, "test"); err != nil {
		t.Fatal(err)
	}
	createUser := func(subject string) PlatformUser {
		t.Helper()
		user, err := store.ResolveLoginIdentity(ctx, Identity{ProviderID: "mock", ProviderType: ProviderMock, EnterpriseID: "test", SubjectID: subject, DisplayName: subject})
		if err != nil {
			t.Fatal(err)
		}
		return user
	}
	first := createUser("first")
	second := createUser("second")

	if err := store.SetSystemAdmin(ctx, first.PlatformUserID, true); err != nil {
		t.Fatal(err)
	}
	if err := store.SetSystemAdmin(ctx, first.PlatformUserID, false); !errors.Is(err, ErrLastSystemAdmin) {
		t.Fatalf("revoke last system admin = %v, want ErrLastSystemAdmin", err)
	}
	if err := store.UpdatePlatformUserAccess(ctx, first.PlatformUserID, "suspended", true); !errors.Is(err, ErrLastSystemAdmin) {
		t.Fatalf("suspend last system admin = %v, want ErrLastSystemAdmin", err)
	}
	if err := store.SetSystemAdmin(ctx, second.PlatformUserID, true); err != nil {
		t.Fatal(err)
	}
	if err := store.SetSystemAdmin(ctx, first.PlatformUserID, false); err != nil {
		t.Fatalf("revoke with another usable system admin: %v", err)
	}

	if err := store.CreateTenant(ctx, "trailforge", "TrailForge", first.PlatformUserID); err != nil {
		t.Fatal(err)
	}
	if status, err := store.TenantStatus(ctx, "trailforge"); err != nil || status != TenantActive {
		t.Fatalf("tenant status = %q, %v", status, err)
	}
	if err := store.SetTenantStatus(ctx, "trailforge", TenantSuspended); err != nil {
		t.Fatalf("suspend tenant: %v", err)
	}
	if err := store.SetTenantStatus(ctx, "trailforge", TenantActive); err != nil {
		t.Fatalf("reactivate tenant with active admin: %v", err)
	}
	memberships, err := store.ListTenantMemberships(ctx, first.PlatformUserID)
	if err != nil || len(memberships) != 1 || memberships[0].Role != RoleAdmin || memberships[0].Status != "active" {
		t.Fatalf("initial tenant membership = %#v, %v", memberships, err)
	}
	if err := store.SetTenantMembership(ctx, "trailforge", first.PlatformUserID, RoleMember, "active"); !errors.Is(err, ErrLastTenantAdmin) {
		t.Fatalf("downgrade last tenant admin = %v, want ErrLastTenantAdmin", err)
	}
	if err := store.SetTenantMembership(ctx, "trailforge", second.PlatformUserID, RoleAdmin, "active"); err != nil {
		t.Fatal(err)
	}
	if err := store.SetTenantMembership(ctx, "trailforge", first.PlatformUserID, RoleMember, "active"); err != nil {
		t.Fatalf("downgrade with another active tenant admin: %v", err)
	}
}

func TestMemoryIdentityStoreRejectsObsoleteOperatorRole(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryIdentityStore()
	if err := store.UpsertLoginProvider(ctx, ProviderDescriptor{ProviderID: "mock", Type: ProviderMock, DisplayName: "Mock"}, "test"); err != nil {
		t.Fatal(err)
	}
	user, err := store.ResolveLoginIdentity(ctx, Identity{ProviderID: "mock", ProviderType: ProviderMock, EnterpriseID: "test", SubjectID: "user", DisplayName: "User"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertTenant(ctx, "trailforge", "TrailForge"); err != nil {
		t.Fatal(err)
	}
	if err := store.SetTenantMembership(ctx, "trailforge", user.PlatformUserID, Role("operator"), "active"); err == nil {
		t.Fatal("obsolete operator role must be rejected")
	}
}

func TestConversationContentAuditIsExplicitAdminOnlyPermission(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryIdentityStore()
	if err := store.UpsertLoginProvider(ctx, ProviderDescriptor{ProviderID: "mock", Type: ProviderMock, DisplayName: "Mock"}, "test"); err != nil {
		t.Fatal(err)
	}
	admin, err := store.ResolveLoginIdentity(ctx, Identity{ProviderID: "mock", ProviderType: ProviderMock, EnterpriseID: "test", SubjectID: "admin", DisplayName: "Admin"})
	if err != nil {
		t.Fatal(err)
	}
	member, err := store.ResolveLoginIdentity(ctx, Identity{ProviderID: "mock", ProviderType: ProviderMock, EnterpriseID: "test", SubjectID: "member", DisplayName: "Member"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateTenant(ctx, "trailforge", "TrailForge", admin.PlatformUserID); err != nil {
		t.Fatal(err)
	}
	if err := store.SetTenantMembership(ctx, "trailforge", member.PlatformUserID, RoleMember, "active"); err != nil {
		t.Fatal(err)
	}
	if err := store.SetConversationContentAudit(ctx, "trailforge", member.PlatformUserID, true); err == nil {
		t.Fatal("tenant member must not receive conversation content audit permission")
	}
	if err := store.SetConversationContentAudit(ctx, "trailforge", admin.PlatformUserID, true); err != nil {
		t.Fatalf("grant audit permission to admin: %v", err)
	}
	principal, err := store.ResolveSessionUser(ctx, admin.PlatformUserID)
	if err != nil || len(principal.Tenants) != 1 || !principal.Tenants[0].ConversationContentAudit {
		t.Fatalf("resolved audit permission = %+v, %v", principal, err)
	}
	if err := store.SetTenantMembership(ctx, "trailforge", member.PlatformUserID, RoleAdmin, "active"); err != nil {
		t.Fatal(err)
	}
	if err := store.SetTenantMembership(ctx, "trailforge", admin.PlatformUserID, RoleMember, "active"); err != nil {
		t.Fatal(err)
	}
	memberships, err := store.ListTenantMemberships(ctx, admin.PlatformUserID)
	if err != nil || len(memberships) != 1 || memberships[0].ConversationContentAudit {
		t.Fatalf("downgrade must clear content audit permission = %+v, %v", memberships, err)
	}
}
