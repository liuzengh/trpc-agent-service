package identity

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

func newSupportUser(t *testing.T, store *MemoryIdentityStore, subject string) PlatformUser {
	t.Helper()
	ctx := context.Background()
	if _, ok := store.providers["support-login"]; !ok {
		if err := store.UpsertLoginProvider(ctx, ProviderDescriptor{ProviderID: "support-login", Type: ProviderMock, DisplayName: "测试登录"}, "support-boundary"); err != nil {
			t.Fatal(err)
		}
	}
	user, err := store.ResolveLoginIdentity(ctx, Identity{
		ProviderID: "support-login", ProviderType: ProviderMock, EnterpriseID: "support-boundary",
		SubjectID: subject, DisplayName: "客服用户 " + subject, Email: subject + "@example.test",
	})
	if err != nil {
		t.Fatal(err)
	}
	return user
}

func TestMemoryIdentityStoreLocalCredentialLifecycle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryIdentityStore()

	user, err := store.CreateLocalUser(ctx, " support.agent ", "", "support@example.test", "hash-v1", true)
	if err != nil {
		t.Fatalf("CreateLocalUser() error = %v", err)
	}
	if user.DisplayName != "support.agent" || user.Email != "support@example.test" {
		t.Fatalf("local user = %+v", user)
	}
	if _, err := store.CreateLocalUser(ctx, "support.agent", "重复", "", "hash-v1", false); !errors.Is(err, ErrLocalUsernameTaken) {
		t.Fatalf("duplicate local user error = %v", err)
	}
	if _, err := store.CreateLocalUser(ctx, "bad user", "", "", "hash-v1", false); err == nil {
		t.Fatal("invalid username error = nil")
	}
	if _, err := store.CreateLocalUser(ctx, "support.two", "", "", "", false); err == nil {
		t.Fatal("empty password hash error = nil")
	}

	credential, gotUser, err := store.LookupLocalCredential(ctx, " SUPPORT.AGENT ")
	if err != nil || gotUser.PlatformUserID != user.PlatformUserID || !credential.MustChangePassword || credential.PasswordHash != "hash-v1" {
		t.Fatalf("LookupLocalCredential() = %+v, %+v, %v", credential, gotUser, err)
	}
	byUser, err := store.LocalCredentialForUser(ctx, user.PlatformUserID)
	if err != nil || byUser.Username != "support.agent" {
		t.Fatalf("LocalCredentialForUser() = %+v, %v", byUser, err)
	}
	if _, err := store.LocalCredentialForUser(ctx, "missing"); !errors.Is(err, ErrLocalCredentialNotFound) {
		t.Fatalf("LocalCredentialForUser(missing) error = %v", err)
	}
	if _, _, err := store.LookupLocalCredential(ctx, "bad user"); !errors.Is(err, ErrLocalCredentialNotFound) {
		t.Fatalf("LookupLocalCredential(invalid) error = %v", err)
	}
	if _, _, err := store.LookupLocalCredential(ctx, "missing"); !errors.Is(err, ErrLocalCredentialNotFound) {
		t.Fatalf("LookupLocalCredential(missing) error = %v", err)
	}

	principal, err := store.ResolveSessionUser(ctx, user.PlatformUserID)
	if err != nil || !principal.MustChangePassword {
		t.Fatalf("ResolveSessionUser() = %+v, %v", principal, err)
	}
	if err := store.SetLocalPassword(ctx, user.PlatformUserID, "hash-v2", false); err != nil {
		t.Fatal(err)
	}
	updated, _ := store.LocalCredentialForUser(ctx, user.PlatformUserID)
	if updated.PasswordHash != "hash-v2" || updated.MustChangePassword {
		t.Fatalf("updated credential = %+v", updated)
	}
	if err := store.SetLocalPassword(ctx, user.PlatformUserID, "", false); err == nil {
		t.Fatal("empty password hash error = nil")
	}
	if err := store.SetLocalPassword(ctx, "missing", "hash", false); !errors.Is(err, ErrLocalCredentialNotFound) {
		t.Fatalf("SetLocalPassword(missing) error = %v", err)
	}

	store.mu.Lock()
	delete(store.users, user.PlatformUserID)
	store.mu.Unlock()
	if _, _, err := store.LookupLocalCredential(ctx, "support.agent"); !errors.Is(err, ErrLocalCredentialNotFound) {
		t.Fatalf("orphan credential error = %v", err)
	}
}

func TestMemoryIdentityStoreLoginLinkingLifecycle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryIdentityStore()
	owner := newSupportUser(t, store, "owner")
	other := newSupportUser(t, store, "other")

	if err := store.UpsertLoginProvider(ctx, ProviderDescriptor{ProviderID: "secondary-login", Type: ProviderMock, DisplayName: "备用测试登录"}, "secondary-boundary"); err != nil {
		t.Fatal(err)
	}
	linked := Identity{ProviderID: "secondary-login", ProviderType: ProviderMock, EnterpriseID: "secondary-boundary", SubjectID: "owner-alt", DisplayName: "备用客服用户", Email: "alt@example.test"}
	if err := store.LinkLoginIdentity(ctx, owner.PlatformUserID, linked); err != nil {
		t.Fatalf("LinkLoginIdentity() error = %v", err)
	}
	linked.DisplayName = ""
	linked.Email = ""
	if err := store.LinkLoginIdentity(ctx, owner.PlatformUserID, linked); err != nil {
		t.Fatalf("re-link same identity error = %v", err)
	}
	login, err := store.LookupLoginIdentity(ctx, linked.ProviderID, linked.SubjectID)
	if err != nil || login.DisplayName != "备用客服用户" || login.Email != "alt@example.test" {
		t.Fatalf("linked login = %+v, %v", login, err)
	}
	if err := store.LinkLoginIdentity(ctx, other.PlatformUserID, linked); !errors.Is(err, ErrLoginIdentityConflict) {
		t.Fatalf("identity takeover error = %v", err)
	}
	if err := store.LinkLoginIdentity(ctx, "missing", linked); !errors.Is(err, ErrPlatformUserNotFound) {
		t.Fatalf("missing owner error = %v", err)
	}
	wrongBoundary := linked
	wrongBoundary.SubjectID = "wrong-boundary"
	wrongBoundary.EnterpriseID = "other-boundary"
	if err := store.LinkLoginIdentity(ctx, owner.PlatformUserID, wrongBoundary); err == nil {
		t.Fatal("provider boundary mismatch error = nil")
	}
	local := linked
	local.ProviderType = ProviderLocal
	if err := store.LinkLoginIdentity(ctx, owner.PlatformUserID, local); err == nil {
		t.Fatal("link local identity error = nil")
	}

	methods, err := store.ListLoginMethods(ctx, owner.PlatformUserID)
	if err != nil || len(methods) != 2 || methods[0].ProviderID != "secondary-login" || methods[1].ProviderID != "support-login" {
		t.Fatalf("ListLoginMethods() = %+v, %v", methods, err)
	}
	if _, ok, err := store.LatestLoginAtForProvider(ctx, "secondary-login"); err != nil || !ok {
		t.Fatalf("LatestLoginAtForProvider() ok=%v err=%v", ok, err)
	}
	if _, ok, err := store.LatestLoginAtForProvider(ctx, "unused-provider"); err != nil || ok {
		t.Fatalf("LatestLoginAtForProvider(unused) ok=%v err=%v", ok, err)
	}
	if _, _, err := store.LatestLoginAtForProvider(ctx, " "); err == nil {
		t.Fatal("LatestLoginAtForProvider(empty) error = nil")
	}

	if err := store.RemoveLoginIdentity(ctx, owner.PlatformUserID, "secondary-login", "missing"); !errors.Is(err, ErrPlatformUserNotFound) {
		t.Fatalf("RemoveLoginIdentity(missing) error = %v", err)
	}
	if err := store.RemoveLoginIdentity(ctx, owner.PlatformUserID, linked.ProviderID, linked.SubjectID); err != nil {
		t.Fatalf("RemoveLoginIdentity() error = %v", err)
	}
	if err := store.RemoveLoginIdentity(ctx, owner.PlatformUserID, "support-login", "owner"); !errors.Is(err, ErrLastLoginIdentity) {
		t.Fatalf("remove last login error = %v", err)
	}
}

func TestMemoryIdentityStoreRemovesLocalCredentialWhenLocalLoginIsUnlinked(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryIdentityStore()
	local, err := store.CreateLocalUser(ctx, "support.local", "客服本地用户", "", "hash", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertLoginProvider(ctx, ProviderDescriptor{ProviderID: "backup", Type: ProviderMock, DisplayName: "备用登录"}, "backup-boundary"); err != nil {
		t.Fatal(err)
	}
	if err := store.LinkLoginIdentity(ctx, local.PlatformUserID, Identity{ProviderID: "backup", ProviderType: ProviderMock, EnterpriseID: "backup-boundary", SubjectID: "support-local"}); err != nil {
		t.Fatal(err)
	}
	if err := store.RemoveLoginIdentity(ctx, local.PlatformUserID, "local", "support.local"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LocalCredentialForUser(ctx, local.PlatformUserID); !errors.Is(err, ErrLocalCredentialNotFound) {
		t.Fatalf("local credential after unlink error = %v", err)
	}
}

func TestMemoryIdentityStoreTenantModelsRolesAndVisibility(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryIdentityStore()
	admin := newSupportUser(t, store, "admin")
	member := newSupportUser(t, store, "member")
	if err := store.CreateTenant(ctx, "support", "客服业务", admin.PlatformUserID); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateTenant(ctx, "support", "重复", admin.PlatformUserID); err == nil {
		t.Fatal("duplicate tenant error = nil")
	}
	if err := store.CreateTenant(ctx, "", "客服业务", admin.PlatformUserID); err == nil {
		t.Fatal("invalid tenant error = nil")
	}
	if err := store.CreateTenant(ctx, "missing-admin", "客服业务", "missing"); !errors.Is(err, ErrPlatformUserNotFound) {
		t.Fatalf("missing initial admin error = %v", err)
	}

	want := []TenantModelGrant{{ProviderID: "secondary", ModelName: "fallback"}, {ProviderID: "primary", ModelName: "support"}}
	if err := store.ReplaceTenantModelGrants(ctx, "support", want); err != nil {
		t.Fatal(err)
	}
	got, err := store.ListTenantModelGrants(ctx, "support")
	if err != nil || !reflect.DeepEqual(got, []TenantModelGrant{{ProviderID: "primary", ModelName: "support"}, {ProviderID: "secondary", ModelName: "fallback"}}) {
		t.Fatalf("ListTenantModelGrants() = %+v, %v", got, err)
	}
	got[0].ModelName = "mutated"
	again, _ := store.ListTenantModelGrants(ctx, "support")
	if again[0].ModelName != "support" {
		t.Fatal("tenant model grants alias internal state")
	}
	if _, err := store.ListTenantModelGrants(ctx, "missing"); !errors.Is(err, ErrTenantNotFound) {
		t.Fatalf("ListTenantModelGrants(missing) error = %v", err)
	}
	if err := store.ReplaceTenantModelGrants(ctx, "missing", nil); !errors.Is(err, ErrTenantNotFound) {
		t.Fatalf("ReplaceTenantModelGrants(missing) error = %v", err)
	}
	if err := store.ReplaceTenantModelGrants(ctx, "support", []TenantModelGrant{{ProviderID: "", ModelName: "bad"}}); err == nil {
		t.Fatal("invalid model grant error = nil")
	}

	if err := store.GrantMembership(ctx, "support", member.PlatformUserID, RoleMember); err != nil {
		t.Fatal(err)
	}
	if role, err := store.RoleFor(ctx, "support", member.PlatformUserID); err != nil || role != RoleMember {
		t.Fatalf("RoleFor() = %q, %v", role, err)
	}
	if err := store.SetTenantMembership(ctx, "support", member.PlatformUserID, RoleMember, "suspended"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RoleFor(ctx, "support", member.PlatformUserID); err == nil {
		t.Fatal("RoleFor(suspended) error = nil")
	}
	all, err := store.ListTenantSummaries(ctx, member.PlatformUserID, true)
	if err != nil || len(all) != 1 || all[0].Role != "" {
		t.Fatalf("ListTenantSummaries(includeAll) = %+v, %v", all, err)
	}
}

func TestMemoryIdentityStoreAccessValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryIdentityStore()
	first := newSupportUser(t, store, "first")
	second := newSupportUser(t, store, "second")

	if err := store.SetSystemAdmin(ctx, "missing", true); !errors.Is(err, ErrPlatformUserNotFound) {
		t.Fatalf("SetSystemAdmin(missing) error = %v", err)
	}
	store.users[first.PlatformUserID] = PlatformUser{PlatformUserID: first.PlatformUserID, Status: "suspended"}
	if err := store.SetSystemAdmin(ctx, first.PlatformUserID, true); err == nil {
		t.Fatal("suspended system admin error = nil")
	}
	store.users[first.PlatformUserID] = first

	if err := store.UpdatePlatformUserProfile(ctx, first.PlatformUserID, ""); err == nil {
		t.Fatal("empty display name error = nil")
	}
	if err := store.UpdatePlatformUserProfile(ctx, first.PlatformUserID, strings.Repeat("界", 81)); err == nil {
		t.Fatal("long display name error = nil")
	}
	if err := store.UpdatePlatformUserProfile(ctx, "missing", "客服用户"); !errors.Is(err, ErrPlatformUserNotFound) {
		t.Fatalf("UpdatePlatformUserProfile(missing) error = %v", err)
	}
	if err := store.UpdatePlatformUserAccess(ctx, first.PlatformUserID, "invalid", false); err == nil {
		t.Fatal("invalid access status error = nil")
	}
	if err := store.UpdatePlatformUserAccess(ctx, "missing", "active", false); !errors.Is(err, ErrPlatformUserNotFound) {
		t.Fatalf("UpdatePlatformUserAccess(missing) error = %v", err)
	}
	if err := store.UpdatePlatformUserAccess(ctx, first.PlatformUserID, "suspended", true); err == nil {
		t.Fatal("suspended system admin assignment error = nil")
	}

	if err := store.CreateTenant(ctx, "support", "客服业务", first.PlatformUserID); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdatePlatformUserAccess(ctx, first.PlatformUserID, "suspended", false); !errors.Is(err, ErrLastTenantAdmin) {
		t.Fatalf("suspend last tenant admin error = %v", err)
	}
	if err := store.GrantMembership(ctx, "support", second.PlatformUserID, RoleAdmin); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdatePlatformUserAccess(ctx, first.PlatformUserID, "suspended", false); err != nil {
		t.Fatalf("suspend with replacement admin error = %v", err)
	}
	if _, err := store.ResolveSessionUser(ctx, first.PlatformUserID); !errors.Is(err, ErrPlatformUserSuspended) {
		t.Fatalf("ResolveSessionUser(suspended) error = %v", err)
	}
	if _, err := store.ResolveSessionUser(ctx, "missing"); !errors.Is(err, ErrPlatformUserNotFound) {
		t.Fatalf("ResolveSessionUser(missing) error = %v", err)
	}
}

func TestMemoryIdentityStoreChannelIdentityUnlinkAndList(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryIdentityStore()
	user := newSupportUser(t, store, "channel-owner")
	first := ChannelIdentity{TenantID: "support", Channel: channels.Telegram, BindingID: "support-bot", ExternalUserID: "customer-1", PlatformUserID: user.PlatformUserID}
	second := ChannelIdentity{TenantID: "support", Channel: channels.Feishu, BindingID: "support-feishu", ExternalUserID: "customer-2", PlatformUserID: user.PlatformUserID}
	if err := store.LinkChannelIdentity(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := store.LinkChannelIdentity(ctx, second); err != nil {
		t.Fatal(err)
	}
	listed, err := store.ListChannelIdentities(ctx, "support", user.PlatformUserID)
	if err != nil || len(listed) != 2 || listed[0].Channel != channels.Feishu || listed[1].Channel != channels.Telegram {
		t.Fatalf("ListChannelIdentities() = %+v, %v", listed, err)
	}
	if err := store.UnlinkChannelIdentity(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := store.UnlinkChannelIdentity(ctx, first); !errors.Is(err, ErrChannelIdentityNotLinked) {
		t.Fatalf("second unlink error = %v", err)
	}
	invalid := first
	invalid.PlatformUserID = ""
	if err := store.UnlinkChannelIdentity(ctx, invalid); err == nil {
		t.Fatal("invalid unlink error = nil")
	}
	listed, _ = store.ListChannelIdentities(ctx, "support", user.PlatformUserID)
	if len(listed) != 1 || listed[0].Channel != channels.Feishu {
		t.Fatalf("channel identities after unlink = %+v", listed)
	}
}
