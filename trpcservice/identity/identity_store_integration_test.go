package identity

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/migrations"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

func testIdentityStore(t *testing.T) *PostgresIdentityStore {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	if !strings.Contains(dsn, "test") {
		t.Skip("TEST_POSTGRES_DSN must contain 'test' (protects the dev database)")
	}
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := migrations.Apply(context.Background(), database); err != nil {
		t.Fatalf("migrations.Apply() error = %v", err)
	}
	store, err := NewPostgresIdentityStore(database)
	if err != nil {
		t.Fatalf("NewPostgresIdentityStore() error = %v", err)
	}
	return store
}

func registerPostgresTestProvider(t *testing.T, store IdentityStore, providerID, subjectID string) Identity {
	t.Helper()
	boundary := "test:" + providerID
	if err := store.UpsertLoginProvider(context.Background(), ProviderDescriptor{
		ProviderID:  providerID,
		Type:        ProviderMock,
		DisplayName: "测试登录",
	}, boundary); err != nil {
		t.Fatalf("UpsertLoginProvider() error = %v", err)
	}
	return Identity{
		ProviderID:   providerID,
		ProviderType: ProviderMock,
		EnterpriseID: boundary,
		SubjectID:    subjectID,
		DisplayName:  "客服用户",
	}
}

func TestPostgresIdentityStoreUserLifecycle(t *testing.T) {
	store := testIdentityStore(t)
	ctx := context.Background()
	external := registerPostgresTestProvider(t, store, "support-login", "support-user")

	first, err := store.ResolveLoginIdentity(ctx, external)
	if err != nil {
		t.Fatalf("ResolveLoginIdentity() error = %v", err)
	}
	second, err := store.ResolveLoginIdentity(ctx, external)
	if err != nil {
		t.Fatalf("second ResolveLoginIdentity() error = %v", err)
	}
	if first.PlatformUserID == "" || first.PlatformUserID != second.PlatformUserID {
		t.Fatalf("platform users = %#v / %#v", first, second)
	}
	login, err := store.LookupLoginIdentity(ctx, external.ProviderID, external.SubjectID)
	if err != nil {
		t.Fatalf("LookupLoginIdentity() error = %v", err)
	}
	if login.PlatformUserID != first.PlatformUserID {
		t.Fatalf("login = %+v", login)
	}
	if _, err := store.LookupLoginIdentity(ctx, external.ProviderID, "missing"); !errors.Is(err, ErrPlatformUserNotFound) {
		t.Fatalf("LookupLoginIdentity(missing) error = %v, want ErrPlatformUserNotFound", err)
	}
}

func TestPostgresIdentityStoreMemberships(t *testing.T) {
	store := testIdentityStore(t)
	ctx := context.Background()
	external := registerPostgresTestProvider(t, store, "support-membership-login", "support-user")

	user, err := store.ResolveLoginIdentity(ctx, external)
	if err != nil {
		t.Fatalf("ResolveLoginIdentity() error = %v", err)
	}
	if _, err := store.database.ExecContext(ctx,
		`INSERT INTO tenants (id, display_name) VALUES ('support-test', '客服业务')
		 ON CONFLICT (id) DO UPDATE SET display_name=EXCLUDED.display_name`); err != nil {
		t.Fatalf("seed tenant error = %v", err)
	}
	if err := store.GrantMembership(ctx, "support-test", user.PlatformUserID, RoleAdmin); err != nil {
		t.Fatalf("GrantMembership() error = %v", err)
	}
	memberships, err := store.ListTenantMemberships(ctx, user.PlatformUserID)
	if err != nil {
		t.Fatalf("ListTenantMemberships() error = %v", err)
	}
	if len(memberships) != 1 || memberships[0].TenantID != "support-test" || memberships[0].PlatformUserID != user.PlatformUserID || memberships[0].Role != RoleAdmin {
		t.Fatalf("ListTenantMemberships() = %+v", memberships)
	}
	role, err := store.RoleFor(ctx, "support-test", user.PlatformUserID)
	if err != nil || role != RoleAdmin {
		t.Fatalf("RoleFor() = %q, %v; want admin", role, err)
	}
	summaries, err := store.ListTenantSummaries(ctx, user.PlatformUserID, false)
	if err != nil || len(summaries) != 1 || summaries[0].DisplayName != "客服业务" {
		t.Fatalf("ListTenantSummaries() = %+v, %v", summaries, err)
	}
	if _, err := store.RoleFor(ctx, "missing-tenant", user.PlatformUserID); err == nil {
		t.Fatal("RoleFor(non-member tenant) error = nil, want failure")
	}
}

func TestPostgresIdentityStoreAdministrativeAndChannelLifecycle(t *testing.T) {
	store := testIdentityStore(t)
	ctx := context.Background()
	suffix := uuid.NewString()

	adminIdentity := registerPostgresTestProvider(t, store, "support-admin-"+suffix, "support-admin")
	adminIdentity.DisplayName = "客服管理员"
	admin, err := store.ResolveLoginIdentity(ctx, adminIdentity)
	if err != nil {
		t.Fatalf("ResolveLoginIdentity(admin) error = %v", err)
	}
	memberIdentity := registerPostgresTestProvider(t, store, "support-member-"+suffix, "support-member")
	memberIdentity.DisplayName = "客服成员"
	member, err := store.ResolveLoginIdentity(ctx, memberIdentity)
	if err != nil {
		t.Fatalf("ResolveLoginIdentity(member) error = %v", err)
	}

	methods, err := store.ListLoginMethods(ctx, admin.PlatformUserID)
	if err != nil || len(methods) != 1 || methods[0].ProviderID != adminIdentity.ProviderID {
		t.Fatalf("ListLoginMethods() = %+v, %v", methods, err)
	}
	if _, ok, err := store.LatestLoginAtForProvider(ctx, adminIdentity.ProviderID); err != nil || !ok {
		t.Fatalf("LatestLoginAtForProvider() ok=%v error=%v", ok, err)
	}

	localUsername := "support-" + strings.ReplaceAll(suffix, "-", "")[:12]
	local, err := store.CreateLocalUser(ctx, localUsername, "客服本地用户", "", "hash-v1", true)
	if err != nil {
		t.Fatalf("CreateLocalUser() error = %v", err)
	}
	credential, localUser, err := store.LookupLocalCredential(ctx, localUsername)
	if err != nil || localUser.PlatformUserID != local.PlatformUserID || credential.PasswordHash != "hash-v1" || !credential.MustChangePassword {
		t.Fatalf("LookupLocalCredential() = %+v, %+v, %v", credential, localUser, err)
	}
	if _, err := store.LocalCredentialForUser(ctx, local.PlatformUserID); err != nil {
		t.Fatalf("LocalCredentialForUser() error = %v", err)
	}
	if err := store.SetLocalPassword(ctx, local.PlatformUserID, "hash-v2", false); err != nil {
		t.Fatalf("SetLocalPassword() error = %v", err)
	}

	if usable, err := store.HasUsableSystemAdmin(ctx); err != nil {
		t.Fatalf("HasUsableSystemAdmin() error = %v", err)
	} else if usable {
		t.Fatal("HasUsableSystemAdmin() = true before administrator grant")
	}
	if err := store.SetSystemAdmin(ctx, admin.PlatformUserID, true); err != nil {
		t.Fatalf("SetSystemAdmin(admin,true) error = %v", err)
	}
	if isAdmin, err := store.IsSystemAdmin(ctx, admin.PlatformUserID); err != nil || !isAdmin {
		t.Fatalf("IsSystemAdmin() = %v, %v", isAdmin, err)
	}
	if err := store.SetSystemAdmin(ctx, admin.PlatformUserID, false); !errors.Is(err, ErrLastSystemAdmin) {
		t.Fatalf("SetSystemAdmin(last,false) error = %v, want ErrLastSystemAdmin", err)
	}
	if err := store.SetSystemAdmin(ctx, member.PlatformUserID, true); err != nil {
		t.Fatalf("SetSystemAdmin(member,true) error = %v", err)
	}
	if err := store.SetSystemAdmin(ctx, admin.PlatformUserID, false); err != nil {
		t.Fatalf("SetSystemAdmin(admin,false) error = %v", err)
	}

	if err := store.UpdatePlatformUserProfile(ctx, admin.PlatformUserID, "客服负责人"); err != nil {
		t.Fatalf("UpdatePlatformUserProfile() error = %v", err)
	}
	page, err := store.ListPlatformUsers(ctx, MemberPageRequest{Limit: 1, Query: "客服"})
	if err != nil || len(page.Members) != 1 || page.Next == nil {
		t.Fatalf("ListPlatformUsers(first page) = %+v, %v", page, err)
	}
	next, err := store.ListPlatformUsers(ctx, MemberPageRequest{Limit: 20, Query: "客服", After: page.Next})
	if err != nil || len(next.Members) == 0 {
		t.Fatalf("ListPlatformUsers(next page) = %+v, %v", next, err)
	}

	tenantID := "support-tenant-" + suffix
	if err := store.CreateTenant(ctx, tenantID, "客服业务", admin.PlatformUserID); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	if status, err := store.TenantStatus(ctx, tenantID); err != nil || status != TenantActive {
		t.Fatalf("TenantStatus() = %q, %v", status, err)
	}
	if err := store.SetTenantStatus(ctx, tenantID, TenantSuspended); err != nil {
		t.Fatalf("SetTenantStatus(suspended) error = %v", err)
	}
	if err := store.SetTenantStatus(ctx, tenantID, TenantActive); err != nil {
		t.Fatalf("SetTenantStatus(active) error = %v", err)
	}
	wantGrants := []TenantModelGrant{{ProviderID: "primary", ModelName: "support-model"}, {ProviderID: "secondary", ModelName: "fallback-model"}}
	if err := store.ReplaceTenantModelGrants(ctx, tenantID, wantGrants); err != nil {
		t.Fatalf("ReplaceTenantModelGrants() error = %v", err)
	}
	grants, err := store.ListTenantModelGrants(ctx, tenantID)
	if err != nil || !reflect.DeepEqual(grants, wantGrants) {
		t.Fatalf("ListTenantModelGrants() = %+v, %v", grants, err)
	}

	if err := store.SetTenantMembership(ctx, tenantID, member.PlatformUserID, RoleMember, "active"); err != nil {
		t.Fatalf("SetTenantMembership(member) error = %v", err)
	}
	if err := store.SetConversationContentAudit(ctx, tenantID, admin.PlatformUserID, true); err != nil {
		t.Fatalf("SetConversationContentAudit(admin) error = %v", err)
	}
	if err := store.SetConversationContentAudit(ctx, tenantID, member.PlatformUserID, true); err == nil {
		t.Fatal("SetConversationContentAudit(member) error = nil")
	}
	members, err := store.ListTenantMembers(ctx, tenantID, MemberPageRequest{Limit: 10})
	if err != nil || len(members.Members) != 2 {
		t.Fatalf("ListTenantMembers() = %+v, %v", members, err)
	}
	candidates, err := store.ListTenantMemberCandidates(ctx, tenantID, MemberPageRequest{Limit: 50})
	if err != nil {
		t.Fatalf("ListTenantMemberCandidates() error = %v", err)
	}
	for _, candidate := range candidates.Members {
		if candidate.PlatformUserID == admin.PlatformUserID || candidate.PlatformUserID == member.PlatformUserID {
			t.Fatalf("member leaked into candidates: %+v", candidate)
		}
	}
	principal, err := store.ResolveSessionUser(ctx, admin.PlatformUserID)
	if err != nil || len(principal.Tenants) != 1 || principal.Tenants[0].TenantID != tenantID || !principal.Tenants[0].ConversationContentAudit {
		t.Fatalf("ResolveSessionUser() = %+v, %v", principal, err)
	}

	appCode := "support"
	bindingID := "support-bot-" + suffix
	if _, err := store.database.ExecContext(ctx, `INSERT INTO applications (tenant_id,app_code,status) VALUES ($1,$2,'active')`, tenantID, appCode); err != nil {
		t.Fatalf("seed application: %v", err)
	}
	if _, err := store.database.ExecContext(ctx, `INSERT INTO channel_bindings (channel_type,external_binding_id,tenant_id,app_code,access_policy,allowlist) VALUES ('telegram',$1,$2,$3,'public','[]')`, bindingID, tenantID, appCode); err != nil {
		t.Fatalf("seed channel binding: %v", err)
	}
	resolved, linked, err := store.ResolveChannelIdentity(ctx, tenantID, channels.Telegram, bindingID, "customer-42", "")
	if err != nil || linked || resolved.PlatformUserID != "" {
		t.Fatalf("ResolveChannelIdentity(unlinked) = %+v, linked=%v, %v", resolved, linked, err)
	}
	link := ChannelIdentity{TenantID: tenantID, Channel: channels.Telegram, BindingID: bindingID, ExternalUserID: "customer-42", PlatformUserID: admin.PlatformUserID}
	if err := store.LinkChannelIdentity(ctx, link); err != nil {
		t.Fatalf("LinkChannelIdentity() error = %v", err)
	}
	conflict := link
	conflict.PlatformUserID = member.PlatformUserID
	if err := store.LinkChannelIdentity(ctx, conflict); !errors.Is(err, ErrChannelIdentityConflict) {
		t.Fatalf("LinkChannelIdentity(conflict) error = %v", err)
	}
	resolved, linked, err = store.ResolveChannelIdentity(ctx, tenantID, channels.Telegram, bindingID, "customer-42", "")
	if err != nil || !linked || resolved.PlatformUserID != admin.PlatformUserID {
		t.Fatalf("ResolveChannelIdentity(linked) = %+v, linked=%v, %v", resolved, linked, err)
	}
	linkedIdentities, err := store.ListChannelIdentities(ctx, tenantID, admin.PlatformUserID)
	if err != nil || len(linkedIdentities) != 1 {
		t.Fatalf("ListChannelIdentities() = %+v, %v", linkedIdentities, err)
	}
	if err := store.UnlinkChannelIdentity(ctx, link); err != nil {
		t.Fatalf("UnlinkChannelIdentity() error = %v", err)
	}
	if err := store.UnlinkChannelIdentity(ctx, link); !errors.Is(err, ErrChannelIdentityNotLinked) {
		t.Fatalf("UnlinkChannelIdentity(second) error = %v", err)
	}
}
