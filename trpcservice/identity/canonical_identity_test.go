package identity

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

func TestMemoryIdentityStoreResolvesStablePlatformUser(t *testing.T) {
	store := NewMemoryIdentityStore()
	ctx := context.Background()
	registerTestWeComProvider(t, store, "org-trailforge", "wecom-trailforge", "corp-trailforge")

	first, err := store.ResolveLoginIdentity(ctx, testWeComIdentity("org-trailforge", "wecom-trailforge", "corp-trailforge", "ming"))
	if err != nil {
		t.Fatalf("ResolveLoginIdentity() error = %v", err)
	}
	second, err := store.ResolveLoginIdentity(ctx, testWeComIdentity("org-trailforge", "wecom-trailforge", "corp-trailforge", "ming"))
	if err != nil {
		t.Fatalf("second ResolveLoginIdentity() error = %v", err)
	}
	if first.PlatformUserID == "" || first.PlatformUserID != second.PlatformUserID {
		t.Fatalf("platform user IDs = %q, %q; want one stable non-empty ID", first.PlatformUserID, second.PlatformUserID)
	}
	login, err := store.LookupLoginIdentity(ctx, "wecom-trailforge", "ming")
	if err != nil {
		t.Fatalf("LookupWeComLoginIdentity() error = %v", err)
	}
	if login.PlatformUserID != first.PlatformUserID {
		t.Fatalf("login platform user = %q, want %q", login.PlatformUserID, first.PlatformUserID)
	}
}

func TestMemoryIdentityStoreKeepsPlatformNicknameAcrossProviderRefresh(t *testing.T) {
	store := NewMemoryIdentityStore()
	ctx := context.Background()
	registerTestWeComProvider(t, store, "org-trailforge", "wecom-trailforge", "corp-trailforge")
	external := testWeComIdentity("org-trailforge", "wecom-trailforge", "corp-trailforge", "ming")
	external.DisplayName = "企业通讯录名称"
	user, err := store.ResolveLoginIdentity(ctx, external)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdatePlatformUserProfile(ctx, user.PlatformUserID, "我的平台昵称"); err != nil {
		t.Fatal(err)
	}
	external.DisplayName = "企业通讯录新名称"
	refreshed, err := store.ResolveLoginIdentity(ctx, external)
	if err != nil {
		t.Fatal(err)
	}
	if refreshed.DisplayName != "我的平台昵称" {
		t.Fatalf("platform nickname = %q, want user-edited nickname", refreshed.DisplayName)
	}
}

func TestMemoryIdentityStoreMembershipsUsePlatformUser(t *testing.T) {
	store := NewMemoryIdentityStore()
	ctx := context.Background()
	registerTestWeComProvider(t, store, "org-trailforge", "wecom-trailforge", "corp-trailforge")
	user, err := store.ResolveLoginIdentity(ctx, testWeComIdentity("org-trailforge", "wecom-trailforge", "corp-trailforge", "ming"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertTenant(ctx, "trailforge", "TrailForge"); err != nil {
		t.Fatal(err)
	}
	if err := store.GrantMembership(ctx, "trailforge", user.PlatformUserID, RoleAdmin); err != nil {
		t.Fatal(err)
	}

	memberships, err := store.ListTenantMemberships(ctx, user.PlatformUserID)
	if err != nil {
		t.Fatal(err)
	}
	if len(memberships) != 1 || memberships[0].PlatformUserID != user.PlatformUserID || memberships[0].Role != RoleAdmin {
		t.Fatalf("memberships = %#v", memberships)
	}
	summaries, err := store.ListTenantSummaries(ctx, user.PlatformUserID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 1 || summaries[0].TenantID != "trailforge" || summaries[0].DisplayName != "TrailForge" || summaries[0].Role != RoleAdmin {
		t.Fatalf("tenant summaries = %#v", summaries)
	}
}

func TestMemoryIdentityStoreMemberPagesAndCandidates(t *testing.T) {
	store := NewMemoryIdentityStore()
	ctx := context.Background()
	registerTestWeComProvider(t, store, "org-trailforge", "wecom-trailforge", "corp-trailforge")

	users := make([]PlatformUser, 0, 3)
	for _, item := range []struct {
		subject string
		name    string
		email   string
	}{{"alice", "Alice", "alice@example.com"}, {"bob", "Bob", "bob@example.com"}, {"carol", "Carol", "carol@example.com"}} {
		user, err := store.ResolveLoginIdentity(ctx, Identity{
			ProviderID: "wecom-trailforge", ProviderType: ProviderWeCom, EnterpriseID: "corp-trailforge",
			SubjectID: item.subject, DisplayName: item.name, Email: item.email,
		})
		if err != nil {
			t.Fatal(err)
		}
		users = append(users, user)
	}

	first, err := store.ListPlatformUsers(ctx, MemberPageRequest{Limit: 2})
	if err != nil || len(first.Members) != 2 || first.Next == nil || first.Members[0].DisplayName != "Alice" || first.Members[1].DisplayName != "Bob" {
		t.Fatalf("first page = %+v, %v", first, err)
	}
	second, err := store.ListPlatformUsers(ctx, MemberPageRequest{Limit: 2, After: first.Next})
	if err != nil || len(second.Members) != 1 || second.Next != nil || second.Members[0].DisplayName != "Carol" {
		t.Fatalf("second page = %+v, %v", second, err)
	}
	search, err := store.ListPlatformUsers(ctx, MemberPageRequest{Limit: 10, Query: "bob@"})
	if err != nil || len(search.Members) != 1 || search.Members[0].PlatformUserID != users[1].PlatformUserID {
		t.Fatalf("search page = %+v, %v", search, err)
	}

	if err := store.UpsertTenant(ctx, "trailforge", "TrailForge"); err != nil {
		t.Fatal(err)
	}
	if err := store.GrantMembership(ctx, "trailforge", users[0].PlatformUserID, RoleAdmin); err != nil {
		t.Fatal(err)
	}
	members, err := store.ListTenantMembers(ctx, "trailforge", MemberPageRequest{Limit: 10})
	if err != nil || len(members.Members) != 1 || members.Members[0].PlatformUserID != users[0].PlatformUserID {
		t.Fatalf("tenant members = %+v, %v", members, err)
	}
	candidates, err := store.ListTenantMemberCandidates(ctx, "trailforge", MemberPageRequest{Limit: 10})
	if err != nil || len(candidates.Members) != 2 {
		t.Fatalf("tenant candidates = %+v, %v", candidates, err)
	}
	for _, candidate := range candidates.Members {
		if candidate.PlatformUserID == users[0].PlatformUserID {
			t.Fatalf("existing member leaked into candidates: %+v", candidate)
		}
	}
}

func TestMemoryIdentityStoreResolvesTrustedWeComLoginAcrossBindings(t *testing.T) {
	store := NewMemoryIdentityStore()
	ctx := context.Background()
	registerTestWeComProvider(t, store, "org-support", "wecom-support", "corp-support")
	user, err := store.ResolveLoginIdentity(ctx, testWeComIdentity("org-support", "wecom-support", "corp-support", "customer-1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertTenant(ctx, "support", "客服业务"); err != nil {
		t.Fatal(err)
	}
	if err := store.GrantMembership(ctx, "support", user.PlatformUserID, RoleMember); err != nil {
		t.Fatal(err)
	}
	first, ok, err := store.ResolveChannelIdentity(ctx, "support", channels.WeCom, "wecom-a", "customer-1", "corp-support")
	if err != nil || !ok || first.PlatformUserID != user.PlatformUserID {
		t.Fatalf("first trusted WeCom identity = %+v, %v, %v", first, ok, err)
	}
	resolved, ok, err := store.ResolveChannelIdentity(ctx, "support", channels.WeCom, "wecom-b", "customer-1", "corp-support")
	if err != nil || !ok || resolved.PlatformUserID != user.PlatformUserID || resolved.BindingID != "wecom-b" {
		t.Fatalf("cross-binding WeCom identity = %+v, %v, %v", resolved, ok, err)
	}
}

func TestMemoryIdentityStoreResolvesTrustedFeishuLoginAcrossBindings(t *testing.T) {
	store := NewMemoryIdentityStore()
	ctx := context.Background()
	const boundary = "tenant-key-support"
	if err := store.UpsertLoginProvider(ctx, ProviderDescriptor{
		ProviderID: "feishu-support", Type: ProviderFeishu, DisplayName: "飞书登录",
	}, boundary); err != nil {
		t.Fatal(err)
	}
	user, err := store.ResolveLoginIdentity(ctx, Identity{
		ProviderID: "feishu-support", ProviderType: ProviderFeishu, EnterpriseID: boundary,
		SubjectID: "ou_customer_1", DisplayName: "客服成员",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertTenant(ctx, "support", "客服业务"); err != nil {
		t.Fatal(err)
	}
	if err := store.GrantMembership(ctx, "support", user.PlatformUserID, RoleMember); err != nil {
		t.Fatal(err)
	}
	first, linked, err := store.ResolveChannelIdentity(ctx, "support", channels.Feishu, "feishu-a", "ou_customer_1", boundary)
	if err != nil || !linked || first.PlatformUserID != user.PlatformUserID || first.TrustedEnterpriseID != boundary {
		t.Fatalf("first trusted Feishu identity = %+v, linked=%v, %v", first, linked, err)
	}
	second, linked, err := store.ResolveChannelIdentity(ctx, "support", channels.Feishu, "feishu-b", "ou_customer_1", boundary)
	if err != nil || !linked || second.PlatformUserID != user.PlatformUserID || second.BindingID != "feishu-b" {
		t.Fatalf("cross-binding Feishu identity = %+v, linked=%v, %v", second, linked, err)
	}
}
