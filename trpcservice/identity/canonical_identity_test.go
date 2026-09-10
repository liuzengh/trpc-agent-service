package identity

import (
	"context"
	"errors"
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

func TestMemoryIdentityStoreRejectsChannelIdentityTakeover(t *testing.T) {
	store := NewMemoryIdentityStore()
	ctx := context.Background()
	registerTestWeComProvider(t, store, "org-trailforge", "wecom-trailforge", "corp-trailforge")
	first, _ := store.ResolveLoginIdentity(ctx, testWeComIdentity("org-trailforge", "wecom-trailforge", "corp-trailforge", "ming"))
	second, _ := store.ResolveLoginIdentity(ctx, testWeComIdentity("org-trailforge", "wecom-trailforge", "corp-trailforge", "other"))
	link := ChannelIdentity{TenantID: "trailforge", Channel: channels.Telegram, BindingID: "tg-main", ExternalUserID: "42", PlatformUserID: first.PlatformUserID}
	if err := store.LinkChannelIdentity(ctx, link); err != nil {
		t.Fatal(err)
	}
	link.PlatformUserID = second.PlatformUserID
	if err := store.LinkChannelIdentity(ctx, link); !errors.Is(err, ErrChannelIdentityConflict) {
		t.Fatalf("takeover error = %v, want conflict", err)
	}
}

func TestMemoryIdentityStoreSharesTrustedWeComIdentityAcrossBindings(t *testing.T) {
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
	if err := store.GrantMembership(ctx, "trailforge", user.PlatformUserID, RoleMember); err != nil {
		t.Fatal(err)
	}
	if err := store.LinkChannelIdentity(ctx, ChannelIdentity{
		TenantID: "trailforge", Channel: channels.WeCom, BindingID: "wecom-support", ExternalUserID: "ming",
		PlatformUserID: user.PlatformUserID, TrustedEnterpriseID: "corp-trailforge",
	}); err != nil {
		t.Fatal(err)
	}
	resolved, ok, err := store.ResolveChannelIdentity(ctx, "trailforge", channels.WeCom, "wecom-sales", "ming", "corp-trailforge")
	if err != nil || !ok || resolved.PlatformUserID != user.PlatformUserID || resolved.BindingID != "wecom-sales" {
		t.Fatalf("cross-binding WeCom identity = %+v, %v, %v", resolved, ok, err)
	}
}
