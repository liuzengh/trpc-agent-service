package messaging

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func seedCanonicalLoginUser(t *testing.T, ctx context.Context, identities interface {
	identity.LoginProviderRegistrar
	ResolveLoginIdentity(context.Context, identity.Identity) (identity.PlatformUser, error)
	CreateTenant(context.Context, string, string, string) error
}, subjectID string) identity.PlatformUser {
	t.Helper()
	if err := identities.UpsertLoginProvider(ctx, identity.ProviderDescriptor{
		ProviderID: "wecom-login", Type: identity.ProviderWeCom, DisplayName: "企业微信",
	}, "corp-trailforge"); err != nil {
		t.Fatal(err)
	}
	user, err := identities.ResolveLoginIdentity(ctx, identity.Identity{
		ProviderID: "wecom-login", ProviderType: identity.ProviderWeCom,
		EnterpriseID: "corp-trailforge", SubjectID: subjectID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := identities.CreateTenant(ctx, "trailforge", "TrailForge", user.PlatformUserID); err != nil {
		t.Fatal(err)
	}
	return user
}

func TestResolveInboundSessionKeepsTrustedWeComSeparateFromWeb(t *testing.T) {
	ctx := context.Background()
	identities := identity.NewMemoryIdentityStore()
	user := seedCanonicalLoginUser(t, ctx, identities, "ming")
	state := storage.NewMemoryStateStore()
	snapshot := tenant.Snapshot{Config: config.TenantConfig{
		TenantID: "trailforge", AppCode: "assistant", ConfigVersion: 1, Status: config.AgentActive,
		Channels: []config.ChannelBinding{{Type: "wecom", BindingID: "wecom-main", TrustedEnterpriseID: "corp-trailforge"}},
	}}

	webSession, webInbound, err := ResolveInboundSession(ctx, state, identities, snapshot, "web-console", channels.InboundMessage{
		MessageID: "web-1", Channel: channels.Web, ConversationID: "web-c1", SenderID: user.PlatformUserID,
		WebOwnerID: user.PlatformUserID, Text: "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	wecomSession, wecomInbound, err := ResolveInboundSession(ctx, state, identities, snapshot, "wecom-main", channels.InboundMessage{
		MessageID: "wx-1", Channel: channels.WeCom, ConversationID: "ming", SenderID: "ming", Text: "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	if webSession == wecomSession {
		t.Fatalf("sessions = %q / %q; want independent channel conversations", webSession, wecomSession)
	}
	for _, inbound := range []channels.InboundMessage{webInbound, wecomInbound} {
		if inbound.SubjectID != user.PlatformUserID || inbound.OwnerPlatformUserID != user.PlatformUserID || inbound.ActorPlatformUserID != user.PlatformUserID {
			t.Fatalf("canonical inbound = %+v", inbound)
		}
	}
}

func TestResolveInboundSessionKeepsUntrustedWeComAnonymous(t *testing.T) {
	ctx := context.Background()
	identities := identity.NewMemoryIdentityStore()
	user := seedCanonicalLoginUser(t, ctx, identities, "ming")
	snapshot := tenant.Snapshot{Config: config.TenantConfig{TenantID: "trailforge", AppCode: "assistant", ConfigVersion: 1, Status: config.AgentActive, Channels: []config.ChannelBinding{{Type: "wecom", BindingID: "wecom-other", AccessPolicy: config.ChannelAccessPublic}}}}
	_, inbound, err := ResolveInboundSession(ctx, storage.NewMemoryStateStore(), identities, snapshot, "wecom-other", channels.InboundMessage{
		MessageID: "wx-2", Channel: channels.WeCom, ConversationID: "ming", SenderID: "ming", Text: "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	if inbound.SubjectID == user.PlatformUserID || inbound.OwnerPlatformUserID != "" || inbound.ActorPlatformUserID != "" {
		t.Fatalf("untrusted WeCom unexpectedly resolved = %+v", inbound)
	}
	if inbound.SubjectID != "external:wecom:wecom-other:ming" {
		t.Fatalf("anonymous subject = %q", inbound.SubjectID)
	}
}

func TestResolveInboundSessionKeepsGroupSubjectSeparateFromActor(t *testing.T) {
	ctx := context.Background()
	identities := identity.NewMemoryIdentityStore()
	user := seedCanonicalLoginUser(t, ctx, identities, "ming")
	snapshot := tenant.Snapshot{Config: config.TenantConfig{TenantID: "trailforge", AppCode: "assistant", ConfigVersion: 1, Status: config.AgentActive, Channels: []config.ChannelBinding{{Type: "wecom", BindingID: "wecom-main", TrustedEnterpriseID: "corp-trailforge"}}}}
	_, inbound, err := ResolveInboundSession(ctx, storage.NewMemoryStateStore(), identities, snapshot, "wecom-main", channels.InboundMessage{
		MessageID: "wx-group-1", Channel: channels.WeCom, ConversationID: "group-1", SenderID: "ming",
		ConversationScope: channels.ConversationGroup, TriggerType: channels.TriggerMention, Text: "@bot hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	if inbound.SubjectID != "group:wecom:wecom-main:group-1" || inbound.OwnerPlatformUserID != "" || inbound.ActorPlatformUserID != user.PlatformUserID {
		t.Fatalf("group inbound = %+v", inbound)
	}
}

func TestResolveInboundSessionEnforcesBindingAccessPolicies(t *testing.T) {
	ctx := context.Background()
	identities := identity.NewMemoryIdentityStore()
	member := seedCanonicalLoginUser(t, ctx, identities, "member")
	outsider, err := identities.ResolveLoginIdentity(ctx, identity.Identity{
		ProviderID: "wecom-login", ProviderType: identity.ProviderWeCom, EnterpriseID: "corp-trailforge",
		SubjectID: "outsider", DisplayName: "Outsider",
	})
	if err != nil {
		t.Fatal(err)
	}

	base := config.TenantConfig{TenantID: "trailforge", AppCode: "assistant", ConfigVersion: 1, Status: config.AgentActive}
	state := storage.NewMemoryStateStore()

	memberOnly := base
	memberOnly.Channels = []config.ChannelBinding{{Type: "wecom", BindingID: "members", TrustedEnterpriseID: "corp-trailforge", AccessPolicy: config.ChannelAccessMemberOnly}}
	if _, _, err := ResolveInboundSession(ctx, state, identities, tenant.Snapshot{Config: memberOnly}, "members", channels.InboundMessage{
		MessageID: "m1", Channel: channels.WeCom, ConversationID: "member", SenderID: "member", Text: "hi",
	}); err != nil {
		t.Fatalf("member_only member denied: %v", err)
	}
	if _, _, err := ResolveInboundSession(ctx, state, identities, tenant.Snapshot{Config: memberOnly}, "members", channels.InboundMessage{
		MessageID: "m2", Channel: channels.WeCom, ConversationID: "outsider", SenderID: "outsider", Text: "hi",
	}); err == nil {
		t.Fatal("member_only must reject non-member external identity")
	}

	allowlisted := base
	allowlisted.Channels = []config.ChannelBinding{{Type: "feishu", BindingID: "allow", AccessPolicy: config.ChannelAccessAllowlist, Allowlist: []string{"ou_guest"}}}
	if _, inbound, err := ResolveInboundSession(ctx, state, identities, tenant.Snapshot{Config: allowlisted}, "allow", channels.InboundMessage{
		MessageID: "m3", Channel: channels.Feishu, ConversationID: "ou_guest", SenderID: "ou_guest", Text: "hi",
	}); err != nil || inbound.OwnerPlatformUserID != "" {
		t.Fatalf("allowlisted anonymous identity = %+v, %v", inbound, err)
	}

	public := base
	public.Channels = []config.ChannelBinding{{Type: "telegram", BindingID: "public", AccessPolicy: config.ChannelAccessPublic}}
	if _, _, err := ResolveInboundSession(ctx, state, identities, tenant.Snapshot{Config: public}, "public", channels.InboundMessage{
		MessageID: "m4", Channel: channels.Telegram, ConversationID: "guest", SenderID: "guest", Text: "hi",
	}); err != nil {
		t.Fatalf("public anonymous identity denied: %v", err)
	}

	defaultPublic := base
	defaultPublic.Channels = []config.ChannelBinding{{Type: "feishu", BindingID: "legacy-default"}}
	if _, _, err := ResolveInboundSession(ctx, state, identities, tenant.Snapshot{Config: defaultPublic}, "legacy-default", channels.InboundMessage{
		MessageID: "m4-default", Channel: channels.Feishu, ConversationID: "ou_guest_default", SenderID: "ou_guest_default", Text: "hi",
	}); err != nil {
		t.Fatalf("binding without access policy must preserve public access: %v", err)
	}

	trustedPublic := base
	trustedPublic.Channels = []config.ChannelBinding{{Type: "wecom", BindingID: "trusted-public", TrustedEnterpriseID: "corp-trailforge", AccessPolicy: config.ChannelAccessPublic}}
	if err := identities.GrantMembership(ctx, "trailforge", outsider.PlatformUserID, identity.RoleMember); err != nil {
		t.Fatal(err)
	}
	if _, inbound, err := ResolveInboundSession(ctx, state, identities, tenant.Snapshot{Config: trustedPublic}, "trusted-public", channels.InboundMessage{
		MessageID: "m5-prime", Channel: channels.WeCom, ConversationID: "outsider", SenderID: "outsider", Text: "hi",
	}); err != nil || inbound.ActorPlatformUserID != outsider.PlatformUserID {
		t.Fatalf("trusted public identity was not resolved before suspension: %+v, %v", inbound, err)
	}
	if err := identities.UpdatePlatformUserAccess(ctx, outsider.PlatformUserID, "suspended", false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ResolveInboundSession(ctx, state, identities, tenant.Snapshot{Config: trustedPublic}, "trusted-public", channels.InboundMessage{
		MessageID: "m5", Channel: channels.WeCom, ConversationID: "outsider", SenderID: "outsider", Text: "hi",
	}); err == nil {
		t.Fatal("public binding must reject a trusted external identity already resolved to a suspended platform user")
	}

	_ = member
}
