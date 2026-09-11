package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type newSessionIdentityResolver struct {
	platformUserID string
	role           identity.Role
}

func (r newSessionIdentityResolver) ResolveChannelIdentity(_ context.Context, tenantID string, channel channels.Channel, bindingID, externalUserID, trustedEnterpriseID string) (identity.ChannelIdentity, bool, error) {
	if r.platformUserID == "" {
		return identity.ChannelIdentity{TenantID: tenantID, Channel: channel, BindingID: bindingID, ExternalUserID: externalUserID, TrustedEnterpriseID: trustedEnterpriseID}, false, nil
	}
	return identity.ChannelIdentity{
		TenantID: tenantID, Channel: channel, BindingID: bindingID, ExternalUserID: externalUserID,
		PlatformUserID: r.platformUserID, TrustedEnterpriseID: trustedEnterpriseID,
	}, true, nil
}

func (r newSessionIdentityResolver) ResolveSessionUser(_ context.Context, platformUserID string) (identity.SessionUser, error) {
	if r.platformUserID == "" || platformUserID != r.platformUserID {
		return identity.SessionUser{}, errors.New("user not found")
	}
	return identity.SessionUser{PlatformUserID: platformUserID}, nil
}

func (r newSessionIdentityResolver) RoleFor(_ context.Context, _ string, platformUserID string) (identity.Role, error) {
	if r.platformUserID == "" || platformUserID != r.platformUserID || r.role == "" {
		return "", errors.New("membership not found")
	}
	return r.role, nil
}

func (newSessionIdentityResolver) TenantStatus(context.Context, string) (string, error) {
	return identity.TenantActive, nil
}

type newSessionProgressSender struct {
	starts int
}

func (s *newSessionProgressSender) Send(context.Context, channels.ReplyTarget, channels.OutboundMessage) (channels.SendReceipt, error) {
	return channels.SendReceipt{ExternalMessageID: "sent"}, nil
}

func (s *newSessionProgressSender) StartProgress(context.Context, channels.ReplyTarget) (channels.SendReceipt, error) {
	s.starts++
	return channels.SendReceipt{ExternalMessageID: "stream-new-1"}, nil
}

func (s *newSessionProgressSender) UpdateProgress(context.Context, channels.ReplyTarget, string, string) error {
	return nil
}

func TestWeComNewCommandSwitchesNextMessageToFreshSession(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	state := storage.NewMemoryStateStore()
	identities := identity.NewMemoryIdentityStore()
	if err := identities.UpsertTenant(ctx, "trailforge", "TrailForge"); err != nil {
		t.Fatal(err)
	}
	snapshot := tenant.Snapshot{Config: config.TenantConfig{
		TenantID: "trailforge", AppCode: "assistant", Status: config.AgentActive, ConfigVersion: 1,
		Channels: []config.ChannelBinding{{
			Type: config.ChannelWeCom, BindingID: "wecom-main", AccessPolicy: config.ChannelAccessPublic,
		}},
	}}
	message := channels.InboundMessage{
		MessageID: "before-1", Channel: channels.WeCom, ConversationID: "user-1", SenderID: "user-1",
		ConversationScope: channels.ConversationDirect, ProviderReplyToken: "callback-new-1", Text: "记住这一轮上下文",
	}
	oldSession, _, err := messaging.ResolveInboundSession(ctx, state, identities, snapshot, "wecom-main", message)
	if err != nil {
		t.Fatal(err)
	}

	sender := &newSessionProgressSender{}
	ingress := &channelIngress{
		sessions: state, identities: identities,
		resolveSender: func(context.Context, string, string, uint64, channels.BindingKey) (channels.Sender, error) {
			return sender, nil
		},
	}
	command := message
	command.MessageID = "command-new-1"
	command.Text = "/new"
	if err := ingress.handleNewSession(ctx, snapshot, "wecom-main", command); err != nil {
		t.Fatalf("handleNewSession() error = %v", err)
	}

	after := message
	after.MessageID = "after-1"
	after.Text = "新的问题"
	newSession, _, err := messaging.ResolveInboundSession(ctx, state, identities, snapshot, "wecom-main", after)
	if err != nil {
		t.Fatal(err)
	}
	if newSession == oldSession {
		t.Fatalf("/new kept the old WeCom session %q", oldSession)
	}
	old, err := state.GetSession(ctx, snapshot.Config.TenantID, oldSession)
	if err != nil {
		t.Fatal(err)
	}
	if old.Status != "archived" {
		t.Fatalf("old WeCom session status = %q, want archived", old.Status)
	}
	if sender.starts != 1 {
		t.Fatalf("WeCom /new progress starts = %d, want 1", sender.starts)
	}
	pending, err := state.ListPendingOutbox(ctx, snapshot.Config.TenantID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatalf("pending /new replies = %#v, want one", pending)
	}
	var payload struct {
		ProviderReplyToken string `json:"provider_reply_token"`
		ProgressMessageID  string `json:"progress_message_id"`
		Text               string `json:"text"`
	}
	if err := json.Unmarshal(pending[0].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.ProviderReplyToken != command.ProviderReplyToken || payload.ProgressMessageID != "stream-new-1" || payload.Text != channels.NewSessionSuccessReply {
		t.Fatalf("/new outbox payload = %#v", payload)
	}
	if got := channels.RoutePlatformCommand(command); got != channels.PlatformCommandNewSession {
		t.Fatalf("RoutePlatformCommand(/new) = %q, want %q", got, channels.PlatformCommandNewSession)
	}
}

func TestGroupNewCommandRequiresTenantAdministrator(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	snapshot := tenant.Snapshot{Config: config.TenantConfig{
		TenantID: "trailforge", AppCode: "assistant", Status: config.AgentActive, ConfigVersion: 1,
		Channels: []config.ChannelBinding{{
			Type: config.ChannelTelegram, BindingID: "telegram-main", AccessPolicy: config.ChannelAccessPublic,
		}},
	}}
	command := channels.InboundMessage{
		MessageID: "group-new-1", Channel: channels.Telegram, ConversationID: "group-1", SenderID: "member-1",
		ConversationScope: channels.ConversationGroup, TriggerType: channels.TriggerCommand, Text: "/new",
	}

	t.Run("ordinary member denied", func(t *testing.T) {
		state := storage.NewMemoryStateStore()
		ingress := &channelIngress{
			sessions:   state,
			identities: newSessionIdentityResolver{platformUserID: "platform-member", role: identity.RoleMember},
		}
		err := ingress.handleNewSession(ctx, snapshot, "telegram-main", command)
		if !errors.Is(err, errChannelIngressRejected) || !strings.Contains(err.Error(), "group /new requires a tenant administrator") {
			t.Fatalf("handleNewSession(member) error = %v", err)
		}
		pending, listErr := state.ListPendingOutbox(ctx, snapshot.Config.TenantID, 10)
		if listErr != nil || len(pending) != 0 {
			t.Fatalf("member /new outbox = %#v, %v", pending, listErr)
		}
	})

	t.Run("anonymous sender denied", func(t *testing.T) {
		state := storage.NewMemoryStateStore()
		ingress := &channelIngress{sessions: state, identities: newSessionIdentityResolver{}}
		if err := ingress.handleNewSession(ctx, snapshot, "telegram-main", command); !errors.Is(err, errChannelIngressRejected) {
			t.Fatalf("handleNewSession(anonymous) error = %v", err)
		}
	})

	t.Run("tenant administrator allowed", func(t *testing.T) {
		state := storage.NewMemoryStateStore()
		ingress := &channelIngress{
			sessions:   state,
			identities: newSessionIdentityResolver{platformUserID: "platform-admin", role: identity.RoleAdmin},
		}
		if err := ingress.handleNewSession(ctx, snapshot, "telegram-main", command); err != nil {
			t.Fatalf("handleNewSession(admin) error = %v", err)
		}
		pending, err := state.ListPendingOutbox(ctx, snapshot.Config.TenantID, 10)
		if err != nil || len(pending) != 1 {
			t.Fatalf("admin /new outbox = %#v, %v", pending, err)
		}
	})
}
