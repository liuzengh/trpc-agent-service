package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

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
