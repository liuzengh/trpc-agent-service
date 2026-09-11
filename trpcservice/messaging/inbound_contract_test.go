package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func supportInboundMessage(channel channels.Channel) channels.InboundMessage {
	return channels.InboundMessage{
		MessageID: "message-1", Channel: channel, ConversationID: "conversation-1",
		SenderID: "customer-1", Text: "查询订单状态",
	}
}

func supportSnapshot(channel channels.Channel, access string) tenant.Snapshot {
	return tenant.Snapshot{Config: config.TenantConfig{
		TenantID: "support", AppCode: "assistant", Status: config.AgentActive, ConfigVersion: 1,
		Channels: []config.ChannelBinding{{Type: string(channel), BindingID: "support-main", AccessPolicy: access}},
	}}
}

func supportIdentityStore(t *testing.T) *identity.MemoryIdentityStore {
	t.Helper()
	store := identity.NewMemoryIdentityStore()
	if err := store.UpsertTenant(context.Background(), "support", "客服业务"); err != nil {
		t.Fatal(err)
	}
	return store
}

func TestNewInboundEnvelopeValidatesRoutingContract(t *testing.T) {
	t.Parallel()
	codec := testExecutionManifestCodec(t)
	selection := tenant.ReleaseSelection{Snapshot: supportSnapshot(channels.Telegram, config.ChannelAccessPublic), Variant: tenant.ReleaseStable}
	inbound := supportInboundMessage(channels.Telegram)
	sessionKey := "support/assistant/session/session-1"

	envelope, err := NewInboundEnvelope(selection, "support-main", sessionKey, inbound, codec)
	if err != nil {
		t.Fatalf("NewInboundEnvelope() error = %v", err)
	}
	if envelope.TraceParent != "" || envelope.EventID != inbound.MessageID || envelope.TenantID != "support" || envelope.Manifest == nil {
		t.Fatalf("NewInboundEnvelope() = %+v", envelope)
	}
	if _, err := NewInboundEnvelope(selection, "support-main", sessionKey, inbound, nil); err == nil {
		t.Fatal("missing manifest codec error = nil")
	}
	if _, err := NewInboundEnvelope(selection, " ", sessionKey, inbound, codec); err == nil {
		t.Fatal("empty binding error = nil")
	}
	invalidInbound := inbound
	invalidInbound.MessageID = ""
	if _, err := NewInboundEnvelope(selection, "support-main", sessionKey, invalidInbound, codec); err == nil {
		t.Fatal("invalid inbound error = nil")
	}
	invalidSelection := selection
	invalidSelection.Snapshot.Config.TenantID = ""
	if _, err := NewInboundEnvelope(invalidSelection, "support-main", sessionKey, inbound, codec); err == nil {
		t.Fatal("invalid snapshot error = nil")
	}
	if _, err := NewInboundEnvelope(selection, "support-main", "other/assistant/session/1", inbound, codec); err == nil {
		t.Fatal("wrong session scope error = nil")
	}
}

func TestDecodeInboundPayloadValidationMatrix(t *testing.T) {
	t.Parallel()
	codec := testExecutionManifestCodec(t)
	selection := tenant.ReleaseSelection{Snapshot: supportSnapshot(channels.Telegram, config.ChannelAccessPublic), Variant: tenant.ReleaseStable}
	inbound := supportInboundMessage(channels.Telegram)
	envelope, err := NewInboundEnvelope(selection, "support-main", "support/assistant/session/session-1", inbound, codec)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := DecodeInboundPayload(envelope)
	if err != nil || payload.BindingID != "support-main" || payload.AppCode != "assistant" || payload.ConfigVersion != 1 || payload.ReleaseVariant != tenant.ReleaseStable {
		t.Fatalf("DecodeInboundPayload() = %+v, %v", payload, err)
	}

	tests := []struct {
		name string
		edit func(*Envelope)
	}{
		{"invalid envelope", func(e *Envelope) { e.EventID = "" }},
		{"wrong type", func(e *Envelope) { e.Type = "other.type" }},
		{"invalid json", func(e *Envelope) { e.Payload = []byte("not-json") }},
		{"wrong session scope", func(e *Envelope) { e.SessionKey = "support/other/session/session-1" }},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			candidate := envelope
			tt.edit(&candidate)
			if _, err := DecodeInboundPayload(candidate); err == nil {
				t.Fatal("DecodeInboundPayload(invalid) error = nil")
			}
		})
	}

	var decoded InboundPayload
	if err := json.Unmarshal(envelope.Payload, &decoded); err != nil {
		t.Fatal(err)
	}
	payloadTests := []struct {
		name string
		edit func(*InboundPayload)
	}{
		{"missing binding", func(p *InboundPayload) { p.BindingID = "" }},
		{"missing app", func(p *InboundPayload) { p.AppCode = "" }},
		{"zero version", func(p *InboundPayload) { p.ConfigVersion = 0 }},
		{"invalid variant", func(p *InboundPayload) { p.ReleaseVariant = tenant.ReleaseVariant("other") }},
		{"invalid inbound", func(p *InboundPayload) { p.Inbound.MessageID = "" }},
	}
	for _, tt := range payloadTests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			candidatePayload := decoded
			tt.edit(&candidatePayload)
			candidate := envelope
			candidate.Payload, _ = json.Marshal(candidatePayload)
			if _, err := DecodeInboundPayload(candidate); err == nil {
				t.Fatal("DecodeInboundPayload(invalid payload) error = nil")
			}
		})
	}
}

func TestResolveInboundSessionWebDefaultsAndPrepareRoute(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	identities := supportIdentityStore(t)
	state := storage.NewMemoryStateStore()
	snapshot := supportSnapshot(channels.Web, config.ChannelAccessPublic)
	inbound := supportInboundMessage(channels.Web)
	inbound.WebOwnerID = "platform-user-1"

	sessionKey, normalized, err := ResolveInboundSession(ctx, state, identities, snapshot, "web-console", inbound)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(sessionKey, "support/assistant/session/") || normalized.SubjectID != "platform-user-1" || normalized.OwnerPlatformUserID != "platform-user-1" || normalized.ActorPlatformUserID != "platform-user-1" || normalized.ConversationScope != channels.ConversationDirect || normalized.TriggerType != channels.TriggerDirect {
		t.Fatalf("normalized web inbound = %+v, session=%q", normalized, sessionKey)
	}

	route, prepared, err := PrepareInboundSessionRoute(ctx, identities, snapshot, "web-console", inbound)
	if err != nil {
		t.Fatal(err)
	}
	if route.TenantID != "support" || route.AppCode != "assistant" || route.SubjectID != "platform-user-1" || prepared.SubjectID != "platform-user-1" {
		t.Fatalf("prepared route = %+v inbound=%+v", route, prepared)
	}
}

func TestResolveInboundSessionAnonymousPublicAndAllowlistedIM(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	identities := supportIdentityStore(t)
	for _, access := range []string{config.ChannelAccessPublic, config.ChannelAccessAllowlist} {
		access := access
		t.Run(access, func(t *testing.T) {
			t.Parallel()
			snapshot := supportSnapshot(channels.Telegram, access)
			if access == config.ChannelAccessAllowlist {
				snapshot.Config.Channels[0].Allowlist = []string{" customer-1 "}
			}
			inbound := supportInboundMessage(channels.Telegram)
			sessionKey, normalized, err := ResolveInboundSession(ctx, storage.NewMemoryStateStore(), identities, snapshot, "support-main", inbound)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(normalized.SubjectID, "external:telegram:support-main:customer-1") || normalized.OwnerPlatformUserID != "" || normalized.ActorPlatformUserID != "" || !strings.HasPrefix(sessionKey, "support/assistant/session/") {
				t.Fatalf("anonymous inbound = %+v, session=%q", normalized, sessionKey)
			}
		})
	}
}

func TestResolveInboundSessionGroupUsesStableGroupSubject(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	identities := supportIdentityStore(t)
	snapshot := supportSnapshot(channels.Feishu, config.ChannelAccessPublic)
	inbound := supportInboundMessage(channels.Feishu)
	inbound.ConversationScope = channels.ConversationGroup
	sessionKey, normalized, err := ResolveInboundSession(ctx, storage.NewMemoryStateStore(), identities, snapshot, "support-main", inbound)
	if err != nil {
		t.Fatal(err)
	}
	if normalized.SubjectID != "group:feishu:support-main:conversation-1" || normalized.OwnerPlatformUserID != "" || normalized.TriggerType != channels.TriggerMention || !strings.HasPrefix(sessionKey, "support/assistant/session/") {
		t.Fatalf("group inbound = %+v, session=%q", normalized, sessionKey)
	}
}

func TestResolveInboundSessionRejectsInvalidDependenciesAndAccess(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	identities := supportIdentityStore(t)
	state := storage.NewMemoryStateStore()
	snapshot := supportSnapshot(channels.Telegram, config.ChannelAccessPublic)
	inbound := supportInboundMessage(channels.Telegram)

	if _, _, err := ResolveInboundSession(ctx, nil, identities, snapshot, "support-main", inbound); err == nil {
		t.Fatal("nil state error = nil")
	}
	invalid := inbound
	invalid.MessageID = ""
	if _, _, err := ResolveInboundSession(ctx, state, identities, snapshot, "support-main", invalid); err == nil {
		t.Fatal("invalid inbound error = nil")
	}
	if _, _, err := ResolveInboundSession(ctx, state, nil, snapshot, "support-main", inbound); err == nil {
		t.Fatal("nil identity resolver error = nil")
	}
	if _, _, err := ResolveInboundSession(ctx, state, identities, snapshot, " ", inbound); err == nil {
		t.Fatal("empty binding error = nil")
	}
	if _, _, err := ResolveInboundSession(ctx, state, identities, snapshot, "other-binding", inbound); err == nil {
		t.Fatal("unowned binding error = nil")
	}

	disabled := snapshot
	disabled.Config.Status = config.AgentDisabled
	if _, _, err := ResolveInboundSession(ctx, state, identities, disabled, "support-main", inbound); err == nil {
		t.Fatal("disabled agent error = nil")
	}
	if err := identities.SetTenantStatus(ctx, "support", identity.TenantSuspended); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ResolveInboundSession(ctx, state, identities, snapshot, "support-main", inbound); err == nil {
		t.Fatal("suspended tenant error = nil")
	}
}

type failingSessionManager struct{ err error }

func (m failingSessionManager) ResolveSession(context.Context, storage.SessionRoute, string) (string, error) {
	return "", m.err
}
func (failingSessionManager) ArchiveSession(context.Context, string, string) error { return nil }

func TestResolveInboundSessionPropagatesSessionResolutionFailure(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("session store unavailable")
	identities := supportIdentityStore(t)
	snapshot := supportSnapshot(channels.Web, config.ChannelAccessPublic)
	inbound := supportInboundMessage(channels.Web)
	inbound.WebOwnerID = "platform-user-1"
	if _, _, err := ResolveInboundSession(context.Background(), failingSessionManager{err: wantErr}, identities, snapshot, "web-console", inbound); !errors.Is(err, wantErr) {
		t.Fatalf("ResolveInboundSession() error = %v", err)
	}
}
