package routing

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestRouterUsesPersonForDirectAndGroupSubjectForGroup(t *testing.T) {
	router := newTestRouter(t)
	directA := resolveTestInbound(t, router, message.InboundMessage{ActorUserID: "user-a", ConversationID: "user-a", ConversationType: message.ConversationDirect, PlatformMessageID: "direct-a"})
	directB := resolveTestInbound(t, router, message.InboundMessage{ActorUserID: "user-b", ConversationID: "user-b", ConversationType: message.ConversationDirect, PlatformMessageID: "direct-b"})
	if directA.RunnerUserID == directB.RunnerUserID || directA.SessionID == directB.SessionID {
		t.Fatalf("direct users shared scope: %#v %#v", directA, directB)
	}

	groupA := resolveTestInbound(t, router, message.InboundMessage{ActorUserID: "user-a", ConversationID: "group-a", ConversationType: message.ConversationGroup, PlatformMessageID: "group-a-1"})
	groupB := resolveTestInbound(t, router, message.InboundMessage{ActorUserID: "user-b", ConversationID: "group-a", ConversationType: message.ConversationGroup, PlatformMessageID: "group-a-2"})
	if groupA.RunnerUserID != groupB.RunnerUserID || groupA.SessionID != groupB.SessionID {
		t.Fatalf("same group did not share scope: %#v %#v", groupA, groupB)
	}
	if groupA.ActorUserID != "user-a" || groupB.ActorUserID != "user-b" {
		t.Fatalf("group actors were not preserved: %#v %#v", groupA, groupB)
	}
}

func TestRouterIgnoresUntrustedExternalAccount(t *testing.T) {
	router := newTestRouter(t)
	task := resolveTestInbound(t, router, message.InboundMessage{
		ActorUserID: "user-a", ConversationID: "user-a", ConversationType: message.ConversationDirect,
		PlatformMessageID: "message-a", ExternalAccountID: "forged-account",
	})
	if task.ExternalAccountID != "trusted-account" || task.TenantID != "tenant-a" || task.AgentAppID != "assistant" {
		t.Fatalf("trusted routing projection = %#v", task)
	}
}

func TestRouterDigestV2CreatesTraceParent(t *testing.T) {
	catalog := tenant.Catalog{
		Tenants:         []tenant.Tenant{{ID: "tenant-a", Enabled: true}},
		StorageProfiles: []tenant.StorageProfile{{TenantID: "tenant-a", ID: "memory", Kind: tenant.StorageKindInMemory}},
		AgentApps:       []tenant.AgentApp{{TenantID: "tenant-a", ID: "assistant", Enabled: true, ActiveConfigVersion: "v1"}},
		ConfigVersions:  []tenant.ConfigVersion{{TenantID: "tenant-a", AgentAppID: "assistant", Version: "v1", StorageProfileID: "memory", Instruction: "test", Model: tenant.ModelConfig{Name: "model", BaseURL: "https://example.test", CredentialRef: "env:MODEL", RequestTimeout: time.Second, MaxOutputTokens: 32}}},
		ChannelBindings: []tenant.ChannelBinding{{ID: "binding-a", Channel: "telegram", ExternalAccountID: "trusted-account", CredentialRef: "env:TELEGRAM_TOKEN", TenantID: "tenant-a", AgentAppID: "assistant", Enabled: true}},
	}
	repository, err := tenant.NewPresetRepository(catalog)
	if err != nil {
		t.Fatal(err)
	}
	router, err := NewWithDigestV2(repository, []byte("01234567890123456789012345678901"), true)
	if err != nil {
		t.Fatal(err)
	}
	task, err := router.Resolve(context.Background(), message.InboundMessage{Channel: "telegram", BindingID: "binding-a", PlatformMessageID: "message-v2", ActorUserID: "user-a", ConversationID: "user-a", ConversationType: message.ConversationDirect, Text: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if task.DigestVersion != 2 || task.TraceParent == "" || !task.ValidDigest() {
		t.Fatalf("task=%#v", task)
	}
}

func TestRouterDigestV2DisabledKeepsPhase55TaskBytes(t *testing.T) {
	router := newTestRouter(t)
	base := message.InboundMessage{
		ActorUserID: "user-a", ConversationID: "user-a", ConversationType: message.ConversationDirect,
		PlatformMessageID: "message-legacy", RequestID: "request-legacy", TraceID: "0123456789abcdef0123456789abcdef",
		ReceivedAt: time.Date(2026, time.September, 5, 12, 0, 0, 0, time.UTC),
	}
	withoutTrace := resolveTestInbound(t, router, base)
	withTraceInput := base
	withTraceInput.TraceParent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
	withTraceInput.DigestVersion = 2
	withTrace := resolveTestInbound(t, router, withTraceInput)

	withTrace.TaskID = withoutTrace.TaskID
	withoutBytes, err := json.Marshal(withoutTrace)
	if err != nil {
		t.Fatal(err)
	}
	withBytes, err := json.Marshal(withTrace)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(withoutBytes, withBytes) {
		t.Fatalf("disabled v2 changed Phase 5.5 task bytes:\nwithout=%s\nwith=%s", withoutBytes, withBytes)
	}
	if bytes.Contains(withBytes, []byte("trace_parent")) || bytes.Contains(withBytes, []byte("digest_version")) {
		t.Fatalf("disabled v2 emitted new wire fields: %s", withBytes)
	}
}

func resolveTestInbound(t *testing.T, router *Router, inbound message.InboundMessage) message.ExecutionTask {
	t.Helper()
	inbound.Channel = "telegram"
	inbound.BindingID = "binding-a"
	inbound.Text = "hello"
	inbound.RequestID = "request-" + inbound.PlatformMessageID
	inbound.TraceID = "trace-" + inbound.PlatformMessageID
	inbound.ReceivedAt = time.Now().UTC()
	task, err := router.Resolve(context.Background(), inbound)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func newTestRouter(t *testing.T) *Router {
	t.Helper()
	catalog := tenant.Catalog{
		Tenants:         []tenant.Tenant{{ID: "tenant-a", Enabled: true}},
		StorageProfiles: []tenant.StorageProfile{{TenantID: "tenant-a", ID: "memory", Kind: tenant.StorageKindInMemory}},
		AgentApps:       []tenant.AgentApp{{TenantID: "tenant-a", ID: "assistant", Enabled: true, ActiveConfigVersion: "v1"}},
		ConfigVersions: []tenant.ConfigVersion{{
			TenantID: "tenant-a", AgentAppID: "assistant", Version: "v1", StorageProfileID: "memory", Instruction: "test",
			Model: tenant.ModelConfig{Name: "model", BaseURL: "https://example.test", CredentialRef: "env:MODEL", RequestTimeout: time.Second, MaxOutputTokens: 32},
		}},
		ChannelBindings: []tenant.ChannelBinding{{
			ID: "binding-a", Channel: "telegram", ExternalAccountID: "trusted-account", CredentialRef: "env:TELEGRAM_TOKEN",
			TenantID: "tenant-a", AgentAppID: "assistant", Enabled: true,
		}},
	}
	repository, err := tenant.NewPresetRepository(catalog)
	if err != nil {
		t.Fatal(err)
	}
	router, err := New(repository, []byte("01234567890123456789012345678901"))
	if err != nil {
		t.Fatal(err)
	}
	return router
}
