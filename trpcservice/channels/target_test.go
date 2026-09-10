package channels_test

import (
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestNormalizeExternalIDPreservesCase(t *testing.T) {
	normalized, err := channels.NormalizeExternalID("  User-ABC  ")
	if err != nil {
		t.Fatalf("normalize external id: %v", err)
	}
	if normalized != "User-ABC" {
		t.Fatalf("normalized external id = %q, want User-ABC", normalized)
	}
	if _, err := channels.NormalizeExternalID(" "); err == nil {
		t.Fatal("normalize whitespace-only external id succeeded")
	}
	if _, err := channels.NormalizeExternalID(string([]byte{0xff})); err == nil {
		t.Fatal("normalize invalid utf-8 external id succeeded")
	}
}

func TestTargetPlaintextValidateMatchesPurpose(t *testing.T) {
	target := channels.TargetPlaintext{
		Version:        channels.TargetVersion,
		Channel:        channels.ChannelWeCom,
		TargetKind:     channels.TargetKindUser,
		ExternalUserID: "user-1",
		ProviderTarget: "user-target",
	}
	if err := target.Validate(channels.TargetPurposeIdentityUser); err != nil {
		t.Fatalf("validate user target: %v", err)
	}
	if err := target.Validate(channels.TargetPurposeConversationChat); err == nil {
		t.Fatal("user target accepted conversation purpose")
	}
	target.ProviderTarget = ""
	if err := target.Validate(channels.TargetPurposeIdentityUser); err == nil {
		t.Fatal("target without provider target succeeded")
	}
}

func TestTargetContextValidateRequiresScopedEntity(t *testing.T) {
	valid := channels.TargetContext{
		Scope:            tenant.Scope{TenantID: "tenant-a", AppID: "app-a"},
		BindingID:        "binding-a",
		Channel:          channels.ChannelFeishu,
		EntityType:       channels.TargetEntityConversation,
		InternalEntityID: "conversation-a",
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("validate target context: %v", err)
	}
	valid.Scope.TenantID = ""
	if err := valid.Validate(); err == nil {
		t.Fatal("target context without tenant succeeded")
	}
	valid = channels.TargetContext{
		Scope:            tenant.Scope{TenantID: "tenant-a", AppID: "app-a"},
		BindingID:        "binding-a",
		Channel:          channels.ChannelFeishu,
		EntityType:       channels.TargetEntityIdentity,
		InternalEntityID: "user-a",
	}
	if err := valid.ValidateFor(channels.TargetPurposeConversationChat); err == nil {
		t.Fatal("identity target context accepted conversation purpose")
	}
}

func TestMappedPrincipalValidateSeparatesDirectAndSharedSessions(t *testing.T) {
	direct := channels.MappedPrincipal{
		Identity:           channels.Identity{UserID: "user-a"},
		SessionPrincipalID: "user-a",
		SessionID:          channels.DefaultSessionID,
	}
	if err := direct.Validate(); err != nil {
		t.Fatalf("validate direct mapping: %v", err)
	}
	shared := direct
	shared.Conversation = &channels.Conversation{
		ConversationID:     "conversation-a",
		SessionPrincipalID: "conversation-a",
	}
	shared.SessionPrincipalID = "conversation-a"
	if err := shared.Validate(); err != nil {
		t.Fatalf("validate shared mapping: %v", err)
	}
}
