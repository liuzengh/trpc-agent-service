package channel

import (
	"strings"
	"testing"
)

func TestSessionIDGeneratorScopesAndBoundsIdentifiers(t *testing.T) {
	generator := &DefaultSessionIDGenerator{}
	base := &InboundMessage{
		TenantID:         "tenant-a",
		ChannelType:      "telegram",
		ChannelAccountID: "bot-a",
		ExternalUserID:   strings.Repeat("user", 200),
		ConversationID:   strings.Repeat("group", 200),
	}
	direct := generator.Generate(base)
	if len(direct) != 69 || !strings.HasPrefix(direct, "sess_") {
		t.Fatalf("unexpected bounded session ID %q", direct)
	}
	group := *base
	group.IsGroupChat = true
	groupID := generator.Generate(&group)
	if groupID == direct {
		t.Fatal("group and direct scopes collided")
	}
	otherTenant := *base
	otherTenant.TenantID = "tenant-b"
	if generator.Generate(&otherTenant) == direct {
		t.Fatal("cross-tenant session IDs collided")
	}
	if generator.Generate(nil) != "" {
		t.Fatal("nil message must not produce a routable session ID")
	}
	for _, incomplete := range []*InboundMessage{
		{ChannelType: "telegram", ChannelAccountID: "bot", ExternalUserID: "user"},
		{TenantID: "tenant", ChannelAccountID: "bot", ExternalUserID: "user"},
		{TenantID: "tenant", ChannelType: "telegram", ExternalUserID: "user"},
		{TenantID: "tenant", ChannelType: "telegram", ChannelAccountID: "bot"},
		{TenantID: "tenant", ChannelType: "telegram", ChannelAccountID: "bot", IsGroupChat: true},
	} {
		if got := generator.Generate(incomplete); got != "" {
			t.Fatalf("incomplete message produced routable session ID %q", got)
		}
	}
}

func TestSessionIDGeneratorRejectsAmbiguousComponents(t *testing.T) {
	generator := &DefaultSessionIDGenerator{}
	base := &InboundMessage{
		TenantID:         "tenant-a",
		ChannelType:      "telegram",
		ChannelAccountID: "bot-a",
		ExternalUserID:   "user-a",
	}
	for name, value := range map[string]string{
		"nul":              "user\x00a",
		"newline":          "user\n-a",
		"invalid utf8":     string([]byte{'u', 0xff}),
		"format character": "user\u200d-a",
	} {
		t.Run(name, func(t *testing.T) {
			message := *base
			message.ExternalUserID = value
			if got := generator.Generate(&message); got != "" {
				t.Fatalf("unsafe component produced routable session ID %q", got)
			}
		})
	}
}

func TestSessionIdentitySharesGroupOwnerButPreservesActor(t *testing.T) {
	base := InboundMessage{
		TenantID:         "tenant-a",
		ChannelType:      "telegram",
		ChannelAccountID: "bot-a",
		ConversationID:   "group-a",
		IsGroupChat:      true,
	}
	alice := base
	alice.ExternalUserID = "alice"
	bob := base
	bob.ExternalUserID = "bob"

	aliceIdentity, err := BuildSessionIdentity(&alice)
	if err != nil {
		t.Fatal(err)
	}
	bobIdentity, err := BuildSessionIdentity(&bob)
	if err != nil {
		t.Fatal(err)
	}
	if aliceIdentity.SessionID != bobIdentity.SessionID {
		t.Fatalf("group session IDs differ: %q vs %q", aliceIdentity.SessionID, bobIdentity.SessionID)
	}
	if aliceIdentity.SessionOwnerID != bobIdentity.SessionOwnerID {
		t.Fatalf("group session owners differ: %q vs %q", aliceIdentity.SessionOwnerID, bobIdentity.SessionOwnerID)
	}
	if aliceIdentity.SessionOwnerID == aliceIdentity.ActorUserID || bobIdentity.SessionOwnerID == bobIdentity.ActorUserID {
		t.Fatal("group owner must not be an individual actor")
	}
	if aliceIdentity.ActorUserID == bobIdentity.ActorUserID {
		t.Fatal("group actors must remain distinct")
	}
}

func TestSessionIdentityDirectOwnerIsActor(t *testing.T) {
	msg := &InboundMessage{
		TenantID:         "tenant-a",
		ChannelType:      "telegram",
		ChannelAccountID: "bot-a",
		ExternalUserID:   "alice",
		ConversationID:   "direct-a",
	}
	identity, err := BuildSessionIdentity(msg)
	if err != nil {
		t.Fatal(err)
	}
	if identity.SessionOwnerID != identity.ActorUserID || identity.SessionOwnerID != "alice" {
		t.Fatalf("direct owner=%q actor=%q", identity.SessionOwnerID, identity.ActorUserID)
	}
}

func TestValidateSessionIdentityRejectsTamperedOwner(t *testing.T) {
	msg := &InboundMessage{
		TenantID:         "tenant-a",
		ChannelType:      "telegram",
		ChannelAccountID: "bot-a",
		ExternalUserID:   "alice",
		ConversationID:   "group-a",
		IsGroupChat:      true,
	}
	identity, err := BuildSessionIdentity(msg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateSessionIdentity(msg, identity.SessionID, "group_owner_attacker"); err == nil {
		t.Fatal("tampered group owner was accepted")
	}
}

func TestSessionOwnerForSupportsLegacySessionIDs(t *testing.T) {
	msg := &InboundMessage{
		ExternalUserID: "alice",
		ConversationID: "group-a",
		IsGroupChat:    true,
	}
	owner, err := SessionOwnerIDFor(msg, "legacy-session")
	if err != nil {
		t.Fatal(err)
	}
	again, err := SessionOwnerIDFor(msg, "legacy-session")
	if err != nil || owner != again || owner == "legacy-session" {
		t.Fatalf("legacy owner=%q again=%q err=%v", owner, again, err)
	}
	if err := ValidateSessionIdentity(msg, "legacy-session", owner); err != nil {
		t.Fatalf("legacy session identity was rejected: %v", err)
	}
}
