package channels

import "testing"

func TestSessionIDIsolation(t *testing.T) {
	base := InboundEnvelope{
		TenantID: "tenant-a", BindingID: "binding-a", Channel: "wecom",
		ExternalUserID: "user-a", ExternalConversationID: "chat-a",
		ConversationType: ConversationP2P,
	}
	p2p := base.SessionID()
	changedUser := base
	changedUser.ExternalUserID = "user-b"
	if p2p == changedUser.SessionID() {
		t.Fatal("p2p session must include user id")
	}
	changedTenant := base
	changedTenant.TenantID = "tenant-b"
	if p2p == changedTenant.SessionID() {
		t.Fatal("session must include tenant id")
	}
	group := base
	group.ConversationType = ConversationGroup
	groupFromOtherUser := group
	groupFromOtherUser.ExternalUserID = "user-b"
	if group.SessionID() != groupFromOtherUser.SessionID() {
		t.Fatal("group session must be shared by conversation, not sender")
	}
	groupOtherChat := group
	groupOtherChat.ExternalConversationID = "chat-b"
	if group.SessionID() == groupOtherChat.SessionID() {
		t.Fatal("group session must include conversation id")
	}
}

func TestShouldHandleGroup(t *testing.T) {
	tests := []struct {
		conversation ConversationType
		mentioned    bool
		mentionAll   bool
		want         bool
	}{
		{ConversationP2P, false, false, true},
		{ConversationGroup, false, false, false},
		{ConversationGroup, true, false, true},
		{ConversationGroup, true, true, false},
	}
	for _, test := range tests {
		if got := ShouldHandleGroup(test.conversation, test.mentioned, test.mentionAll); got != test.want {
			t.Fatalf("ShouldHandleGroup(%q,%v,%v)=%v want %v", test.conversation, test.mentioned, test.mentionAll, got, test.want)
		}
	}
}

func TestInboundValidationAndIdempotency(t *testing.T) {
	message := InboundEnvelope{
		TenantID: "t", BindingID: "b", Channel: "feishu", ExternalMessageID: "m",
		ExternalUserID: "u", ConversationType: ConversationP2P, Content: "hello",
	}
	if err := message.Validate(); err != nil {
		t.Fatalf("valid message rejected: %v", err)
	}
	if got, want := message.IdempotencyKey(), "t|b|feishu|m"; got != want {
		t.Fatalf("idempotency key=%q want %q", got, want)
	}
	message.Content = " "
	if err := message.Validate(); err == nil {
		t.Fatal("empty message content should fail")
	}
	base := InboundEnvelope{
		TenantID: "tenant", BindingID: "binding", Channel: "feishu",
		ExternalMessageID: "message", ExternalUserID: "user", ConversationType: ConversationP2P, Content: "hello",
	}
	cases := []InboundEnvelope{base, base, base}
	cases[0].TenantID = ""
	cases[1].ExternalMessageID = ""
	cases[2].ConversationType = "unknown"
	for i, invalid := range cases {
		if err := invalid.Validate(); err == nil {
			t.Fatalf("case %d should fail", i)
		}
	}
}
