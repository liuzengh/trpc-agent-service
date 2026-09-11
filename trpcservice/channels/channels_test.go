package channels

import (
	"errors"
	"strings"
	"testing"
)

func TestBuildSessionKeyIsStableAndTenantScoped(t *testing.T) {
	key, err := BuildSessionKey("acme", "support", "session-42")
	if err != nil {
		t.Fatalf("BuildSessionKey() error = %v", err)
	}
	if got, want := key, "acme/support/session/session-42"; got != want {
		t.Errorf("BuildSessionKey() = %q, want %q", got, want)
	}
	if strings.Contains(key, "telegram") || strings.Contains(key, "wecom") || strings.Contains(key, "feishu") {
		t.Fatalf("BuildSessionKey() leaked channel into %q", key)
	}

	otherTenant, err := BuildSessionKey("globex", "support", "session-42")
	if err != nil {
		t.Fatalf("BuildSessionKey() other tenant error = %v", err)
	}
	if key == otherTenant {
		t.Error("BuildSessionKey() must not collide across tenants")
	}
}

func TestBuildSessionKeyRejectsUnsafeSegments(t *testing.T) {
	tests := []struct {
		name      string
		tenantID  string
		appCode   string
		sessionID string
	}{
		{name: "empty tenant", appCode: "support", sessionID: "42"},
		{name: "slash in app", tenantID: "acme", appCode: "support/other", sessionID: "42"},
		{name: "empty session", tenantID: "acme", appCode: "support"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := BuildSessionKey(test.tenantID, test.appCode, test.sessionID)
			if !errors.Is(err, ErrInvalidSessionKey) {
				t.Errorf("BuildSessionKey() error = %v, want ErrInvalidSessionKey", err)
			}
		})
	}
}

func TestBindingKeyRequiresChannelAndExternalBinding(t *testing.T) {
	key := BindingKey{Channel: Telegram, BindingID: "support-bot"}
	if err := key.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	for _, invalid := range []BindingKey{{}, {Channel: Telegram}, {Channel: Channel("unknown"), BindingID: "bot"}} {
		if err := invalid.Validate(); err == nil {
			t.Fatalf("Validate(%+v) error = nil, want error", invalid)
		}
	}
}

func TestInboundMessageValidate(t *testing.T) {
	message := InboundMessage{
		MessageID:      "update-1",
		Channel:        Telegram,
		ConversationID: "chat-1",
		SenderID:       "user-1",
	}
	if err := message.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	message.MessageID = ""
	if err := message.Validate(); !errors.Is(err, ErrInvalidInboundMessage) {
		t.Errorf("Validate() error = %v, want ErrInvalidInboundMessage", err)
	}

	message.MessageID = "update-1"
	message.ProviderRequestID = strings.Repeat("x", 257)
	if err := message.Validate(); !errors.Is(err, ErrInvalidInboundMessage) {
		t.Errorf("Validate() oversized provider request ID error = %v, want ErrInvalidInboundMessage", err)
	}
}

func TestInboundMessageValidatesFileReferences(t *testing.T) {
	message := InboundMessage{
		MessageID: "update-1", Channel: Web, ConversationID: "chat-1", SenderID: "user-1",
		Files: []InboundFile{{Name: "notes.txt", MimeType: "text/plain", ArtifactName: "input/update-1/00-notes.txt", Version: 0, SizeBytes: 5}},
	}
	if err := message.Validate(); err != nil {
		t.Fatalf("Validate() with file reference error = %v", err)
	}
	message.Files[0].ArtifactName = ""
	if err := message.Validate(); !errors.Is(err, ErrInvalidInboundMessage) {
		t.Fatalf("Validate() invalid file error = %v, want ErrInvalidInboundMessage", err)
	}
}

func TestExternalSubjectIDKeepsChannelsIndependent(t *testing.T) {
	telegram, err := ExternalSubjectID(Telegram, "bot-a", "same-user")
	if err != nil {
		t.Fatalf("ExternalSubjectID(telegram) error = %v", err)
	}
	wecom, err := ExternalSubjectID(WeCom, "bot-b", "same-user")
	if err != nil {
		t.Fatalf("ExternalSubjectID(wecom) error = %v", err)
	}
	if telegram != "external:telegram:bot-a:same-user" || wecom != "external:wecom:bot-b:same-user" || telegram == wecom {
		t.Fatalf("external subjects = %q/%q, want channel-scoped identities", telegram, wecom)
	}
	if _, err := ExternalSubjectID(Web, "web-console", "user-1"); err == nil {
		t.Fatal("ExternalSubjectID(web) error = nil, want rejection")
	}
}
