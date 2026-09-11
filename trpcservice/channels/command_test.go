package channels

import "testing"

func TestRoutePlatformCommandRecognizesOnlyExactNewSessionCommand(t *testing.T) {
	t.Parallel()
	base := InboundMessage{MessageID: "m1", Channel: Feishu, ConversationID: "c1", SenderID: "u1", Text: "/new"}
	if got := RoutePlatformCommand(base); got != PlatformCommandNewSession {
		t.Fatalf("RoutePlatformCommand() = %q, want %q", got, PlatformCommandNewSession)
	}
	for _, text := range []string{"new", "/new please", "请 /new", "/new@bot"} {
		message := base
		message.Text = text
		if got := RoutePlatformCommand(message); got != PlatformCommandNone {
			t.Fatalf("RoutePlatformCommand(%q) = %q, want none", text, got)
		}
	}
	withFile := base
	withFile.ReceivedFiles = []ReceivedFile{{Name: "a.txt", Data: []byte("x")}}
	if got := RoutePlatformCommand(withFile); got != PlatformCommandNone {
		t.Fatalf("RoutePlatformCommand(with file) = %q, want none", got)
	}
}
