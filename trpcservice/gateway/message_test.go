package gateway

import "testing"

func TestDeriveSessionID(t *testing.T) {
	tests := []struct {
		name     string
		tenantID string
		channel  string
		message  InboundMessage
		want     string
	}{
		{
			name:     "p2p",
			tenantID: "tenant-a",
			channel:  "feishu",
			message:  InboundMessage{ChatType: "p2p", SenderID: "user-1"},
			want:     "tenant-a:feishu:p2p:user-1",
		},
		{
			name:     "group",
			tenantID: "tenant-a",
			channel:  "wecom",
			message:  InboundMessage{ChatType: "group", GroupID: "group-1"},
			want:     "tenant-a:wecom:group:group-1",
		},
		{
			name:     "cross tenant isolation",
			tenantID: "tenant-b",
			channel:  "feishu",
			message:  InboundMessage{ChatType: "p2p", SenderID: "user-1"},
			want:     "tenant-b:feishu:p2p:user-1",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := DeriveSessionID(test.tenantID, test.channel, test.message); got != test.want {
				t.Fatalf("DeriveSessionID() = %q, want %q", got, test.want)
			}
		})
	}
}

// TestDeriveSessionIDSeparatesChatTypes kills the namespace-collision attack:
// a p2p sender whose platform ID contains ":group:" must never share a session
// with a real group conversation.
func TestDeriveSessionIDSeparatesChatTypes(t *testing.T) {
	p2p := InboundMessage{ChatType: "p2p", SenderID: "group:evil-user"}
	group := InboundMessage{ChatType: "group", GroupID: "evil-user"}
	if DeriveSessionID("tenant-a", "wecom", p2p) == DeriveSessionID("tenant-a", "wecom", group) {
		t.Fatal("p2p and group session IDs collide across chat types")
	}
}

func TestBuildDedupKeyScopesBinding(t *testing.T) {
	left := BuildDedupKey("tenant-a", "binding-a", "msg-1")
	right := BuildDedupKey("tenant-a", "binding-b", "msg-1")
	if left == right {
		t.Fatalf("dedup keys collide: %q", left)
	}
}
