package bus

import (
	"strings"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestRouteKey(t *testing.T) {
	if got, want := RouteKey("t1", "sess-1"), "route:t1:sess-1"; got != want {
		t.Errorf("RouteKey = %q, want %q", got, want)
	}
	if RouteKey("t1", "s1") == RouteKey("t2", "s1") {
		t.Error("route keys from different tenants must not collide")
	}
}

func TestLockKey(t *testing.T) {
	if got, want := LockKey("t1", "sess-1"), "lock:session:t1:sess-1"; got != want {
		t.Errorf("LockKey = %q, want %q", got, want)
	}
}

func TestIdemKey(t *testing.T) {
	if got, want := IdemKey("msg-42"), "idem:msg-42"; got != want {
		t.Errorf("IdemKey = %q, want %q", got, want)
	}
}

func TestApprovalKeys(t *testing.T) {
	if got, want := ApprovalReqKey("t1", "s1"), "approval:req:t1:s1"; got != want {
		t.Errorf("ApprovalReqKey = %q, want %q", got, want)
	}
	if got, want := ApprovalResKey("t1", "s1"), "approval:res:t1:s1"; got != want {
		t.Errorf("ApprovalResKey = %q, want %q", got, want)
	}
	if ApprovalReqKey("t1", "s1") == ApprovalReqKey("t2", "s1") {
		t.Error("approval keys from different tenants must not collide")
	}
}

func TestStreamKeys(t *testing.T) {
	if StreamInbound != "stream:inbound" {
		t.Errorf("StreamInbound = %q", StreamInbound)
	}
	if StreamOutbound != "stream:outbound" {
		t.Errorf("StreamOutbound = %q", StreamOutbound)
	}
}

// TestKeyBuilderWireFormat pins every key the bus writes, including the ones
// added later (cursor, IM route, retry counter). The exact strings are a
// compatibility contract: renaming a namespace would silently orphan live
// locks, routes and cursors on an upgrade, so a change here must be deliberate.
func TestKeyBuilderWireFormat(t *testing.T) {
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"route", RouteKey("t1", "s1"), "route:t1:s1"},
		{"lock", LockKey("t1", "s1"), "lock:session:t1:s1"},
		{"idem", IdemKey("msg-1"), "idem:msg-1"},
		{"approval request", ApprovalReqKey("t1", "s1"), "approval:req:t1:s1"},
		{"approval result", ApprovalResKey("t1", "s1"), "approval:res:t1:s1"},
		{"outbound cursor", OutboundCursorKey(), "cursor:outbound"},
		{"im route", IMRouteKey("t1:wecom:user:u1"), "imroute:t1:wecom:user:u1"},
		{"dlq retry counter", retryKey("1700000000000-0"), "retry:1700000000000-0"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s key = %q, want %q", c.name, c.got, c.want)
		}
	}

	// No key may be a prefix of another: two features sharing a key space would
	// read each other's state (a route lookup returning a lock token, for
	// instance). Distinct sub-namespaces under one root (approval:req /
	// approval:res) are fine.
	for i := range cases {
		for j := range cases {
			if i == j {
				continue
			}
			if strings.HasPrefix(cases[i].want, cases[j].want) {
				t.Errorf("%s key %q is nested under %s key %q", cases[i].name, cases[i].want, cases[j].name, cases[j].want)
			}
		}
	}
}

func TestMessageRoundTrip(t *testing.T) {
	in := &Message{
		ID:        "msg-1",
		TraceID:   "trace-1",
		TenantID:  "t1",
		AgentID:   "a1",
		SessionID: "sess-1",
		Channel:   "wecom",
		UserID:    "u1",
		Content:   &model.Message{Role: model.RoleUser, Content: "hello"},
		ReplyTo:   "sess-1",
	}
	fields, err := encode(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := decode(fields)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.ID != in.ID || out.TraceID != in.TraceID || out.TenantID != in.TenantID {
		t.Errorf("envelope fields lost: %+v", out)
	}
	if out.AgentID != in.AgentID || out.SessionID != in.SessionID || out.Channel != in.Channel {
		t.Errorf("envelope fields lost: %+v", out)
	}
	if out.UserID != in.UserID || out.ReplyTo != in.ReplyTo {
		t.Errorf("envelope fields lost: %+v", out)
	}
	if out.Content == nil || out.Content.Content != "hello" || out.Content.Role != model.RoleUser {
		t.Errorf("Content not preserved: %+v", out.Content)
	}
}
