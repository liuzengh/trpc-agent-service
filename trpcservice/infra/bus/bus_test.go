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

// TestMessageRoundTripPreservesCardAndMedia pins the two optional payloads that
// a reply/notice depends on. Kind+Segments used to be dropped by the encoder,
// which silently downgraded the interactive approval card to plain text once it
// had been through the outbox; Media carries the attachment manifest the worker
// archives.
func TestMessageRoundTripPreservesCardAndMedia(t *testing.T) {
	in := &Message{
		ID:        "msg-2",
		TenantID:  "t1",
		SessionID: "sess-1",
		Channel:   "feishu",
		Content:   &model.Message{Role: model.RoleAssistant, Content: "approve?"},
		Kind:      "card",
		Segments: []Segment{
			{Type: "title", Text: "需要人工审批"},
			{Type: "actions", Actions: []SegmentAction{
				{Text: "批准", Value: map[string]string{"decision": "approve", "session_id": "sess-1"}},
			}},
		},
		Media: []MediaRef{
			{Kind: "image", Name: "photo.png", MimeType: "image/png", Size: 1234},
			{Kind: "file", Name: "report.pdf", FetchError: "download failed"},
		},
	}
	fields, err := encode(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := decode(fields)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Kind != "card" {
		t.Errorf("Kind lost: %q", out.Kind)
	}
	if len(out.Segments) != 2 || out.Segments[0].Type != "title" {
		t.Fatalf("Segments lost: %+v", out.Segments)
	}
	action := out.Segments[1].Actions
	if len(action) != 1 || action[0].Text != "批准" || action[0].Value["decision"] != "approve" {
		t.Errorf("button callback payload lost: %+v", action)
	}
	if len(out.Media) != 2 {
		t.Fatalf("Media lost: %+v", out.Media)
	}
	if out.Media[0].Kind != "image" || out.Media[0].Size != 1234 {
		t.Errorf("media descriptor lost: %+v", out.Media[0])
	}
	if out.Media[1].FetchError != "download failed" {
		t.Errorf("media fetch error lost: %+v", out.Media[1])
	}
}

// TestDecodeDropsUnreadableOptionalPayloads: a corrupt segments/media field must
// not cost the user their message — the text still decodes.
func TestDecodeDropsUnreadableOptionalPayloads(t *testing.T) {
	fields := map[string]interface{}{
		"id":         "msg-3",
		"session_id": "sess-1",
		"kind":       "card",
		"segments":   "{not json",
		"media":      "[not json",
	}
	out, err := decode(fields)
	if err != nil {
		t.Fatalf("decode must tolerate a corrupt optional field: %v", err)
	}
	if out.Segments != nil || out.Media != nil {
		t.Errorf("corrupt payloads must be dropped: %+v", out)
	}
	if out.Kind != "card" {
		t.Errorf("Kind lost: %q", out.Kind)
	}
}
