package channels

import "testing"

func TestBuildSessionIDSingle(t *testing.T) {
	got := BuildSessionID("t1", "wecom", ChatTypeSingle, "openid-123")
	want := "t1:wecom:user:openid-123"
	if got != want {
		t.Errorf("BuildSessionID(single) = %q, want %q", got, want)
	}
}

func TestBuildSessionIDGroup(t *testing.T) {
	got := BuildSessionID("t1", "feishu", ChatTypeGroup, "chat-456")
	want := "t1:feishu:group:chat-456"
	if got != want {
		t.Errorf("BuildSessionID(group) = %q, want %q", got, want)
	}
}

func TestBuildSessionIDIsolationAcrossTenant(t *testing.T) {
	a := BuildSessionID("t1", "wecom", ChatTypeSingle, "openid-1")
	b := BuildSessionID("t2", "wecom", ChatTypeSingle, "openid-1")
	if a == b {
		t.Error("sessions from different tenants must not collide")
	}
}

func TestBuildSessionIDIsolationAcrossChatType(t *testing.T) {
	a := BuildSessionID("t1", "wecom", ChatTypeSingle, "same-id")
	b := BuildSessionID("t1", "wecom", ChatTypeGroup, "same-id")
	if a == b {
		t.Error("single and group sessions sharing an id must not collide")
	}
}
