package session

import (
	"strings"
	"testing"
)

func validSession() Session {
	return Session{TenantID: "tenant-a", ID: "session-a", AgentAppID: "agent-a", AgentVersion: 1, Channel: "web", BindingID: "binding-a", State: StateActive, StateVersion: 1}
}

func validEvent(seq int64) SessionEvent {
	return SessionEvent{TenantID: "tenant-a", SessionID: "session-a", Seq: seq, EventID: "event-a", EventType: EventUserReceived, MessageID: "message-a", ExecutionID: "execution-a", Attempt: 1, TraceID: "trace-a"}
}

func TestSessionValidationAndTransitions(t *testing.T) {
	value := validSession()
	if err := value.Validate(); err != nil {
		t.Fatal(err)
	}
	next, err := value.Transition(StatePaused)
	if err != nil || next.StateVersion != 2 {
		t.Fatalf("transition failed: %v %+v", err, next)
	}
	if _, err := next.Transition(StateActive); err != nil {
		t.Fatal(err)
	}
	completed, err := next.Transition(StateCompleted)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := completed.Transition(StateActive); err == nil {
		t.Fatal("expected completed -> active rejection")
	}
}

func TestSessionEventValidationRejectsSensitivePayload(t *testing.T) {
	event := validEvent(1)
	event.Payload = map[string]any{"nested": map[string]any{"api_key": "secret"}, "message": "sk-secret"}
	if err := event.Validate(); err == nil {
		t.Fatal("expected sensitive payload rejection")
	}
	event.Payload = RedactPayload(event.Payload)
	if err := event.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestSummaryValidation(t *testing.T) {
	value := Summary{TenantID: "tenant-a", SessionID: "session-a", Version: 1}
	if err := value.Validate(); err != nil {
		t.Fatal(err)
	}
	value.TokenEstimate = -1
	if err := value.Validate(); err == nil {
		t.Fatal("expected negative token estimate rejection")
	}
}

func TestLegacySessionIDAndNewKeyAreStableAndDistinct(t *testing.T) {
	key, err := NewSessionKey("tenant-a", "telegram", "binding-a", "user:chat-a", "")
	if err != nil {
		t.Fatal(err)
	}
	firstID := key.ID()
	if firstID != key.ID() || !strings.HasPrefix(key.String(), "v1:") {
		t.Fatal("new session key is not stable/versioned")
	}
	legacy := LegacySessionID("tenant-a", "telegram", "chat-a", "")
	if len(legacy) != 64 || legacy == key.ID() {
		t.Fatal("legacy compatibility key is invalid")
	}
	compatible := key.CompatibleIDs()
	if len(compatible) != 2 || compatible[0] != key.ID() || compatible[1] != legacy {
		t.Fatalf("unexpected compatible IDs: %v", compatible)
	}
	other, err := NewSessionKey("tenant-b", "telegram", "binding-a", "user:chat-a", "")
	if err != nil {
		t.Fatal(err)
	}
	if other.ID() == key.ID() {
		t.Fatal("tenant keys collided")
	}
}

func TestSessionKeyRejectsSeparatorAndMissingFields(t *testing.T) {
	if _, err := NewSessionKey("tenant-a", "", "binding-a", "user:scope", ""); err == nil {
		t.Fatal("expected missing channel rejection")
	}
	if _, err := NewSessionKey("tenant|a", "web", "binding-a", "user:scope", ""); err == nil {
		t.Fatal("expected separator rejection")
	}
	if _, err := NewSessionKey("tenant-a", "telegram", "binding-a", "user:chat-a", "topic-1"); err == nil {
		t.Fatal("expected topic on user scope rejection")
	}
	if _, err := NewSessionKey("tenant-a", "telegram", "binding-a", "group:chat-a", "topic-1"); err != nil {
		t.Fatal(err)
	}
}
