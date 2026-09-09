package identity

import (
	"regexp"
	"testing"
)

func TestIdentityDerivationIsStableAndScoped(t *testing.T) {
	secret := []byte("01234567890123456789012345678901")
	a := RunnerUserID(secret, "binding", "user")
	if a != RunnerUserID(secret, "binding", "user") {
		t.Fatal("expected stable runner user ID")
	}
	if a == RunnerUserID(secret, "other-binding", "user") {
		t.Fatal("expected binding-scoped runner user ID")
	}
	if SessionID(secret, "binding", "conversation") == SessionID(secret, "binding", "other") {
		t.Fatal("expected conversation-scoped session ID")
	}
	group := GroupRunnerUserID(secret, "binding", "conversation")
	if group != GroupRunnerUserID(secret, "binding", "conversation") || group == GroupRunnerUserID(secret, "binding", "other") {
		t.Fatal("expected stable, conversation-scoped group subject ID")
	}
	if group[:2] != "g_" {
		t.Fatalf("unexpected group subject prefix: %q", group)
	}
}

func TestRequestAndTraceIDFormats(t *testing.T) {
	requestID, err := RequestID()
	if err != nil {
		t.Fatal(err)
	}
	traceID, err := TraceID()
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^[0-9a-f]{16}$`).MatchString(requestID) {
		t.Fatalf("invalid request ID %q", requestID)
	}
	if !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(traceID) {
		t.Fatalf("invalid trace ID %q", traceID)
	}
}
