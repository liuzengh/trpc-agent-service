package contentsafety

import (
	"context"
	"errors"
	"testing"
)

func testRequest(text string) Request {
	return Request{TenantID: "acme", AppName: "assistant", SessionID: "session-1", MessageKey: "message-1",
		ConfigRevision: "r1", Phase: PhaseInput, PolicyVersion: "policy-v1", ContentHash: HashContent(text), Text: text}
}

func TestMemoryCheckerIsIdempotentAndFailClosed(t *testing.T) {
	checker := NewMemory(nil)
	first, err := checker.Check(context.Background(), testRequest("hello"))
	if err != nil || first.Status != StatusAllowed {
		t.Fatalf("allowed decision = %+v, %v", first, err)
	}
	second, err := checker.Check(context.Background(), testRequest("hello"))
	if err != nil || second.DecisionHash != first.DecisionHash || second.Attempts != first.Attempts {
		t.Fatalf("replay decision = %+v, %v", second, err)
	}
	blockedRequest := testRequest("content-safety-block")
	blockedRequest.MessageKey = "message-2"
	blocked, err := checker.Check(context.Background(), blockedRequest)
	if !errors.Is(err, ErrBlocked) || blocked.Status != StatusBlocked {
		t.Fatalf("blocked decision = %+v, %v", blocked, err)
	}
}

func TestMemoryCheckerRejectsContentHashConflict(t *testing.T) {
	checker := NewMemory(nil)
	request := testRequest("hello")
	if _, err := checker.Check(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	request.ContentHash = HashContent("changed")
	if _, err := checker.Check(context.Background(), request); err == nil {
		t.Fatal("same decision key accepted a conflicting content hash")
	}
}
