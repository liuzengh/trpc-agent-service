package gateway

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
)

func TestMemoryJournalAcceptAndDuplicate(t *testing.T) {
	journal := NewMemoryJournal()
	t.Cleanup(func() { _ = journal.Close() })
	request := testInboundRequest(t, "message-1", "hello")
	first, err := journal.Accept(context.Background(), request)
	if err != nil {
		t.Fatalf("accept first: %v", err)
	}
	second, err := journal.Accept(context.Background(), request)
	if err != nil {
		t.Fatalf("accept duplicate: %v", err)
	}
	if first.RequestID != second.RequestID || !second.Duplicate || len(journal.Tasks()) != 1 {
		t.Fatalf("first=%+v second=%+v tasks=%d", first, second, len(journal.Tasks()))
	}
}

func TestMemoryJournalPinsRevisionPerConversation(t *testing.T) {
	journal := NewMemoryJournal()
	t.Cleanup(func() { _ = journal.Close() })
	first := testInboundRequest(t, "message-1", "first")
	resultOne, err := journal.Accept(context.Background(), first)
	if err != nil {
		t.Fatalf("accept first: %v", err)
	}
	second := testInboundRequest(t, "message-2", "second")
	second.Scope.RevisionID = "revision-2"
	resultTwo, err := journal.Accept(context.Background(), second)
	if err != nil {
		t.Fatalf("accept second: %v", err)
	}
	if resultTwo.RevisionID != resultOne.RevisionID || resultTwo.TurnSeq != 2 {
		t.Fatalf("revision was not pinned: first=%+v second=%+v", resultOne, resultTwo)
	}
}

func TestMemoryJournalRejectsPayloadConflict(t *testing.T) {
	journal := NewMemoryJournal()
	t.Cleanup(func() { _ = journal.Close() })
	if _, err := journal.Accept(
		context.Background(),
		testInboundRequest(t, "same-message", "first"),
	); err != nil {
		t.Fatalf("accept first: %v", err)
	}
	if _, err := journal.Accept(
		context.Background(),
		testInboundRequest(t, "same-message", "second"),
	); !errors.Is(err, ErrMessageConflict) {
		t.Fatalf("conflict error = %v", err)
	}
}

func TestMemoryJournalTenantIsolation(t *testing.T) {
	journal := NewMemoryJournal()
	t.Cleanup(func() { _ = journal.Close() })
	first := testInboundRequest(t, "same-message", "hello")
	second := first
	second.Scope, _ = runtimecontext.NewScope(
		"tenant-b", "app-b", "revision-b", "http", "binding-b",
	)
	firstResult, err := journal.Accept(context.Background(), first)
	if err != nil {
		t.Fatalf("accept first tenant: %v", err)
	}
	secondResult, err := journal.Accept(context.Background(), second)
	if err != nil {
		t.Fatalf("accept second tenant: %v", err)
	}
	if firstResult.RequestID == secondResult.RequestID || len(journal.Tasks()) != 2 {
		t.Fatalf("tenant messages collided: first=%+v second=%+v", firstResult, secondResult)
	}
}

func testInboundRequest(t *testing.T, messageID string, text string) InboundRequest {
	t.Helper()
	scope, err := runtimecontext.NewScope(
		"tenant-a", "app-a", "revision-a", "http", "binding-a",
	)
	if err != nil {
		t.Fatalf("new scope: %v", err)
	}
	return InboundRequest{
		Scope:             scope,
		ExternalMessageID: messageID,
		UserID:            "alice",
		SessionID:         "session",
		ChatType:          "direct",
		Text:              text,
	}
}
