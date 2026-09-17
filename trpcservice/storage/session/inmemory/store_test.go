package inmemory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
)

func TestCommitIdempotencyAndStaleFence(t *testing.T) {
	ctx := context.Background()
	s := New()
	key := sessionstore.SessionKey{TenantID: "t", AgentAppID: "a", SessionID: "s"}
	head, err := s.OpenForRun(ctx, sessionstore.OpenForRunRequest{SessionKey: key, RequestID: "r1", InputSeq: 1, Fence: 41})
	if err != nil {
		t.Fatal(err)
	}
	commit := sessionstore.CommitTurnRequest{SessionKey: key, RequestID: "r1", CommitID: "c1", Stage: "terminal", InputSeq: 1, Fence: 41, ExpectedVersion: head.Version, Outcome: runtime.OutcomeSucceeded, ResultRef: "result"}
	first, err := s.CommitTurn(ctx, commit)
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.CommitTurn(ctx, commit)
	if err != nil || again != first {
		t.Fatalf("idempotent result=%#v err=%v", again, err)
	}
	head, err = s.OpenForRun(ctx, sessionstore.OpenForRunRequest{SessionKey: key, RequestID: "r2", InputSeq: 2, Fence: 42})
	if err != nil {
		t.Fatal(err)
	}
	commit2 := sessionstore.CommitTurnRequest{SessionKey: key, RequestID: "r2", CommitID: "c2", Stage: "terminal", InputSeq: 2, Fence: 40, ExpectedVersion: head.Version, Outcome: runtime.OutcomeSucceeded}
	if _, err = s.CommitTurn(ctx, commit2); !errors.Is(err, runtime.ErrStaleFence) {
		t.Fatalf("got %v", err)
	}
}

func TestCommitInputGateVersionAndEffects(t *testing.T) {
	ctx := context.Background()
	store := New()
	key := sessionstore.SessionKey{TenantID: "t", AgentAppID: "a", SessionID: "s"}
	head, err := store.OpenForRun(ctx, sessionstore.OpenForRunRequest{SessionKey: key, RequestID: "r1", InputSeq: 1, Fence: 10})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.OpenForRun(ctx, sessionstore.OpenForRunRequest{SessionKey: key, RequestID: "r2", InputSeq: 2, Fence: 10}); !errors.Is(err, runtime.ErrInputNotReady) {
		t.Fatalf("future input: %v", err)
	}
	request := sessionstore.CommitTurnRequest{
		SessionKey: key, RequestID: "r1", CommitID: "r1:terminal:0", Stage: "terminal",
		InputSeq: 1, Fence: 10, ExpectedVersion: head.Version, Outcome: runtime.OutcomeSucceeded,
		Events:           []sessionstore.BufferedEvent{{EventID: "e1", EventType: "message", PayloadRef: "event://e1", EventSeq: 1}},
		StateDelta:       sessionstore.StateDelta{"last": "e1"},
		SummaryCandidate: &sessionstore.SummaryCandidate{SummaryID: "summary-1", BaseSessionSeq: 1, LastEventID: "e1", ContentRef: "summary://1", CutoffAt: time.Now().UTC()},
		ResultRef:        "result://r1", ReplyCursor: "r1:1",
		Outbox: []sessionstore.OutboxEvent{{Kind: "reply", IdempotencyKey: "reply:r1:1", PayloadRef: "result://r1", EventSeq: 1}, {Kind: "wakeup", IdempotencyKey: "wakeup:s:2", PayloadRef: "session://s", EventSeq: 2}},
	}
	result, err := store.CommitTurn(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.InputSeq != 1 || result.SessionVersion != 1 {
		t.Fatalf("result=%#v", result)
	}
	events, outboxes, summary := store.SnapshotEffects(key)
	if len(events) != 1 || len(outboxes) != 2 || summary == nil || summary.BaseSessionSeq != 1 {
		t.Fatalf("effects events=%#v outboxes=%#v summary=%#v", events, outboxes, summary)
	}
	request.ExpectedVersion = 0
	request.CommitID = "different"
	if _, err := store.CommitTurn(ctx, request); !errors.Is(err, runtime.ErrAlreadyTerminal) {
		t.Fatalf("terminal replay: %v", err)
	}
}

func TestConcurrentTerminalCommitHasOneWinner(t *testing.T) {
	ctx := context.Background()
	store := New()
	key := sessionstore.SessionKey{TenantID: "t", AgentAppID: "a", SessionID: "s"}
	head, err := store.OpenForRun(ctx, sessionstore.OpenForRunRequest{SessionKey: key, RequestID: "r", InputSeq: 1, Fence: 7})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, commitID := range []string{"c1", "c2"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			_, commitErr := store.CommitTurn(ctx, sessionstore.CommitTurnRequest{SessionKey: key, RequestID: "r", CommitID: id, Stage: "terminal", InputSeq: 1, Fence: 7, ExpectedVersion: head.Version, Outcome: runtime.OutcomeSucceeded})
			results <- commitErr
		}(commitID)
	}
	wg.Wait()
	close(results)
	var successes, terminal int
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, runtime.ErrAlreadyTerminal):
			terminal++
		default:
			t.Fatalf("unexpected %v", err)
		}
	}
	if successes != 1 || terminal != 1 {
		t.Fatalf("success=%d terminal=%d", successes, terminal)
	}
}

func TestCommitIDPayloadCollision(t *testing.T) {
	ctx := context.Background()
	store := New()
	key := sessionstore.SessionKey{TenantID: "t", AgentAppID: "a", SessionID: "s"}
	head, err := store.OpenForRun(ctx, sessionstore.OpenForRunRequest{SessionKey: key, RequestID: "r", InputSeq: 1, Fence: 1})
	if err != nil {
		t.Fatal(err)
	}
	request := sessionstore.CommitTurnRequest{SessionKey: key, RequestID: "r", CommitID: "c", Stage: "waiting", InputSeq: 1, Fence: 1, ExpectedVersion: head.Version, Outcome: runtime.OutcomeWaitingConfirmation, ResultRef: "first"}
	if _, err := store.CommitTurn(ctx, request); err != nil {
		t.Fatal(err)
	}
	request.ResultRef = "different"
	if _, err := store.CommitTurn(ctx, request); !errors.Is(err, runtime.ErrCommitConflict) {
		t.Fatalf("got %v", err)
	}
}
