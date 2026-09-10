// Package contracttest contains the backend-neutral AtomicSessionStore suite.
package contracttest

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
)

// Factory returns an isolated store with the supplied request/input pairs
// prepared. PostgreSQL harnesses perform ClaimInbox/PrepareDispatch here; the
// InMemory harness can ignore them because OpenForRun creates its local head.
type Factory func(testing.TB, sessionstore.SessionKey, map[string]uint64) sessionstore.AtomicSessionStore

func Run(t *testing.T, factory Factory) {
	t.Helper()
	const tenantID = "t_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const agentAppID = "app_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	t.Run("commit_idempotency_and_collision", func(t *testing.T) {
		ctx := context.Background()
		key := sessionstore.SessionKey{TenantID: tenantID, AgentAppID: agentAppID, SessionID: "session-idempotency"}
		store := factory(t, key, map[string]uint64{"request-idempotency": 1})
		head, err := store.OpenForRun(ctx, sessionstore.OpenForRunRequest{SessionKey: key, RequestID: "request-idempotency", InputSeq: 1, Fence: 10})
		if err != nil {
			t.Fatal(err)
		}
		commit := sessionstore.CommitTurnRequest{SessionKey: key, RequestID: "request-idempotency", CommitID: "request-idempotency:waiting:0", Stage: "waiting", InputSeq: 1, Fence: 10, ExpectedVersion: head.Version, Outcome: runtime.OutcomeWaitingConfirmation, ResultRef: "checkpoint://one"}
		first, err := store.CommitTurn(ctx, commit)
		if err != nil {
			t.Fatal(err)
		}
		again, err := store.CommitTurn(ctx, commit)
		if err != nil || again != first {
			t.Fatalf("idempotent retry result=%#v err=%v", again, err)
		}
		commit.ResultRef = "checkpoint://different"
		if _, err := store.CommitTurn(ctx, commit); !errors.Is(err, runtime.ErrCommitConflict) {
			t.Fatalf("collision=%v", err)
		}
	})

	t.Run("input_gate_version_and_fence", func(t *testing.T) {
		ctx := context.Background()
		key := sessionstore.SessionKey{TenantID: tenantID, AgentAppID: agentAppID, SessionID: "session-gates"}
		store := factory(t, key, map[string]uint64{"request-gates": 1, "request-second": 2, "request-future": 3})
		head, err := store.OpenForRun(ctx, sessionstore.OpenForRunRequest{SessionKey: key, RequestID: "request-gates", InputSeq: 1, Fence: 20})
		if err != nil {
			t.Fatal(err)
		}
		invalid := sessionstore.CommitTurnRequest{SessionKey: key, RequestID: "request-gates", CommitID: "bad-version", Stage: "waiting", InputSeq: 1, Fence: 20, ExpectedVersion: head.Version + 1, Outcome: runtime.OutcomeWaitingConfirmation}
		if _, err := store.CommitTurn(ctx, invalid); !errors.Is(err, runtime.ErrVersionConflict) {
			t.Fatalf("version=%v", err)
		}
		if fence, err := store.ReadLastFence(ctx, key); err != nil || fence != 0 {
			t.Fatalf("failed commit leaked fence=%d err=%v", fence, err)
		}
		waiting := invalid
		waiting.CommitID, waiting.ExpectedVersion = "waiting", head.Version
		if _, err := store.CommitTurn(ctx, waiting); err != nil {
			t.Fatal(err)
		}
		if _, err := store.OpenForRun(ctx, sessionstore.OpenForRunRequest{SessionKey: key, RequestID: "request-gates", InputSeq: 1, Fence: 19}); !errors.Is(err, runtime.ErrStaleFence) {
			t.Fatalf("stale open=%v", err)
		}
		head, err = store.OpenForRun(ctx, sessionstore.OpenForRunRequest{SessionKey: key, RequestID: "request-gates", InputSeq: 1, Fence: 20})
		if err != nil {
			t.Fatal(err)
		}
		terminal := sessionstore.CommitTurnRequest{SessionKey: key, RequestID: "request-gates", CommitID: "terminal", Stage: "terminal", InputSeq: 1, Fence: 21, ExpectedVersion: head.Version, Outcome: runtime.OutcomeSucceeded, ResultRef: "result://done"}
		if _, err := store.CommitTurn(ctx, terminal); err != nil {
			t.Fatal(err)
		}
		if _, err := store.OpenForRun(ctx, sessionstore.OpenForRunRequest{SessionKey: key, RequestID: "request-future", InputSeq: 3, Fence: 21}); !errors.Is(err, runtime.ErrInputNotReady) {
			t.Fatalf("future input=%v", err)
		}
	})

	t.Run("terminal_replay_returns_authoritative_result", func(t *testing.T) {
		ctx := context.Background()
		key := sessionstore.SessionKey{TenantID: tenantID, AgentAppID: agentAppID, SessionID: "session-terminal"}
		store := factory(t, key, map[string]uint64{"request-terminal": 1})
		head, err := store.OpenForRun(ctx, sessionstore.OpenForRunRequest{SessionKey: key, RequestID: "request-terminal", InputSeq: 1, Fence: 30})
		if err != nil {
			t.Fatal(err)
		}
		commit := sessionstore.CommitTurnRequest{SessionKey: key, RequestID: "request-terminal", CommitID: "winner", Stage: "terminal", InputSeq: 1, Fence: 30, ExpectedVersion: head.Version, Outcome: runtime.OutcomeSucceeded, ResultRef: "result://winner"}
		winner, err := store.CommitTurn(ctx, commit)
		if err != nil {
			t.Fatal(err)
		}
		loser := commit
		loser.CommitID, loser.ResultRef = "loser", "result://loser"
		actual, err := store.CommitTurn(ctx, loser)
		if !errors.Is(err, runtime.ErrAlreadyTerminal) || actual != winner {
			t.Fatalf("terminal replay result=%#v want=%#v err=%v", actual, winner, err)
		}
		stored, err := store.GetTerminalByInputSeq(ctx, sessionstore.TerminalKey{SessionKey: key, InputSeq: 1})
		if err != nil || stored != winner {
			t.Fatalf("terminal read result=%#v err=%v", stored, err)
		}
	})
}
