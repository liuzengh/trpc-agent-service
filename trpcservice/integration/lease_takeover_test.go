package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	coordinationmemory "github.com/liuzengh/trpc-agent-service/trpcservice/coordination/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
	sessionmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session/inmemory"
)

// TestLeaseExpiryTakeoverRejectsOldFenceCommit is a process-local integration
// contract for the crash path. It composes the real local coordinator and
// AtomicSessionStore implementations with an injected clock, so CI does not
// need SIGKILL, a real-time lease wait, or a running Redis/PostgreSQL pair.
func TestLeaseExpiryTakeoverRejectsOldFenceCommit(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.September, 9, 0, 0, 0, 0, time.UTC)
	leases := coordinationmemory.NewWithClock(func() time.Time { return now })
	sessions := sessionmemory.New()
	key := coordination.SessionKey{TenantID: "tenant-a", AgentAppID: "app", SessionID: "session"}

	ownerA, err := leases.Acquire(ctx, key, "worker-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	head, err := sessions.OpenForRun(ctx, sessionstore.OpenForRunRequest{SessionKey: sessionstore.SessionKey(key), RequestID: "request-1", InputSeq: 1, Fence: ownerA.Fence})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessions.CommitTurn(ctx, terminalCommit(sessionstore.SessionKey(key), "request-1", 1, ownerA.Fence, head.Version)); err != nil {
		t.Fatal(err)
	}

	// Worker A has stopped renewing. Advance the injected lease clock past its
	// TTL, prove A lost authority, then let Worker B acquire a higher fence.
	now = now.Add(time.Minute + time.Nanosecond)
	if _, err := leases.Renew(ctx, ownerA, time.Minute); !errors.Is(err, runtime.ErrLeaseLost) {
		t.Fatalf("expired owner renew err=%v, want lease lost", err)
	}
	ownerB, err := leases.Acquire(ctx, key, "worker-b", time.Minute)
	if err != nil || ownerB.Fence <= ownerA.Fence {
		t.Fatalf("takeover lease=%#v old=%#v err=%v", ownerB, ownerA, err)
	}

	head, err = sessions.OpenForRun(ctx, sessionstore.OpenForRunRequest{SessionKey: sessionstore.SessionKey(key), RequestID: "request-2", InputSeq: 2, Fence: ownerB.Fence})
	if err != nil {
		t.Fatal(err)
	}
	committed, err := sessions.CommitTurn(ctx, terminalCommit(sessionstore.SessionKey(key), "request-2", 2, ownerB.Fence, head.Version))
	if err != nil {
		t.Fatal(err)
	}
	if fence, err := sessions.ReadLastFence(ctx, sessionstore.SessionKey(key)); err != nil || fence != ownerB.Fence {
		t.Fatalf("durable fence=%d want=%d err=%v", fence, ownerB.Fence, err)
	}

	// A stale execution cannot append a later turn, even if it observed the
	// current session version before attempting the commit.
	stale := terminalCommit(sessionstore.SessionKey(key), "request-stale", 3, ownerA.Fence, committed.SessionVersion)
	if _, err := sessions.CommitTurn(ctx, stale); !errors.Is(err, runtime.ErrStaleFence) {
		t.Fatalf("stale owner commit err=%v, want stale fence", err)
	}
}

func terminalCommit(key sessionstore.SessionKey, requestID string, inputSeq, fence uint64, version int64) sessionstore.CommitTurnRequest {
	return sessionstore.CommitTurnRequest{SessionKey: key, RequestID: requestID, CommitID: requestID + ":terminal", Stage: "terminal",
		InputSeq: inputSeq, Fence: fence, ExpectedVersion: version, Outcome: runtime.OutcomeDenied}
}
