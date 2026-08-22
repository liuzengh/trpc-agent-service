package storage

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/artifact"
	"github.com/liuzengh/trpc-agent-service/trpcservice/session"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func validContext(tenantID string) tenant.TenantContext {
	return tenant.TenantContext{TenantID: tenantID, AgentAppID: "agent-a", BindingID: "binding-a", Channel: "web", RequestID: "request-a", MessageID: "message-a", TraceID: "trace-a", ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "memory", Memory: "memory", Vector: "none", Object: "memory"}}
}

func validStoredSession(tenantID string) session.Session {
	return session.Session{TenantID: tenantID, ID: "session-a", AgentAppID: "agent-a", AgentVersion: 1, Channel: "web", BindingID: "binding-a", State: session.StateActive, StateVersion: 1}
}

func validStoredEvent(tenantID string, seq int64) session.SessionEvent {
	return session.SessionEvent{TenantID: tenantID, SessionID: "session-a", Seq: seq, EventID: "event-a", EventType: session.EventUserReceived, MessageID: "message-a", ExecutionID: "execution-a", Attempt: 1, TraceID: "trace-a"}
}

func validStoredArtifact(tenantID string) artifact.Artifact {
	return artifact.Artifact{TenantID: tenantID, ID: "artifact-a", SessionID: "session-a", MessageID: "message-a", ObjectKey: "tenants/" + tenantID + "/artifact-a", MIMEType: "text/plain", Status: artifact.StatusPending}
}

func TestFakeRepositoryEnforcesTenantAndCAS(t *testing.T) {
	repo := NewFakeRepository()
	ctx := context.Background()
	tc := validContext("tenant-a")
	if err := repo.Create(ctx, tc, validStoredSession("tenant-a")); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Get(ctx, validContext("tenant-b"), "session-a"); !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("unexpected cross-tenant result: %v", err)
	}
	if _, err := repo.AppendEvent(ctx, tc, 2, validStoredEvent("tenant-a", 1)); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected CAS conflict, got %v", err)
	}
	if _, err := repo.AppendEvent(ctx, tc, 1, validStoredEvent("tenant-a", 3)); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected out-of-order event rejection, got %v", err)
	}
	updated, err := repo.AppendEvent(ctx, tc, 1, validStoredEvent("tenant-a", 1))
	if err != nil || updated.LastEventSeq != 1 || updated.StateVersion != 2 {
		t.Fatalf("append failed: %v %+v", err, updated)
	}
}

func TestFakeClaimHasOneConcurrentWinnerAndFenceRejectsStaleOwner(t *testing.T) {
	repo := NewFakeRepository()
	tc := validContext("tenant-a")
	const contenders = 100
	claims := make(chan Claim, contenders)
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			claim, err := repo.Claim(context.Background(), tc, "external-a", time.Minute)
			if err == nil {
				claims <- claim
			}
		}()
	}
	wg.Wait()
	close(claims)
	var first Claim
	count := 0
	for claim := range claims {
		first = claim
		count++
	}
	if count != contenders {
		t.Fatalf("expected all contenders to observe claim, got %d", count)
	}
	if err := repo.Complete(context.Background(), tc, first.Key, "response-a", first.FenceToken+1); !errors.Is(err, ErrFenceRejected) {
		t.Fatalf("expected stale fence rejection, got %v", err)
	}
	if err := repo.Complete(context.Background(), tc, first.Key, "response-a", first.FenceToken); err != nil {
		t.Fatal(err)
	}
	completed, err := repo.Claim(context.Background(), tc, "external-a", time.Minute)
	if err != nil || completed.Status != ClaimCompleted || completed.ResponseRef != "response-a" {
		t.Fatalf("expected persisted completion: %v %+v", err, completed)
	}
}

func TestFakeClaimCanBeTakenOverAfterExpiry(t *testing.T) {
	repo := NewFakeRepository()
	tc := validContext("tenant-a")
	first, err := repo.Claim(context.Background(), tc, "external-b", time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	var second Claim
	for {
		second, err = repo.Claim(context.Background(), tc, "external-b", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if second.Attempt == first.Attempt+1 || time.Now().After(deadline) {
			break
		}
	}
	if second.Attempt != first.Attempt+1 || second.FenceToken == first.FenceToken {
		t.Fatalf("expected takeover: first=%+v second=%+v", first, second)
	}
}

func TestFakeDedupRejectsAmbiguousIDs(t *testing.T) {
	repo := NewFakeRepository()
	tc := validContext("tenant-a")
	if _, err := repo.Claim(context.Background(), tc, "external\x00id", time.Minute); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected delimiter rejection, got %v", err)
	}
}

func TestFakeOutboxReclaimsExpiredLockAndRejectsStaleOwner(t *testing.T) {
	ctx := context.Background()
	tc := validContext("tenant-a")
	repo := NewFakeRepository()
	if err := repo.Enqueue(ctx, tc, OutboxMessage{TenantID: "tenant-a", ID: "outbox-expired", Kind: "reply"}); err != nil {
		t.Fatal(err)
	}
	messages, err := repo.ClaimBatch(ctx, tc, "worker-a", 1)
	if err != nil || len(messages) != 1 {
		t.Fatalf("initial outbox claim failed: %v %+v", err, messages)
	}
	repo.mu.Lock()
	value := repo.outbox[outboxKey("tenant-a", "outbox-expired")]
	value.LockedUntil = time.Now().UTC().Add(-time.Second)
	repo.outbox[outboxKey("tenant-a", "outbox-expired")] = value
	repo.mu.Unlock()
	if err := repo.MarkCompleted(ctx, tc, "worker-a", "outbox-expired"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expected expired lease rejection, got %v", err)
	}
	reclaimed, err := repo.ClaimBatch(ctx, tc, "worker-b", 1)
	if err != nil || len(reclaimed) != 1 || reclaimed[0].LockedBy != "worker-b" {
		t.Fatalf("expected expired message reclaim: %v %+v", err, reclaimed)
	}
	if err := repo.MarkCompleted(ctx, tc, "worker-a", "outbox-expired"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected stale owner rejection, got %v", err)
	}
}

func TestFakeRepositoriesCoverSummaryArtifactAndOutbox(t *testing.T) {
	ctx := context.Background()
	tc := validContext("tenant-a")
	repo := NewFakeRepository()
	if err := repo.UpsertIfNewer(ctx, tc, session.Summary{TenantID: "tenant-a", SessionID: "session-a", Version: 2, CoveredSeq: 2}); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpsertIfNewer(ctx, tc, session.Summary{TenantID: "tenant-a", SessionID: "session-a", Version: 1, CoveredSeq: 1}); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected summary monotonicity conflict, got %v", err)
	}
	if err := repo.UpsertIfNewer(ctx, tc, session.Summary{TenantID: "tenant-a", SessionID: "session-a", Version: 2, CoveredSeq: 1}); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected covered sequence monotonicity conflict, got %v", err)
	}
	artifacts := NewFakeArtifactRepository()
	if err := artifacts.Create(ctx, tc, validStoredArtifact("tenant-a")); err != nil {
		t.Fatal(err)
	}
	if _, err := artifacts.PresignedURL(ctx, validContext("tenant-b"), "artifact-a", time.Minute); !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("unexpected artifact isolation result: %v", err)
	}
	if err := repo.Enqueue(ctx, tc, OutboxMessage{TenantID: "tenant-a", ID: "outbox-invalid", Kind: "reply", Status: OutboxCompleted}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected invalid initial status rejection, got %v", err)
	}
	if err := repo.Enqueue(ctx, tc, OutboxMessage{TenantID: "tenant-a", ID: "outbox-large", Kind: "reply", Payload: make([]byte, maxOutboxPayloadBytes+1)}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected oversized payload rejection, got %v", err)
	}
	if err := repo.Enqueue(ctx, tc, OutboxMessage{TenantID: "tenant-a", ID: "outbox-a", Kind: "reply"}); err != nil {
		t.Fatal(err)
	}
	messages, err := repo.ClaimBatch(ctx, tc, "worker-a", 1)
	if err != nil || len(messages) != 1 || messages[0].Status != OutboxProcessing {
		t.Fatalf("outbox claim failed: %v %+v", err, messages)
	}
	if err := repo.MarkCompleted(ctx, validContext("tenant-b"), "worker-a", "outbox-a"); !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("expected cross-tenant outbox rejection, got %v", err)
	}
	if err := repo.MarkCompleted(ctx, tc, "worker-b", "outbox-a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected owner isolation, got %v", err)
	}
	if err := repo.MarkCompleted(ctx, tc, "worker-a", "outbox-a"); err != nil {
		t.Fatal(err)
	}
}
