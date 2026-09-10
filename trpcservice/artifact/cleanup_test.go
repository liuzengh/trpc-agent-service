package artifact

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestCleanupWorkerCompletesExactCandidates(t *testing.T) {
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	candidate := cleanupTestCandidate("artifact-1", StatusDeleted, 1)
	store := &cleanupStoreFake{candidates: []CleanupCandidate{candidate}}
	var deleted []string
	worker, err := NewCleanupWorker(store, func(_ context.Context, value CleanupCandidate) error {
		deleted = append(deleted, value.Record.ObjectKey)
		return nil
	}, CleanupOptions{
		Owner:         "worker-1",
		PendingAge:    time.Hour,
		RetentionAge:  24 * time.Hour,
		LeaseDuration: time.Minute,
		BatchSize:     8,
		Now:           func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("new cleanup worker: %v", err)
	}

	count, err := worker.RunPass(context.Background())
	if err != nil {
		t.Fatalf("run cleanup pass: %v", err)
	}
	if count != 1 || !reflect.DeepEqual(deleted, []string{"objects/artifact-1"}) {
		t.Fatalf("deleted count/objects = %d/%v", count, deleted)
	}
	if len(store.completed) != 1 || store.completed[0].Record.ID != candidate.Record.ID {
		t.Fatalf("completed candidates = %#v", store.completed)
	}
	if store.owner != "worker-1" || store.pendingBefore != now.Add(-time.Hour) {
		t.Fatalf("claim arguments = owner %q cutoff %s", store.owner, store.pendingBefore)
	}
	if store.retentionBefore == nil || !store.retentionBefore.Equal(now.Add(-24*time.Hour)) {
		t.Fatalf("retention cutoff = %v", store.retentionBefore)
	}
}

func TestCleanupWorkerProcessesInboundArtifactCleanup(t *testing.T) {
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	candidate := InboundCleanupCandidate{
		TenantID: "tenant-1", AppID: "app-1", BindingID: "binding-1",
		ExternalMessageID: "message-1", ItemNo: 0,
		ArtifactRef: "artifact://inbound/item@0", ConfigVersion: "v1",
		ObjectKey: "objects/inbound-item", Attempts: 1,
	}
	store := &inboundCleanupStoreFake{inboundCandidates: []InboundCleanupCandidate{candidate}}
	var deleted []string
	worker, err := NewCleanupWorker(store, func(context.Context, CleanupCandidate) error { return nil }, CleanupOptions{
		Owner: "worker-1",
		Now:   func() time.Time { return now },
		InboundDeleteObject: func(_ context.Context, value InboundCleanupCandidate) error {
			deleted = append(deleted, value.ObjectKey)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("new cleanup worker: %v", err)
	}

	count, err := worker.RunPass(context.Background())
	if err != nil {
		t.Fatalf("run cleanup pass: %v", err)
	}
	if count != 1 || len(deleted) != 1 || deleted[0] != candidate.ObjectKey {
		t.Fatalf("inbound cleanup count/objects = %d/%v", count, deleted)
	}
	if len(store.inboundCompleted) != 1 || store.inboundCompleted[0].ArtifactRef != candidate.ArtifactRef {
		t.Fatalf("completed inbound candidates = %#v", store.inboundCompleted)
	}
}

func TestCleanupWorkerPersistsRetryAfterDeleteFailure(t *testing.T) {
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	candidate := cleanupTestCandidate("artifact-2", StatusPending, 3)
	store := &cleanupStoreFake{candidates: []CleanupCandidate{candidate}}
	deleteErr := errors.New("object storage unavailable")
	worker, err := NewCleanupWorker(store, func(context.Context, CleanupCandidate) error {
		return deleteErr
	}, CleanupOptions{
		Owner:        "worker-1",
		RetryInitial: 2 * time.Second,
		RetryMax:     10 * time.Second,
		Now:          func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("new cleanup worker: %v", err)
	}

	count, err := worker.RunPass(context.Background())
	if count != 0 || !errors.Is(err, deleteErr) {
		t.Fatalf("run cleanup pass = %d/%v, want delete failure", count, err)
	}
	if len(store.retried) != 1 || !errors.Is(store.retried[0].cause, deleteErr) {
		t.Fatalf("retry records = %#v", store.retried)
	}
	if got, want := store.retried[0].nextAttempt, now.Add(8*time.Second); !got.Equal(want) {
		t.Fatalf("retry time = %s, want %s", got, want)
	}
	if len(store.completed) != 0 {
		t.Fatalf("completed candidates = %#v", store.completed)
	}
}

func TestCleanupWorkerCanDisableRetentionWithoutDisablingOrphanCleanup(t *testing.T) {
	store := &cleanupStoreFake{}
	worker, err := NewCleanupWorker(store, func(context.Context, CleanupCandidate) error { return nil }, CleanupOptions{
		Owner: "worker-1",
		Now:   func() time.Time { return time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatalf("new cleanup worker: %v", err)
	}
	if _, err := worker.RunPass(context.Background()); err != nil {
		t.Fatalf("run cleanup pass: %v", err)
	}
	if store.retentionBefore != nil {
		t.Fatalf("retention cutoff = %v, want disabled", store.retentionBefore)
	}
}

func TestNewCleanupWorkerRejectsInvalidRetention(t *testing.T) {
	_, err := NewCleanupWorker(&cleanupStoreFake{}, func(context.Context, CleanupCandidate) error { return nil }, CleanupOptions{
		Owner:        "worker-1",
		RetentionAge: -time.Second,
	})
	if err == nil {
		t.Fatal("negative retention age was accepted")
	}
}

func cleanupTestCandidate(id string, status Status, attempts int) CleanupCandidate {
	return CleanupCandidate{
		Record: Record{
			ID:                 id,
			TenantID:           "tenant-1",
			AppID:              "app-1",
			ConfigVersion:      "v1",
			SessionPrincipalID: "user-1",
			SessionID:          "session-1",
			Filename:           "report.txt",
			Version:            0,
			ObjectKey:          "objects/" + id,
			Status:             status,
		},
		Attempts: attempts,
	}
}

type cleanupStoreFake struct {
	candidates      []CleanupCandidate
	owner           string
	pendingBefore   time.Time
	retentionBefore *time.Time
	completed       []CleanupCandidate
	retried         []cleanupRetry
}

type inboundCleanupStoreFake struct {
	cleanupStoreFake
	inboundCandidates []InboundCleanupCandidate
	inboundCompleted  []InboundCleanupCandidate
}

type cleanupRetry struct {
	candidate   CleanupCandidate
	owner       string
	nextAttempt time.Time
	cause       error
}

func (s *cleanupStoreFake) ClaimArtifactCleanup(
	_ context.Context,
	owner string,
	pendingBefore time.Time,
	retentionBefore *time.Time,
	_ time.Duration,
	_ int,
) ([]CleanupCandidate, error) {
	s.owner = owner
	s.pendingBefore = pendingBefore
	s.retentionBefore = retentionBefore
	return s.candidates, nil
}

func (s *cleanupStoreFake) CompleteArtifactCleanup(_ context.Context, candidate CleanupCandidate, _ string) error {
	s.completed = append(s.completed, candidate)
	return nil
}

func (s *cleanupStoreFake) RetryArtifactCleanup(_ context.Context, candidate CleanupCandidate, owner string, nextAttempt time.Time, cause error) error {
	s.retried = append(s.retried, cleanupRetry{
		candidate: candidate, owner: owner, nextAttempt: nextAttempt, cause: cause,
	})
	return nil
}

var _ CleanupStore = (*cleanupStoreFake)(nil)

func (s *inboundCleanupStoreFake) ClaimInboundArtifactCleanup(
	_ context.Context,
	_ string,
	_ time.Time,
	_ time.Duration,
	_ int,
) ([]InboundCleanupCandidate, error) {
	return s.inboundCandidates, nil
}

func (s *inboundCleanupStoreFake) CompleteInboundArtifactCleanup(
	_ context.Context,
	candidate InboundCleanupCandidate,
	_ string,
) error {
	s.inboundCompleted = append(s.inboundCompleted, candidate)
	return nil
}

func (s *inboundCleanupStoreFake) RetryInboundArtifactCleanup(
	_ context.Context,
	_ InboundCleanupCandidate,
	_ string,
	_ time.Time,
	_ error,
) error {
	return nil
}

var _ InboundCleanupStore = (*inboundCleanupStoreFake)(nil)
