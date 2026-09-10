package migration

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestTransitionGatesFailClosed(t *testing.T) {
	clock := time.Date(2026, 9, 8, 8, 0, 0, 0, time.UTC)
	current, err := NewMigration(CreateRequest{TenantID: "tenant-a", MigrationID: "migration-1", Domain: "session", Epoch: 1,
		Source: Binding{ConfigVersion: 1, BackendProfileID: "source", BackendVersion: 1},
		Target: Binding{ConfigVersion: 2, BackendProfileID: "target", BackendVersion: 1}, CreatedAt: clock})
	if err != nil {
		t.Fatal(err)
	}
	request := TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version,
		To: StateDualWrite, At: clock.Add(time.Minute), DualWriteRef: "log://migration"}
	if _, err := ApplyTransition(current, request); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("skipped state accepted: %v", err)
	}
	request.To, request.SnapshotWatermark = StateSnapshot, "snapshot:1"
	current, err = ApplyTransition(current, request)
	if err != nil {
		t.Fatal(err)
	}
	request = TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version,
		To: StateDualWrite, At: clock.Add(2 * time.Minute), DualWriteRef: "log://migration"}
	current, err = ApplyTransition(current, request)
	if err != nil {
		t.Fatal(err)
	}
	request = TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version,
		To: StateBackfill, At: clock.Add(3 * time.Minute)}
	current, err = ApplyTransition(current, request)
	if err != nil {
		t.Fatal(err)
	}
	request = TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version,
		To: StateVerify, At: clock.Add(4 * time.Minute)}
	if _, err := ApplyTransition(current, request); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("verify before complete accepted: %v", err)
	}
}

func TestPhaseGraphIsExplicitAndFailClosed(t *testing.T) {
	states := []State{StatePlanned, StateSnapshot, StateDualWrite, StateBackfill, StateVerify, StateCutover, StateObserve, StateCleanup}
	for index, from := range states {
		for targetIndex, to := range states {
			want := index < len(states)-1 && targetIndex == index+1
			if got := CanTransition(from, to); got != want {
				t.Fatalf("CanTransition(%q, %q)=%t, want %t", from, to, got, want)
			}
		}
	}
	if !Terminal(StateCleanup) || Terminal(StateObserve) || Terminal(State("unknown")) {
		t.Fatalf("terminal phase classification is not fail closed")
	}
}

func TestPauseResumeAndAbortFailClosed(t *testing.T) {
	clock := time.Date(2026, 9, 8, 8, 0, 0, 0, time.UTC)
	current, err := NewMigration(CreateRequest{TenantID: "tenant-a", MigrationID: "migration-1", Domain: "session", Epoch: 1,
		Source: Binding{ConfigVersion: 1, BackendProfileID: "source", BackendVersion: 1},
		Target: Binding{ConfigVersion: 2, BackendProfileID: "target", BackendVersion: 1}, CreatedAt: clock})
	if err != nil {
		t.Fatal(err)
	}
	control := ControlRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version,
		At: clock.Add(time.Minute), Metadata: ControlMetadata{ActorID: "operator", ReasonCode: "maintenance", CorrelationID: "request-1", TraceID: "trace-1"}}
	paused, err := ApplyPause(current, control)
	if err != nil || paused.State != StatePaused || paused.PausedFrom != StatePlanned || Terminal(paused.State) {
		t.Fatalf("paused=%+v err=%v", paused, err)
	}
	if _, err := ApplyTransition(paused, TransitionRequest{TenantID: paused.TenantID, MigrationID: paused.MigrationID, ExpectedVersion: paused.Version, To: StateSnapshot, At: clock.Add(2 * time.Minute), SnapshotWatermark: "snapshot:1"}); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("paused transition=%v", err)
	}
	control.ExpectedVersion, control.At = paused.Version, clock.Add(2*time.Minute)
	resumed, err := ApplyResume(paused, control)
	if err != nil || resumed.State != StatePlanned || resumed.PausedFrom != "" {
		t.Fatalf("resumed=%+v err=%v", resumed, err)
	}
	control.ExpectedVersion, control.At = resumed.Version, clock.Add(3*time.Minute)
	aborted, err := ApplyAbort(resumed, control)
	if err != nil || aborted.State != StateAborted || !Terminal(aborted.State) {
		t.Fatalf("aborted=%+v err=%v", aborted, err)
	}
	if _, err := ApplyResume(aborted, ControlRequest{TenantID: aborted.TenantID, MigrationID: aborted.MigrationID, ExpectedVersion: aborted.Version, At: clock.Add(4 * time.Minute), Metadata: control.Metadata}); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("terminal resume=%v", err)
	}
}

func TestDigestCanonicalForm(t *testing.T) {
	if validDigest(strings.Repeat("A", 64)) {
		t.Fatal("uppercase digest accepted")
	}
	if !validDigest(strings.Repeat("a", 64)) {
		t.Fatal("lowercase digest rejected")
	}
}

func TestBatchAndCutoverEvidenceFailClosed(t *testing.T) {
	clock := time.Date(2026, 9, 8, 8, 0, 0, 0, time.UTC)
	current := Migration{TenantID: "tenant-a", MigrationID: "migration-1", Domain: "session", Epoch: 3,
		Source: Binding{ConfigVersion: 1, BackendProfileID: "source", BackendVersion: 1},
		Target: Binding{ConfigVersion: 2, BackendProfileID: "target", BackendVersion: 1}, State: StateBackfill,
		BackfillCheckpoint: "cursor:10", NextBatchSeq: 2, Version: 4, CreatedAt: clock, UpdatedAt: clock}
	batch := BatchRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, BatchID: "batch-2", Epoch: 3,
		ExpectedVersion: 4, BatchSeq: 2, FromCheckpoint: "wrong", ToCheckpoint: "cursor:20",
		Digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", RecordCount: 10, CommittedAt: clock.Add(time.Minute)}
	if _, _, err := ApplyBatch(current, batch); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("discontinuous checkpoint accepted: %v", err)
	}
	current.State, current.BackfillComplete, current.Version = StateVerify, true, 6
	verification := Verification{SourceCount: 10, TargetCount: 9,
		SourceDigest:    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		TargetDigest:    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		SourceWatermark: "watermark:10", TargetWatermark: "watermark:10",
		SampleDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	request := TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version,
		To: StateCutover, At: clock.Add(2 * time.Minute), Verification: verification, CutoverConfigVersion: 2}
	if _, err := ApplyTransition(current, request); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("divergent verification accepted: %v", err)
	}
}
