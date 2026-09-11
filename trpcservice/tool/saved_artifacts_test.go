package tool

import (
	"context"
	"testing"
)

func TestSavedArtifactRecorderSnapshotIsIndependent(t *testing.T) {
	recorder := NewSavedArtifactRecorder()
	recorder.Record(SavedArtifact{Filename: "report.txt", Version: 1})
	snapshot := recorder.Snapshot()
	if len(snapshot) != 1 || snapshot[0].Filename != "report.txt" {
		t.Fatalf("Snapshot() = %#v", snapshot)
	}
	snapshot[0].Filename = "changed.txt"
	if got := recorder.Snapshot()[0].Filename; got != "report.txt" {
		t.Fatalf("recorder mutated through snapshot: %q", got)
	}
}

func TestSavedArtifactRecorderNilAndContextBoundaries(t *testing.T) {
	var recorder *savedArtifactRecorder
	recorder.Record(SavedArtifact{Filename: "ignored.txt"})
	if got := recorder.Snapshot(); got != nil {
		t.Fatalf("nil Snapshot() = %#v", got)
	}

	ctx := context.Background()
	if got := WithSavedArtifactRecorder(ctx, nil); got != ctx {
		t.Fatal("WithSavedArtifactRecorder(nil) replaced context")
	}
	recordSavedArtifact(nil, SavedArtifact{Filename: "ignored.txt"})
	recordSavedArtifact(ctx, SavedArtifact{Filename: "ignored.txt"})

	active := NewSavedArtifactRecorder()
	bound := WithSavedArtifactRecorder(ctx, active)
	recordSavedArtifact(bound, SavedArtifact{Filename: "saved.txt", Version: 2})
	if got := active.Snapshot(); len(got) != 1 || got[0].Filename != "saved.txt" || got[0].Version != 2 {
		t.Fatalf("recorded artifacts = %#v", got)
	}
}
