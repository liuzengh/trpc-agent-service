package main

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

type recordingSessionArchiver struct {
	before time.Time
	limit  int
}

func (a *recordingSessionArchiver) ArchiveIdleSessions(_ context.Context, before time.Time, limit int) (int64, error) {
	a.before, a.limit = before, limit
	return 3, nil
}

func TestArchiveIdleSessionsUsesThirtyDayCutoff(t *testing.T) {
	archiver := &recordingSessionArchiver{}
	app := &application{sessionArchiver: archiver}
	now := time.Date(2026, time.September, 3, 0, 0, 0, 0, time.UTC)
	count, err := app.archiveIdleSessionsOnce(context.Background(), now)
	if err != nil || count != 3 {
		t.Fatalf("archiveIdleSessionsOnce() = %d, %v", count, err)
	}
	if want := now.Add(-30 * 24 * time.Hour); !archiver.before.Equal(want) {
		t.Fatalf("archive cutoff = %v, want %v", archiver.before, want)
	}
	if archiver.limit != 1000 {
		t.Fatalf("archive limit = %d, want 1000", archiver.limit)
	}
}

var _ storage.IdleSessionArchiver = (*recordingSessionArchiver)(nil)
