package main

import (
	"context"
	"testing"
	"time"
)

func TestArchiveIdleSessionsUsesConfiguredCutoff(t *testing.T) {
	archiver := &recordingSessionArchiver{}
	app := &application{sessionArchiver: archiver, sessionIdleArchiveAge: 12 * time.Hour}
	now := time.Date(2026, time.September, 3, 0, 0, 0, 0, time.UTC)
	if _, err := app.archiveIdleSessionsOnce(context.Background(), now); err != nil {
		t.Fatalf("archiveIdleSessionsOnce() error = %v", err)
	}
	if want := now.Add(-12 * time.Hour); !archiver.before.Equal(want) {
		t.Fatalf("archive cutoff = %v, want %v", archiver.before, want)
	}
}
