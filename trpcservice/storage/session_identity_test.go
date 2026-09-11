package storage

import (
	"context"
	"testing"
	"time"
)

func TestMemoryStateStoreArchivesOnlyIdleActiveSessions(t *testing.T) {
	store := NewMemoryStateStore()
	store.now = func() time.Time { return time.Unix(100, 0).UTC() }
	store.sessions[tenantSessionKey("tenant-a", "tenant-a/app/web/old")] = Session{
		TenantID: "tenant-a", SessionKey: "tenant-a/app/web/old", Status: "active", UpdatedAt: time.Unix(10, 0).UTC(),
	}
	store.sessions[tenantSessionKey("tenant-a", "tenant-a/app/web/new")] = Session{
		TenantID: "tenant-a", SessionKey: "tenant-a/app/web/new", Status: "active", UpdatedAt: time.Unix(90, 0).UTC(),
	}
	count, err := store.ArchiveIdleSessions(context.Background(), time.Unix(50, 0).UTC(), 10)
	if err != nil || count != 1 {
		t.Fatalf("ArchiveIdleSessions() = %d, %v; want 1, nil", count, err)
	}
}
