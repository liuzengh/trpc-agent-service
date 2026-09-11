package storage

import (
	"context"
	"testing"
)

func TestMemoriesTenantIsolation(t *testing.T) {
	ctx := context.Background()
	m, err := NewMemories(MemoryConfig{Backend: BackendInMemory})
	if err != nil {
		t.Fatalf("NewMemories: %v", err)
	}

	if err := m.Add(ctx, "tenant-a", "user-1", "prefers dark mode", nil); err != nil {
		t.Fatalf("add tenant-a: %v", err)
	}

	entries, err := m.Read(ctx, "tenant-a", "user-1", 10)
	if err != nil {
		t.Fatalf("read tenant-a: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("tenant-a entries = %d, want 1", len(entries))
	}

	entries, err = m.Read(ctx, "tenant-b", "user-1", 10)
	if err != nil {
		t.Fatalf("read tenant-b: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("tenant-b entries = %d, want 0 (isolated)", len(entries))
	}
}
