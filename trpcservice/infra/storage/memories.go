package storage

import (
	"context"
	"fmt"

	"trpc.group/trpc-go/trpc-agent-go/memory"
)

// Memories wraps a memory service with tenant-scoped access, mapping the
// tenant id to the framework's AppName isolation boundary. The inner service is
// decorated with spans (see trace_memories.go) so preload reads and
// agent-driven writes appear in the same trace as agent.run.
type Memories struct {
	svc     memory.Service
	backend Backend
}

// NewMemories builds a memory service for the given backend (see backends.go
// for the backend table).
func NewMemories(cfg MemoryConfig) (*Memories, error) {
	b, ok := backendTable[cfg.Backend]
	if !ok || b.memory == nil {
		return nil, fmt.Errorf("storage: unknown memory backend %q (supported: %v)", cfg.Backend, SupportedBackends())
	}
	svc, err := b.memory(cfg)
	if err != nil {
		return nil, err
	}
	return &Memories{svc: withTracingMemories(svc, cfg.Backend), backend: cfg.Backend}, nil
}

// Add stores a memory entry for the tenant's user.
func (m *Memories) Add(ctx context.Context, tenantID, userID, content string, topics []string) error {
	key := memory.UserKey{AppName: tenantID, UserID: userID}
	return m.svc.AddMemory(ctx, key, content, topics)
}

// Read returns memory entries for the tenant's user.
func (m *Memories) Read(ctx context.Context, tenantID, userID string, limit int) ([]*memory.Entry, error) {
	key := memory.UserKey{AppName: tenantID, UserID: userID}
	return m.svc.ReadMemories(ctx, key, limit)
}

// Clear removes all memory entries for the tenant's user.
func (m *Memories) Clear(ctx context.Context, tenantID, userID string) error {
	key := memory.UserKey{AppName: tenantID, UserID: userID}
	return m.svc.ClearMemories(ctx, key)
}

// Service exposes the underlying framework memory service, e.g. for the
// cross-backend migration tool that needs full-fidelity Read/Add.
func (m *Memories) Service() memory.Service {
	return m.svc
}
