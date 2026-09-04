package storage

import (
	"context"
	"fmt"

	"trpc.group/trpc-go/trpc-agent-go/memory"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	memoryredis "trpc.group/trpc-go/trpc-agent-go/memory/redis"
)

// Memories wraps a memory service with tenant-scoped access, mapping the
// tenant id to the framework's AppName isolation boundary.
type Memories struct {
	svc memory.Service
}

// NewMemories builds a memory service for the given backend.
func NewMemories(cfg MemoryConfig) (*Memories, error) {
	switch cfg.Backend {
	case BackendInMemory:
		return &Memories{svc: memoryinmemory.NewMemoryService()}, nil
	case BackendRedis:
		svc, err := memoryredis.NewService(memoryredis.WithRedisClientURL(cfg.RedisURL))
		if err != nil {
			return nil, fmt.Errorf("storage: redis memory: %w", err)
		}
		return &Memories{svc: svc}, nil
	default:
		return nil, fmt.Errorf("storage: unknown memory backend %q", cfg.Backend)
	}
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
