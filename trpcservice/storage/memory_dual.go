package storage

import (
	"context"
	"fmt"

	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// dualMemoryService writes the current read-primary first and then the
// secondary. Returning a secondary error keeps the caller retryable; Add is
// canonical-ID idempotent in supported backends.
type dualMemoryService struct {
	primary          memory.Service
	secondary        memory.Service
	onSecondaryError func(context.Context, error)
}

func (s *dualMemoryService) secondaryError(ctx context.Context, err error) error {
	if s.onSecondaryError != nil {
		s.onSecondaryError(ctx, err)
	}
	return fmt.Errorf("secondary memory write: %w", err)
}

func (s *dualMemoryService) AddMemory(
	ctx context.Context,
	key memory.UserKey,
	value string,
	topics []string,
	opts ...memory.AddOption,
) error {
	if err := s.primary.AddMemory(ctx, key, value, topics, opts...); err != nil {
		return err
	}
	if err := s.secondary.AddMemory(ctx, key, value, topics, opts...); err != nil {
		return s.secondaryError(ctx, err)
	}
	return nil
}

func (s *dualMemoryService) UpdateMemory(
	ctx context.Context,
	key memory.Key,
	value string,
	topics []string,
	opts ...memory.UpdateOption,
) error {
	if err := s.primary.UpdateMemory(ctx, key, value, topics, opts...); err != nil {
		return err
	}
	if err := s.secondary.UpdateMemory(ctx, key, value, topics, opts...); err != nil {
		return s.secondaryError(ctx, err)
	}
	return nil
}

func (s *dualMemoryService) DeleteMemory(ctx context.Context, key memory.Key) error {
	if err := s.primary.DeleteMemory(ctx, key); err != nil {
		return err
	}
	if err := s.secondary.DeleteMemory(ctx, key); err != nil {
		return s.secondaryError(ctx, err)
	}
	return nil
}

func (s *dualMemoryService) ClearMemories(ctx context.Context, key memory.UserKey) error {
	if err := s.primary.ClearMemories(ctx, key); err != nil {
		return err
	}
	if err := s.secondary.ClearMemories(ctx, key); err != nil {
		return s.secondaryError(ctx, err)
	}
	return nil
}

func (s *dualMemoryService) ReadMemories(
	ctx context.Context,
	key memory.UserKey,
	limit int,
) ([]*memory.Entry, error) {
	return s.primary.ReadMemories(ctx, key, limit)
}

func (s *dualMemoryService) SearchMemories(
	ctx context.Context,
	key memory.UserKey,
	query string,
	opts ...memory.SearchOption,
) ([]*memory.Entry, error) {
	return s.primary.SearchMemories(ctx, key, query, opts...)
}

func (s *dualMemoryService) Tools() []tool.Tool { return nil }

func (s *dualMemoryService) EnqueueAutoMemoryJob(
	ctx context.Context,
	sess *session.Session,
) error {
	return s.primary.EnqueueAutoMemoryJob(ctx, sess)
}

func (s *dualMemoryService) Close() error { return nil }

var _ memory.Service = (*dualMemoryService)(nil)
