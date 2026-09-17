package memory

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	agentmemory "trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// telemetryService observes calls at the public trpc-agent-go memory.Service
// boundary. It deliberately has no backend knowledge: backend selection,
// tenant scoping and memory semantics remain owned by Resolver and the
// upstream service respectively.
type telemetryService struct {
	inner    agentmemory.Service
	provider telemetry.Provider
}

func (s telemetryService) ReadMemories(ctx context.Context, key agentmemory.UserKey, limit int) ([]*agentmemory.Entry, error) {
	child, done := telemetry.StartOperation(ctx, s.provider, "", telemetry.OperationMemoryRead)
	value, err := s.inner.ReadMemories(child, key, limit)
	done(err)
	return value, err
}

func (s telemetryService) SearchMemories(ctx context.Context, key agentmemory.UserKey, query string, opts ...agentmemory.SearchOption) ([]*agentmemory.Entry, error) {
	child, done := telemetry.StartOperation(ctx, s.provider, "", telemetry.OperationMemorySearch)
	value, err := s.inner.SearchMemories(child, key, query, opts...)
	done(err)
	return value, err
}

func (s telemetryService) AddMemory(ctx context.Context, key agentmemory.UserKey, value string, topics []string, opts ...agentmemory.AddOption) error {
	child, done := telemetry.StartOperation(ctx, s.provider, "", telemetry.OperationMemoryAdd)
	err := s.inner.AddMemory(child, key, value, topics, opts...)
	done(err)
	return err
}

func (s telemetryService) UpdateMemory(ctx context.Context, key agentmemory.Key, value string, topics []string, opts ...agentmemory.UpdateOption) error {
	child, done := telemetry.StartOperation(ctx, s.provider, "", telemetry.OperationMemoryUpdate)
	err := s.inner.UpdateMemory(child, key, value, topics, opts...)
	done(err)
	return err
}

func (s telemetryService) DeleteMemory(ctx context.Context, key agentmemory.Key) error {
	child, done := telemetry.StartOperation(ctx, s.provider, "", telemetry.OperationMemoryDelete)
	err := s.inner.DeleteMemory(child, key)
	done(err)
	return err
}

func (s telemetryService) ClearMemories(ctx context.Context, key agentmemory.UserKey) error {
	child, done := telemetry.StartOperation(ctx, s.provider, "", telemetry.OperationMemoryClear)
	err := s.inner.ClearMemories(child, key)
	done(err)
	return err
}

func (s telemetryService) Tools() []tool.Tool { return s.inner.Tools() }

func (s telemetryService) EnqueueAutoMemoryJob(ctx context.Context, value *session.Session) error {
	child, done := telemetry.StartOperation(ctx, s.provider, "", telemetry.OperationMemoryIngest)
	err := s.inner.EnqueueAutoMemoryJob(child, value)
	done(err)
	return err
}

func (s telemetryService) Close() error { return s.inner.Close() }

var _ agentmemory.Service = telemetryService{}
