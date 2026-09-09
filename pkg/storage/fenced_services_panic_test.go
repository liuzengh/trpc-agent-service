package storage

import (
	"context"
	"sync/atomic"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/fence"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type countingFenceAuthorizer struct {
	acquired atomic.Int32
	released atomic.Int32
}

func (a *countingFenceAuthorizer) Acquire(context.Context, fence.Token) (func() error, error) {
	a.acquired.Add(1)
	return func() error {
		a.released.Add(1)
		return nil
	}, nil
}

type panicSessionService struct{ session.Service }

func (panicSessionService) DeleteSession(context.Context, session.Key, ...session.Option) error {
	panic("session backend panic")
}

type panicMemoryService struct{ memory.Service }

func (panicMemoryService) AddMemory(context.Context, memory.UserKey, string, []string, ...memory.AddOption) error {
	panic("memory backend panic")
}

func panicFenceContext() context.Context {
	return fence.WithToken(context.Background(), fence.Token{
		TenantID: "tenant-a", AgentAppID: "app-a", SessionID: "session-a",
		ExecutionID: 1, Generation: 1, Value: "execution-token",
	})
}

func TestFencedSessionServiceReleasesOnBackendPanic(t *testing.T) {
	authorizer := &countingFenceAuthorizer{}
	service := NewFencedSessionService(panicSessionService{}, authorizer)
	defer func() {
		if recovered := recover(); recovered != "session backend panic" {
			t.Fatalf("panic=%v, want backend panic", recovered)
		}
		if got := authorizer.released.Load(); got != 1 {
			t.Fatalf("release count=%d, want 1", got)
		}
	}()
	_ = service.DeleteSession(panicFenceContext(), session.Key{AppName: "app-a", UserID: "user-a", SessionID: "session-a"})
}

func TestFencedMemoryServiceReleasesOnBackendPanic(t *testing.T) {
	authorizer := &countingFenceAuthorizer{}
	service := NewFencedMemoryService(panicMemoryService{}, authorizer)
	defer func() {
		if recovered := recover(); recovered != "memory backend panic" {
			t.Fatalf("panic=%v, want backend panic", recovered)
		}
		if got := authorizer.released.Load(); got != 1 {
			t.Fatalf("release count=%d, want 1", got)
		}
	}()
	_ = service.AddMemory(panicFenceContext(), memory.UserKey{AppName: "app-a", UserID: "user-a"}, "text", nil)
}
