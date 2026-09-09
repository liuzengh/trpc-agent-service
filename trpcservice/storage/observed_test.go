package storage

import (
	"context"
	"errors"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/memory"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestObservedSessionAndMemoryExposeOnlyBoundedScope(t *testing.T) {
	const canary = "user-session-or-content-canary"
	var observations []OperationObservation
	observer := func(_ context.Context, value OperationObservation) { observations = append(observations, value) }
	sessions := &ObservedSession{Delegate: sessioninmemory.NewSessionService(), TenantID: "tenant-a", AppID: "app-a", Backend: "postgres", Observe: observer}
	key := session.Key{AppName: "tenant/tenant-a/app/app-a", UserID: canary, SessionID: canary}
	if _, err := sessions.CreateSession(context.Background(), key, nil); err != nil {
		t.Fatal(err)
	}
	memories := &ObservedMemory{Delegate: memoryinmemory.NewMemoryService(), TenantID: "tenant-a", AppID: "app-a", Backend: "postgres", Observe: observer}
	if err := memories.AddMemory(context.Background(), memory.UserKey{AppName: key.AppName, UserID: canary}, canary, nil); err != nil {
		t.Fatal(err)
	}
	if len(observations) != 2 {
		t.Fatalf("observations=%+v", observations)
	}
	for _, observation := range observations {
		if observation.TenantID != "tenant-a" || observation.AppID != "app-a" || observation.Backend != "postgres" || observation.Status != "success" || observation.Duration < 0 {
			t.Fatalf("unsafe or incomplete observation: %+v", observation)
		}
		if observation.Domain != "session" && observation.Domain != "memory" {
			t.Fatalf("domain=%q", observation.Domain)
		}
		if observation.Operation == canary || observation.Backend == canary {
			t.Fatalf("observation leaked request data: %+v", observation)
		}
	}
}

func TestObservedSessionClassifiesFailureWithoutExportingError(t *testing.T) {
	var got OperationObservation
	service := &ObservedSession{Delegate: failingSession{Service: sessioninmemory.NewSessionService()}, TenantID: "tenant", AppID: "app", Backend: "postgres", Observe: func(_ context.Context, value OperationObservation) { got = value }}
	if _, err := service.GetSession(context.Background(), session.Key{AppName: "app", UserID: "user", SessionID: "session"}); err == nil {
		t.Fatal("expected failure")
	}
	if got.Status != "failed" || got.Operation != "get" {
		t.Fatalf("observation=%+v", got)
	}
}

type failingSession struct{ session.Service }

func (failingSession) GetSession(context.Context, session.Key, ...session.Option) (*session.Session, error) {
	return nil, errors.New("secret error text must not enter metrics")
}
