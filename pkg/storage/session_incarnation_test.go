package storage

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func incarnationLeaseContext(t *testing.T, key session.Key) (context.Context, *Lease) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	lease, err := NewSessionLockManager(client).AcquireLease(context.Background(), SessionInvocationLeaseKey("tenant-a", key), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(context.Background()) })
	return ContextWithSessionLease(strictFenceContext(nil), key, lease), lease
}

type simultaneousUnmarkedReads struct {
	session.Service
	reads   atomic.Int32
	ready   chan struct{}
	mu      sync.Mutex
	arrived int
}

func (s *simultaneousUnmarkedReads) GetSession(ctx context.Context, key session.Key, opts ...session.Option) (*session.Session, error) {
	value, err := s.Service.GetSession(ctx, key, opts...)
	if s.reads.Add(1) <= 2 {
		s.mu.Lock()
		s.arrived++
		if s.arrived == 2 {
			close(s.ready)
		}
		s.mu.Unlock()
		<-s.ready
	}
	return value, err
}

func TestSessionIncarnationConcurrentLegacyReadsBindOnce(t *testing.T) {
	key := session.Key{AppName: "tsa1:8:tenant-a:support", UserID: "user-a", SessionID: "session-a"}
	inner := inmemory.NewSessionService()
	if _, err := inner.CreateSession(context.Background(), key, nil); err != nil {
		t.Fatal(err)
	}
	raw := &simultaneousUnmarkedReads{Service: inner, ready: make(chan struct{})}
	service, err := NewStrictFencedSessionService(raw, &countingFenceAuthorizer{}, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	ctx, _ := incarnationLeaseContext(t, key)
	type result struct {
		value *session.Session
		err   error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() { value, err := service.GetSession(ctx, key); results <- result{value, err} }()
	}
	for range 2 {
		got := <-results
		if got.err != nil {
			t.Fatal(got.err)
		}
		id, err := SessionIncarnationID(got.value)
		if err != nil || id == "" || id != SessionIncarnationFromContext(ctx) {
			t.Fatalf("concurrent binding = %q, err=%v", id, err)
		}
	}
	persisted, err := inner.GetSession(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	id, err := SessionIncarnationID(persisted)
	if err != nil || id != SessionIncarnationFromContext(ctx) {
		t.Fatalf("persisted ID=%q err=%v", id, err)
	}
}

func TestSessionIncarnationRejectsLostLeaseAndOrdinaryMutation(t *testing.T) {
	key := session.Key{AppName: "tsa1:8:tenant-a:support", UserID: "user-a", SessionID: "session-a"}
	inner := inmemory.NewSessionService()
	if _, err := inner.CreateSession(context.Background(), key, nil); err != nil {
		t.Fatal(err)
	}
	service, err := NewStrictFencedSessionService(inner, &countingFenceAuthorizer{}, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	ctx, lease := incarnationLeaseContext(t, key)
	value, err := service.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	id, err := SessionIncarnationID(value)
	if err != nil || id == "" {
		t.Fatalf("binding failed: %q %v", id, err)
	}
	for _, state := range []session.StateMap{{SessionIncarnationStateKey: nil}, {SessionIncarnationStateKey: []byte("forged")}} {
		if err := service.UpdateSessionState(ctx, key, state); !errors.Is(err, ErrSessionIncarnation) {
			t.Fatalf("reserved update accepted: %v", err)
		}
		if err := service.AppendEvent(ctx, value, &event.Event{ID: "forged", StateDelta: state}); !errors.Is(err, ErrSessionIncarnation) {
			t.Fatalf("reserved event delta accepted: %v", err)
		}
		if _, err := service.CreateSession(ctx, key, state); !errors.Is(err, ErrSessionIncarnation) {
			t.Fatalf("reserved create accepted: %v", err)
		}
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetSession(ctx, key); !errors.Is(err, ErrStaleWriter) {
		t.Fatalf("released lease accepted: %v", err)
	}
	current, err := inner.GetSession(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := SessionIncarnationID(current); err != nil || got != id {
		t.Fatalf("reserved state changed: %q %v", got, err)
	}
}

func TestFencedSessionReadPreservesNotFoundForRunnerCreation(t *testing.T) {
	key := session.Key{AppName: "tsa1:8:tenant-a:support", UserID: "user-a", SessionID: "session-a"}
	service, err := NewStrictFencedSessionService(inmemory.NewSessionService(), &countingFenceAuthorizer{}, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	value, err := service.GetSession(strictFenceContext(nil), key)
	if err != nil || value != nil {
		t.Fatalf("missing session must return nil,nil: %#v %v", value, err)
	}
}

func TestSessionIncarnationCreateRejectsLostLeaseBeforeWriting(t *testing.T) {
	key := session.Key{AppName: "tsa1:8:tenant-a:support", UserID: "user-a", SessionID: "session-a"}
	inner := inmemory.NewSessionService()
	t.Cleanup(func() { _ = inner.Close() })
	service, err := NewStrictFencedSessionService(inner, &countingFenceAuthorizer{}, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	ctx, lease := incarnationLeaseContext(t, key)
	if err := lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateSession(ctx, key, nil); !errors.Is(err, ErrStaleWriter) {
		t.Fatalf("CreateSession with a lost lease = %v, want stale writer", err)
	}
	value, err := inner.GetSession(context.Background(), key)
	if err != nil || value != nil || SessionIncarnationFromContext(ctx) != "" {
		t.Fatalf("rejected creation persisted or bound a Session: value=%v err=%v", value, err)
	}
}
