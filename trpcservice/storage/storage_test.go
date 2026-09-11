package storage

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestNewSessionServiceMemory(t *testing.T) {
	for _, backend := range []string{"", "memory", "MEMORY"} {
		svc, err := NewSessionService(SessionConfig{Backend: backend})
		if err != nil {
			t.Fatalf("backend %q: %v", backend, err)
		}
		if err := svc.Close(); err != nil {
			t.Fatalf("backend %q close: %v", backend, err)
		}
	}
}

func TestNewSessionServiceUnknownBackend(t *testing.T) {
	_, err := NewSessionService(SessionConfig{Backend: "mysql"})
	if err == nil {
		t.Fatal("unknown backend must be rejected")
	}
	if !strings.Contains(err.Error(), "unknown session backend") {
		t.Fatalf("error = %v", err)
	}
}

// TestNewSessionServiceRedisMiniredis proves the redis backend works end to
// end without a local Redis (official framework tests use the same approach)
// and that data lives in Redis, not in the service process: a second
// instance over the same miniredis reads back what the first one wrote.
func TestNewSessionServiceRedisMiniredis(t *testing.T) {
	mr := miniredis.RunT(t)
	sc := SessionConfig{
		Backend:    "redis",
		RedisURL:   "redis://" + mr.Addr(),
		KeyPrefix:  "test:",
		SessionTTL: time.Hour,
	}

	svc, err := NewSessionService(sc) // probe already ran create/append/get/delete
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	ctx := context.Background()
	key := session.Key{AppName: "app", UserID: "u1", SessionID: "demo:webchat:u1"}
	sess, err := svc.CreateSession(ctx, key, session.StateMap{})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	appendTestEvent(t, svc, sess, "ev1", "hello persisted")
	if err := svc.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	svc2, err := NewSessionService(sc)
	if err != nil {
		t.Fatalf("second instance: %v", err)
	}
	defer svc2.Close()
	got, err := svc2.GetSession(ctx, key)
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if got == nil || len(got.Events) != 1 || got.Events[0].ID != "ev1" {
		t.Fatalf("session did not survive a service restart: %+v", got)
	}
}

// TestNewSessionServiceRedisUnreachable verifies the fail-fast contract: a
// redis URL that refuses connections must surface as a startup error.
func TestNewSessionServiceRedisUnreachable(t *testing.T) {
	mr := miniredis.RunT(t)
	url := "redis://" + mr.Addr()
	mr.Close()

	_, err := NewSessionService(SessionConfig{Backend: "redis", RedisURL: url})
	if err == nil {
		t.Fatal("unreachable redis must fail fast")
	}
	if !strings.Contains(err.Error(), "probe") {
		t.Fatalf("error should mention the probe: %v", err)
	}
}

// TestPingMemory covers the development backend: readiness must never be the
// reason a local run cannot serve.
func TestPingMemory(t *testing.T) {
	svc, err := NewSessionService(SessionConfig{Backend: "memory"})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()
	if err := Ping(context.Background(), svc); err != nil {
		t.Fatalf("ping memory: %v", err)
	}
}

// TestPingRedisIsReadOnlyAndTracksTheBackend covers the three readiness facts
// that matter in a deployment: a missing probe key is healthy, the probe adds
// nothing to a shared backend, and a backend that goes away turns into an
// error instead of a silent success.
func TestPingRedisIsReadOnlyAndTracksTheBackend(t *testing.T) {
	mr := miniredis.RunT(t)
	svc, err := NewSessionService(SessionConfig{
		Backend: "redis", RedisURL: "redis://" + mr.Addr(), KeyPrefix: "test:",
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	ctx := context.Background()
	if err := Ping(ctx, svc); err != nil {
		t.Fatalf("ping healthy redis: %v", err)
	}

	// Readiness fires every few seconds forever; a probe that wrote would
	// litter the shared backend with one garbage key pair per beat.
	before := mr.Keys()
	for i := 0; i < 5; i++ {
		if err := Ping(ctx, svc); err != nil {
			t.Fatalf("ping %d: %v", i, err)
		}
	}
	if after := mr.Keys(); len(after) != len(before) {
		t.Fatalf("ping wrote to redis: %d keys before, %d after", len(before), len(after))
	}

	mr.Close()
	if err := Ping(ctx, svc); err == nil {
		t.Fatal("ping must fail once redis is down")
	} else if !strings.Contains(err.Error(), "session backend read") {
		t.Fatalf("error should name the read path: %v", err)
	}
}

func TestPingNilService(t *testing.T) {
	if err := Ping(context.Background(), nil); err == nil {
		t.Fatal("ping without a session service must be unhealthy")
	}
}

// TestPingHonoursContextCancellation proves the probe cannot outlive a
// cancelled readiness check, which is what keeps a hung backend from wedging
// the orchestrator's probe worker.
func TestPingHonoursContextCancellation(t *testing.T) {
	mr := miniredis.RunT(t)
	svc, err := NewSessionService(SessionConfig{
		Backend: "redis", RedisURL: "redis://" + mr.Addr(), KeyPrefix: "test:",
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Ping(ctx, svc); err == nil {
		t.Fatal("ping with a cancelled context must fail")
	}
}

func appendTestEvent(t *testing.T, svc session.Service, sess *session.Session, id, content string) {
	t.Helper()
	ev := &event.Event{
		ID:           id,
		Timestamp:    time.Now(),
		Author:       "user",
		InvocationID: "inv-" + id,
		Response: &model.Response{
			Choices: []model.Choice{{Message: model.Message{Role: model.RoleUser, Content: content}}},
		},
	}
	if err := svc.AppendEvent(context.Background(), sess, ev); err != nil {
		t.Fatalf("append %s: %v", id, err)
	}
}
