package identity

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newRedisSessionTestStore(t *testing.T) (*RedisSessionStore, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr(), MaxRetries: -1, DialTimeout: 50 * time.Millisecond})
	t.Cleanup(func() { _ = client.Close() })
	store, err := NewRedisSessionStore(client)
	if err != nil {
		t.Fatalf("NewRedisSessionStore() error = %v", err)
	}
	return store, server
}

func TestNewRedisSessionStoreRejectsNilClient(t *testing.T) {
	if _, err := NewRedisSessionStore(nil); err == nil {
		t.Fatal("NewRedisSessionStore(nil) error = nil, want failure")
	}
}

func TestRedisSessionStoreLifecycleAndExpiry(t *testing.T) {
	store, server := newRedisSessionTestStore(t)
	want := SessionUser{PlatformUserID: "platform-user-1", Role: RoleAdmin, Tenants: []TenantRole{{TenantID: "tenant-1", DisplayName: "TrailForge", Role: RoleAdmin}}}
	sessionID, err := store.Create(context.Background(), want, time.Hour)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	got, err := store.Get(context.Background(), sessionID)
	if err != nil || got.PlatformUserID != want.PlatformUserID || len(got.Tenants) != 1 || got.Tenants[0].DisplayName != "TrailForge" {
		t.Fatalf("Get() = %+v, %v; want %+v", got, err, want)
	}
	server.FastForward(time.Hour)
	if _, err := store.Get(context.Background(), sessionID); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("Get(expired) error = %v, want ErrSessionNotFound", err)
	}
}

func TestRedisSessionStoreRejectsIncompleteIdentityBeforeWrite(t *testing.T) {
	store, server := newRedisSessionTestStore(t)
	if _, err := store.Create(context.Background(), SessionUser{}, time.Hour); err == nil || !strings.Contains(err.Error(), "missing identity") {
		t.Fatalf("Create(empty) error = %v, want missing identity", err)
	}
	if got := server.DB(0).Keys(); len(got) != 0 {
		t.Fatalf("Redis keys after rejected sessions = %v, want none", got)
	}
}

func TestRedisSessionStoreRejectsCorruptPayload(t *testing.T) {
	store, server := newRedisSessionTestStore(t)
	server.Set("dsh_session:corrupt", "{\"user_id\":")
	if _, err := store.Get(context.Background(), "corrupt"); err == nil || !strings.Contains(err.Error(), "decode session user") {
		t.Fatalf("Get(corrupt) error = %v, want decode error", err)
	}
	server.Set("dsh_session:incomplete", "{\"role\":\"member\"}")
	if _, err := store.Get(context.Background(), "incomplete"); err == nil || !strings.Contains(err.Error(), "missing identity") {
		t.Fatalf("Get(incomplete) error = %v, want missing identity", err)
	}
}

func TestRedisSessionStoreFailsClosedWhenRedisUnavailable(t *testing.T) {
	store, server := newRedisSessionTestStore(t)
	server.Close()
	if _, err := store.Create(context.Background(), SessionUser{PlatformUserID: "platform-user"}, time.Hour); err == nil || !strings.Contains(err.Error(), "store session") {
		t.Fatalf("Create() error = %v, want store session failure", err)
	}
	if _, err := store.Get(context.Background(), "session"); err == nil || !strings.Contains(err.Error(), "read session") {
		t.Fatalf("Get() error = %v, want read session failure", err)
	}
	if err := store.Delete(context.Background(), "session"); err == nil || (!strings.Contains(err.Error(), "read session before delete") && !strings.Contains(err.Error(), "delete session")) {
		t.Fatalf("Delete() error = %v, want Redis session delete failure", err)
	}
}
