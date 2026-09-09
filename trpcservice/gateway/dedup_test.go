package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisDeduperClaimMarkAndDuplicate(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	deduper, err := NewRedisDeduper(client, 90*time.Second, 24*time.Hour)
	if err != nil {
		t.Fatalf("NewRedisDeduper() error = %v", err)
	}
	ctx := context.Background()

	token, err := deduper.Claim(ctx, "tenant-a:binding-a:msg-1")
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if _, err := deduper.Claim(ctx, "tenant-a:binding-a:msg-1"); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("duplicate Claim() error = %v, want ErrDuplicate", err)
	}
	if err := deduper.Mark(ctx, "tenant-a:binding-a:msg-1", token); err != nil {
		t.Fatalf("Mark() error = %v", err)
	}
	value, err := server.Get(dedupPrefix + "tenant-a:binding-a:msg-1")
	if err != nil {
		t.Fatalf("read marked key: %v", err)
	}
	if value != "done" {
		t.Fatalf("marked value = %q, want done", value)
	}
	if _, err := deduper.Claim(ctx, "tenant-a:binding-a:msg-1"); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("Claim() after Mark error = %v, want ErrDuplicate", err)
	}
}

func TestRedisDeduperReleaseAllowsRedelivery(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	deduper, err := NewRedisDeduper(client, 90*time.Second, time.Hour)
	if err != nil {
		t.Fatalf("NewRedisDeduper() error = %v", err)
	}
	ctx := context.Background()
	key := "tenant-a:binding-a:msg-crash"

	token, err := deduper.Claim(ctx, key)
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if err := deduper.Release(ctx, key, token); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if _, err := deduper.Claim(ctx, key); err != nil {
		t.Fatalf("Claim() after simulated crash release error = %v", err)
	}
}

func TestRedisDeduperWrongTokenCannotMutateClaim(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	deduper, err := NewRedisDeduper(client, 90*time.Second, time.Hour)
	if err != nil {
		t.Fatalf("NewRedisDeduper() error = %v", err)
	}
	ctx := context.Background()
	key := "tenant-a:binding-a:msg-fenced"

	token, err := deduper.Claim(ctx, key)
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if err := deduper.Mark(ctx, key, "wrong-token"); err != nil {
		t.Fatalf("Mark(wrong token) error = %v", err)
	}
	value, err := server.Get(dedupPrefix + key)
	if err != nil {
		t.Fatalf("read claim: %v", err)
	}
	if value != token {
		t.Fatalf("claim value changed to %q", value)
	}
	if err := deduper.Release(ctx, key, "wrong-token"); err != nil {
		t.Fatalf("Release(wrong token) error = %v", err)
	}
	if !server.Exists(dedupPrefix + key) {
		t.Fatal("wrong owner deleted the claim")
	}
}
