package coordination

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func testRedis(t *testing.T) *Redis {
	t.Helper()
	m := miniredis.RunT(t)
	r, err := NewRedis("redis://"+m.Addr(), "test:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func TestRedisClaimAndState(t *testing.T) {
	r := testRedis(t)
	ctx := context.Background()
	if ok, err := r.Claim(ctx, "m1", time.Minute); err != nil || !ok {
		t.Fatalf("first claim = %v, %v", ok, err)
	}
	if ok, err := r.Claim(ctx, "m1", time.Minute); err != nil || ok {
		t.Fatalf("duplicate claim = %v, %v", ok, err)
	}
	if err := r.Set(ctx, "cursor", "C1"); err != nil {
		t.Fatal(err)
	}
	if got, err := r.Get(ctx, "cursor"); err != nil || got != "C1" {
		t.Fatalf("state = %q, %v", got, err)
	}
}

func TestRedisLockRespectsContext(t *testing.T) {
	r := testRedis(t)
	release, err := r.Lock(context.Background(), "session", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := r.Lock(ctx, "session", time.Minute); err == nil {
		t.Fatal("second lock must time out")
	}
	release()
	if unlock, err := r.Lock(context.Background(), "session", time.Minute); err != nil {
		t.Fatalf("lock after release: %v", err)
	} else {
		unlock()
	}
}
