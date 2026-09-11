package memoryvisibility

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestMemoryWatermarkDeclaresNodeLocalBoundary(t *testing.T) {
	first := NewMemory("first")
	second := NewMemory("second")
	scope := Scope{TenantID: "tenant", AppName: "app", PrincipalID: "user"}
	written, err := first.WriteWatermark(context.Background(), scope)
	if err != nil || written.Value != 1 {
		t.Fatalf("WriteWatermark() = %#v, %v", written, err)
	}
	if first.CrossNode() {
		t.Fatal("memory backend claimed cross-node consistency")
	}
	read, err := second.ReadAtLeast(context.Background(), scope, 1)
	if err != nil || read.Visible {
		t.Fatalf("independent memory backend read = %#v, %v", read, err)
	}
}

func TestRedisWatermarksAreSharedAndAtomic(t *testing.T) {
	server := miniredis.RunT(t)
	clientA := redis.NewClient(&redis.Options{Addr: server.Addr()})
	clientB := redis.NewClient(&redis.Options{Addr: server.Addr()})
	a, err := NewRedis(clientA, "test-watermark", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewRedis(clientB, "test-watermark", "node-b")
	if err != nil {
		t.Fatal(err)
	}
	scope := Scope{TenantID: "tenant", AppName: "app", PrincipalID: "user"}
	const writes = 40
	results := make(chan Watermark, writes)
	for i := 0; i < writes; i++ {
		go func(i int) {
			store := a
			if i%2 == 1 {
				store = b
			}
			value, writeErr := store.WriteWatermark(context.Background(), scope)
			if writeErr == nil {
				results <- value
			}
		}(i)
	}
	max := int64(0)
	for i := 0; i < writes; i++ {
		value := <-results
		if value.Value > max {
			max = value.Value
		}
	}
	if max != writes {
		t.Fatalf("shared watermark max = %d, want %d", max, writes)
	}
	read, err := b.ReadAtLeast(context.Background(), scope, writes)
	if err != nil || !read.Visible || !read.Watermark.CrossNode {
		t.Fatalf("cross-node read = %#v, %v", read, err)
	}
}

func TestWaitUntilVisibleReturnsDegradedTimeout(t *testing.T) {
	store := NewMemory("node")
	scope := Scope{TenantID: "tenant", AppName: "app", PrincipalID: "user"}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	result, err := store.WaitUntilVisible(ctx, scope, 1)
	if !result.Degraded || !errors.Is(err, ErrCrossNodeUnsupported) {
		t.Fatalf("WaitUntilVisible() = %#v, %v", result, err)
	}
}
