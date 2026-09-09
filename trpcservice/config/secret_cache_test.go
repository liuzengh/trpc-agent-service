package config

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// countingResolver counts Resolve calls, failing after `failFrom` calls.
type countingResolver struct {
	calls    atomic.Int64
	value    string
	failFrom int64
}

func (c *countingResolver) Resolve(context.Context, string) (string, error) {
	n := c.calls.Add(1)
	if c.failFrom > 0 && n >= c.failFrom {
		return "", errors.New("backend down")
	}
	return c.value, nil
}

func TestCachedResolverCaches(t *testing.T) {
	inner := &countingResolver{value: "s3cret"}
	r := NewCachedResolver(inner, time.Minute)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		v, err := r.Resolve(ctx, "ref")
		if err != nil || v != "s3cret" {
			t.Fatalf("resolve %d: %v %q", i, err, v)
		}
	}
	if n := inner.calls.Load(); n != 1 {
		t.Fatalf("backend called %d times, want 1 (cache)", n)
	}
}

func TestCachedResolverTTLRefresh(t *testing.T) {
	inner := &countingResolver{value: "s3cret"}
	r := NewCachedResolver(inner, 20*time.Millisecond)
	ctx := context.Background()
	if _, err := r.Resolve(ctx, "ref"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if _, err := r.Resolve(ctx, "ref"); err != nil {
		t.Fatal(err)
	}
	if n := inner.calls.Load(); n != 2 {
		t.Fatalf("backend called %d times, want 2 (TTL refresh)", n)
	}
}

func TestCachedResolverServesStaleOnOutage(t *testing.T) {
	inner := &countingResolver{value: "s3cret", failFrom: 2}
	r := NewCachedResolver(inner, 20*time.Millisecond)
	ctx := context.Background()
	if _, err := r.Resolve(ctx, "ref"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	// Past TTL, backend down: the stale value rides through.
	v, err := r.Resolve(ctx, "ref")
	if err != nil || v != "s3cret" {
		t.Fatalf("stale value must serve through an outage: %v %q", err, v)
	}
}

func TestCachedResolverNoStaleNoAnswer(t *testing.T) {
	inner := &countingResolver{value: "", failFrom: 1}
	r := NewCachedResolver(inner, time.Minute)
	if _, err := r.Resolve(context.Background(), "ref"); err == nil {
		t.Fatal("never-resolved ref must error when the backend is down")
	}
}
