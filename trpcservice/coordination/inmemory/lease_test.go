package inmemory

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
)

func TestFenceNeverMovesBackward(t *testing.T) {
	ctx := context.Background()
	m := New()
	key := coordination.SessionKey{TenantID: "t", AgentAppID: "a", SessionID: "s"}
	if err := m.EnsureFenceAtLeast(ctx, key, 40); err != nil {
		t.Fatal(err)
	}
	first, err := m.Acquire(ctx, key, "w1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if first.Fence != 41 {
		t.Fatalf("fence=%d", first.Fence)
	}
	if err := m.Release(ctx, first); err != nil {
		t.Fatal(err)
	}
	second, err := m.Acquire(ctx, key, "w2", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if second.Fence != 42 {
		t.Fatalf("fence=%d", second.Fence)
	}
}
