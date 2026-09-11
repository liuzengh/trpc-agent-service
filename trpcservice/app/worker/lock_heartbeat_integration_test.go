//go:build integration

package worker

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/bus"
)

// heartbeatMessage is the minimal inbound envelope the heartbeat needs: it
// reads the tenant, session and message id from it.
func heartbeatMessage(id, tenantID, sessionID string) *bus.Message {
	return &bus.Message{ID: id, TenantID: tenantID, SessionID: sessionID, AgentID: "a-hb"}
}

// TestWorkerLockHeartbeatKeepsLockAlive is the regression guard for the session
// lock TTL mismatch: a turn may run up to runTimeout (300s) while the lock TTL is
// only 30s, so without a heartbeat the lock expires mid-turn and a second message
// can enter the same session concurrently.
//
// The heartbeat is exercised for real (no short TTL injected): with the default
// 30s TTL and a 10s refresh interval, a holder must not survive 30s unless it
// refreshes. The test asserts both halves of the contract — an un-refreshed lock
// expires, and a heartbeat-held lock survives past the TTL while a competing
// worker is refused.
func TestWorkerLockHeartbeatKeepsLockAlive(t *testing.T) {
	once.Do(startContainers)
	if platform.err != nil {
		t.Skip(platform.err)
	}
	ctx := context.Background()

	rb, err := bus.NewRedisFromURL(platform.redisURL)
	if err != nil {
		t.Fatalf("redis bus: %v", err)
	}
	t.Cleanup(func() { _ = rb.Client().Close() })

	const (
		tenant  = "t-hb"
		session = "s-hb"
	)
	lockCtx, cancelLock := context.WithCancel(ctx)
	defer cancelLock()

	// A worker with no other dependencies is enough: the heartbeat only needs
	// the bus.
	w := New(rb, nil, nil, nil, nil, nil, nil, nil, nil)

	// The heartbeat takes its interval from the bus, so a longer-than-TTL wait
	// stays comfortably ahead of expiry.
	interval := bus.SessionLockRefreshInterval()
	if interval <= 0 {
		t.Fatalf("session lock refresh interval = %v, want > 0", interval)
	}
	t.Logf("lock TTL=%v refresh interval=%v", bus.SessionLockTTL(), interval)

	held, err := rb.LockSession(ctx, tenant, session, "token-hb")
	if err != nil || !held {
		t.Fatalf("LockSession: held=%v err=%v", held, err)
	}
	hbMsg := heartbeatMessage("msg-hb", tenant, session)
	stop := w.startTurnHeartbeat(lockCtx, hbMsg, "token-hb")

	// Wait past the TTL: without the heartbeat the lock is gone by now.
	wait := bus.SessionLockTTL() + 5*time.Second
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-time.After(wait):
		case <-ctx.Done():
		}
	}()
	<-done

	// A competing worker must still be refused: the lock survived.
	competing, err := rb.LockSession(ctx, tenant, session, "token-other")
	if err != nil {
		t.Fatalf("competing LockSession: %v", err)
	}
	if competing {
		t.Fatal("lock expired despite the heartbeat: a competing worker acquired the same session")
	}

	// The heartbeat must stop cleanly and let the holder release the lock.
	stop()
	if err := rb.UnlockSession(ctx, tenant, session, "token-hb"); err != nil {
		t.Fatalf("UnlockSession: %v", err)
	}

	// After release the lock is free again (the heartbeat is really gone and did
	// not silently keep refreshing).
	acquired, err := rb.LockSession(ctx, tenant, session, "token-later")
	if err != nil {
		t.Fatalf("post-release LockSession: %v", err)
	}
	if !acquired {
		t.Fatal("lock should be free after the holder released it")
	}
	_ = rb.UnlockSession(ctx, tenant, session, "token-later")
}

// TestWorkerLockHeartbeatStopsOnCancel pins the goroutine contract: cancelling the
// turn context (shutdown, timeout) must stop the heartbeat instead of leaking it.
func TestWorkerLockHeartbeatStopsOnCancel(t *testing.T) {
	once.Do(startContainers)
	if platform.err != nil {
		t.Skip(platform.err)
	}
	ctx := context.Background()

	rb, err := bus.NewRedisFromURL(platform.redisURL)
	if err != nil {
		t.Fatalf("redis bus: %v", err)
	}
	t.Cleanup(func() { _ = rb.Client().Close() })

	w := New(rb, nil, nil, nil, nil, nil, nil, nil, nil)

	if _, err := rb.LockSession(ctx, "t-hb2", "s-hb2", "tok"); err != nil {
		t.Fatalf("LockSession: %v", err)
	}
	turnCtx, cancelTurn := context.WithCancel(ctx)
	stop := w.startTurnHeartbeat(turnCtx, heartbeatMessage("msg-hb2", "t-hb2", "s-hb2"), "tok")

	// Cancelling the turn must let stop() return promptly (it waits for the
	// goroutine), which is how the test observes that no goroutine leaked.
	cancelTurn()
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		stop()
	}()
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("stop() did not return after the turn context was cancelled: heartbeat goroutine leaked")
	}
}
