package agent

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/testenv"
)

// Unit tests for the worker's session-lock plumbing: acquireSession's spin,
// timeout, cancellation and error paths, and the lock-renewal watchdog.

// A lock held by another owner for longer than LockWait leaves the message
// pending: the reaper takes it over once the session goes quiet.
func TestAcquireSessionLeavesPendingOnTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)
	lock := storage.NewLock(rdb)
	appID, sessKey := "unit-app", "sess-timeout-"+t.Name()
	lockKey := "lock:sess:" + appID + ":" + sessKey
	t.Cleanup(func() { rdb.Del(context.Background(), lockKey) })
	if ok, err := lock.TryAcquire(ctx, appID, sessKey, "other-owner", 30*time.Second); err != nil || !ok {
		t.Fatalf("pre-lock failed: ok=%v err=%v", ok, err)
	}

	w := &Worker{Name: "unit-w", Lock: lock, LockTTL: time.Second, LockWait: 50 * time.Millisecond}
	owner, stop, ok := w.acquireSession(ctx, storage.Message{ID: "m-1"}, appID, sessKey, func(error) {})
	if ok {
		t.Fatal("acquireSession must not succeed while another owner holds the lock")
	}
	if owner != "" || stop != nil {
		t.Fatalf("timeout must return zero values, got owner=%q stop!=nil=%t", owner, stop != nil)
	}
}

// A context cancellation during the spin aborts the wait immediately.
func TestAcquireSessionStopsOnContextCancel(t *testing.T) {
	rdb := testenv.Redis(t)
	lock := storage.NewLock(rdb)
	appID, sessKey := "unit-app", "sess-cancel-"+t.Name()
	lockKey := "lock:sess:" + appID + ":" + sessKey
	t.Cleanup(func() { rdb.Del(context.Background(), lockKey) })
	if ok, err := lock.TryAcquire(context.Background(), appID, sessKey, "other-owner", 30*time.Second); err != nil || !ok {
		t.Fatalf("pre-lock failed: ok=%v err=%v", ok, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := &Worker{Name: "unit-w", Lock: lock, LockTTL: time.Second, LockWait: 30 * time.Second}
	type result struct {
		owner string
		stop  func()
		ok    bool
	}
	done := make(chan result, 1)
	go func() {
		owner, stop, ok := w.acquireSession(ctx, storage.Message{ID: "m-2"}, appID, sessKey, func(error) {})
		done <- result{owner, stop, ok}
	}()
	time.Sleep(100 * time.Millisecond) // let the spin reach its select
	cancel()

	select {
	case r := <-done:
		if r.ok || r.owner != "" || r.stop != nil {
			t.Fatalf("cancel must return zero values, got ok=%v owner=%q stop!=nil=%t", r.ok, r.owner, r.stop != nil)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("acquireSession did not return after context cancel")
	}
}

// A Redis error on TryAcquire is logged and retried until LockWait expires,
// not treated as a fatal failure.
func TestAcquireSessionWarnsOnLockError(t *testing.T) {
	rdb := testenv.Redis(t)
	_ = rdb.Close() // every lock operation now fails
	lock := storage.NewLock(rdb)

	w := &Worker{Name: "unit-w", Lock: lock, LockTTL: time.Second, LockWait: 50 * time.Millisecond}
	owner, stop, ok := w.acquireSession(context.Background(), storage.Message{ID: "m-3"}, "unit-app", "sess-closed", func(error) {})
	if ok || owner != "" || stop != nil {
		t.Fatalf("closed Redis must fail the acquire, got ok=%v owner=%q stop!=nil=%t", ok, owner, stop != nil)
	}
}

// On a free lock the acquire succeeds and returns the owner token plus a
// working watchdog stop; the lock is releasable afterwards.
func TestAcquireSessionSuccessStartsWatchdog(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)
	lock := storage.NewLock(rdb)
	appID, sessKey := "unit-app", "sess-ok-"+t.Name()
	lockKey := "lock:sess:" + appID + ":" + sessKey
	t.Cleanup(func() { rdb.Del(context.Background(), lockKey) })

	w := &Worker{Name: "unit-w", Lock: lock, LockTTL: time.Second, LockWait: time.Second}
	owner, stop, ok := w.acquireSession(ctx, storage.Message{ID: "m-4"}, appID, sessKey, func(error) {})
	if !ok {
		t.Fatal("acquireSession must succeed on a free lock")
	}
	if owner != "unit-w:m-4" || stop == nil {
		t.Fatalf("unexpected result: owner=%q", owner)
	}
	stop()
	if err := lock.Release(context.Background(), appID, sessKey, owner); err != nil {
		t.Fatalf("release after acquire: %v", err)
	}
}

// The watchdog renews the lease every TTL/3: with a lease that would
// otherwise expire mid-test, the key must still be ours afterwards.
func TestLockWatchdogRenewsLease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)
	lock := storage.NewLock(rdb)
	appID, sessKey := "unit-app", "sess-renew-"+t.Name()
	lockKey := "lock:sess:" + appID + ":" + sessKey
	t.Cleanup(func() { rdb.Del(context.Background(), lockKey) })
	if ok, err := lock.TryAcquire(ctx, appID, sessKey, "unit-owner", 400*time.Millisecond); err != nil || !ok {
		t.Fatalf("pre-lock failed: ok=%v err=%v", ok, err)
	}

	w := &Worker{Name: "unit-w", Lock: lock, LockTTL: 400 * time.Millisecond}
	runCtx, cancelRun := context.WithCancelCause(ctx)
	stop := w.startLockWatchdog(ctx, appID, sessKey, "unit-owner", cancelRun)
	defer stop()

	// Without renewal the 400ms lease would be gone long before this point.
	time.Sleep(600 * time.Millisecond)
	got, err := rdb.Get(context.Background(), lockKey).Result()
	if err != nil {
		t.Fatalf("lock key vanished despite watchdog renewal: %v", err)
	}
	if got != "unit-owner" {
		t.Fatalf("lock owner changed unexpectedly: %q", got)
	}
	if err := runCtx.Err(); err != nil {
		t.Fatalf("healthy renewal must not abort the run: %v (cause %v)", err, context.Cause(runCtx))
	}
}

// When the lock is lost (expired and taken over by another owner), the
// watchdog stops renewing, leaves the new owner untouched, and cancels the
// run: from that moment a peer owns the session, so the generation in flight
// must not keep spending tokens or emit a second reply.
func TestLockWatchdogStopsOnLostLock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)
	lock := storage.NewLock(rdb)
	appID, sessKey := "unit-app", "sess-lost-"+t.Name()
	lockKey := "lock:sess:" + appID + ":" + sessKey
	t.Cleanup(func() { rdb.Del(context.Background(), lockKey) })
	if ok, err := lock.TryAcquire(ctx, appID, sessKey, "unit-owner", 400*time.Millisecond); err != nil || !ok {
		t.Fatalf("pre-lock failed: ok=%v err=%v", ok, err)
	}

	w := &Worker{Name: "unit-w", Lock: lock, LockTTL: 400 * time.Millisecond}
	runCtx, cancelRun := context.WithCancelCause(ctx)
	stop := w.startLockWatchdog(ctx, appID, sessKey, "unit-owner", cancelRun)
	defer stop()

	// Simulate a takeover: the key is stolen while the watchdog runs.
	if err := rdb.Del(ctx, lockKey).Err(); err != nil {
		t.Fatal(err)
	}
	if ok, err := lock.TryAcquire(ctx, appID, sessKey, "thief", 30*time.Second); err != nil || !ok {
		t.Fatalf("takeover failed: ok=%v err=%v", ok, err)
	}
	time.Sleep(200 * time.Millisecond) // at least one renewal tick

	got, err := rdb.Get(ctx, lockKey).Result()
	if err != nil || got != "thief" {
		t.Fatalf("watchdog must not disturb the new owner, got %q err=%v", got, err)
	}
	if err := runCtx.Err(); err == nil {
		t.Fatal("losing the lease must cancel the in-flight run")
	}
	if cause := context.Cause(runCtx); !errors.Is(cause, errSessionLockLost) {
		t.Fatalf("run must be cancelled with errSessionLockLost, got %v", cause)
	}
}

// A Redis hiccup during renewal is logged and retried on the next tick; the
// watchdog exits through the context path, not by wedging or panicking, and it
// must not abort the run — an unreachable Redis is not a takeover.
func TestLockWatchdogToleratesRedisErrors(t *testing.T) {
	rdb := testenv.Redis(t)
	_ = rdb.Close() // every Extend now fails
	lock := storage.NewLock(rdb)

	w := &Worker{Name: "unit-w", Lock: lock, LockTTL: 100 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	runCtx, cancelRun := context.WithCancelCause(ctx)
	stop := w.startLockWatchdog(ctx, "unit-app", "sess-errors", "unit-owner", cancelRun)

	time.Sleep(150 * time.Millisecond) // several failing renewal ticks
	if err := runCtx.Err(); err != nil {
		t.Fatalf("a Redis outage must not abort the run: %v (cause %v)", err, context.Cause(runCtx))
	}
	cancel()
	stop()
}

// blockProcessor runs until its context dies and reports the cancellation
// cause, so a test can see why the run stopped.
type blockProcessor struct {
	started chan struct{}
	abort   chan error
	once    sync.Once
}

func (p *blockProcessor) Process(ctx context.Context, _ channels.InboundMessage) (channels.OutboundMessage, error) {
	p.once.Do(func() { close(p.started) })
	<-ctx.Done()
	p.abort <- context.Cause(ctx)
	return channels.OutboundMessage{}, ctx.Err()
}

// End to end: a lease stolen mid-run must abort the generation and leave the
// entry pending. Binding the run to the lease is what stops a takeover from
// producing two replies for one message and two LLM bills for one question.
func TestWorkerAbortsRunWhenSessionLockIsStolen(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)
	stream := storage.NewStream(rdb)
	lock := storage.NewLock(rdb)
	appID := "unit-app"
	sessKey := "sess-stolen-" + t.Name()
	inbound := "test:steal:in:" + t.Name()
	outbound := "test:steal:out:" + t.Name()
	lockKey := "lock:sess:" + appID + ":" + sessKey
	t.Cleanup(func() { rdb.Del(context.Background(), inbound, outbound, lockKey) })
	if err := stream.EnsureGroup(ctx, inbound, "workers"); err != nil {
		t.Fatal(err)
	}

	p := &blockProcessor{started: make(chan struct{}), abort: make(chan error, 1)}
	w := &Worker{
		Name: "unit-w", Stream: stream, Processor: p,
		InStream: inbound, OutStream: outbound,
		// A short lease keeps the watchdog ticking every 100ms.
		Lock: lock, LockTTL: 300 * time.Millisecond, LockWait: time.Second,
	}
	go func() { _ = w.Run(ctx) }()

	payload, err := json.Marshal(channels.InboundMessage{
		Channel: "mock", MsgID: "steal-1", SessionKey: sessKey, AppID: appID,
		UserID: "u1", Text: "long generation",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Add(ctx, inbound, payload); err != nil {
		t.Fatal(err)
	}
	<-p.started

	// Steal the lease out from under the running generation.
	if err := rdb.Del(ctx, lockKey).Err(); err != nil {
		t.Fatal(err)
	}
	if ok, err := lock.TryAcquire(ctx, appID, sessKey, "thief", 30*time.Second); err != nil || !ok {
		t.Fatalf("takeover failed: ok=%v err=%v", ok, err)
	}

	select {
	case cause := <-p.abort:
		if !errors.Is(cause, errSessionLockLost) {
			t.Fatalf("run must be aborted by the lost lease, got cause %v", cause)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the run kept going after its lease was stolen")
	}

	if n, _ := stream.Len(ctx, outbound); n != 0 {
		t.Fatalf("an aborted run must not publish a reply, got %d", n)
	}
	pending, _, err := stream.Pending(ctx, inbound, "workers")
	if err != nil {
		t.Fatal(err)
	}
	if pending != 1 {
		t.Fatalf("the abandoned message must stay pending for the new owner, got %d", pending)
	}
	if got, err := rdb.Get(ctx, lockKey).Result(); err != nil || got != "thief" {
		t.Fatalf("the aborted run must not release the thief's lock, got %q err=%v", got, err)
	}
}
