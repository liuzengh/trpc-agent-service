package idempotency

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLocalStoreCompletesAndReplays(t *testing.T) {
	store := NewLocalStore()
	t.Cleanup(func() { _ = store.Close() })
	key := testKey("message-1")

	begin, err := store.Begin(context.Background(), key, "fingerprint")
	if err != nil {
		t.Fatalf("begin first attempt: %v", err)
	}
	if begin.Status != BeginStarted || begin.Attempt == nil {
		t.Fatalf("unexpected first begin: %+v", begin)
	}
	duplicate, err := store.Begin(context.Background(), key, "fingerprint")
	if err != nil || duplicate.Status != BeginProcessing {
		t.Fatalf("duplicate begin = %+v, err = %v", duplicate, err)
	}

	waited := make(chan Result, 1)
	waitErr := make(chan error, 1)
	go func() {
		result, waitError := store.Wait(context.Background(), key, "fingerprint")
		waited <- result
		waitErr <- waitError
	}()
	result := Result{Reply: "hello", RequestID: "request-1", EventCount: 3}
	if err := begin.Attempt.Complete(context.Background(), result); err != nil {
		t.Fatalf("complete attempt: %v", err)
	}
	if err := <-waitErr; err != nil {
		t.Fatalf("wait result: %v", err)
	}
	if got := <-waited; got != result {
		t.Fatalf("wait result = %+v, want %+v", got, result)
	}

	replay, err := store.Begin(context.Background(), key, "fingerprint")
	if err != nil || replay.Status != BeginCompleted || replay.Result != result {
		t.Fatalf("replay = %+v, err = %v", replay, err)
	}
}

func TestLocalStoreFailureAllowsRetry(t *testing.T) {
	store := NewLocalStore()
	t.Cleanup(func() { _ = store.Close() })
	key := testKey("retry")
	begin, err := store.Begin(context.Background(), key, "fingerprint")
	if err != nil {
		t.Fatalf("begin attempt: %v", err)
	}
	if err := begin.Attempt.Fail(context.Background()); err != nil {
		t.Fatalf("fail attempt: %v", err)
	}
	if _, err := store.Wait(context.Background(), key, "fingerprint"); !errors.Is(err, ErrRetry) {
		t.Fatalf("wait after failure error = %v", err)
	}
	retry, err := store.Begin(context.Background(), key, "fingerprint")
	if err != nil || retry.Status != BeginStarted {
		t.Fatalf("retry begin = %+v, err = %v", retry, err)
	}
	if err := retry.Attempt.Fail(context.Background()); err != nil {
		t.Fatalf("clean retry attempt: %v", err)
	}
}

func TestLocalStoreRejectsMessageIDConflict(t *testing.T) {
	store := NewLocalStore()
	t.Cleanup(func() { _ = store.Close() })
	key := testKey("conflict")
	begin, err := store.Begin(context.Background(), key, "first")
	if err != nil {
		t.Fatalf("begin attempt: %v", err)
	}
	if _, err := store.Begin(context.Background(), key, "second"); !errors.Is(err, ErrKeyConflict) {
		t.Fatalf("conflict error = %v", err)
	}
	if err := begin.Attempt.Fail(context.Background()); err != nil {
		t.Fatalf("clean attempt: %v", err)
	}
}

func TestLocalStoreCompletedResultExpires(t *testing.T) {
	store := NewLocalStoreWithCompletedTTL(10 * time.Millisecond)
	t.Cleanup(func() { _ = store.Close() })
	key := testKey("expires")
	begin, err := store.Begin(context.Background(), key, "fingerprint")
	if err != nil {
		t.Fatalf("begin attempt: %v", err)
	}
	if err := begin.Attempt.Complete(context.Background(), Result{Reply: "old"}); err != nil {
		t.Fatalf("complete attempt: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	afterExpiry, err := store.Begin(context.Background(), key, "fingerprint")
	if err != nil || afterExpiry.Status != BeginStarted {
		t.Fatalf("begin after completed TTL = %+v, err = %v", afterExpiry, err)
	}
	if err := afterExpiry.Attempt.Fail(context.Background()); err != nil {
		t.Fatalf("clean new attempt: %v", err)
	}
}

func TestLocalStoreCloseCancelsAttemptAndWaiter(t *testing.T) {
	store := NewLocalStore()
	key := testKey("close")
	begin, err := store.Begin(context.Background(), key, "fingerprint")
	if err != nil {
		t.Fatalf("begin attempt: %v", err)
	}
	waitErr := make(chan error, 1)
	go func() {
		_, waitError := store.Wait(context.Background(), key, "fingerprint")
		waitErr <- waitError
	}()
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	select {
	case <-begin.Attempt.Context().Done():
		if !errors.Is(context.Cause(begin.Attempt.Context()), ErrStoreClosed) {
			t.Fatalf("attempt cause = %v", context.Cause(begin.Attempt.Context()))
		}
	case <-time.After(time.Second):
		t.Fatal("attempt context was not cancelled")
	}
	if err := <-waitErr; !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("wait error = %v", err)
	}
}

func testKey(messageID string) Key {
	return Key{
		AppName:   "tutorial-app",
		UserID:    "alice",
		SessionID: "session",
		MessageID: messageID,
	}
}
