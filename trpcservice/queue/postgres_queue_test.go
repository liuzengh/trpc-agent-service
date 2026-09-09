package queue

import (
	"errors"
	"testing"
	"time"
)

func TestNewPostgresQueueValidation(t *testing.T) {
	if _, err := NewPostgresQueue(nil, PostgresQueueConfig{}); err == nil {
		t.Fatal("expected nil pool to fail")
	}
	if _, err := (PostgresQueueConfig{MaxJobAge: -time.Second}).withDefaults(); err == nil {
		t.Fatal("expected invalid queue duration to fail")
	}
	if !jsonEquivalent([]byte(`{"a": 1}`), []byte(`{"a":1}`)) {
		t.Fatal("expected semantically equal JSON to compare equal")
	}
	if jsonEquivalent([]byte(`{"a": 1}`), []byte(`{"a":2}`)) {
		t.Fatal("expected different JSON to compare different")
	}
}

func TestPostgresQueueErrorClassificationIsStable(t *testing.T) {
	if !errors.Is(ErrQueueBackend, ErrQueueBackend) || !errors.Is(ErrQueueOperationAmbiguous, ErrQueueOperationAmbiguous) || !errors.Is(ErrEnqueueConflict, ErrEnqueueConflict) {
		t.Fatal("queue sentinel errors must remain comparable")
	}
}
