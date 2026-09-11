package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/safego"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

type panickingIdempotencyStore struct{ storage.IdempotencyStore }

func (s panickingIdempotencyStore) Renew(context.Context, storage.Lease, time.Duration) error {
	panic("renew panic")
}

func TestIdempotencyHeartbeatConvertsPanicToError(t *testing.T) {
	base := storage.NewMemoryIdempotencyStore()
	acquired, err := base.Acquire(context.Background(), "tenant/support/message", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	heartbeat := startIdempotencyLeaseHeartbeat(context.Background(), panickingIdempotencyStore{IdempotencyStore: base}, acquired.Lease, 300*time.Millisecond)
	t.Cleanup(heartbeat.Stop)
	select {
	case <-heartbeat.Done():
	case <-time.After(time.Second):
		t.Fatal("heartbeat did not stop after recovered panic")
	}
	var panicErr *safego.PanicError
	if !errors.As(heartbeat.Err(), &panicErr) || panicErr.Component != "idempotency lease heartbeat" {
		t.Fatalf("heartbeat.Err() = %#v", heartbeat.Err())
	}
}
