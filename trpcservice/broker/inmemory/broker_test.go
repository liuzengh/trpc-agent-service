package inmemory

import (
	"context"
	"errors"
	"testing"
	"time"

	brokercontract "github.com/liuzengh/trpc-agent-service/trpcservice/broker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestReclaimWaitsForPendingIdleThreshold(t *testing.T) {
	instance := NewWithReclaimIdle(20 * time.Millisecond)
	envelope := runtime.ExecutionEnvelope{
		SchemaVersion: 1, TenantID: "tenant", TenantVersion: 1, AgentAppID: "app", AgentAppVersion: 1,
		AgentAppRevision: 1, AgentContentDigest: "digest", ConfigVersion: 1, PolicyVersion: 1,
		RequestID: "request", SessionID: "session", UserID: "user", Channel: "fake", InputSeq: 1,
		PayloadRef: "payload://request", CreatedAt: time.Now().UTC(),
	}
	if err := instance.Publish(context.Background(), 0, envelope); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	delivered := make(chan struct{})
	consumeDone := make(chan error, 1)
	go func() {
		consumeDone <- instance.Consume(ctx, brokercontract.ConsumerOptions{ConsumerID: "worker-1", Shards: []brokercontract.Shard{0}}, func(context.Context, brokercontract.Delivery) error {
			close(delivered)
			return nil
		})
	}()
	defer func() {
		cancel()
		select {
		case err := <-consumeDone:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("Consume err=%v, want context cancellation", err)
			}
		case <-time.After(time.Second):
			t.Error("Consume did not stop after cancellation")
		}
	}()
	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("Consume did not receive the published delivery")
	}
	if result, err := instance.Reclaim(context.Background(), brokercontract.ReclaimOptions{ConsumerID: "worker-2"}); err != nil || len(result) != 0 {
		t.Fatalf("early reclaim=%#v err=%v", result, err)
	}
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for {
		result, err := instance.Reclaim(context.Background(), brokercontract.ReclaimOptions{ConsumerID: "worker-2"})
		if err != nil {
			t.Fatalf("reclaim err=%v", err)
		}
		if len(result) == 1 {
			if result[0].Envelope != envelope {
				t.Fatalf("reclaim=%#v, want envelope=%#v", result, envelope)
			}
			break
		}
		if len(result) != 0 {
			t.Fatalf("reclaim=%#v, want zero or one delivery", result)
		}
		select {
		case <-deadline.C:
			t.Fatal("pending delivery was not reclaimable before deadline")
		case <-poll.C:
		}
	}
}
