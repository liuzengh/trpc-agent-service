package cluster

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

type queueBackend struct {
	items chan StreamMessage
	next  atomic.Int64
	acks  atomic.Int64
	reads atomic.Int64
}

func newQueueBackend() *queueBackend                                            { return &queueBackend{items: make(chan StreamMessage, 128)} }
func (*queueBackend) CreateConsumerGroup(context.Context, string, string) error { return nil }
func (backend *queueBackend) AddStream(_ context.Context, _ string, payload []byte) error {
	id := backend.next.Add(1)
	backend.items <- StreamMessage{ID: time.Unix(0, id).Format(time.RFC3339Nano), Payload: append([]byte(nil), payload...)}
	return nil
}
func (backend *queueBackend) ReadGroup(ctx context.Context, _, _, _, start string, _ int64, block time.Duration) ([]StreamMessage, error) {
	backend.reads.Add(1)
	if start == "0" {
		return nil, nil
	}
	timer := time.NewTimer(block)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case item := <-backend.items:
		return []StreamMessage{item}, nil
	case <-timer.C:
		return nil, nil
	}
}

func TestWorkSubmitterNeverStartsAConsumer(t *testing.T) {
	backend := newQueueBackend()
	submitter, err := NewWorkSubmitter(context.Background(), backend, "role-test")
	if err != nil {
		t.Fatal(err)
	}
	if err := submitter.Submit(gateway.RunRequest{InboxID: "inbox", ClaimToken: "claim"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if backend.reads.Load() != 0 {
		t.Fatalf("producer-only submitter performed %d reads", backend.reads.Load())
	}
	if backend.next.Load() != 1 {
		t.Fatalf("published=%d", backend.next.Load())
	}
}
func (backend *queueBackend) AckStream(context.Context, string, string, string) error {
	backend.acks.Add(1)
	return nil
}

func TestWorkQueueConsumerGroupRunsEachRequestOnceAcrossNodes(t *testing.T) {
	backend := newQueueBackend()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	seen := make(map[string]int)
	handler := func(_ context.Context, request gateway.RunRequest) error {
		mu.Lock()
		seen[request.InboxID]++
		mu.Unlock()
		return nil
	}
	first, err := NewWorkQueue(ctx, backend, handler, WorkQueueConfig{NodeID: "node-a", Concurrency: 2, ReadBlock: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewWorkQueue(ctx, backend, handler, WorkQueueConfig{NodeID: "node-b", Concurrency: 2, ReadBlock: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close(context.Background())
	defer second.Close(context.Background())
	for index := 0; index < 40; index++ {
		id := time.Unix(0, int64(index+1)).Format(time.RFC3339Nano)
		if err := first.Submit(gateway.RunRequest{InboxID: id, ClaimToken: "claim-" + id}); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && backend.acks.Load() != 40 {
		time.Sleep(5 * time.Millisecond)
	}
	if backend.acks.Load() != 40 {
		t.Fatalf("acks=%d", backend.acks.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 40 {
		t.Fatalf("unique requests=%d", len(seen))
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("request %q executions=%d", id, count)
		}
	}
}

func TestWorkQueueCloseDrainsActiveHandlerBeforeCancel(t *testing.T) {
	backend := newQueueBackend()
	started := make(chan struct{})
	release := make(chan struct{})
	canceled := make(chan struct{}, 1)
	queue, err := NewWorkQueue(context.Background(), backend, func(ctx context.Context, _ gateway.RunRequest) error {
		close(started)
		select {
		case <-ctx.Done():
			canceled <- struct{}{}
			return ctx.Err()
		case <-release:
			return nil
		}
	}, WorkQueueConfig{NodeID: "drain-node", Concurrency: 1, ReadBlock: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Submit(gateway.RunRequest{InboxID: "in-flight", ClaimToken: "claim"}); err != nil {
		t.Fatal(err)
	}
	<-started
	closeDone := make(chan error, 1)
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { closeDone <- queue.Close(closeCtx) }()
	select {
	case <-canceled:
		t.Fatal("graceful close canceled an active handler before the drain deadline")
	case err := <-closeDone:
		t.Fatalf("close returned before active handler completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
}
