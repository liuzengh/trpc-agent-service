package summary

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

type boundedPollGenerator struct {
	mu        sync.Mutex
	active    int
	maxActive int
	completed int
	cancel    context.CancelFunc
}

func (g *boundedPollGenerator) Generate(ctx context.Context, job Job) (Candidate, error) {
	g.mu.Lock()
	g.active++
	if g.active > g.maxActive {
		g.maxActive = g.active
	}
	g.mu.Unlock()
	select {
	case <-ctx.Done():
		return Candidate{}, ctx.Err()
	case <-time.After(10 * time.Millisecond):
	}
	g.mu.Lock()
	g.active--
	g.completed++
	if g.completed == 6 {
		g.cancel()
	}
	g.mu.Unlock()
	return candidateFor(job.Key, job.TargetEventSequence, "summary"), nil
}

func TestPollerBoundsConcurrencyAndDrainsOnCancellation(t *testing.T) {
	store := NewMemoryStore(nil)
	sink := NewMemorySinkWithLeaseValidator(nil, store.ValidateLease)
	for i := 0; i < 6; i++ {
		key := summaryKey()
		key.SessionID = fmt.Sprintf("session-%d", i)
		if _, err := store.Enqueue(context.Background(), summaryRequest(key, 1)); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	generator := &boundedPollGenerator{cancel: cancel}
	poller, err := NewPoller(PollerConfig{
		OwnerPrefix: "summary-test", Concurrency: 2, PollInterval: time.Millisecond,
		LeaseTTL: time.Second, JobTimeout: time.Second,
		Store: store, Sink: sink, Generator: generator,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := poller.Run(ctx); err != nil {
		t.Fatal(err)
	}
	generator.mu.Lock()
	defer generator.mu.Unlock()
	if generator.completed != 6 || generator.active != 0 || generator.maxActive > 2 {
		t.Fatalf("completed=%d active=%d max=%d", generator.completed, generator.active, generator.maxActive)
	}
}

func TestPollerRejectsUnboundedConfiguration(t *testing.T) {
	_, err := NewPoller(PollerConfig{
		OwnerPrefix: "summary-test", Concurrency: MaxPollConcurrency + 1,
		PollInterval: time.Millisecond, LeaseTTL: time.Second, JobTimeout: time.Second,
		Store: NewMemoryStore(nil), Sink: NewMemorySink(nil), Generator: scriptedGenerator{},
	})
	if err == nil {
		t.Fatal("unbounded concurrency accepted")
	}
}

type drainPollGenerator struct {
	started chan struct{}
	release chan struct{}
	key     Key
}

func (g *drainPollGenerator) Generate(ctx context.Context, job Job) (Candidate, error) {
	close(g.started)
	select {
	case <-ctx.Done():
		return Candidate{}, ctx.Err()
	case <-g.release:
		return candidateFor(g.key, job.TargetEventSequence, "drained summary"), nil
	}
}

func TestPollerStopsClaimingButDrainsActiveJobOnCancellation(t *testing.T) {
	store := NewMemoryStore(nil)
	sink := NewMemorySinkWithLeaseValidator(nil, store.ValidateLease)
	key := summaryKey()
	enqueued, err := store.Enqueue(context.Background(), summaryRequest(key, 1))
	if err != nil {
		t.Fatal(err)
	}
	generator := &drainPollGenerator{
		started: make(chan struct{}), release: make(chan struct{}), key: key,
	}
	poller, err := NewPoller(PollerConfig{
		OwnerPrefix: "summary-drain", Concurrency: 1, PollInterval: time.Millisecond,
		LeaseTTL: time.Second, JobTimeout: time.Second,
		Store: store, Sink: sink, Generator: generator,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- poller.Run(ctx) }()
	select {
	case <-generator.started:
	case <-time.After(time.Second):
		t.Fatal("generator did not start")
	}
	cancel()
	close(generator.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("poller did not drain")
	}
	persisted, err := store.Get(context.Background(), enqueued.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Status != StatusCompleted || persisted.CompletedEventSequence != 1 {
		t.Fatalf("active job was not drained: %#v", persisted)
	}
}
