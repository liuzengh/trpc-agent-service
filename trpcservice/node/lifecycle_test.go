package node

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type memoryStore struct {
	mu      sync.Mutex
	records map[string]Record
}

func newMemoryStore() *memoryStore { return &memoryStore{records: make(map[string]Record)} }

func (s *memoryStore) Register(_ context.Context, record Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[record.NodeID] = record
	return nil
}

func (s *memoryStore) Heartbeat(_ context.Context, nodeID, bootID string, state State, inflight int, leaseUntil time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[nodeID]
	if !ok || record.BootID != bootID {
		return ErrLeaseLost
	}
	record.State = state
	record.Inflight = inflight
	record.LastHeartbeat = time.Now().UTC()
	record.LeaseUntil = leaseUntil
	s.records[nodeID] = record
	return nil
}

func (s *memoryStore) List(context.Context) ([]Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Record, 0, len(s.records))
	for _, record := range s.records {
		result = append(result, record)
	}
	return result, nil
}

func (s *memoryStore) record(nodeID string) Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.records[nodeID]
}

func TestLifecycleRegistersHeartbeatsAndDrains(t *testing.T) {
	t.Parallel()

	store := newMemoryStore()
	var active atomic.Int64
	active.Store(2)
	lifecycle, err := NewLifecycle(store, LifecycleConfig{
		NodeID: "worker-a", Role: "worker", BuildVersion: "test",
		HeartbeatInterval: 10 * time.Millisecond, OfflineAfter: 50 * time.Millisecond,
		Inflight: func() int { return int(active.Load()) },
	})
	if err != nil {
		t.Fatalf("NewLifecycle() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := lifecycle.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = lifecycle.Close() })

	registered := store.record("worker-a")
	if registered.State != StateReady || registered.Role != "worker" || registered.Inflight != 2 || registered.BootID == "" {
		t.Fatalf("registered node = %+v", registered)
	}
	if err := lifecycle.Ready(context.Background()); err != nil {
		t.Fatalf("Ready() error = %v", err)
	}

	active.Store(1)
	deadline := time.Now().Add(time.Second)
	for store.record("worker-a").Inflight != 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := store.record("worker-a").Inflight; got != 1 {
		t.Fatalf("heartbeat inflight = %d, want 1", got)
	}

	if err := lifecycle.BeginDrain(context.Background()); err != nil {
		t.Fatalf("BeginDrain() error = %v", err)
	}
	if err := lifecycle.Ready(context.Background()); err == nil {
		t.Fatal("Ready() error = nil while draining")
	}
	draining := store.record("worker-a")
	if draining.State != StateDraining || draining.Inflight != 1 {
		t.Fatalf("draining node = %+v", draining)
	}
}

func TestLifecycleRejectsInvalidTiming(t *testing.T) {
	t.Parallel()

	_, err := NewLifecycle(newMemoryStore(), LifecycleConfig{
		NodeID: "worker-a", Role: "worker",
		HeartbeatInterval: time.Second, OfflineAfter: time.Second,
	})
	if err == nil {
		t.Fatal("NewLifecycle() error = nil when heartbeat is not shorter than offline window")
	}
}
