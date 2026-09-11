package node

import (
	"context"
	"errors"
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

func TestRecordValidateRejectsInvalidPersistedNodeState(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	valid := Record{
		NodeID: "worker-a", BootID: "boot-a", Role: "worker", State: StateReady,
		BuildVersion: "test", StartedAt: now, LastHeartbeat: now, LeaseUntil: now.Add(time.Minute),
	}
	if err := valid.validate(); err != nil {
		t.Fatalf("valid record rejected: %v", err)
	}
	draining := valid
	draining.State = StateDraining
	if err := draining.validate(); err != nil {
		t.Fatalf("draining record rejected: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*Record)
	}{
		{name: "node id", mutate: func(r *Record) { r.NodeID = " " }},
		{name: "boot id", mutate: func(r *Record) { r.BootID = "" }},
		{name: "role", mutate: func(r *Record) { r.Role = "scheduler" }},
		{name: "state", mutate: func(r *Record) { r.State = StateOffline }},
		{name: "inflight", mutate: func(r *Record) { r.Inflight = -1 }},
		{name: "started", mutate: func(r *Record) { r.StartedAt = time.Time{} }},
		{name: "heartbeat", mutate: func(r *Record) { r.LastHeartbeat = time.Time{} }},
		{name: "lease", mutate: func(r *Record) { r.LeaseUntil = time.Time{} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := valid
			test.mutate(&record)
			if err := record.validate(); err == nil {
				t.Fatalf("invalid record accepted: %+v", record)
			}
		})
	}
}

type lifecycleFailureStore struct {
	*memoryStore
	registerErr  error
	heartbeatErr error
}

func (s *lifecycleFailureStore) Register(ctx context.Context, record Record) error {
	if s.registerErr != nil {
		return s.registerErr
	}
	return s.memoryStore.Register(ctx, record)
}

func (s *lifecycleFailureStore) Heartbeat(ctx context.Context, nodeID, bootID string, state State, inflight int, leaseUntil time.Time) error {
	if s.heartbeatErr != nil {
		return s.heartbeatErr
	}
	return s.memoryStore.Heartbeat(ctx, nodeID, bootID, state, inflight, leaseUntil)
}

func TestLifecycleValidationDefaultsAndFailureBoundaries(t *testing.T) {
	t.Parallel()
	if _, err := NewLifecycle(nil, LifecycleConfig{}); err == nil {
		t.Fatal("NewLifecycle(nil) succeeded")
	}
	for _, cfg := range []LifecycleConfig{
		{Role: "worker", HeartbeatInterval: time.Second, OfflineAfter: 2 * time.Second},
		{NodeID: "node-a", HeartbeatInterval: time.Second, OfflineAfter: 2 * time.Second},
		{NodeID: "node-a", Role: "worker", HeartbeatInterval: 0, OfflineAfter: time.Second},
		{NodeID: "node-a", Role: "worker", HeartbeatInterval: 2 * time.Second, OfflineAfter: time.Second},
	} {
		if _, err := NewLifecycle(newMemoryStore(), cfg); err == nil {
			t.Fatalf("invalid lifecycle config accepted: %+v", cfg)
		}
	}
	lifecycle, err := NewLifecycle(newMemoryStore(), LifecycleConfig{
		NodeID: " node-a ", Role: " WORKER ", HeartbeatInterval: time.Second, OfflineAfter: 2 * time.Second,
		Inflight: func() int { return -4 },
	})
	if err != nil {
		t.Fatal(err)
	}
	if lifecycle.cfg.NodeID != "node-a" || lifecycle.cfg.Role != "worker" || lifecycle.cfg.BuildVersion != "unknown" || lifecycle.inflight() != 0 {
		t.Fatalf("normalized lifecycle = %+v inflight=%d", lifecycle.cfg, lifecycle.inflight())
	}
	if err := lifecycle.Start(nil); err != nil {
		t.Fatalf("Start(nil) error = %v", err)
	}
	if err := lifecycle.Start(context.Background()); err == nil {
		t.Fatal("second Start() succeeded")
	}
	if err := lifecycle.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lifecycle.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	var nilLifecycle *Lifecycle
	if err := nilLifecycle.Start(context.Background()); err == nil {
		t.Fatal("nil lifecycle Start() succeeded")
	}
	if err := nilLifecycle.BeginDrain(context.Background()); err != nil {
		t.Fatalf("nil BeginDrain() error = %v", err)
	}
	if err := nilLifecycle.Close(); err != nil {
		t.Fatalf("nil Close() error = %v", err)
	}
}

func TestLifecycleRegistrationAndHeartbeatFailuresMakeNodeUnavailable(t *testing.T) {
	t.Parallel()
	wantRegisterErr := errors.New("register unavailable")
	store := &lifecycleFailureStore{memoryStore: newMemoryStore(), registerErr: wantRegisterErr}
	lifecycle, err := NewLifecycle(store, LifecycleConfig{
		NodeID: "worker-a", Role: "worker", HeartbeatInterval: time.Second, OfflineAfter: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := lifecycle.Start(context.Background()); !errors.Is(err, wantRegisterErr) {
		t.Fatalf("Start() error = %v", err)
	}
	if err := lifecycle.Ready(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Ready() after register failure = %v", err)
	}

	store.registerErr = nil
	if err := lifecycle.Start(context.Background()); err != nil {
		t.Fatalf("Start() retry error = %v", err)
	}
	t.Cleanup(func() { _ = lifecycle.Close() })
	wantHeartbeatErr := errors.New("heartbeat unavailable")
	store.heartbeatErr = wantHeartbeatErr
	if err := lifecycle.heartbeat(context.Background(), StateReady); !errors.Is(err, wantHeartbeatErr) {
		t.Fatalf("heartbeat() error = %v", err)
	}
	if err := lifecycle.Ready(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Ready() after heartbeat failure = %v", err)
	}
}
