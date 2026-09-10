package migration

import (
	"context"
	"sync"
)

// MemorySource and MemoryDestination are deterministic adapters for tests and
// local dry-runs. They model a monotonic source change log and idempotent sink.
// MemorySource is a deterministic source adapter for tests and dry-runs.
type MemorySource struct {
	mu      sync.Mutex
	records map[string]map[string]Record
	changes map[string][]Change
	seq     map[string]int64
	dual    map[string]bool
}

// MemoryStateStore is a deterministic process-local StateStore for tests and
// dry-runs. Production callers should provide a shared durable implementation.
type MemoryStateStore struct {
	mu     sync.Mutex
	states map[string]State
}

// NewMemoryStateStore creates an empty migration state store.
func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{states: map[string]State{}}
}

// Get returns one tenant's migration phase state.
func (s *MemoryStateStore) Get(ctx context.Context, tenantID string) (State, error) {
	if err := validate(ctx, tenantID); err != nil {
		return State{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.states[tenantID]
	if !ok {
		return State{}, ErrNotFound
	}
	return state, nil
}

// Put stores one tenant's migration phase state.
func (s *MemoryStateStore) Put(ctx context.Context, state State) error {
	if err := validate(ctx, state.TenantID); err != nil {
		return err
	}
	if state.Barrier < 0 || (state.PreviousBackend != "" && state.PreviousBackend != BackendSource && state.PreviousBackend != BackendDestination) {
		return ErrInvalid
	}
	s.mu.Lock()
	s.states[state.TenantID] = state
	s.mu.Unlock()
	return nil
}

// NewMemorySource creates an empty source adapter.
func NewMemorySource() *MemorySource {
	return &MemorySource{records: map[string]map[string]Record{}, changes: map[string][]Change{}, seq: map[string]int64{}, dual: map[string]bool{}}
}

// BeginDualWrite records the current source watermark.
func (s *MemorySource) BeginDualWrite(_ context.Context, tenantID string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if tenantID == "" {
		return 0, ErrInvalid
	}
	s.dual[tenantID] = true
	return s.seq[tenantID], nil
}

// Put appends one source record and advances its watermark.
func (s *MemorySource) Put(tenantID string, record Record) error {
	if tenantID == "" || record.Kind == "" || record.Key == "" {
		return ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record.TenantID = tenantID
	if s.records[tenantID] == nil {
		s.records[tenantID] = map[string]Record{}
	}
	s.seq[tenantID]++
	record.Version = s.seq[tenantID]
	s.records[tenantID][record.Kind+"\x00"+record.Key] = cloneRecord(record)
	s.changes[tenantID] = append(s.changes[tenantID], Change{Sequence: s.seq[tenantID], Record: cloneRecord(record)})
	return nil
}

// Snapshot returns the current tenant records and watermark.
func (s *MemorySource) Snapshot(_ context.Context, tenantID string) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if tenantID == "" {
		return Snapshot{}, ErrInvalid
	}
	result := make([]Record, 0, len(s.records[tenantID]))
	for _, record := range s.records[tenantID] {
		result = append(result, cloneRecord(record))
	}
	return Snapshot{Records: result, Watermark: s.seq[tenantID]}, nil
}

// Changes returns source log entries after a watermark.
func (s *MemorySource) Changes(_ context.Context, tenantID string, after int64) ([]Change, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if tenantID == "" || after < 0 {
		return nil, 0, ErrInvalid
	}
	result := make([]Change, 0)
	for _, change := range s.changes[tenantID] {
		if change.Sequence > after {
			result = append(result, Change{Sequence: change.Sequence, Record: cloneRecord(change.Record)})
		}
	}
	return result, s.seq[tenantID], nil
}

// MemoryDestination is an idempotent destination adapter for tests.
type MemoryDestination struct {
	mu      sync.Mutex
	records map[string]map[string]Record
	water   map[string]int64
}

// NewMemoryDestination creates an empty destination adapter.
func NewMemoryDestination() *MemoryDestination {
	return &MemoryDestination{records: map[string]map[string]Record{}, water: map[string]int64{}}
}

// Apply upserts tenant records into the destination.
func (d *MemoryDestination) Apply(_ context.Context, records []Record) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, record := range records {
		if record.TenantID == "" || record.Kind == "" || record.Key == "" {
			return ErrInvalid
		}
		if d.records[record.TenantID] == nil {
			d.records[record.TenantID] = map[string]Record{}
		}
		d.records[record.TenantID][record.Kind+"\x00"+record.Key] = cloneRecord(record)
		if record.Version > d.water[record.TenantID] {
			d.water[record.TenantID] = record.Version
		}
	}
	return nil
}

// Snapshot returns destination records and its applied watermark.
func (d *MemoryDestination) Snapshot(_ context.Context, tenantID string) (Snapshot, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if tenantID == "" {
		return Snapshot{}, ErrInvalid
	}
	result := make([]Record, 0, len(d.records[tenantID]))
	for _, record := range d.records[tenantID] {
		result = append(result, cloneRecord(record))
	}
	return Snapshot{Records: result, Watermark: d.water[tenantID]}, nil
}

// MemoryRouter stores tenant backend selection for tests.
type MemoryRouter struct {
	mu      sync.Mutex
	backend map[string]Backend
}

// NewMemoryRouter creates a source-default router.
func NewMemoryRouter() *MemoryRouter { return &MemoryRouter{backend: map[string]Backend{}} }

// Current reads the tenant's active backend.
func (r *MemoryRouter) Current(_ context.Context, tenantID string) (Backend, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if tenantID == "" {
		return "", ErrInvalid
	}
	value, ok := r.backend[tenantID]
	if !ok {
		return BackendSource, nil
	}
	return value, nil
}

// Set changes the tenant's active backend.
func (r *MemoryRouter) Set(_ context.Context, tenantID string, backend Backend) error {
	if tenantID == "" || (backend != BackendSource && backend != BackendDestination) {
		return ErrInvalid
	}
	r.mu.Lock()
	r.backend[tenantID] = backend
	r.mu.Unlock()
	return nil
}

func cloneRecord(value Record) Record {
	value.Payload = append([]byte(nil), value.Payload...)
	return value
}

var _ Source = (*MemorySource)(nil)
var _ Destination = (*MemoryDestination)(nil)
var _ Router = (*MemoryRouter)(nil)
var _ StateStore = (*MemoryStateStore)(nil)
