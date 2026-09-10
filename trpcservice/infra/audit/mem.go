// mem.go provides an in-memory audit Recorder for dev/test and as the second
// backend behind per-tenant audit routing. It keeps entries in a slice so
// tests can assert on what was recorded; it never touches the network.
package audit

import (
	"context"
	"sync"
)

// MemRecorder is the zero-dependency in-memory audit backend. It stores
// entries in-process (lost on restart); production uses MySQLRecorder.
type MemRecorder struct {
	mu      sync.Mutex
	entries []Entry
	usage   []UsageEntry
}

// NewMemRecorder returns an empty in-memory recorder.
func NewMemRecorder() *MemRecorder {
	return &MemRecorder{}
}

// Record appends one audit entry.
func (r *MemRecorder) Record(e Entry) {
	r.mu.Lock()
	r.entries = append(r.entries, e)
	r.mu.Unlock()
}

// RecordUsage appends one usage entry and always succeeds.
func (r *MemRecorder) RecordUsage(_ context.Context, e UsageEntry) error {
	r.mu.Lock()
	r.usage = append(r.usage, e)
	r.mu.Unlock()
	return nil
}

// Close is a no-op (nothing to drain).
func (r *MemRecorder) Close() error { return nil }

// Entries returns a copy of the recorded audit entries.
func (r *MemRecorder) Entries() []Entry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Entry(nil), r.entries...)
}

// Usage returns a copy of the recorded usage entries.
func (r *MemRecorder) Usage() []UsageEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]UsageEntry(nil), r.usage...)
}
