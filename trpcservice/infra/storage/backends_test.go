package storage

import (
	"strings"
	"testing"
)

// TestBackendTableCoversBothDomains is the invariant the table exists for: a
// backend that the tenant can select must be buildable for the session domain
// and the memory domain alike. Two parallel switches could drift and leave one
// domain rejecting a backend the other accepts.
func TestBackendTableCoversBothDomains(t *testing.T) {
	for id, b := range backendTable {
		if b.session == nil {
			t.Errorf("backend %q has no session constructor", id)
		}
		if b.memory == nil {
			t.Errorf("backend %q has no memory constructor", id)
		}
	}
	for _, want := range []Backend{BackendInMemory, BackendMySQL, BackendRedis} {
		if _, ok := backendTable[want]; !ok {
			t.Errorf("backend %q is missing from the table", want)
		}
	}
	if got := len(SupportedBackends()); got != len(backendTable) {
		t.Errorf("SupportedBackends() = %d entries, want %d", got, len(backendTable))
	}
}

// TestUnknownBackendIsRejectedByBothDomains keeps the error path honest: a
// typo in the tenant's data_backend must fail loudly on either domain, and the
// message must name the supported set.
func TestUnknownBackendIsRejectedByBothDomains(t *testing.T) {
	if _, err := NewSessions(SessionConfig{Backend: Backend("nope")}); err == nil {
		t.Error("NewSessions accepts an unknown backend")
	} else if !strings.Contains(err.Error(), "supported") {
		t.Errorf("NewSessions error = %v, want the supported backends listed", err)
	}
	if _, err := NewMemories(MemoryConfig{Backend: Backend("nope")}); err == nil {
		t.Error("NewMemories accepts an unknown backend")
	} else if !strings.Contains(err.Error(), "supported") {
		t.Errorf("NewMemories error = %v, want the supported backends listed", err)
	}
	// A vector/object-store backend id is not a session backend either.
	if _, err := NewSessions(SessionConfig{Backend: BackendMilvus}); err == nil {
		t.Error("NewSessions accepts the milvus backend id, which is not a session backend")
	}
}

// TestInMemoryBackendBuildsWithoutDependencies covers the dev path the whole
// test suite depends on.
func TestInMemoryBackendBuildsWithoutDependencies(t *testing.T) {
	s, err := NewSessions(SessionConfig{Backend: BackendInMemory})
	if err != nil || s == nil {
		t.Fatalf("NewSessions(inmemory) = %v, %v", s, err)
	}
	m, err := NewMemories(MemoryConfig{Backend: BackendInMemory})
	if err != nil || m == nil {
		t.Fatalf("NewMemories(inmemory) = %v, %v", m, err)
	}
}
