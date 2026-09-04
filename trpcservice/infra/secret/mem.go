package secret

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

// MemStore keeps secrets in memory in plaintext. It is the zero-dependency
// dev/test backend; the MySQL backend must be used in production.
type MemStore struct {
	mu   sync.RWMutex
	data map[string]memEntry
}

type memEntry struct {
	value     string
	updatedAt time.Time
}

// NewMemStore returns an in-memory secret store.
func NewMemStore() *MemStore {
	return &MemStore{data: make(map[string]memEntry)}
}

// Put stores a secret (overwriting an existing key).
func (s *MemStore) Put(_ context.Context, key, value string) error {
	if key == "" {
		return errors.New("secret: key is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = memEntry{value: value, updatedAt: time.Now().UTC()}
	return nil
}

// Get returns the stored value, or ErrNotFound.
func (s *MemStore) Get(_ context.Context, key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok {
		return "", ErrNotFound
	}
	return e.value, nil
}

// List returns metadata only (key + updated_at), never values.
func (s *MemStore) List(_ context.Context) ([]Secret, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Secret, 0, len(s.data))
	for k, e := range s.data {
		out = append(out, Secret{Key: k, UpdatedAt: e.updatedAt})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// Delete removes a secret; deleting a missing key is a no-op.
func (s *MemStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	return nil
}
