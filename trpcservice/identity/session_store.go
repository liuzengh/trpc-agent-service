package identity

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

// MemorySessionStore is the process-local backend for tests and local development.
type MemorySessionStore struct {
	mu      sync.Mutex
	entries map[string]memorySession
	now     func() time.Time
}

type memorySession struct {
	user      SessionUser
	expiresAt time.Time
}

// NewMemorySessionStore constructs an isolated local store.
func NewMemorySessionStore() *MemorySessionStore {
	return &MemorySessionStore{
		entries: make(map[string]memorySession),
		now:     time.Now,
	}
}

// Create issues a random session ID valid for ttl.
func (s *MemorySessionStore) Create(_ context.Context, user SessionUser, ttl time.Duration) (string, error) {
	if ttl <= 0 {
		return "", errors.New("session TTL must be positive")
	}
	if user.PlatformUserID == "" {
		return "", errors.New("session user identity is required")
	}
	token, err := newSessionID()
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[token] = memorySession{user: user, expiresAt: s.now().Add(ttl)}
	return token, nil
}

// Get returns the live session user; expired entries are removed and reported
// as ErrSessionExpired so the caller can write the session_expire audit event.
func (s *MemorySessionStore) Get(_ context.Context, sessionID string) (SessionUser, error) {
	if sessionID == "" {
		return SessionUser{}, ErrSessionNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, exists := s.entries[sessionID]
	if !exists {
		return SessionUser{}, ErrSessionNotFound
	}
	if !entry.expiresAt.After(s.now()) {
		delete(s.entries, sessionID)
		return SessionUser{}, ErrSessionExpired
	}
	return entry.user, nil
}

// Delete invalidates a session (idempotent for unknown IDs).
func (s *MemorySessionStore) Delete(_ context.Context, sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, sessionID)
	return nil
}

func (s *MemorySessionStore) DeleteForUser(_ context.Context, platformUserID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for sessionID, entry := range s.entries {
		if entry.user.PlatformUserID == platformUserID {
			delete(s.entries, sessionID)
		}
	}
	return nil
}

func newSessionID() (string, error) {
	buffer := make([]byte, 32)
	if _, err := rand.Read(buffer); err != nil {
		return "", errors.New("generate session ID: " + err.Error())
	}
	return hex.EncodeToString(buffer), nil
}
