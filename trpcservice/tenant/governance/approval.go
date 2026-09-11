package governance

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

// ApprovalBackend stores short-lived, argument-bound authorizations. Consume
// must atomically compare the scope and remove the token on a match only.
// An uncertain consume must return an error; callers must never infer approval.
type ApprovalBackend interface {
	IssueApproval(ctx context.Context, scope string, ttl time.Duration) (string, error)
	ConsumeApproval(ctx context.Context, nonce, scope string) (bool, error)
}

var ErrApprovalUnavailable = errors.New("approval storage is temporarily unavailable")

type approval struct {
	scope   string
	expires time.Time
}

type ApprovalStore struct {
	mu       sync.Mutex
	items    map[string]approval
	now      func() time.Time
	newNonce func() (string, error)
}

var _ ApprovalBackend = (*ApprovalStore)(nil)

func NewApprovalStore() *ApprovalStore {
	return &ApprovalStore{items: make(map[string]approval), now: time.Now, newNonce: newApprovalNonce}
}

// Issue retains the original in-memory API for existing callers.
func (s *ApprovalStore) Issue(scope string, ttl time.Duration) (string, error) {
	return s.IssueApproval(context.Background(), scope, ttl)
}

func (s *ApprovalStore) IssueApproval(ctx context.Context, scope string, ttl time.Duration) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if s == nil || scope == "" || ttl < time.Millisecond {
		return "", ErrApprovalUnavailable
	}
	for attempt := 0; attempt < 3; attempt++ {
		nonce, err := s.newNonce()
		if err != nil {
			return "", ErrApprovalUnavailable
		}
		s.mu.Lock()
		if err := ctx.Err(); err != nil {
			s.mu.Unlock()
			return "", err
		}
		now := s.now()
		s.deleteExpired(now)
		if len(s.items) >= maxPendingApprovals {
			s.mu.Unlock()
			return "", errors.New("approval capacity reached")
		}
		if _, collision := s.items[nonce]; collision {
			s.mu.Unlock()
			continue
		}
		s.items[nonce] = approval{scope: scope, expires: now.Add(ttl)}
		s.mu.Unlock()
		return nonce, nil
	}
	return "", errors.New("generate unique approval token")
}

// Consume retains the original API; backend errors never grant approval.
func (s *ApprovalStore) Consume(nonce, scope string) bool {
	approved, err := s.ConsumeApproval(context.Background(), nonce, scope)
	return err == nil && approved
}

func (s *ApprovalStore) ConsumeApproval(ctx context.Context, nonce, scope string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if s == nil {
		return false, ErrApprovalUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	now := s.now()
	s.deleteExpired(now)
	if nonce == "" || scope == "" {
		return false, nil
	}
	item, ok := s.items[nonce]
	if !ok || item.scope != scope {
		return false, nil
	}
	delete(s.items, nonce)
	return true, nil
}

func (s *ApprovalStore) deleteExpired(now time.Time) {
	for nonce, item := range s.items {
		if !now.Before(item.expires) {
			delete(s.items, nonce)
		}
	}
}

func newApprovalNonce() (string, error) {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return "", ErrApprovalUnavailable
	}
	return hex.EncodeToString(random), nil
}
