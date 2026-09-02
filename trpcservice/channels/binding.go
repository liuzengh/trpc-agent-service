package channels

import (
	"context"
	"errors"
	"sync"
	"time"
)

// IM channel/account values for ChannelBinding.Channel.
const (
	ChannelWeCom  = "wecom"
	ChannelFeishu = "feishu"
)

// ErrBindingNotFound is returned when a binding does not exist.
var ErrBindingNotFound = errors.New("channels: binding not found")

// ErrBindingDuplicate is returned when the same channel+account is bound again.
var ErrBindingDuplicate = errors.New("channels: channel account already bound")

// ChannelBinding binds an IM account (a channel + account identity) to a
// tenant + agent. credential_ref is a secret-store reference (never a
// plaintext token). Deleting is soft (is_deleted) to keep the unique key.
type ChannelBinding struct {
	BindingID     string    `json:"binding_id"`
	TenantID      string    `json:"tenant_id"`
	AgentID       string    `json:"agent_id"`
	Channel       string    `json:"channel"` // wecom | feishu
	AccountID     string    `json:"account_id"`
	CredentialRef string    `json:"credential_ref,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

// BindingStore persists IM channel bindings behind a swappable backend.
type BindingStore interface {
	Create(ctx context.Context, b ChannelBinding) error
	List(ctx context.Context, tenantID, channel string) ([]ChannelBinding, error)
	Get(ctx context.Context, bindingID string) (*ChannelBinding, error)
	Delete(ctx context.Context, bindingID string) error
}

// NewMemBindingStore returns an in-memory binding store (dev/test).
func NewMemBindingStore() BindingStore {
	return &memBindingStore{items: make(map[string]ChannelBinding)}
}

type memBindingStore struct {
	mu    sync.RWMutex
	items map[string]ChannelBinding // binding_id -> binding
	byKey map[string]string         // channel:account -> binding_id
}

func (s *memBindingStore) Create(_ context.Context, b ChannelBinding) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b.BindingID == "" || b.Channel == "" || b.AccountID == "" {
		return errors.New("channels: binding id, channel and account are required")
	}
	if s.byKey == nil {
		s.byKey = make(map[string]string)
	}
	key := b.Channel + ":" + b.AccountID
	if _, dup := s.byKey[key]; dup {
		return ErrBindingDuplicate
	}
	if b.CreatedAt.IsZero() {
		b.CreatedAt = time.Now().UTC()
	}
	s.items[b.BindingID] = b
	s.byKey[key] = b.BindingID
	return nil
}

func (s *memBindingStore) List(_ context.Context, tenantID, channel string) ([]ChannelBinding, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ChannelBinding, 0, len(s.items))
	for _, b := range s.items {
		if (tenantID == "" || b.TenantID == tenantID) && (channel == "" || b.Channel == channel) {
			out = append(out, b)
		}
	}
	return out, nil
}

func (s *memBindingStore) Get(_ context.Context, bindingID string) (*ChannelBinding, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.items[bindingID]
	if !ok {
		return nil, ErrBindingNotFound
	}
	cp := b
	return &cp, nil
}

func (s *memBindingStore) Delete(_ context.Context, bindingID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.items[bindingID]
	if !ok {
		return ErrBindingNotFound
	}
	delete(s.items, bindingID)
	delete(s.byKey, b.Channel+":"+b.AccountID)
	return nil
}
