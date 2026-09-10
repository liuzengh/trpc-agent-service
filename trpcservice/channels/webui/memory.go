package webui

import (
	"context"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type MemoryMailbox struct {
	mu       sync.RWMutex
	messages map[string]Message
}

func NewMemoryMailbox() *MemoryMailbox { return &MemoryMailbox{messages: make(map[string]Message)} }

func (m *MemoryMailbox) PutMessage(ctx context.Context, value Message) (Message, error) {
	if err := ctx.Err(); err != nil {
		return Message{}, err
	}
	if err := validMessage(value); err != nil {
		return Message{}, err
	}
	key := value.TenantID + "\x00" + value.ClientRequestID
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.messages[key]; ok {
		value.CreatedAt = existing.CreatedAt
		if existing != value {
			return Message{}, runtime.ErrIdempotencyCollision
		}
		return existing, nil
	}
	if value.CreatedAt.IsZero() {
		value.CreatedAt = time.Now().UTC()
	}
	m.messages[key] = value
	return value, nil
}

func (m *MemoryMailbox) GetMessageByClientRequestID(ctx context.Context, tenantID, clientRequestID string) (Message, error) {
	if err := ctx.Err(); err != nil {
		return Message{}, err
	}
	if tenantID == "" || clientRequestID == "" {
		return Message{}, runtime.ErrTenantScope
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, ok := m.messages[tenantID+"\x00"+clientRequestID]
	if !ok {
		return Message{}, runtime.ErrNotFound
	}
	return value, nil
}

func (m *MemoryMailbox) ListMessages(ctx context.Context, query MessageQuery) ([]Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if query.TenantID == "" || query.ChannelBindingID == "" || query.ExternalAccountID == "" ||
		query.ExternalUserID == "" || query.ExternalChatID == "" {
		return nil, runtime.ErrTenantScope
	}
	limit := query.Limit
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]Message, 0, limit)
	for _, value := range m.messages {
		if value.TenantID == query.TenantID && value.ChannelBindingID == query.ChannelBindingID &&
			value.ExternalAccountID == query.ExternalAccountID && value.ExternalUserID == query.ExternalUserID &&
			value.ExternalChatID == query.ExternalChatID && value.CreatedAt.After(query.After) {
			result = append(result, value)
			if len(result) == limit {
				break
			}
		}
	}
	return result, nil
}

func validMessage(value Message) error {
	if value.TenantID == "" || value.ChannelBindingID == "" || value.ExternalAccountID == "" || value.ExternalUserID == "" ||
		value.ExternalChatID == "" || value.RequestID == "" || value.ClientRequestID == "" || value.ProviderMessageID == "" ||
		value.ContentRef == "" || len(value.ContentDigest) != 64 || value.ConfigVersion < 1 {
		return runtime.ErrInvariantViolation
	}
	return nil
}

var _ Mailbox = (*MemoryMailbox)(nil)
var _ MessageReader = (*MemoryMailbox)(nil)
