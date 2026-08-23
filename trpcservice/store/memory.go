package store

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/DocJlm/trpc-agent-service/trpcservice/channels"
	"github.com/DocJlm/trpc-agent-service/trpcservice/tenant"
)

type memoryItem[T any] struct {
	value       T
	state       string
	lockedUntil time.Time
	attempts    int
}

type MemoryRepository struct {
	mu       sync.Mutex
	inbound  map[string]channels.InboundEnvelope
	dispatch map[string]*memoryItem[channels.InboundEnvelope]
	replies  map[string]*memoryItem[channels.OutboundEnvelope]
	audits   []AuditLog
	tenants  map[string]tenant.Tenant
	agents   map[string]*memoryAgent
	bindings map[string]tenant.ChannelBinding
	backends map[string]tenant.BackendProfile
	sessions map[string][]map[string]string
}

type memoryAgent struct {
	name      string
	versions  map[string]json.RawMessage
	published string
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		inbound:  make(map[string]channels.InboundEnvelope),
		dispatch: make(map[string]*memoryItem[channels.InboundEnvelope]),
		replies:  make(map[string]*memoryItem[channels.OutboundEnvelope]),
		tenants:  make(map[string]tenant.Tenant),
		agents:   make(map[string]*memoryAgent),
		bindings: make(map[string]tenant.ChannelBinding),
		backends: make(map[string]tenant.BackendProfile),
		sessions: make(map[string][]map[string]string),
	}
}

func (r *MemoryRepository) Migrate(context.Context) error { return nil }

func (r *MemoryRepository) SeedTenants(_ context.Context, tenants []tenant.Tenant) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, item := range tenants {
		r.tenants[item.ID] = item
	}
	return nil
}

func (r *MemoryRepository) ListTenants(context.Context) ([]tenant.Tenant, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]tenant.Tenant, 0, len(r.tenants))
	for _, item := range r.tenants {
		result = append(result, item)
	}
	return result, nil
}

func (r *MemoryRepository) CreateAgent(_ context.Context, tenantID, id, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.tenants[tenantID]; !ok {
		return errors.New("tenant not found")
	}
	key := tenantID + "|" + id
	if _, ok := r.agents[key]; ok {
		return errors.New("agent already exists")
	}
	r.agents[key] = &memoryAgent{name: name, versions: make(map[string]json.RawMessage)}
	return nil
}

func (r *MemoryRepository) CreateAgentVersion(_ context.Context, tenantID, agentID, version string, profile json.RawMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	agent := r.agents[tenantID+"|"+agentID]
	if agent == nil {
		return errors.New("agent not found")
	}
	if _, ok := agent.versions[version]; ok {
		return errors.New("agent version already exists")
	}
	agent.versions[version] = append(json.RawMessage(nil), profile...)
	return nil
}

func (r *MemoryRepository) PublishAgent(_ context.Context, tenantID, agentID, version string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	agent := r.agents[tenantID+"|"+agentID]
	if agent == nil {
		return errors.New("agent not found")
	}
	if _, ok := agent.versions[version]; !ok {
		return errors.New("agent version not found")
	}
	agent.published = version
	return nil
}

func (r *MemoryRepository) SaveChannelBinding(_ context.Context, tenantID string, binding tenant.ChannelBinding) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.tenants[tenantID]; !ok {
		return errors.New("tenant not found")
	}
	r.bindings[tenantID+"|"+binding.ID] = binding
	return nil
}

func (r *MemoryRepository) SaveBackendProfile(_ context.Context, tenantID, id string, profile tenant.BackendProfile) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.tenants[tenantID]; !ok {
		return errors.New("tenant not found")
	}
	r.backends[tenantID+"|"+id] = profile
	return nil
}

func (r *MemoryRepository) AcceptInbound(_ context.Context, msg channels.InboundEnvelope) (bool, error) {
	if err := msg.Validate(); err != nil {
		return false, err
	}
	key := msg.IdempotencyKey()
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.inbound[key]; exists {
		return false, nil
	}
	r.inbound[key] = msg
	r.dispatch[key] = &memoryItem[channels.InboundEnvelope]{value: msg, state: "pending"}
	return true, nil
}

func (r *MemoryRepository) ClaimDispatch(_ context.Context, _ string, limit int, lease time.Duration) ([]DispatchTask, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	result := make([]DispatchTask, 0, limit)
	for id, item := range r.dispatch {
		if len(result) >= limit {
			break
		}
		if item.state == "done" || item.state == "processing" && item.lockedUntil.After(now) {
			continue
		}
		item.state = "processing"
		item.lockedUntil = now.Add(lease)
		item.attempts++
		result = append(result, DispatchTask{ID: id, Message: item.value})
	}
	return result, nil
}

func (r *MemoryRepository) CompleteDispatch(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	item, ok := r.dispatch[id]
	if !ok {
		return errors.New("dispatch task not found")
	}
	item.state = "done"
	return nil
}

func (r *MemoryRepository) RetryDispatch(_ context.Context, id string, _ error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	item, ok := r.dispatch[id]
	if !ok {
		return errors.New("dispatch task not found")
	}
	item.state = "pending"
	item.lockedUntil = time.Time{}
	return nil
}

func (r *MemoryRepository) StoreReply(_ context.Context, msg channels.OutboundEnvelope) error {
	if msg.ID == "" {
		return errors.New("reply id is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.replies[msg.ID]; exists {
		return nil
	}
	r.replies[msg.ID] = &memoryItem[channels.OutboundEnvelope]{value: msg, state: "pending"}
	return nil
}

func (r *MemoryRepository) CommitAgentResult(_ context.Context, in channels.InboundEnvelope, out channels.OutboundEnvelope) error {
	if out.ID == "" {
		return errors.New("reply id is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.replies[out.ID]; exists {
		return nil
	}
	key := in.TenantID + "|" + in.SessionID()
	r.sessions[key] = append(r.sessions[key],
		map[string]string{"role": "user", "content": in.Content, "trace_id": in.TraceID},
		map[string]string{"role": "assistant", "content": out.Content, "trace_id": out.TraceID},
	)
	r.replies[out.ID] = &memoryItem[channels.OutboundEnvelope]{value: out, state: "pending"}
	return nil
}

func (r *MemoryRepository) ReplyExists(_ context.Context, id string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.replies[id]
	return ok, nil
}

func (r *MemoryRepository) ClaimReplies(_ context.Context, _ string, limit int, lease time.Duration) ([]ReplyTask, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	result := make([]ReplyTask, 0, limit)
	for id, item := range r.replies {
		if len(result) >= limit {
			break
		}
		if item.state == "done" || item.state == "processing" && item.lockedUntil.After(now) {
			continue
		}
		item.state = "processing"
		item.lockedUntil = now.Add(lease)
		item.attempts++
		item.value.Attempts = item.attempts
		result = append(result, ReplyTask{ID: id, Message: item.value})
	}
	return result, nil
}

func (r *MemoryRepository) CompleteReply(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	item, ok := r.replies[id]
	if !ok {
		return errors.New("reply task not found")
	}
	item.state = "done"
	return nil
}

func (r *MemoryRepository) RetryReply(_ context.Context, id string, _ error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	item, ok := r.replies[id]
	if !ok {
		return errors.New("reply task not found")
	}
	item.state = "pending"
	item.lockedUntil = time.Time{}
	return nil
}

func (r *MemoryRepository) AppendAudit(_ context.Context, audit AuditLog) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if audit.CreatedAt.IsZero() {
		audit.CreatedAt = time.Now().UTC()
	}
	r.audits = append(r.audits, audit)
	return nil
}

func (r *MemoryRepository) Stats(context.Context) (Stats, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	stats := Stats{InboundTotal: int64(len(r.inbound)), AuditTotal: int64(len(r.audits))}
	for _, item := range r.dispatch {
		if item.state != "done" {
			stats.DispatchReady++
		}
	}
	for _, item := range r.replies {
		if item.state != "done" {
			stats.ReplyReady++
		}
	}
	return stats, nil
}

func (r *MemoryRepository) Close() {}
