package platform

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

type Tenant struct {
	ID            string           `json:"tenant_id"`
	Name          string           `json:"name"`
	Agent         AgentConfig      `json:"agent"`
	Channels      []ChannelBinding `json:"channels"`
	Backend       BackendConfig    `json:"backend"`
	ToolAllowlist []string         `json:"tool_allowlist"`
	BudgetCents   int64            `json:"budget_cents"`
}
type AgentConfig struct{ Name, Model, SystemPrompt string }
type ChannelBinding struct {
	Type, ID, SecretRef string `json:"-"`
	Enabled             bool   `json:"enabled"`
}
type BackendConfig struct{ Session, Memory, Vector string }

type Message struct {
	ID        string
	TenantID  string
	BindingID string
	Channel   string
	UserID    string
	SessionID string
	Role      string
	Content   string
	CreatedAt time.Time
}
type Event struct {
	ID, TenantID, SessionID, Type, Payload string
	Sequence                               int64
	CreatedAt                              time.Time
}
type Audit struct {
	ID, TenantID, Channel, UserID, SessionID, AgentName, ToolName, Decision, ErrorType, TraceID string
	Latency                                                                                     time.Duration
	CostCents                                                                                   int64
	CreatedAt                                                                                   time.Time
}

type Store interface {
	GetTenant(context.Context, string) (Tenant, error)
	SaveTenant(context.Context, Tenant) error
	AppendMessage(context.Context, Message) error
	Messages(context.Context, string, string) ([]Message, error)
	AppendEvent(context.Context, Event) error
	Claim(context.Context, string, string, string, string) (bool, error)
	Audit(context.Context, Audit) error
}

type MemoryStore struct {
	mu       sync.RWMutex
	tenants  map[string]Tenant
	messages map[string][]Message
	events   map[string][]Event
	seen     map[string]struct{}
	audits   []Audit
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{tenants: map[string]Tenant{}, messages: map[string][]Message{}, events: map[string][]Event{}, seen: map[string]struct{}{}}
}
func (s *MemoryStore) GetTenant(ctx context.Context, id string) (Tenant, error) {
	if err := ctx.Err(); err != nil {
		return Tenant{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tenants[id]
	if !ok {
		return Tenant{}, fmt.Errorf("tenant %q not found", id)
	}
	return t, nil
}
func (s *MemoryStore) SaveTenant(ctx context.Context, t Tenant) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(t.ID) == "" {
		return errors.New("tenant_id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tenants[t.ID] = t
	return nil
}
func key(t, session string) string { return t + "/" + session }
func (s *MemoryStore) AppendMessage(ctx context.Context, m Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.TenantID == "" || m.SessionID == "" {
		return errors.New("tenant_id and session_id are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages[key(m.TenantID, m.SessionID)] = append(s.messages[key(m.TenantID, m.SessionID)], m)
	return nil
}
func (s *MemoryStore) Messages(ctx context.Context, t, session string) ([]Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := append([]Message(nil), s.messages[key(t, session)]...)
	return out, nil
}
func (s *MemoryStore) AppendEvent(ctx context.Context, e Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(e.TenantID, e.SessionID)
	e.Sequence = int64(len(s.events[k]) + 1)
	s.events[k] = append(s.events[k], e)
	return nil
}
func (s *MemoryStore) Claim(ctx context.Context, tenantID, channel, bindingID, id string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if tenantID == "" || channel == "" || bindingID == "" || id == "" {
		return false, errors.New("tenant_id, channel, binding_id, and message_id are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	claimKey := key(tenantID, channel+"/"+bindingID+"/"+id)
	if _, exists := s.seen[claimKey]; exists {
		return false, nil
	}
	s.seen[claimKey] = struct{}{}
	return true, nil
}
func (s *MemoryStore) Audit(ctx context.Context, a Audit) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.audits = append(s.audits, a)
	return nil
}

// Backend interfaces keep workers stateless. Redis, SQL and vector implementations can be supplied without changing Runner.
type VectorStore interface {
	Search(context.Context, string, string, int) ([]string, error)
	Upsert(context.Context, string, string, string) error
}
type SQLStore interface{ Store }
type Responder interface {
	Respond(context.Context, Tenant, []Message, string) (string, error)
}
type EchoResponder struct{}

func (EchoResponder) Respond(_ context.Context, _ Tenant, _ []Message, input string) (string, error) {
	return "收到：" + input, nil
}

type Runner struct {
	Store     Store
	Responder Responder
}

func (r Runner) Run(ctx context.Context, in Message) (string, string, error) {
	if r.Store == nil {
		return "", "", errors.New("store is not configured")
	}
	if r.Responder == nil {
		return "", "", errors.New("responder is not configured")
	}
	t, err := r.Store.GetTenant(ctx, in.TenantID)
	if err != nil {
		return "", "", err
	}
	trace := traceID(in)
	claimed, err := r.Store.Claim(ctx, in.TenantID, in.Channel, in.BindingID, in.ID)
	if err != nil {
		return trace, "", err
	}
	if !claimed {
		return trace, "", nil
	}
	in.Role = "user"
	if in.CreatedAt.IsZero() {
		in.CreatedAt = time.Now().UTC()
	}
	if err = r.Store.AppendMessage(ctx, in); err != nil {
		return trace, "", err
	}
	started := time.Now()
	history, err := r.Store.Messages(ctx, in.TenantID, in.SessionID)
	if err != nil {
		return trace, "", err
	}
	out, err := r.Responder.Respond(ctx, t, history, in.Content)
	audit := Audit{ID: in.ID, TenantID: in.TenantID, Channel: in.Channel, UserID: in.UserID, SessionID: in.SessionID, AgentName: t.Agent.Name, Decision: "allow", TraceID: trace, Latency: time.Since(started), CreatedAt: time.Now().UTC()}
	if err != nil {
		audit.Decision = "error"
		audit.ErrorType = "agent"
		if auditErr := r.Store.Audit(ctx, audit); auditErr != nil {
			return trace, "", fmt.Errorf("record agent failure audit: %w (agent error: %v)", auditErr, err)
		}
		return trace, "", err
	}
	reply := Message{ID: in.ID + ":reply", TenantID: in.TenantID, Channel: in.Channel, UserID: in.UserID, SessionID: in.SessionID, Role: "assistant", Content: out, CreatedAt: time.Now().UTC()}
	if err := r.Store.AppendMessage(ctx, reply); err != nil {
		return trace, "", fmt.Errorf("append assistant message: %w", err)
	}
	if err := r.Store.AppendEvent(ctx, Event{ID: reply.ID, TenantID: in.TenantID, SessionID: in.SessionID, Type: "assistant.completed", Payload: out, CreatedAt: reply.CreatedAt}); err != nil {
		return trace, "", fmt.Errorf("append assistant event: %w", err)
	}
	if err := r.Store.Audit(ctx, audit); err != nil {
		return trace, "", fmt.Errorf("record agent audit: %w", err)
	}
	return trace, out, nil
}
func traceID(m Message) string {
	h := sha256.Sum256([]byte(m.TenantID + "|" + m.Channel + "|" + m.ID))
	return hex.EncodeToString(h[:8])
}
