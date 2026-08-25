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

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	tenantpkg "github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
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
type AgentConfig struct {
	Name, Model, SystemPrompt string
	ModelConfigRef            string
	ToolPolicyRef             string
	GuardrailRef              string
}
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

type AssistantCommitter interface {
	CommitAssistant(context.Context, Message, Event) error
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

func (s *MemoryStore) CommitAssistant(ctx context.Context, message Message, event Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if message.TenantID == "" || message.SessionID == "" || event.TenantID != message.TenantID || event.SessionID != message.SessionID {
		return errors.New("assistant commit tenant and session mismatch")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	messageKey := key(message.TenantID, message.SessionID)
	eventKey := key(event.TenantID, event.SessionID)
	event.Sequence = int64(len(s.events[eventKey]) + 1)
	s.messages[messageKey] = append(s.messages[messageKey], message)
	s.events[eventKey] = append(s.events[eventKey], event)
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

// RuntimeResponder bridges the platform's synchronous responder contract to
// the framework-isolated agent runtime.
type RuntimeResponder struct {
	Factory agent.AgentFactory
	Spec    func(tenantpkg.TenantContext, Tenant) agent.AgentSpec
}

func (r RuntimeResponder) Respond(ctx context.Context, t Tenant, history []Message, input string) (string, error) {
	if r.Factory == nil {
		return "", errors.New("runtime factory is not configured")
	}
	tc, err := tenantpkg.RequireTenant(ctx, t.ID)
	if err != nil {
		return "", fmt.Errorf("runtime tenant context: %w", err)
	}
	spec := defaultAgentSpec(tc, t)
	if r.Spec != nil {
		spec = r.Spec(tc, t)
	}
	runtimeValue, err := r.Factory.Build(ctx, tc, spec)
	if err != nil {
		return "", fmt.Errorf("build agent runtime: %w", err)
	}
	messages := make([]agent.Message, 0, len(history))
	for _, message := range history {
		messages = append(messages, agent.Message{ID: message.ID, Role: message.Role, Content: message.Content, CreatedAt: message.CreatedAt})
	}
	current := agent.Message{ID: tc.MessageID, Role: "user", Content: input, CreatedAt: time.Now().UTC()}
	result, err := runtimeValue.Run(ctx, agent.AgentInput{TenantContext: tc, Agent: spec, History: messages, Input: current})
	if err != nil {
		return "", err
	}
	return result.Text, nil
}

func defaultAgentSpec(tc tenantpkg.TenantContext, t Tenant) agent.AgentSpec {
	name := t.Agent.Name
	if name == "" {
		name = tc.AgentAppID
	}
	provider := t.Agent.Model
	if provider == "" {
		provider = "configured"
	}
	return agent.AgentSpec{TenantID: tc.TenantID, AgentAppID: tc.AgentAppID, Version: tc.ConfigVersion, Name: name, ModelProvider: provider, ModelConfigRef: t.Agent.ModelConfigRef, ToolPolicyRef: t.Agent.ToolPolicyRef, GuardrailRef: t.Agent.GuardrailRef, SystemPrompt: t.Agent.SystemPrompt}
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
	auditCtx, auditCancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer auditCancel()
	if err != nil {
		audit.Decision = "error"
		audit.ErrorType = classifyAuditError(err)
		if auditErr := r.Store.Audit(auditCtx, audit); auditErr != nil {
			return trace, "", fmt.Errorf("record agent failure audit: %w (agent error: %v)", auditErr, err)
		}
		return trace, "", err
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		audit.Decision = "canceled_before_commit"
		audit.ErrorType = classifyAuditError(ctxErr)
		_ = r.Store.Audit(auditCtx, audit)
		return trace, "", ctxErr
	}
	reply := Message{ID: in.ID + ":reply", TenantID: in.TenantID, Channel: in.Channel, UserID: in.UserID, SessionID: in.SessionID, Role: "assistant", Content: out, CreatedAt: time.Now().UTC()}
	event := Event{ID: reply.ID, TenantID: in.TenantID, SessionID: in.SessionID, Type: "assistant.completed", Payload: out, CreatedAt: reply.CreatedAt}
	if committer, ok := r.Store.(AssistantCommitter); ok {
		if err := committer.CommitAssistant(ctx, reply, event); err != nil {
			audit.Decision = "commit_error"
			audit.ErrorType = classifyAuditError(err)
			_ = r.Store.Audit(auditCtx, audit)
			return trace, "", fmt.Errorf("commit assistant: %w", err)
		}
	} else {
		if err := ctx.Err(); err != nil {
			return trace, "", err
		}
		if err := r.Store.AppendMessage(ctx, reply); err != nil {
			return trace, "", fmt.Errorf("append assistant message: %w", err)
		}
		if err := ctx.Err(); err != nil {
			return trace, "", err
		}
		if err := r.Store.AppendEvent(ctx, event); err != nil {
			return trace, "", fmt.Errorf("append assistant event: %w", err)
		}
	}
	if err := r.Store.Audit(auditCtx, audit); err != nil {
		return trace, "", fmt.Errorf("record agent audit: %w", err)
	}
	return trace, out, nil
}
func classifyAuditError(err error) string {
	if err == nil {
		return ""
	}
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, agent.ErrDrainTimeout):
		return "drain_timeout"
	case errors.Is(err, agent.ErrProviderFailure):
		return "provider"
	case errors.Is(err, agent.ErrToolFailure):
		return "tool"
	case errors.Is(err, agent.ErrFrameworkFailure):
		return "framework"
	default:
		return "agent"
	}
}

func traceID(m Message) string {
	h := sha256.Sum256([]byte(m.TenantID + "|" + m.Channel + "|" + m.ID))
	return hex.EncodeToString(h[:8])
}
