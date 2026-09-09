// Package governance contains task-level policy enforcement shared by the
// Gateway, Worker and Agent runtime.
package governance

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/control"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

var (
	ErrActorForbidden         = errors.New("actor is not authorized for tenant")
	ErrToolForbidden          = errors.New("tool is not allowed by tenant policy")
	ErrDangerousConfirmation  = errors.New("dangerous tool confirmation is required")
	ErrConfirmationExpired    = errors.New("dangerous tool confirmation expired")
	ErrPolicyUnavailable      = errors.New("tenant policy is unavailable")
	ErrPolicyMissing          = errors.New("tenant policy is missing")
	ErrInvalidConfirmationCmd = errors.New("invalid confirmation command")
)

type PolicySnapshot struct {
	Policy    control.TenantPolicy
	LoadedAt  time.Time
	ExpiresAt time.Time
}

type PolicyCache struct {
	repository control.Repository
	ttl        time.Duration
	clock      func() time.Time
	mu         sync.RWMutex
	entries    map[string]PolicySnapshot
	inflight   map[string]*policyRefresh
}

type policyRefresh struct {
	done chan struct{}
	err  error
}

const policyRefreshTimeout = 2 * time.Second

func NewPolicyCache(repository control.Repository, ttl time.Duration) (*PolicyCache, error) {
	if repository == nil {
		return nil, errors.New("control repository is required")
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &PolicyCache{
		repository: repository, ttl: ttl, clock: time.Now,
		entries: make(map[string]PolicySnapshot), inflight: make(map[string]*policyRefresh),
	}, nil
}

func (c *PolicyCache) Warm(ctx context.Context, tenantIDs []string) error {
	for _, tenantID := range tenantIDs {
		if _, err := c.Snapshot(ctx, tenantID); err != nil {
			return err
		}
	}
	return nil
}

func (c *PolicyCache) Snapshot(ctx context.Context, tenantID string) (PolicySnapshot, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	now := c.clock().UTC()
	c.mu.Lock()
	entry, ok := c.entries[tenantID]
	if ok && now.Before(entry.ExpiresAt) {
		if _, refreshing := c.inflight[tenantID]; !refreshing {
			refresh := &policyRefresh{done: make(chan struct{})}
			c.inflight[tenantID] = refresh
			go c.refreshInBackground(tenantID, refresh)
		}
		c.mu.Unlock()
		return cloneSnapshot(entry), nil
	}
	refresh, refreshing := c.inflight[tenantID]
	if !refreshing {
		refresh = &policyRefresh{done: make(chan struct{})}
		c.inflight[tenantID] = refresh
	}
	c.mu.Unlock()
	if !refreshing {
		c.refresh(ctx, tenantID, refresh)
	} else {
		select {
		case <-ctx.Done():
			return PolicySnapshot{}, fmt.Errorf("%w: %s", ErrPolicyUnavailable, tenantID)
		case <-refresh.done:
		}
	}
	return c.snapshotAfterRefresh(tenantID, c.clock().UTC(), refresh.err)
}

func (c *PolicyCache) refreshInBackground(tenantID string, refresh *policyRefresh) {
	ctx, cancel := context.WithTimeout(context.Background(), policyRefreshTimeout)
	defer cancel()
	c.refresh(ctx, tenantID, refresh)
}

func (c *PolicyCache) refresh(ctx context.Context, tenantID string, refresh *policyRefresh) {
	policy, err := c.repository.GetPolicy(ctx, tenantID)
	now := c.clock().UTC()
	c.mu.Lock()
	if err == nil {
		c.entries[tenantID] = PolicySnapshot{Policy: clonePolicy(policy), LoadedAt: now, ExpiresAt: now.Add(c.ttl)}
	} else if errors.Is(err, control.ErrPolicyMissing) {
		delete(c.entries, tenantID)
	}
	refresh.err = err
	delete(c.inflight, tenantID)
	close(refresh.done)
	c.mu.Unlock()
}

func (c *PolicyCache) snapshotAfterRefresh(tenantID string, now time.Time, refreshErr error) (PolicySnapshot, error) {
	c.mu.RLock()
	entry, ok := c.entries[tenantID]
	c.mu.RUnlock()
	if refreshErr == nil && ok {
		return cloneSnapshot(entry), nil
	}
	if errors.Is(refreshErr, control.ErrPolicyMissing) {
		return PolicySnapshot{}, fmt.Errorf("%w: %s", ErrPolicyMissing, tenantID)
	}
	if ok && now.Before(entry.LoadedAt.Add(c.ttl)) {
		return cloneSnapshot(entry), nil
	}
	return PolicySnapshot{}, fmt.Errorf("%w: %s", ErrPolicyUnavailable, tenantID)
}

func (c *PolicyCache) Invalidate(tenantID string) {
	c.mu.Lock()
	delete(c.entries, tenantID)
	c.mu.Unlock()
}

func cloneSnapshot(in PolicySnapshot) PolicySnapshot {
	in.Policy = clonePolicy(in.Policy)
	in.Policy.ActorAllowlistHashes = append([]string(nil), in.Policy.ActorAllowlistHashes...)
	in.Policy.ToolAllowlist = append([]string(nil), in.Policy.ToolAllowlist...)
	in.Policy.DangerousTools = append([]string(nil), in.Policy.DangerousTools...)
	in.Policy.RedactionPatterns = append([]string(nil), in.Policy.RedactionPatterns...)
	return in
}

func clonePolicy(in control.TenantPolicy) control.TenantPolicy {
	in.ActorAllowlistHashes = append([]string(nil), in.ActorAllowlistHashes...)
	in.ToolAllowlist = append([]string(nil), in.ToolAllowlist...)
	in.DangerousTools = append([]string(nil), in.DangerousTools...)
	in.RedactionPatterns = append([]string(nil), in.RedactionPatterns...)
	return in
}

func ActorHash(secret []byte, actor string) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(actor))
	return hex.EncodeToString(mac.Sum(nil))
}

func AuthorizeActor(policy control.TenantPolicy, identitySecret []byte, actor string) error {
	if len(policy.ActorAllowlistHashes) == 0 {
		return nil
	}
	digest := ActorHash(identitySecret, actor)
	for _, allowed := range policy.ActorAllowlistHashes {
		if hmac.Equal([]byte(strings.ToLower(allowed)), []byte(digest)) {
			return nil
		}
	}
	return ErrActorForbidden
}

func ToolAllowed(policy control.TenantPolicy, toolName string) error {
	for _, allowed := range policy.ToolAllowlist {
		if allowed == toolName {
			return nil
		}
	}
	return ErrToolForbidden
}

func IsDangerous(policy control.TenantPolicy, toolName string) bool {
	for _, dangerous := range policy.DangerousTools {
		if dangerous == toolName {
			return true
		}
	}
	return false
}

type ConfirmationManager struct {
	repository     control.Repository
	ttl            time.Duration
	identitySecret []byte
	mu             sync.Mutex
	pending        map[string]control.Confirmation
}

type confirmationMatcher interface {
	ConsumeConfirmationMatch(context.Context, string, string, string, string, string) error
}

func NewConfirmationManager(repository control.Repository, ttl time.Duration, identitySecret ...[]byte) (*ConfirmationManager, error) {
	if repository == nil {
		return nil, errors.New("control repository is required")
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	var secret []byte
	if len(identitySecret) > 0 {
		secret = append([]byte(nil), identitySecret[0]...)
	}
	return &ConfirmationManager{repository: repository, ttl: ttl, identitySecret: secret, pending: make(map[string]control.Confirmation)}, nil
}

func (m *ConfirmationManager) Request(ctx context.Context, tenantID, actor, sessionID, toolName, argsDigest string) (control.Confirmation, error) {
	nonceBytes := make([]byte, 16)
	if _, err := rand.Read(nonceBytes); err != nil {
		return control.Confirmation{}, err
	}
	nonce := hex.EncodeToString(nonceBytes)
	confirmation := control.Confirmation{TenantID: tenantID, ActorUserIDHash: ActorHash(m.identitySecret, actor), SessionID: sessionID, ToolName: toolName, ArgsDigest: argsDigest, Nonce: nonce, State: "requested", ExpiresAt: time.Now().UTC().Add(m.ttl)}
	if err := m.repository.CreateConfirmation(ctx, confirmation, m.ttl); err != nil {
		return control.Confirmation{}, err
	}
	m.mu.Lock()
	m.pending[confirmationKey(confirmation.TenantID, confirmation.ActorUserIDHash, confirmation.SessionID, confirmation.ToolName, confirmation.ArgsDigest)] = confirmation
	m.mu.Unlock()
	return confirmation, nil
}

func (m *ConfirmationManager) RequestHashed(ctx context.Context, tenantID, actorHash, sessionID, toolName, argsDigest string) (control.Confirmation, error) {
	nonceBytes := make([]byte, 16)
	if _, err := rand.Read(nonceBytes); err != nil {
		return control.Confirmation{}, err
	}
	confirmation := control.Confirmation{TenantID: tenantID, ActorUserIDHash: actorHash, SessionID: sessionID, ToolName: toolName, ArgsDigest: argsDigest, Nonce: hex.EncodeToString(nonceBytes), State: "requested", ExpiresAt: time.Now().UTC().Add(m.ttl)}
	if err := m.repository.CreateConfirmation(ctx, confirmation, m.ttl); err != nil {
		return control.Confirmation{}, err
	}
	m.mu.Lock()
	m.pending[confirmationKey(confirmation.TenantID, confirmation.ActorUserIDHash, confirmation.SessionID, confirmation.ToolName, confirmation.ArgsDigest)] = confirmation
	m.mu.Unlock()
	return confirmation, nil
}

func (m *ConfirmationManager) Approve(ctx context.Context, tenantID, actorHash, sessionID, nonce string) (control.Confirmation, error) {
	confirmation, err := m.repository.ApproveConfirmation(ctx, tenantID, actorHash, sessionID, nonce)
	if err == nil {
		m.mu.Lock()
		m.pending[confirmationKey(confirmation.TenantID, confirmation.ActorUserIDHash, confirmation.SessionID, confirmation.ToolName, confirmation.ArgsDigest)] = confirmation
		m.mu.Unlock()
	}
	return confirmation, err
}

func (m *ConfirmationManager) Consume(ctx context.Context, confirmation control.Confirmation) error {
	err := m.repository.ConsumeConfirmation(ctx, confirmation)
	if err == nil {
		m.mu.Lock()
		delete(m.pending, confirmationKey(confirmation.TenantID, confirmation.ActorUserIDHash, confirmation.SessionID, confirmation.ToolName, confirmation.ArgsDigest))
		m.mu.Unlock()
	}
	return err
}

func (m *ConfirmationManager) ConsumeMatching(ctx context.Context, tenantID, actorHash, sessionID, toolName, argsDigest string) error {
	key := confirmationKey(tenantID, actorHash, sessionID, toolName, argsDigest)
	m.mu.Lock()
	known, knownLocally := m.pending[key]
	if knownLocally && !time.Now().UTC().Before(known.ExpiresAt) {
		delete(m.pending, key)
	}
	m.mu.Unlock()
	if knownLocally && !time.Now().UTC().Before(known.ExpiresAt) {
		return ErrConfirmationExpired
	}
	if matcher, ok := m.repository.(confirmationMatcher); ok {
		return matcher.ConsumeConfirmationMatch(ctx, tenantID, actorHash, sessionID, toolName, argsDigest)
	}
	m.mu.Lock()
	confirmation, ok := m.pending[key]
	m.mu.Unlock()
	if !ok {
		return control.ErrConfirmationNotFound
	}
	return m.Consume(ctx, confirmation)
}

func confirmationKey(tenantID, actorHash, sessionID, toolName, argsDigest string) string {
	return tenantID + "\x00" + actorHash + "\x00" + sessionID + "\x00" + toolName + "\x00" + argsDigest
}

func ParseConfirmationCommand(text string) (string, bool) {
	text = strings.TrimSpace(text)
	if !strings.HasPrefix(text, "confirm ") {
		return "", false
	}
	nonce := strings.TrimSpace(strings.TrimPrefix(text, "confirm "))
	if nonce == "" || strings.ContainsAny(nonce, " \t\r\n") {
		return "", false
	}
	return nonce, true
}

type TaskAuthorizer interface {
	AuthorizeTask(context.Context, message.ExecutionTask) error
}

type contextKey int

const (
	policyContextKey contextKey = iota
	confirmationContextKey
	actorHashContextKey
	sessionContextKey
	taskAuditContextKey
	executionErrorContextKey
)

type AuditSink interface {
	Emit(context.Context, control.AuditRecord)
}

type taskAuditContext struct {
	sink          AuditSink
	task          message.ExecutionTask
	actorHash     string
	sessionIDHash string
	sequence      atomic.Int64
}

type executionErrorState struct {
	mu  sync.Mutex
	err error
}

// WithExecutionErrorCapture preserves callback-level governance errors that
// the framework otherwise projects as a generic agent error event.
func WithExecutionErrorCapture(ctx context.Context) (context.Context, func() error) {
	state := &executionErrorState{}
	return context.WithValue(ctx, executionErrorContextKey, state), func() error {
		state.mu.Lock()
		defer state.mu.Unlock()
		return state.err
	}
}

func captureExecutionError(ctx context.Context, err error) {
	if ctx == nil || err == nil {
		return
	}
	state, _ := ctx.Value(executionErrorContextKey).(*executionErrorState)
	if state == nil {
		return
	}
	state.mu.Lock()
	if state.err == nil {
		state.err = err
	}
	state.mu.Unlock()
}

func WithPolicy(ctx context.Context, snapshot PolicySnapshot) context.Context {
	return context.WithValue(ctx, policyContextKey, cloneSnapshot(snapshot))
}

func PolicyFromContext(ctx context.Context) (PolicySnapshot, bool) {
	if ctx == nil {
		return PolicySnapshot{}, false
	}
	value, ok := ctx.Value(policyContextKey).(PolicySnapshot)
	return cloneSnapshot(value), ok
}

func WithConfirmationManager(ctx context.Context, manager *ConfirmationManager, actorHash string, sessionID ...string) context.Context {
	ctx = context.WithValue(ctx, confirmationContextKey, manager)
	ctx = context.WithValue(ctx, actorHashContextKey, actorHash)
	if len(sessionID) > 0 {
		ctx = context.WithValue(ctx, sessionContextKey, sessionID[0])
	}
	return ctx
}

func ConfirmationFromContext(ctx context.Context) (*ConfirmationManager, string, string, bool) {
	if ctx == nil {
		return nil, "", "", false
	}
	manager, managerOK := ctx.Value(confirmationContextKey).(*ConfirmationManager)
	actorHash, hashOK := ctx.Value(actorHashContextKey).(string)
	sessionID, _ := ctx.Value(sessionContextKey).(string)
	return manager, actorHash, sessionID, managerOK && hashOK
}

func WithTaskAudit(ctx context.Context, sink AuditSink, task message.ExecutionTask, identitySecret []byte) context.Context {
	if sink == nil {
		return ctx
	}
	value := &taskAuditContext{
		sink: sink, task: task,
		actorHash: ActorHash(identitySecret, task.ActorUserID), sessionIDHash: ActorHash(identitySecret, task.SessionID),
	}
	return context.WithValue(ctx, taskAuditContextKey, value)
}

func emitTaskAudit(ctx context.Context, eventType, decision, errorType, toolName string) {
	if ctx == nil {
		return
	}
	value, ok := ctx.Value(taskAuditContextKey).(*taskAuditContext)
	if !ok || value == nil || value.sink == nil {
		return
	}
	sequence := int(value.sequence.Add(1) - 1)
	value.sink.Emit(ctx, control.AuditRecord{
		ID: value.task.TaskID + "-" + eventType + "-" + strconv.Itoa(sequence), TenantID: value.task.TenantID,
		Channel: value.task.Channel, ActorUserIDHash: value.actorHash, SessionIDHash: value.sessionIDHash,
		AgentAppID: value.task.AgentAppID, ToolName: toolName, EventType: eventType, Decision: decision,
		ErrorType: errorType, TraceID: value.task.TraceID, RequestID: value.task.RequestID, TaskID: value.task.TaskID,
		Sequence: sequence, OccurredAt: time.Now().UTC(),
	})
}

func AuthorizeToolCall(ctx context.Context, toolName string, arguments []byte) (string, error) {
	snapshot, ok := PolicyFromContext(ctx)
	if !ok {
		return "", ErrPolicyUnavailable
	}
	if err := ToolAllowed(snapshot.Policy, toolName); err != nil {
		captureExecutionError(ctx, err)
		telemetry.RecordTool(ctx, "rejected")
		return "", err
	}
	if !IsDangerous(snapshot.Policy, toolName) {
		telemetry.RecordTool(ctx, "allowed")
		return "", nil
	}
	manager, actorHash, sessionID, ok := ConfirmationFromContext(ctx)
	if !ok || manager == nil {
		emitTaskAudit(ctx, "denied", "denied", "confirmation_denied", toolName)
		captureExecutionError(ctx, ErrDangerousConfirmation)
		telemetry.RecordTool(ctx, "confirmation_required")
		return "", ErrDangerousConfirmation
	}
	digest := sha256.Sum256(arguments)
	argsDigest := hex.EncodeToString(digest[:])
	consumeErr := manager.ConsumeMatching(ctx, snapshot.Policy.TenantID, actorHash, sessionID, toolName, argsDigest)
	if consumeErr == nil {
		telemetry.RecordTool(ctx, "succeeded")
		emitTaskAudit(ctx, "allowed", "allowed", "", toolName)
		return "", nil
	}
	if errors.Is(consumeErr, ErrConfirmationExpired) {
		emitTaskAudit(ctx, "expired", "expired", "confirmation_denied", toolName)
	} else if !errors.Is(consumeErr, control.ErrConfirmationNotFound) {
		emitTaskAudit(ctx, "denied", "denied", "confirmation_denied", toolName)
		return "", consumeErr
	}
	confirmation, err := manager.RequestHashed(ctx, snapshot.Policy.TenantID, actorHash, sessionID, toolName, argsDigest)
	if err != nil {
		emitTaskAudit(ctx, "denied", "denied", "confirmation_denied", toolName)
		return "", err
	}
	emitTaskAudit(ctx, "confirmation_required", "confirmation_required", "", toolName)
	telemetry.RecordTool(ctx, "confirmation_required")
	return confirmation.Nonce, nil
}

var _ TaskAuthorizer = (*PolicyEnforcer)(nil)

type PolicyEnforcer struct {
	cache          *PolicyCache
	identitySecret []byte
	repository     control.Repository
	auditSink      AuditSink
}

func NewPolicyEnforcer(cache *PolicyCache, identitySecret []byte, repository ...control.Repository) *PolicyEnforcer {
	var repo control.Repository
	if len(repository) > 0 {
		repo = repository[0]
	}
	return &PolicyEnforcer{cache: cache, identitySecret: append([]byte(nil), identitySecret...), repository: repo}
}

func (e *PolicyEnforcer) SetAuditSink(sink AuditSink) {
	if e != nil {
		e.auditSink = sink
	}
}

func (e *PolicyEnforcer) AuthorizeTask(ctx context.Context, task message.ExecutionTask) error {
	return e.authorizeTask(ctx, task, "", true)
}

// ReauthorizeTask repeats the policy check at the Worker execution boundary.
// The Gateway owns the successful ingress audit; Worker failures use distinct
// event types so a policy change or outage remains visible without duplicating
// the successful authorization record.
func (e *PolicyEnforcer) ReauthorizeTask(ctx context.Context, task message.ExecutionTask) error {
	return e.authorizeTask(ctx, task, "worker_", false)
}

func (e *PolicyEnforcer) authorizeTask(ctx context.Context, task message.ExecutionTask, eventPrefix string, auditAllowed bool) error {
	if e == nil || e.cache == nil {
		return nil
	}
	snapshot, err := e.cache.Snapshot(ctx, task.TenantID)
	if err != nil {
		if e.repository != nil {
			_ = e.repository.SetTenantDegraded(ctx, task.TenantID, policyErrorReason(err))
		}
		e.emitAuthorization(ctx, task, eventPrefix+"policy_authorization", "denied", policyErrorReason(err))
		return err
	}
	decision := "allowed"
	authErr := AuthorizeActor(snapshot.Policy, e.identitySecret, task.ActorUserID)
	if authErr != nil {
		decision = "denied"
	}
	if e.repository != nil {
		_ = e.repository.ClearTenantDegraded(ctx, task.TenantID)
	}
	if auditAllowed || authErr != nil {
		e.emitAuthorization(ctx, task, eventPrefix+"actor_authorization", decision, errorType(authErr))
	}
	return authErr
}

func (e *PolicyEnforcer) emitAuthorization(ctx context.Context, task message.ExecutionTask, eventType, decision, errorType string) {
	if e == nil || e.auditSink == nil {
		return
	}
	e.auditSink.Emit(ctx, control.AuditRecord{
		ID: task.TaskID + "-" + eventType + "-0", TenantID: task.TenantID, Channel: task.Channel,
		ActorUserIDHash: ActorHash(e.identitySecret, task.ActorUserID), SessionIDHash: ActorHash(e.identitySecret, task.SessionID),
		AgentAppID: task.AgentAppID, EventType: eventType, Decision: decision, ErrorType: errorType,
		TraceID: task.TraceID, RequestID: task.RequestID, TaskID: task.TaskID, Sequence: 0, OccurredAt: time.Now().UTC(),
	})
}

func policyErrorReason(err error) string {
	if errors.Is(err, ErrPolicyMissing) {
		return "tenant_policy_missing"
	}
	return "tenant_policy_unavailable"
}
func errorType(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, ErrActorForbidden) {
		return "actor_forbidden"
	}
	return "unknown"
}
