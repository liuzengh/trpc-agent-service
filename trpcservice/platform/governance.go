package platform

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type TenantPolicy struct {
	TenantID                 string             `json:"tenant_id"`
	AgentAppID               string             `json:"agent_app_id"`
	Revision                 uint64             `json:"revision"`
	AllowedTools             []string           `json:"allowed_tools"`
	AllowedMCP               []string           `json:"allowed_mcp"`
	DangerousTools           []string           `json:"dangerous_tools"`
	DeniedInputPatterns      []string           `json:"denied_input_patterns"`
	DeniedOutputPatterns     []string           `json:"denied_output_patterns"`
	RedactedPatterns         []string           `json:"redacted_patterns"`
	AllowedIMUsers           []string           `json:"allowed_im_users"`
	AllowedIMSubjects        []string           `json:"allowed_im_subjects"`
	AllowedProviderAccounts  []string           `json:"allowed_provider_accounts"`
	AllowedConversationTypes []string           `json:"allowed_conversation_types"`
	TokenBudget              int64              `json:"token_budget"`
	CostBudget               float64            `json:"cost_budget"`
	CostPerToken             float64            `json:"cost_per_token"`
	ToolCosts                map[string]float64 `json:"tool_costs"`
	EstimatedTokensPerRun    int64              `json:"estimated_tokens_per_run"`
	RateLimit                int                `json:"rate_limit"`
	RateWindowSeconds        int                `json:"rate_window_seconds"`
	RuntimeTimeoutMS         int                `json:"runtime_timeout_ms"`
	UpdatedAt                time.Time          `json:"updated_at"`
}

type GovernanceRequest struct {
	TenantID         string
	AgentAppID       string
	UserID           string
	SessionID        string
	RequestID        string
	Channel          string
	ProviderAccount  string
	ConversationType string
	ExternalSubject  string
	Input            string
	RequiredTools    []string
	RequiredMCP      []string
	PolicyRevision   uint64
}

type GovernanceResult struct {
	Input           string `json:"-"`
	TraceID         string `json:"trace_id"`
	PolicyRevision  uint64 `json:"policy_revision"`
	ConfirmationID  string `json:"confirmation_id,omitempty"`
	NewExecution    bool   `json:"-"`
	ExecutionActive bool   `json:"-"`
}

type GovernanceCompletion struct {
	TenantID        string
	AgentAppID      string
	RequestID       string
	UserID          string
	SessionID       string
	Channel         string
	ExternalSubject string
	Output          string
	Tokens          int64
	ErrorType       string
	NoUsage         bool
	Cancelled       bool
}

type GovernanceError struct {
	Code           string
	TraceID        string
	ConfirmationID string
}

func (e *GovernanceError) Error() string { return e.Code }

func IsGovernanceError(err error, code string) bool {
	var target *GovernanceError
	return errors.As(err, &target) && target.Code == code
}

type ConfirmationStatus string

const (
	ConfirmationPending        ConfirmationStatus = "pending"
	ConfirmationApproved       ConfirmationStatus = "approved"
	ConfirmationRejected       ConfirmationStatus = "rejected"
	ConfirmationExpired        ConfirmationStatus = "expired"
	ConfirmationExecuting      ConfirmationStatus = "executing"
	ConfirmationRunning        ConfirmationStatus = ConfirmationExecuting
	ConfirmationCompleted      ConfirmationStatus = "completed"
	ConfirmationFailed         ConfirmationStatus = "failed"
	ConfirmationOutcomeUnknown ConfirmationStatus = "outcome_unknown"
	ConfirmationCancelled      ConfirmationStatus = ConfirmationOutcomeUnknown
)

type ToolConfirmation struct {
	ID              string             `json:"id"`
	TenantID        string             `json:"tenant_id"`
	AgentAppID      string             `json:"agent_app_id"`
	SessionID       string             `json:"session_id"`
	RequestID       string             `json:"request_id"`
	UserID          string             `json:"user_id"`
	ToolName        string             `json:"tool_name"`
	ArgumentSummary string             `json:"argument_summary"`
	PolicyRevision  uint64             `json:"policy_revision"`
	TraceID         string             `json:"trace_id"`
	Status          ConfirmationStatus `json:"status"`
	CreatedAt       time.Time          `json:"created_at"`
	ExpiresAt       time.Time          `json:"expires_at"`
	DecidedAt       time.Time          `json:"decided_at,omitempty"`
	DecidedBy       string             `json:"decided_by,omitempty"`
	InvokedAt       time.Time          `json:"invoked_at,omitempty"`
	CompletedAt     time.Time          `json:"completed_at,omitempty"`
}

type AuditQuery struct {
	TenantID  string
	Channel   string
	AgentName string
	Decision  string
	ErrorType string
	UserID    string
	SessionID string
	RequestID string
	TraceID   string
	From      time.Time
	To        time.Time
	Offset    int
	Limit     int
}

type TenantMetrics struct {
	TenantID           string    `json:"tenant_id"`
	Requests           int64     `json:"requests"`
	Active             int64     `json:"active_executions"`
	Completed          int64     `json:"completed_executions"`
	Failed             int64     `json:"failed_executions"`
	Denied             int64     `json:"denied_requests"`
	RateLimited        int64     `json:"rate_limited_requests"`
	Tokens             int64     `json:"tokens"`
	Cost               float64   `json:"cost"`
	ModelLatencyMS     int64     `json:"model_latency_ms"`
	ExecutionLatencyMS int64     `json:"execution_latency_ms"`
	ToolLatencyMS      int64     `json:"tool_latency_ms"`
	StorageLatencyMS   int64     `json:"storage_latency_ms"`
	IMDelivered        int64     `json:"im_delivered"`
	IMFailed           int64     `json:"im_failed"`
	TokenBudget        int64     `json:"token_budget"`
	TokensRemaining    int64     `json:"tokens_remaining"`
	CostBudget         float64   `json:"cost_budget"`
	CostRemaining      float64   `json:"cost_remaining"`
	BudgetPeriodFrom   time.Time `json:"budget_period_from,omitempty"`
}

type MetricsQuery struct {
	TenantID   string
	AgentAppID string
	Provider   string
	From       time.Time
	To         time.Time
}

type MetricSample struct {
	TenantID   string        `json:"tenant_id"`
	AgentAppID string        `json:"agent_app_id,omitempty"`
	Provider   string        `json:"provider,omitempty"`
	OccurredAt time.Time     `json:"occurred_at"`
	Delta      TenantMetrics `json:"delta"`
}

type TraceSpan struct {
	SpanID       string    `json:"span_id"`
	ParentSpanID string    `json:"parent_span_id,omitempty"`
	Name         string    `json:"name"`
	Status       string    `json:"status"`
	OccurredAt   time.Time `json:"occurred_at"`
}

type PlatformTrace struct {
	TraceID    string      `json:"trace_id"`
	TenantID   string      `json:"tenant_id"`
	RequestID  string      `json:"request_id"`
	SessionID  string      `json:"session_id"`
	AgentAppID string      `json:"agent_app_id"`
	Spans      []TraceSpan `json:"spans"`
}

type governanceExecution struct {
	traceID         string
	appID           string
	sessionID       string
	requestID       string
	userID          string
	channel         string
	externalSubject string
	provider        string
	reserved        int64
	reservedCost    float64
	costPerToken    float64
	policyRevision  uint64
	toolCosts       map[string]float64
	toolCost        float64
	started         time.Time
	active          bool
	expired         bool
}

type persistedGovernanceExecution struct {
	TraceID         string             `json:"trace_id"`
	AppID           string             `json:"app_id"`
	SessionID       string             `json:"session_id"`
	RequestID       string             `json:"request_id"`
	UserID          string             `json:"user_id,omitempty"`
	Channel         string             `json:"channel,omitempty"`
	ExternalSubject string             `json:"external_subject,omitempty"`
	Provider        string             `json:"provider,omitempty"`
	Reserved        int64              `json:"reserved"`
	ReservedCost    float64            `json:"reserved_cost"`
	CostPerToken    float64            `json:"cost_per_token"`
	PolicyRevision  uint64             `json:"policy_revision"`
	ToolCosts       map[string]float64 `json:"tool_costs,omitempty"`
	ToolCost        float64            `json:"tool_cost"`
	Started         time.Time          `json:"started"`
	Active          bool               `json:"active"`
	Expired         bool               `json:"expired,omitempty"`
}

type rateWindow struct {
	started time.Time
	count   int
}

// GovernanceCenter is the server-owned policy, audit, accounting, and trace
// boundary. It is deterministic and process-local by default so adapters can
// replace its persistence without changing HTTP or runtime contracts.
type GovernanceCenter struct {
	auditStore    AuditStore
	mu            sync.Mutex
	policies      map[string]TenantPolicy
	audits        []AuditEvent
	confirmations map[string]ToolConfirmation
	executions    map[string]governanceExecution
	metrics       map[string]TenantMetrics
	rateWindows   map[string]rateWindow
	traces        map[string]PlatformTrace
	usedTokens    map[string]int64
	toolStarts    map[string]time.Time
	metricSamples []MetricSample
	auditPolicies map[string]AuditPolicy
	now           func() time.Time
	path          string
	policyStore   interface {
		loadGovernancePolicies(context.Context) (map[string]TenantPolicy, error)
		saveGovernancePolicy(context.Context, TenantPolicy) (TenantPolicy, error)
	}
}

type governanceSnapshot struct {
	Policies                  map[string]TenantPolicy                 `json:"policies"`
	EncryptedRedactedPatterns map[string]string                       `json:"encrypted_redacted_patterns,omitempty"`
	Audits                    []AuditEvent                            `json:"audits"`
	Confirmations             map[string]ToolConfirmation             `json:"confirmations"`
	Executions                map[string]persistedGovernanceExecution `json:"executions,omitempty"`
	ToolStarts                map[string]time.Time                    `json:"tool_starts,omitempty"`
	Metrics                   map[string]TenantMetrics                `json:"metrics"`
	UsedTokens                map[string]int64                        `json:"used_tokens"`
	Traces                    map[string]PlatformTrace                `json:"traces"`
	MetricSamples             []MetricSample                          `json:"metric_samples,omitempty"`
	AuditPolicies             map[string]AuditPolicy                  `json:"audit_policies,omitempty"`
}

type governanceState struct {
	policies      map[string]TenantPolicy
	audits        []AuditEvent
	confirmations map[string]ToolConfirmation
	executions    map[string]governanceExecution
	metrics       map[string]TenantMetrics
	rateWindows   map[string]rateWindow
	traces        map[string]PlatformTrace
	usedTokens    map[string]int64
	toolStarts    map[string]time.Time
	metricSamples []MetricSample
	auditPolicies map[string]AuditPolicy
}

func NewGovernanceCenter() *GovernanceCenter {
	return &GovernanceCenter{
		policies: map[string]TenantPolicy{}, confirmations: map[string]ToolConfirmation{}, executions: map[string]governanceExecution{},
		metrics: map[string]TenantMetrics{}, rateWindows: map[string]rateWindow{}, traces: map[string]PlatformTrace{}, usedTokens: map[string]int64{}, toolStarts: map[string]time.Time{}, auditPolicies: map[string]AuditPolicy{}, now: time.Now,
	}
}

func (g *GovernanceCenter) configurePolicyStore(store interface {
	loadGovernancePolicies(context.Context) (map[string]TenantPolicy, error)
	saveGovernancePolicy(context.Context, TenantPolicy) (TenantPolicy, error)
}) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.policyStore = store
	shared, err := g.policyStore.loadGovernancePolicies(context.Background())
	if err != nil {
		return
	}
	if len(shared) == 0 && len(g.policies) > 0 {
		keys := make([]string, 0, len(g.policies))
		for key := range g.policies {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		imported := make(map[string]TenantPolicy, len(keys))
		for _, key := range keys {
			policy, saveErr := g.policyStore.saveGovernancePolicy(context.Background(), g.policies[key])
			if saveErr != nil {
				return
			}
			imported[key] = clonePolicy(policy)
		}
		g.policies = imported
		return
	}
	g.policies = shared
}

func (g *GovernanceCenter) refreshPoliciesLocked(ctx context.Context) error {
	if g.policyStore == nil {
		return nil
	}
	policies, err := g.policyStore.loadGovernancePolicies(ctx)
	if err != nil {
		return err
	}
	g.policies = policies
	return nil
}

func NewPersistentGovernanceCenter(path string) (*GovernanceCenter, error) {
	center := NewGovernanceCenter()
	center.path = path
	if path == "" {
		return center, nil
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return center, nil
	}
	if err != nil {
		return nil, err
	}
	var snapshot governanceSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, err
	}
	if snapshot.Policies != nil {
		center.policies = snapshot.Policies
	}
	if len(snapshot.EncryptedRedactedPatterns) > 0 {
		key, err := loadGovernanceEncryptionKey(path, false)
		if err != nil {
			return nil, err
		}
		for policyKey, encrypted := range snapshot.EncryptedRedactedPatterns {
			patterns, err := decryptRedactionPatterns(key, encrypted)
			if err != nil {
				return nil, err
			}
			policy := center.policies[policyKey]
			policy.RedactedPatterns = patterns
			center.policies[policyKey] = policy
		}
	}
	if snapshot.Audits != nil {
		center.audits = snapshot.Audits
	}
	if snapshot.Confirmations != nil {
		center.confirmations = snapshot.Confirmations
		for id, confirmation := range center.confirmations {
			if confirmation.Status == ConfirmationExecuting {
				confirmation.Status = ConfirmationOutcomeUnknown
				confirmation.CompletedAt = time.Now().UTC()
				center.confirmations[id] = confirmation
			}
		}
	}
	for key, execution := range snapshot.Executions {
		center.executions[key] = governanceExecution{
			traceID: execution.TraceID, appID: execution.AppID, sessionID: execution.SessionID, requestID: execution.RequestID,
			userID: execution.UserID, channel: execution.Channel, externalSubject: execution.ExternalSubject,
			provider: execution.Provider, reserved: execution.Reserved, reservedCost: execution.ReservedCost,
			costPerToken: execution.CostPerToken, policyRevision: execution.PolicyRevision, toolCosts: cloneToolCosts(execution.ToolCosts), toolCost: execution.ToolCost,
			started: execution.Started, active: execution.Active, expired: execution.Expired,
		}
	}
	if snapshot.ToolStarts != nil {
		center.toolStarts = snapshot.ToolStarts
	}
	if snapshot.Metrics != nil {
		center.metrics = snapshot.Metrics
	}
	if snapshot.UsedTokens != nil {
		center.usedTokens = snapshot.UsedTokens
	}
	if snapshot.Traces != nil {
		center.traces = snapshot.Traces
	}
	if snapshot.MetricSamples != nil {
		center.metricSamples = snapshot.MetricSamples
	}
	if snapshot.AuditPolicies != nil {
		center.auditPolicies = make(map[string]AuditPolicy, len(snapshot.AuditPolicies))
		for tenantID, policy := range snapshot.AuditPolicies {
			center.auditPolicies[tenantID] = normalizedAuditPolicy(policy)
		}
	}
	return center, nil
}

// SetAuditPolicy updates the non-secret policy used by audit retention and
// content shaping. Validation keeps high-risk operations fail-closed.
func (g *GovernanceCenter) SetAuditPolicy(tenantID string, policy AuditPolicy) error {
	if strings.TrimSpace(tenantID) == "" {
		return errors.New("invalid_tenant_id")
	}
	normalized, err := policy.Normalize()
	if err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.auditPolicies == nil {
		g.auditPolicies = make(map[string]AuditPolicy)
	}
	previous, existed := g.auditPolicies[tenantID]
	g.auditPolicies[tenantID] = normalized
	if err := g.persistLocked(); err != nil {
		if existed {
			g.auditPolicies[tenantID] = previous
		} else {
			delete(g.auditPolicies, tenantID)
		}
		return err
	}
	return nil
}

func (g *GovernanceCenter) auditPolicyLocked(tenantID string) AuditPolicy {
	if policy, ok := g.auditPolicies[tenantID]; ok {
		return normalizedAuditPolicy(policy)
	}
	return DefaultAuditPolicy()
}

func governanceKey(tenantID, appID string) string    { return tenantID + "\x00" + appID }
func executionKey(tenantID, requestID string) string { return tenantID + "\x00" + requestID }

const governanceCleanupTimeout = 2 * time.Second
const defaultRuntimeTimeout = 30 * time.Second
const maxRuntimeTimeoutMS = 300000

func completeGovernance(ctx context.Context, center *GovernanceCenter, completion GovernanceCompletion) (string, error) {
	if center == nil {
		return "", nil
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), governanceCleanupTimeout)
	defer cancel()
	return center.Complete(cleanupCtx, completion)
}

func (g *GovernanceCenter) PutPolicy(ctx context.Context, policy TenantPolicy) (TenantPolicy, error) {
	if err := ctx.Err(); err != nil {
		return TenantPolicy{}, err
	}
	if policy.TenantID == "" || policy.AgentAppID == "" || policy.TokenBudget < 0 || policy.CostBudget < 0 || policy.CostPerToken < 0 || policy.EstimatedTokensPerRun < 0 || policy.RateLimit < 0 || policy.RateWindowSeconds < 0 || policy.RuntimeTimeoutMS < 0 || policy.RuntimeTimeoutMS > maxRuntimeTimeoutMS {
		return TenantPolicy{}, errors.New("invalid_policy")
	}
	normalizedToolCosts := make(map[string]float64, len(policy.ToolCosts))
	for toolName, cost := range policy.ToolCosts {
		toolName = strings.TrimSpace(toolName)
		if toolName == "" || cost < 0 {
			return TenantPolicy{}, errors.New("invalid_policy")
		}
		normalizedToolCosts[toolName] = cost
	}
	policy.ToolCosts = normalizedToolCosts
	policy.AllowedTools = normalizedValues(policy.AllowedTools)
	policy.AllowedMCP = normalizedValues(policy.AllowedMCP)
	policy.DangerousTools = normalizedValues(policy.DangerousTools)
	policy.AllowedIMUsers = normalizedValues(policy.AllowedIMUsers)
	policy.AllowedIMSubjects = normalizedValues(policy.AllowedIMSubjects)
	policy.AllowedProviderAccounts = normalizedValues(policy.AllowedProviderAccounts)
	policy.AllowedConversationTypes = normalizedValues(policy.AllowedConversationTypes)
	for _, conversationType := range policy.AllowedConversationTypes {
		if conversationType != ConversationSingle && conversationType != ConversationGroup {
			return TenantPolicy{}, errors.New("invalid_policy")
		}
	}
	policy.DeniedInputPatterns = normalizedPatterns(policy.DeniedInputPatterns)
	policy.DeniedOutputPatterns = normalizedPatterns(policy.DeniedOutputPatterns)
	policy.RedactedPatterns = normalizedPatterns(policy.RedactedPatterns)
	if !subset(policy.DangerousTools, policy.AllowedTools) {
		return TenantPolicy{}, errors.New("invalid_policy")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	checkpoint := g.snapshotLocked()
	if err := g.refreshPoliciesLocked(ctx); err != nil {
		return TenantPolicy{}, err
	}
	if g.policyStore != nil {
		persisted, err := g.policyStore.saveGovernancePolicy(ctx, policy)
		if err != nil {
			return TenantPolicy{}, err
		}
		policy = persisted
	} else {
		previous := g.policies[governanceKey(policy.TenantID, policy.AgentAppID)]
		policy.Revision = previous.Revision + 1
		policy.UpdatedAt = g.now().UTC()
	}
	g.policies[governanceKey(policy.TenantID, policy.AgentAppID)] = clonePolicy(policy)
	metrics := g.metrics[policy.TenantID]
	metrics.TenantID = policy.TenantID
	metrics.TokenBudget, metrics.CostBudget = policy.TokenBudget, policy.CostBudget
	if metrics.BudgetPeriodFrom.IsZero() {
		metrics.BudgetPeriodFrom = policy.UpdatedAt
	}
	g.metrics[policy.TenantID] = metrics
	traceID := newTraceID()
	g.appendAuditLocked(AuditEvent{TenantID: policy.TenantID, AgentName: policy.AgentAppID, Decision: "policy.updated", TraceID: traceID, OccurredAt: policy.UpdatedAt})
	if err := g.persistLocked(); err != nil {
		g.restoreLocked(checkpoint)
		return TenantPolicy{}, err
	}
	return clonePolicy(policy), nil
}

func (p TenantPolicy) runtimeTimeout() time.Duration {
	if p.RuntimeTimeoutMS <= 0 {
		return defaultRuntimeTimeout
	}
	return time.Duration(p.RuntimeTimeoutMS) * time.Millisecond
}

func (g *GovernanceCenter) Policy(ctx context.Context, tenantID, appID string) (TenantPolicy, bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.refreshPoliciesLocked(ctx); err != nil {
		return TenantPolicy{}, false, err
	}
	policy, ok := g.policies[governanceKey(tenantID, appID)]
	return clonePolicy(policy), ok, nil
}

func (g *GovernanceCenter) Evaluate(ctx context.Context, request GovernanceRequest) (GovernanceResult, error) {
	if err := ctx.Err(); err != nil {
		return GovernanceResult{}, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.refreshPoliciesLocked(ctx); err != nil {
		return GovernanceResult{}, &GovernanceError{Code: "control_plane_unavailable"}
	}
	checkpoint := g.snapshotLocked()
	now := g.now().UTC()
	if g.reconcileExpiredReservationsLocked(now) {
		if err := g.persistLocked(); err != nil {
			g.restoreLocked(checkpoint)
			return GovernanceResult{}, &GovernanceError{Code: "audit_unavailable"}
		}
		checkpoint = g.snapshotLocked()
	}
	key := executionKey(request.TenantID, request.RequestID)
	prior, seen := g.executions[key]
	traceID := prior.traceID
	if traceID == "" {
		traceID = confirmationTraceForRequestLocked(g.confirmations, request.TenantID, request.RequestID)
	}
	if traceID == "" {
		traceID = newTraceID()
	}
	result := GovernanceResult{Input: request.Input, TraceID: traceID, ExecutionActive: seen && prior.active}
	policy, configured := g.policies[governanceKey(request.TenantID, request.AgentAppID)]
	result.PolicyRevision = policy.Revision
	if seen && prior.policyRevision != 0 {
		result.PolicyRevision = prior.policyRevision
	}
	request.PolicyRevision = policy.Revision
	if seen && prior.policyRevision != 0 {
		request.PolicyRevision = prior.policyRevision
	}
	g.recordTraceLocked(request, traceID, "gateway.receive", "ok", now)
	if !seen {
		metrics := g.metrics[request.TenantID]
		metrics.TenantID = request.TenantID
		metrics.Requests++
		g.metrics[request.TenantID] = metrics
		g.recordMetricSampleLocked(request.TenantID, request.AgentAppID, request.Channel, now, TenantMetrics{Requests: 1})
	}
	deny := func(code, decision string) (GovernanceResult, error) {
		metrics := g.metrics[request.TenantID]
		metrics.Denied++
		if code == "tenant_rate_limited" {
			metrics.RateLimited++
		}
		g.metrics[request.TenantID] = metrics
		delta := TenantMetrics{Denied: 1}
		if code == "tenant_rate_limited" {
			delta.RateLimited = 1
		}
		g.recordMetricSampleLocked(request.TenantID, request.AgentAppID, request.Channel, now, delta)
		audit := auditForRequest(request, traceID, decision, code, now)
		audit.Checkpoint, audit.Rule, audit.Reason = policyCheckpoint(decision), code, "request rejected by server-owned governance policy"
		g.appendAuditLocked(audit)
		g.recordTraceLocked(request, traceID, "policy.evaluate", "error", now)
		if err := g.persistLocked(); err != nil {
			g.restoreLocked(checkpoint)
			return result, &GovernanceError{Code: "audit_unavailable", TraceID: traceID}
		}
		return result, &GovernanceError{Code: code, TraceID: traceID, ConfirmationID: result.ConfirmationID}
	}
	if !configured {
		return deny("policy_unavailable", "policy.unavailable")
	}
	if seen && prior.expired {
		delete(g.executions, key)
		seen = false
	}
	if !seen {
		result.PolicyRevision = policy.Revision
		request.PolicyRevision = policy.Revision
	}
	if seen && !prior.active {
		result.Input = redact(request.Input, policy.RedactedPatterns)
		return result, nil
	}
	for _, required := range request.RequiredTools {
		if !contains(policy.AllowedTools, required) {
			return deny("tool_not_allowed", "tool.denied")
		}
		g.appendAuditLocked(AuditEvent{TenantID: request.TenantID, Channel: request.Channel, UserID: request.UserID, SessionID: request.SessionID, AgentName: request.AgentAppID, ToolName: required, Decision: "tool.declared.allowed", TraceID: traceID, RequestID: request.RequestID, PolicyRevision: policy.Revision, Checkpoint: "tool.declaration", Rule: "tool_allowlist", OccurredAt: now})
		g.recordTraceLocked(request, traceID, "tool.authorize", "ok", now)
	}
	for _, required := range request.RequiredMCP {
		if !contains(policy.AllowedMCP, required) {
			return deny("mcp_not_allowed", "mcp.denied")
		}
		g.appendAuditLocked(AuditEvent{TenantID: request.TenantID, Channel: request.Channel, UserID: request.UserID, SessionID: request.SessionID, AgentName: request.AgentAppID, ToolName: "mcp:" + required, Decision: "mcp.allowed", TraceID: traceID, RequestID: request.RequestID, PolicyRevision: policy.Revision, Checkpoint: "mcp.declaration", Rule: "mcp_allowlist", OccurredAt: now})
	}
	for _, pattern := range policy.DeniedInputPatterns {
		if strings.Contains(request.Input, pattern) {
			return deny("policy_denied", "guardrail.input.denied")
		}
	}
	if request.Channel != "" {
		if len(policy.AllowedIMUsers) > 0 && !contains(policy.AllowedIMUsers, request.UserID) {
			return deny("im_user_denied", "im_user.denied")
		}
		if len(policy.AllowedIMSubjects) > 0 && !contains(policy.AllowedIMSubjects, request.ExternalSubject) {
			return deny("im_subject_denied", "im_subject.denied")
		}
		if request.Channel == ChannelTelegram || request.Channel == ChannelEnterpriseWeChat {
			if len(policy.AllowedProviderAccounts) > 0 && !contains(policy.AllowedProviderAccounts, request.ProviderAccount) {
				return deny("provider_account_denied", "provider_account.denied")
			}
			if len(policy.AllowedConversationTypes) > 0 && !contains(policy.AllowedConversationTypes, request.ConversationType) {
				return deny("conversation_type_denied", "conversation_type.denied")
			}
		}
	}
	if prior.active {
		result.Input = redact(request.Input, policy.RedactedPatterns)
		result.ConfirmationID = confirmationForRequestLocked(g.confirmations, request.TenantID, request.RequestID)
		return result, nil
	}
	estimatedToolCost := float64(0)
	for _, required := range request.RequiredTools {
		cost, priced := policy.ToolCosts[required]
		if policy.CostBudget > 0 && !priced {
			return deny("tool_pricing_unknown", "budget.denied")
		}
		estimatedToolCost += cost
	}
	windowSeconds := policy.RateWindowSeconds
	if windowSeconds == 0 {
		windowSeconds = 60
	}
	if policy.RateLimit > 0 {
		window := g.rateWindows[request.TenantID]
		if window.started.IsZero() || now.Sub(window.started) >= time.Duration(windowSeconds)*time.Second {
			window = rateWindow{started: now}
		}
		if window.count >= policy.RateLimit {
			return deny("tenant_rate_limited", "rate_limit.denied")
		}
		window.count++
		g.rateWindows[request.TenantID] = window
	}
	reserved := policy.EstimatedTokensPerRun
	if reserved <= 0 {
		reserved = estimateTokens(request.Input)
	}
	used := g.usedTokens[request.TenantID]
	reservedTotal := int64(0)
	reservedCostTotal := float64(0)
	for existingKey, execution := range g.executions {
		if strings.HasPrefix(existingKey, request.TenantID+"\x00") && execution.active {
			reservedTotal += execution.reserved
			reservedCostTotal += execution.reservedCost
		}
	}
	if policy.TokenBudget > 0 && used+reservedTotal+reserved > policy.TokenBudget {
		return deny("budget_exceeded", "budget.denied")
	}
	reservedCost := float64(reserved)*policy.CostPerToken + estimatedToolCost
	if policy.CostBudget > 0 && g.metrics[request.TenantID].Cost+reservedCostTotal+reservedCost > policy.CostBudget {
		return deny("budget_exceeded", "budget.denied")
	}
	result.Input = redact(request.Input, policy.RedactedPatterns)
	if err := g.allowLocked(request, &result, key, now, reserved, reservedCost, policy); err != nil {
		g.restoreLocked(checkpoint)
		return result, &GovernanceError{Code: "audit_unavailable", TraceID: traceID}
	}
	return result, nil
}

func (g *GovernanceCenter) allowLocked(request GovernanceRequest, result *GovernanceResult, key string, now time.Time, reserved int64, reservedCost float64, policy TenantPolicy) error {
	result.NewExecution = true
	result.ExecutionActive = true
	g.executions[key] = governanceExecution{
		traceID: result.TraceID, appID: request.AgentAppID, sessionID: request.SessionID, requestID: request.RequestID,
		userID: request.UserID, channel: request.Channel, externalSubject: request.ExternalSubject,
		provider: request.Channel, reserved: reserved, reservedCost: reservedCost, costPerToken: policy.CostPerToken, policyRevision: policy.Revision,
		toolCosts: cloneToolCosts(policy.ToolCosts), started: now, active: true,
	}
	metrics := g.metrics[request.TenantID]
	metrics.TenantID = request.TenantID
	metrics.Active++
	g.metrics[request.TenantID] = metrics
	g.recordMetricSampleLocked(request.TenantID, request.AgentAppID, request.Channel, now, TenantMetrics{Active: 1})
	g.appendAuditLocked(auditForRequest(request, result.TraceID, "policy.allowed", "", now))
	g.recordTraceLocked(request, result.TraceID, "policy.evaluate", "ok", now)
	return g.persistLocked()
}

const governanceReservationTTL = 15 * time.Minute

func (g *GovernanceCenter) reconcileExpiredConfirmationsLocked(tenantID string, now time.Time) bool {
	changed := false
	for id, confirmation := range g.confirmations {
		if tenantID != "" && confirmation.TenantID != tenantID {
			continue
		}
		if !now.Before(confirmation.ExpiresAt) && (confirmation.Status == ConfirmationPending || confirmation.Status == ConfirmationApproved) {
			confirmation.Status = ConfirmationExpired
			confirmation.DecidedAt = now
			confirmation.CompletedAt = now
			g.confirmations[id] = confirmation
			g.appendAuditLocked(AuditEvent{
				TenantID: confirmation.TenantID, UserID: confirmation.UserID, SessionID: confirmation.SessionID,
				AgentName: confirmation.AgentAppID, ToolName: confirmation.ToolName, Decision: "tool.confirmation.expired",
				ErrorType: "confirmation_expired", TraceID: confirmation.TraceID, RequestID: confirmation.RequestID,
				PolicyRevision: confirmation.PolicyRevision, Checkpoint: "tool.before_call", OccurredAt: now,
			})
			g.recordTraceLocked(GovernanceRequest{TenantID: confirmation.TenantID, AgentAppID: confirmation.AgentAppID, SessionID: confirmation.SessionID, RequestID: confirmation.RequestID}, confirmation.TraceID, "tool.confirmation", "expired", now)
			changed = true
		}
	}
	return changed
}

func (g *GovernanceCenter) reconcileExpiredReservationsLocked(now time.Time) bool {
	changed := false
	for key, execution := range g.executions {
		if !execution.active || now.Sub(execution.started) < governanceReservationTTL {
			continue
		}
		execution.active = false
		execution.expired = true
		g.executions[key] = execution
		metrics := g.metrics[strings.SplitN(key, "\x00", 2)[0]]
		if metrics.Active > 0 {
			metrics.Active--
		}
		metrics.Failed++
		g.metrics[metrics.TenantID] = metrics
		g.recordMetricSampleLocked(metrics.TenantID, execution.appID, execution.provider, now, TenantMetrics{Active: -1, Failed: 1})
		request := GovernanceRequest{
			TenantID: metrics.TenantID, AgentAppID: execution.appID, UserID: execution.userID, SessionID: execution.sessionID,
			RequestID: execution.requestID, Channel: execution.channel, ExternalSubject: execution.externalSubject,
		}
		g.appendAuditLocked(auditForRequest(request, execution.traceID, "reservation.expired", "reservation_expired", now))
		g.recordTraceLocked(request, execution.traceID, "runner.complete", "expired", now)
		changed = true
	}
	return changed
}

func (g *GovernanceCenter) Complete(ctx context.Context, completion GovernanceCompletion) (string, error) {
	if ctx.Err() != nil {
		return completion.Output, ctx.Err()
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	key := executionKey(completion.TenantID, completion.RequestID)
	execution, ok := g.executions[key]
	if !ok || !execution.active {
		return completion.Output, nil
	}
	checkpoint := g.snapshotLocked()
	execution.active = false
	g.executions[key] = execution
	policy := g.policies[governanceKey(completion.TenantID, completion.AgentAppID)]
	output := completion.Output
	for _, pattern := range policy.DeniedOutputPatterns {
		if strings.Contains(output, pattern) {
			output = "[REDACTED]"
			completion.ErrorType = "output_guardrail"
			break
		}
	}
	output = redact(output, policy.RedactedPatterns)
	tokens := completion.Tokens
	if completion.ErrorType != "storage_error" && (completion.NoUsage || tokens <= 0) {
		tokens = estimateTokens(output)
		if tokens < execution.reserved {
			tokens = execution.reserved
		}
	}
	g.usedTokens[completion.TenantID] += tokens
	executionCost := float64(tokens)*execution.costPerToken + execution.toolCost
	metrics := g.metrics[completion.TenantID]
	if metrics.Active > 0 {
		metrics.Active--
	}
	metrics.Tokens += tokens
	metrics.Cost += executionCost
	executionLatency := g.now().Sub(execution.started).Milliseconds()
	metrics.ExecutionLatencyMS += executionLatency
	decision := "run.completed"
	if completion.Cancelled {
		metrics.Failed++
		decision = "run.cancelled"
	} else if completion.ErrorType != "" {
		metrics.Failed++
		decision = "run.failed"
	} else {
		metrics.Completed++
	}
	g.metrics[completion.TenantID] = metrics
	delta := TenantMetrics{Active: -1, Tokens: tokens, Cost: executionCost, ExecutionLatencyMS: executionLatency}
	if completion.Cancelled || completion.ErrorType != "" {
		delta.Failed = 1
	} else {
		delta.Completed = 1
	}
	g.recordMetricSampleLocked(completion.TenantID, completion.AgentAppID, execution.provider, g.now().UTC(), delta)
	userID, sessionID, channel, externalSubject := execution.userID, execution.sessionID, execution.channel, execution.externalSubject
	if userID == "" {
		userID = completion.UserID
	}
	if sessionID == "" {
		sessionID = completion.SessionID
	}
	if channel == "" {
		channel = completion.Channel
	}
	if externalSubject == "" {
		externalSubject = completion.ExternalSubject
	}
	request := GovernanceRequest{TenantID: completion.TenantID, AgentAppID: completion.AgentAppID, UserID: userID, SessionID: sessionID, RequestID: completion.RequestID, Channel: channel, ExternalSubject: externalSubject}
	auditErrorType := completion.ErrorType
	if completion.Cancelled && auditErrorType == "" {
		auditErrorType = "cancelled"
	}
	audit := auditForRequest(request, execution.traceID, decision, auditErrorType, g.now().UTC())
	audit.Cost = executionCost
	audit.Latency = g.now().Sub(execution.started)
	g.appendAuditLocked(audit)
	traceStatus := "ok"
	if completion.Cancelled {
		traceStatus = "cancelled"
	} else if completion.ErrorType != "" {
		traceStatus = "error"
	}
	g.recordTraceLocked(request, execution.traceID, "runner.complete", traceStatus, g.now().UTC())
	if err := g.persistLocked(); err != nil {
		g.restoreLocked(checkpoint)
		return completion.Output, err
	}
	return output, nil
}

func (g *GovernanceCenter) FilterOutput(tenantID, appID, output string) (string, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	policy := g.policies[governanceKey(tenantID, appID)]
	for _, pattern := range policy.DeniedOutputPatterns {
		if strings.Contains(output, pattern) {
			return "[REDACTED]", true
		}
	}
	return redact(output, policy.RedactedPatterns), false
}

func (g *GovernanceCenter) RequiresBufferedOutput(tenantID, appID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	policy := g.policies[governanceKey(tenantID, appID)]
	return len(policy.DeniedOutputPatterns) > 0 || len(policy.RedactedPatterns) > 0
}

func (g *GovernanceCenter) RecordStorageLatency(tenantID string, latency time.Duration) {
	g.RecordStorageLatencyFor(tenantID, "", "", latency)
}

func (g *GovernanceCenter) RecordStorageLatencyFor(tenantID, appID, provider string, latency time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	checkpoint := g.snapshotLocked()
	metrics := g.metrics[tenantID]
	metrics.TenantID = tenantID
	metrics.StorageLatencyMS += latency.Milliseconds()
	g.metrics[tenantID] = metrics
	g.recordMetricSampleLocked(tenantID, appID, provider, g.now().UTC(), TenantMetrics{StorageLatencyMS: latency.Milliseconds()})
	if err := g.persistLocked(); err != nil {
		g.restoreLocked(checkpoint)
	}
}

func (g *GovernanceCenter) RecordModelCall(request GovernanceRequest, traceID string, latency time.Duration, callErr error) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	checkpoint := g.snapshotLocked()
	latencyMS := latency.Milliseconds()
	metrics := g.metrics[request.TenantID]
	metrics.TenantID = request.TenantID
	metrics.ModelLatencyMS += latencyMS
	g.metrics[request.TenantID] = metrics
	g.recordMetricSampleLocked(request.TenantID, request.AgentAppID, request.Channel, g.now().UTC(), TenantMetrics{ModelLatencyMS: latencyMS})
	status := "ok"
	if callErr != nil {
		status = "error"
	}
	g.recordTraceLocked(request, traceID, "model.call", status, g.now().UTC())
	if err := g.persistLocked(); err != nil {
		g.restoreLocked(checkpoint)
		return err
	}
	return nil
}

func (g *GovernanceCenter) AuthorizeTool(ctx context.Context, request GovernanceRequest, traceID, toolName string, arguments []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.refreshPoliciesLocked(ctx); err != nil {
		return &GovernanceError{Code: "control_plane_unavailable", TraceID: traceID}
	}
	checkpoint := g.snapshotLocked()
	now := g.now().UTC()
	policy, configured := g.policies[governanceKey(request.TenantID, request.AgentAppID)]
	deny := func(code string) error {
		g.appendAuditLocked(AuditEvent{TenantID: request.TenantID, UserID: request.UserID, SessionID: request.SessionID, AgentName: request.AgentAppID, ToolName: toolName, Decision: "tool.denied", ErrorType: code, TraceID: traceID, RequestID: request.RequestID, PolicyRevision: policy.Revision, Checkpoint: "tool.before_call", Rule: code, Reason: "Tool call rejected before invocation", OccurredAt: now})
		g.recordTraceLocked(request, traceID, "tool.authorize", "error", now)
		if err := g.persistLocked(); err != nil {
			g.restoreLocked(checkpoint)
			return &GovernanceError{Code: "audit_unavailable", TraceID: traceID}
		}
		return &GovernanceError{Code: code, TraceID: traceID}
	}
	if request.PolicyRevision != 0 && policy.Revision != request.PolicyRevision {
		return deny("policy_revision_stale")
	}
	if !configured {
		return deny("policy_unavailable")
	}
	if !contains(policy.AllowedTools, toolName) {
		return deny("tool_not_allowed")
	}
	if contains(policy.DangerousTools, toolName) {
		confirmationID := "confirmation-" + stableID(request.TenantID+"\x00"+request.RequestID+"\x00"+toolName)
		confirmation, ok := g.confirmations[confirmationID]
		argumentSummary := argumentSummary(arguments)
		if !ok {
			confirmation = ToolConfirmation{
				ID: confirmationID, TenantID: request.TenantID, AgentAppID: request.AgentAppID, SessionID: request.SessionID,
				RequestID: request.RequestID, UserID: request.UserID, ToolName: toolName, ArgumentSummary: argumentSummary,
				PolicyRevision: policy.Revision, TraceID: traceID, Status: ConfirmationPending, CreatedAt: now, ExpiresAt: now.Add(15 * time.Minute),
			}
			g.confirmations[confirmationID] = confirmation
			g.appendAuditLocked(AuditEvent{TenantID: request.TenantID, UserID: request.UserID, SessionID: request.SessionID, AgentName: request.AgentAppID, ToolName: toolName, Decision: "tool.confirmation.pending", TraceID: traceID, RequestID: request.RequestID, PolicyRevision: policy.Revision, Checkpoint: "tool.before_call", OccurredAt: now})
			g.recordTraceLocked(request, traceID, "tool.confirmation", "pending", now)
			if err := g.persistLocked(); err != nil {
				g.restoreLocked(checkpoint)
				return &GovernanceError{Code: "audit_unavailable", TraceID: traceID}
			}
			return &GovernanceError{Code: "confirmation_required", TraceID: traceID, ConfirmationID: confirmationID}
		}
		if confirmation.ArgumentSummary != argumentSummary {
			return deny("confirmation_arguments_changed")
		}
		if !now.Before(confirmation.ExpiresAt) && (confirmation.Status == ConfirmationPending || confirmation.Status == ConfirmationApproved) {
			confirmation.Status = ConfirmationExpired
			confirmation.CompletedAt = now
			g.confirmations[confirmationID] = confirmation
			g.appendAuditLocked(AuditEvent{TenantID: request.TenantID, UserID: request.UserID, SessionID: request.SessionID, AgentName: request.AgentAppID, ToolName: toolName, Decision: "tool.confirmation.expired", TraceID: traceID, RequestID: request.RequestID, PolicyRevision: policy.Revision, OccurredAt: now})
			if err := g.persistLocked(); err != nil {
				g.restoreLocked(checkpoint)
				return &GovernanceError{Code: "audit_unavailable", TraceID: traceID}
			}
			return &GovernanceError{Code: "confirmation_rejected", TraceID: traceID, ConfirmationID: confirmationID}
		}
		switch confirmation.Status {
		case ConfirmationPending:
			return &GovernanceError{Code: "confirmation_required", TraceID: traceID, ConfirmationID: confirmationID}
		case ConfirmationRejected, ConfirmationExpired:
			return &GovernanceError{Code: "confirmation_rejected", TraceID: traceID, ConfirmationID: confirmationID}
		case ConfirmationApproved:
			confirmation.Status = ConfirmationExecuting
			confirmation.InvokedAt = now
		case ConfirmationExecuting, ConfirmationCompleted, ConfirmationFailed, ConfirmationOutcomeUnknown:
			return deny("confirmation_consumed")
		default:
			return deny("confirmation_rejected")
		}
		g.confirmations[confirmationID] = confirmation
	}
	g.toolStarts[toolExecutionKey(request.TenantID, request.RequestID, toolName)] = now
	g.appendAuditLocked(AuditEvent{TenantID: request.TenantID, UserID: request.UserID, SessionID: request.SessionID, AgentName: request.AgentAppID, ToolName: toolName, Decision: "tool.allowed", TraceID: traceID, RequestID: request.RequestID, PolicyRevision: policy.Revision, Checkpoint: "tool.before_call", Rule: "tool_allowlist", OccurredAt: now})
	g.recordTraceLocked(request, traceID, "tool.authorize", "ok", now)
	if err := g.persistLocked(); err != nil {
		g.restoreLocked(checkpoint)
		return &GovernanceError{Code: "audit_unavailable", TraceID: traceID}
	}
	return nil
}

func (g *GovernanceCenter) CompleteTool(ctx context.Context, request GovernanceRequest, traceID, toolName string, runErr error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.now().UTC()
	key := toolExecutionKey(request.TenantID, request.RequestID, toolName)
	started, authorized := g.toolStarts[key]
	if !authorized {
		return nil
	}
	delete(g.toolStarts, key)
	latency := time.Duration(0)
	if !started.IsZero() {
		latency = now.Sub(started)
	}
	metrics := g.metrics[request.TenantID]
	metrics.TenantID = request.TenantID
	metrics.ToolLatencyMS += latency.Milliseconds()
	g.metrics[request.TenantID] = metrics
	provider := ""
	if execution, ok := g.executions[executionKey(request.TenantID, request.RequestID)]; ok {
		provider = execution.provider
	}
	g.recordMetricSampleLocked(request.TenantID, request.AgentAppID, provider, now, TenantMetrics{ToolLatencyMS: latency.Milliseconds()})
	policy := g.policies[governanceKey(request.TenantID, request.AgentAppID)]
	toolCost := float64(0)
	if execution, ok := g.executions[executionKey(request.TenantID, request.RequestID)]; ok && execution.active {
		toolCost = execution.toolCosts[toolName]
		execution.toolCost += toolCost
		g.executions[executionKey(request.TenantID, request.RequestID)] = execution
	}
	decision, status, errorType := "tool.completed", "ok", ""
	if errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) {
		decision, status, errorType = "tool.cancelled", "cancelled", "cancelled"
	} else if runErr != nil {
		decision, status, errorType = "tool.failed", "error", "tool_failed"
	}
	confirmationID := "confirmation-" + stableID(request.TenantID+"\x00"+request.RequestID+"\x00"+toolName)
	if confirmation, ok := g.confirmations[confirmationID]; ok && confirmation.Status == ConfirmationExecuting {
		if errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) {
			confirmation.Status = ConfirmationOutcomeUnknown
		} else if runErr != nil {
			confirmation.Status = ConfirmationFailed
		} else {
			confirmation.Status = ConfirmationCompleted
		}
		confirmation.CompletedAt = now
		g.confirmations[confirmationID] = confirmation
	}
	g.appendAuditLocked(AuditEvent{TenantID: request.TenantID, UserID: request.UserID, SessionID: request.SessionID, AgentName: request.AgentAppID, ToolName: toolName, Decision: decision, ErrorType: errorType, Cost: toolCost, TraceID: traceID, RequestID: request.RequestID, PolicyRevision: policy.Revision, Checkpoint: "tool.after_call", Latency: latency, OccurredAt: now})
	g.recordTraceLocked(request, traceID, "tool.execute", status, now)
	if err := g.persistLocked(); err != nil {
		// The Tool has already run by the time AfterTool is invoked. Keep its
		// terminal state in memory when the audit snapshot is unavailable so a
		// retry can persist the outcome without invoking the Tool again.
		return &GovernanceError{Code: "audit_unavailable", TraceID: traceID}
	}
	return nil
}

func (g *GovernanceCenter) MarkExecutingToolsOutcomeUnknown(ctx context.Context, request GovernanceRequest, traceID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	checkpoint := g.snapshotLocked()
	now := g.now().UTC()
	changed := false
	for id, confirmation := range g.confirmations {
		if confirmation.TenantID != request.TenantID || confirmation.RequestID != request.RequestID || confirmation.Status != ConfirmationExecuting {
			continue
		}
		confirmation.Status = ConfirmationOutcomeUnknown
		confirmation.CompletedAt = now
		g.confirmations[id] = confirmation
		delete(g.toolStarts, toolExecutionKey(request.TenantID, request.RequestID, confirmation.ToolName))
		g.appendAuditLocked(AuditEvent{
			TenantID: request.TenantID, UserID: request.UserID, SessionID: request.SessionID,
			AgentName: request.AgentAppID, ToolName: confirmation.ToolName, Decision: "tool.outcome_unknown",
			ErrorType: "worker_lost", TraceID: traceID, RequestID: request.RequestID,
			PolicyRevision: confirmation.PolicyRevision, Checkpoint: "tool.after_call", OccurredAt: now,
		})
		g.recordTraceLocked(request, traceID, "tool.execute", "outcome_unknown", now)
		changed = true
	}
	if !changed {
		return nil
	}
	if err := g.persistLocked(); err != nil {
		g.restoreLocked(checkpoint)
		return err
	}
	return nil
}

func (g *GovernanceCenter) DecideConfirmation(ctx context.Context, tenantID, id, decidedBy string, approve bool) (ToolConfirmation, error) {
	if err := ctx.Err(); err != nil {
		return ToolConfirmation{}, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	checkpoint := g.snapshotLocked()
	confirmation, ok := g.confirmations[id]
	if !ok || confirmation.TenantID != tenantID {
		return ToolConfirmation{}, ErrNotFound
	}
	if confirmation.Status != ConfirmationPending {
		return confirmation, nil
	}
	if !g.now().Before(confirmation.ExpiresAt) {
		confirmation.Status = ConfirmationExpired
	} else if approve {
		confirmation.Status = ConfirmationApproved
	} else {
		confirmation.Status = ConfirmationRejected
	}
	confirmation.DecidedAt = g.now().UTC()
	confirmation.DecidedBy = decidedBy
	g.confirmations[id] = confirmation
	g.appendAuditLocked(AuditEvent{TenantID: tenantID, UserID: decidedBy, SessionID: confirmation.SessionID, AgentName: confirmation.AgentAppID, ToolName: confirmation.ToolName, Decision: "tool.confirmation." + string(confirmation.Status), TraceID: confirmation.TraceID, RequestID: confirmation.RequestID, OccurredAt: confirmation.DecidedAt})
	if err := g.persistLocked(); err != nil {
		g.restoreLocked(checkpoint)
		return ToolConfirmation{}, err
	}
	return confirmation, nil
}

func (g *GovernanceCenter) Confirmations(tenantID string) []ToolConfirmation {
	g.mu.Lock()
	defer g.mu.Unlock()
	checkpoint := g.snapshotLocked()
	if g.reconcileExpiredConfirmationsLocked(tenantID, g.now().UTC()) {
		if err := g.persistLocked(); err != nil {
			g.restoreLocked(checkpoint)
		}
	}
	result := []ToolConfirmation{}
	for _, item := range g.confirmations {
		if item.TenantID == tenantID {
			result = append(result, item)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.After(result[j].CreatedAt) })
	return result
}

func (g *GovernanceCenter) ConfirmationForRequest(tenantID, requestID string) (ToolConfirmation, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, confirmation := range g.confirmations {
		if confirmation.TenantID == tenantID && confirmation.RequestID == requestID {
			return confirmation, true
		}
	}
	return ToolConfirmation{}, false
}

// ToolReplayResult returns a model-visible result for a Tool that already
// reached a terminal confirmation state. This lets a retry reconcile an
// already-executed Tool without invoking its side effect again.
func (g *GovernanceCenter) ToolReplayResult(tenantID, requestID, toolName string) (any, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	confirmationID := "confirmation-" + stableID(tenantID+"\x00"+requestID+"\x00"+toolName)
	confirmation, ok := g.confirmations[confirmationID]
	if !ok {
		return nil, false
	}
	switch confirmation.Status {
	case ConfirmationCompleted:
		return map[string]string{"status": "completed"}, true
	case ConfirmationFailed:
		return map[string]string{"status": "failed"}, true
	case ConfirmationCancelled:
		return map[string]string{"status": "cancelled"}, true
	default:
		return nil, false
	}
}

func (g *GovernanceCenter) AuditEvents(query AuditQuery) []AuditEvent {
	g.mu.Lock()
	defer g.mu.Unlock()
	limit := query.Limit
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	result := []AuditEvent{}
	now := g.now().UTC()
	for index := len(g.audits) - 1; index >= 0 && len(result) < limit; index-- {
		event := g.audits[index]
		policy := g.auditPolicyLocked(event.TenantID)
		if event.OccurredAt.Before(now.Add(-time.Duration(policy.RetentionDays) * 24 * time.Hour)) {
			continue
		}
		if query.TenantID != "" && event.TenantID != query.TenantID || query.Channel != "" && event.Channel != query.Channel || query.AgentName != "" && event.AgentName != query.AgentName || query.Decision != "" && event.Decision != query.Decision || query.ErrorType != "" && event.ErrorType != query.ErrorType || query.UserID != "" && event.UserID != query.UserID || query.SessionID != "" && event.SessionID != query.SessionID || query.RequestID != "" && event.RequestID != query.RequestID || query.TraceID != "" && event.TraceID != query.TraceID || !query.From.IsZero() && event.OccurredAt.Before(query.From) || !query.To.IsZero() && event.OccurredAt.After(query.To) {
			continue
		}
		if query.Offset > 0 {
			query.Offset--
			continue
		}
		result = append(result, event)
	}
	return result
}

func (g *GovernanceCenter) Metrics(tenantID string) TenantMetrics {
	g.mu.Lock()
	defer g.mu.Unlock()
	metrics := g.metrics[tenantID]
	metrics.TenantID = tenantID
	if metrics.TokenBudget > 0 {
		metrics.TokensRemaining = max(int64(0), metrics.TokenBudget-metrics.Tokens)
	}
	if metrics.CostBudget > 0 {
		metrics.CostRemaining = max(float64(0), metrics.CostBudget-metrics.Cost)
	}
	return metrics
}

func (g *GovernanceCenter) PrometheusMetrics() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	tenantIDs := make([]string, 0, len(g.metrics))
	for tenantID := range g.metrics {
		tenantIDs = append(tenantIDs, tenantID)
	}
	sort.Strings(tenantIDs)
	definitions := []struct {
		name       string
		help       string
		metricType string
		value      func(TenantMetrics) string
	}{
		{"trpc_agent_requests_total", "Accepted tenant requests.", "counter", func(m TenantMetrics) string { return strconv.FormatInt(m.Requests, 10) }},
		{"trpc_agent_active_executions", "Currently active tenant executions.", "gauge", func(m TenantMetrics) string { return strconv.FormatInt(m.Active, 10) }},
		{"trpc_agent_completed_executions_total", "Completed tenant executions.", "counter", func(m TenantMetrics) string { return strconv.FormatInt(m.Completed, 10) }},
		{"trpc_agent_failed_executions_total", "Failed tenant executions.", "counter", func(m TenantMetrics) string { return strconv.FormatInt(m.Failed, 10) }},
		{"trpc_agent_denied_requests_total", "Governance-denied tenant requests.", "counter", func(m TenantMetrics) string { return strconv.FormatInt(m.Denied, 10) }},
		{"trpc_agent_rate_limited_requests_total", "Rate-limited tenant requests.", "counter", func(m TenantMetrics) string { return strconv.FormatInt(m.RateLimited, 10) }},
		{"trpc_agent_tokens_total", "Model tokens attributed to the tenant.", "counter", func(m TenantMetrics) string { return strconv.FormatInt(m.Tokens, 10) }},
		{"trpc_agent_cost_total", "Model and tool cost attributed to the tenant.", "counter", func(m TenantMetrics) string { return strconv.FormatFloat(m.Cost, 'f', -1, 64) }},
		{"trpc_agent_model_latency_milliseconds_total", "Cumulative model call latency.", "counter", func(m TenantMetrics) string { return strconv.FormatInt(m.ModelLatencyMS, 10) }},
		{"trpc_agent_execution_latency_milliseconds_total", "Cumulative end-to-end execution latency.", "counter", func(m TenantMetrics) string { return strconv.FormatInt(m.ExecutionLatencyMS, 10) }},
		{"trpc_agent_tool_latency_milliseconds_total", "Cumulative tool call latency.", "counter", func(m TenantMetrics) string { return strconv.FormatInt(m.ToolLatencyMS, 10) }},
		{"trpc_agent_storage_latency_milliseconds_total", "Cumulative storage operation latency.", "counter", func(m TenantMetrics) string { return strconv.FormatInt(m.StorageLatencyMS, 10) }},
		{"trpc_agent_im_delivered_total", "Successful IM deliveries.", "counter", func(m TenantMetrics) string { return strconv.FormatInt(m.IMDelivered, 10) }},
		{"trpc_agent_im_failed_total", "Failed IM deliveries.", "counter", func(m TenantMetrics) string { return strconv.FormatInt(m.IMFailed, 10) }},
	}
	var output strings.Builder
	for _, definition := range definitions {
		output.WriteString("# HELP " + definition.name + " " + definition.help + "\n")
		output.WriteString("# TYPE " + definition.name + " " + definition.metricType + "\n")
		for _, tenantID := range tenantIDs {
			output.WriteString(definition.name + "{tenant_id=" + strconv.Quote(tenantID) + "} " + definition.value(g.metrics[tenantID]) + "\n")
		}
	}
	return output.String()
}

func (g *GovernanceCenter) QueryMetrics(query MetricsQuery) (TenantMetrics, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.now().UTC()
	if query.To.IsZero() {
		query.To = now
	}
	if query.From.IsZero() {
		query.From = query.To.Add(-24 * time.Hour)
	}
	if query.TenantID == "" || !query.From.Before(query.To) || query.To.Sub(query.From) > 31*24*time.Hour {
		return TenantMetrics{}, errors.New("invalid_metrics_range")
	}
	if query.Provider != "" && query.Provider != ChannelMock && query.Provider != ChannelTelegram && query.Provider != ChannelEnterpriseWeChat {
		return TenantMetrics{}, errors.New("invalid_metrics_provider")
	}
	result := TenantMetrics{TenantID: query.TenantID}
	for _, sample := range g.metricSamples {
		if sample.TenantID != query.TenantID || query.AgentAppID != "" && sample.AgentAppID != query.AgentAppID || query.Provider != "" && sample.Provider != query.Provider || sample.OccurredAt.After(query.To) {
			continue
		}
		result.Active += sample.Delta.Active
		if sample.OccurredAt.Before(query.From) {
			continue
		}
		delta := sample.Delta
		delta.Active = 0
		addMetricDelta(&result, delta)
	}
	if result.Active < 0 {
		result.Active = 0
	}
	tenant := g.metrics[query.TenantID]
	result.TokenBudget, result.CostBudget, result.BudgetPeriodFrom = tenant.TokenBudget, tenant.CostBudget, tenant.BudgetPeriodFrom
	if result.TokenBudget > 0 {
		result.TokensRemaining = max(int64(0), result.TokenBudget-tenant.Tokens)
	}
	if result.CostBudget > 0 {
		result.CostRemaining = max(float64(0), result.CostBudget-tenant.Cost)
	}
	return result, nil
}

func addMetricDelta(metrics *TenantMetrics, delta TenantMetrics) {
	metrics.Requests += delta.Requests
	metrics.Active += delta.Active
	metrics.Completed += delta.Completed
	metrics.Failed += delta.Failed
	metrics.Denied += delta.Denied
	metrics.RateLimited += delta.RateLimited
	metrics.Tokens += delta.Tokens
	metrics.Cost += delta.Cost
	metrics.ModelLatencyMS += delta.ModelLatencyMS
	metrics.ExecutionLatencyMS += delta.ExecutionLatencyMS
	metrics.ToolLatencyMS += delta.ToolLatencyMS
	metrics.StorageLatencyMS += delta.StorageLatencyMS
	metrics.IMDelivered += delta.IMDelivered
	metrics.IMFailed += delta.IMFailed
}

func (g *GovernanceCenter) recordMetricSampleLocked(tenantID, appID, provider string, occurredAt time.Time, delta TenantMetrics) {
	g.metricSamples = append(g.metricSamples, MetricSample{TenantID: tenantID, AgentAppID: appID, Provider: provider, OccurredAt: occurredAt, Delta: delta})
	if len(g.metricSamples) > 100000 {
		g.metricSamples = append([]MetricSample(nil), g.metricSamples[len(g.metricSamples)-100000:]...)
	}
}

func (g *GovernanceCenter) Trace(tenantID, traceID, requestID string) (PlatformTrace, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if traceID != "" {
		trace, ok := g.traces[traceID]
		if ok && trace.TenantID == tenantID {
			return cloneTrace(trace), true
		}
	}
	for _, trace := range g.traces {
		if trace.TenantID == tenantID && requestID != "" && trace.RequestID == requestID {
			return cloneTrace(trace), true
		}
	}
	return PlatformTrace{}, false
}

func (g *GovernanceCenter) RecordSpan(request GovernanceRequest, traceID, name, status string) error {
	if traceID == "" {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	checkpoint := g.snapshotLocked()
	now := g.now().UTC()
	if status != "ok" && replaceableTraceSpan(name) {
		trace := g.traces[traceID]
		replaced := false
		for index := len(trace.Spans) - 1; index >= 0; index-- {
			if trace.Spans[index].Name == name && trace.Spans[index].Status == "ok" {
				trace.Spans[index].Status = status
				replaced = true
				break
			}
		}
		if replaced {
			g.traces[traceID] = trace
		} else {
			g.recordTraceLocked(request, traceID, name, status, now)
		}
	} else {
		g.recordTraceLocked(request, traceID, name, status, now)
	}
	if err := g.persistLocked(); err != nil {
		g.restoreLocked(checkpoint)
		return err
	}
	return nil
}

func replaceableTraceSpan(name string) bool {
	switch name {
	case "worker.execute", "agent_factory.resolve", "runner.run":
		return true
	default:
		return false
	}
}

// SetPersistencePath changes the local governance snapshot destination while
// preserving the same lock used by persistence operations. It is useful for
// controlled failure injection and runtime reconfiguration.
func (g *GovernanceCenter) SetPersistencePath(path string) {
	g.mu.Lock()
	g.path = path
	g.mu.Unlock()
}

func (g *GovernanceCenter) RecordDelivery(tenantID string, delivered bool) {
	g.RecordDeliveryFor(tenantID, "", "", delivered)
}

func (g *GovernanceCenter) RecordDeliveryFor(tenantID, appID, provider string, delivered bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	checkpoint := g.snapshotLocked()
	metrics := g.metrics[tenantID]
	metrics.TenantID = tenantID
	if delivered {
		metrics.IMDelivered++
	} else {
		metrics.IMFailed++
	}
	g.metrics[tenantID] = metrics
	delta := TenantMetrics{}
	if delivered {
		delta.IMDelivered = 1
	} else {
		delta.IMFailed = 1
	}
	g.recordMetricSampleLocked(tenantID, appID, provider, g.now().UTC(), delta)
	if err := g.persistLocked(); err != nil {
		g.restoreLocked(checkpoint)
	}
}

func (g *GovernanceCenter) appendAuditLocked(event AuditEvent) {
	policy := g.auditPolicyLocked(event.TenantID)
	if event.ID == "" {
		event.ID = "audit-" + stableID(event.TenantID+event.TraceID+event.Decision+event.OccurredAt.String())
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = g.now().UTC()
	}
	if policy.ContentMode == AuditMetadataOnly {
		event.Content = ""
	} else {
		event.Content = redactAuditContent(event.Content)
	}
	g.audits = append(g.audits, event)
	kept := g.audits[:0]
	now := g.now().UTC()
	for _, candidate := range g.audits {
		candidatePolicy := g.auditPolicyLocked(candidate.TenantID)
		if !candidate.OccurredAt.Before(now.Add(-time.Duration(candidatePolicy.RetentionDays) * 24 * time.Hour)) {
			kept = append(kept, candidate)
		}
	}
	g.audits = kept
	if len(g.audits) > 10000 {
		g.audits = append([]AuditEvent(nil), g.audits[len(g.audits)-10000:]...)
	}
}

func (g *GovernanceCenter) Record(ctx context.Context, event AuditEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	checkpoint := g.snapshotLocked()
	g.appendAuditLocked(event)
	if err := g.persistLocked(); err != nil {
		g.restoreLocked(checkpoint)
		return err
	}
	return nil
}

func (g *GovernanceCenter) persistLocked() error {
	if g.auditStore != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := g.auditStore.AppendAuditEvents(ctx, g.audits)
		cancel()
		if err != nil {
			return err
		}
	}
	if g.path == "" {
		return nil
	}
	persistedPolicies := make(map[string]TenantPolicy, len(g.policies))
	encryptedPatterns := make(map[string]string)
	var encryptionKey []byte
	for key, policy := range g.policies {
		persisted := clonePolicy(policy)
		if len(policy.RedactedPatterns) > 0 {
			if encryptionKey == nil {
				var err error
				encryptionKey, err = loadGovernanceEncryptionKey(g.path, true)
				if err != nil {
					return err
				}
			}
			encrypted, err := encryptRedactionPatterns(encryptionKey, policy.RedactedPatterns)
			if err != nil {
				return err
			}
			encryptedPatterns[key] = encrypted
			persisted.RedactedPatterns = []string{"[ENCRYPTED]"}
		}
		persistedPolicies[key] = persisted
	}
	persistedExecutions := make(map[string]persistedGovernanceExecution, len(g.executions))
	for key, execution := range g.executions {
		persistedExecutions[key] = persistedGovernanceExecution{
			TraceID: execution.traceID, AppID: execution.appID, SessionID: execution.sessionID, RequestID: execution.requestID,
			UserID: execution.userID, Channel: execution.channel, ExternalSubject: execution.externalSubject, Provider: execution.provider,
			Reserved: execution.reserved, ReservedCost: execution.reservedCost,
			CostPerToken: execution.costPerToken, PolicyRevision: execution.policyRevision, ToolCosts: cloneToolCosts(execution.toolCosts), ToolCost: execution.toolCost,
			Started: execution.started, Active: execution.active, Expired: execution.expired,
		}
	}
	auditPolicies := make(map[string]AuditPolicy, len(g.auditPolicies))
	for tenantID, policy := range g.auditPolicies {
		auditPolicies[tenantID] = normalizedAuditPolicy(policy)
	}
	snapshot := governanceSnapshot{Policies: persistedPolicies, EncryptedRedactedPatterns: encryptedPatterns, Audits: g.audits, Confirmations: g.confirmations, Executions: persistedExecutions, ToolStarts: g.toolStarts, Metrics: g.metrics, UsedTokens: g.usedTokens, Traces: g.traces, MetricSamples: g.metricSamples, AuditPolicies: auditPolicies}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(g.path), 0700); err != nil {
		return err
	}
	temporary := g.path + ".tmp"
	if err := os.WriteFile(temporary, data, 0600); err != nil {
		return err
	}
	return os.Rename(temporary, g.path)
}

func loadGovernanceEncryptionKey(path string, create bool) ([]byte, error) {
	keyPath := path + ".key"
	key, err := os.ReadFile(keyPath)
	if err == nil {
		if len(key) != 32 {
			return nil, errors.New("invalid_governance_encryption_key")
		}
		return key, nil
	}
	if !errors.Is(err, os.ErrNotExist) || !create {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(keyPath), 0700); err != nil {
		return nil, err
	}
	key = make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, os.ErrExist) {
		return loadGovernanceEncryptionKey(path, false)
	}
	if err != nil {
		return nil, err
	}
	if _, err := file.Write(key); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	return key, nil
}

func encryptRedactionPatterns(key []byte, patterns []string) (string, error) {
	plaintext, err := json.Marshal(patterns)
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	return base64.RawStdEncoding.EncodeToString(gcm.Seal(nonce, nonce, plaintext, nil)), nil
}

func decryptRedactionPatterns(key []byte, encoded string) ([]string, error) {
	ciphertext, err := base64.RawStdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(ciphertext) < gcm.NonceSize() {
		return nil, errors.New("invalid_encrypted_redaction_patterns")
	}
	plaintext, err := gcm.Open(nil, ciphertext[:gcm.NonceSize()], ciphertext[gcm.NonceSize():], nil)
	if err != nil {
		return nil, err
	}
	var patterns []string
	if err := json.Unmarshal(plaintext, &patterns); err != nil {
		return nil, err
	}
	return patterns, nil
}

func (g *GovernanceCenter) snapshotLocked() governanceState {
	state := governanceState{
		policies: make(map[string]TenantPolicy, len(g.policies)), audits: append([]AuditEvent(nil), g.audits...),
		confirmations: make(map[string]ToolConfirmation, len(g.confirmations)), executions: make(map[string]governanceExecution, len(g.executions)),
		metrics: make(map[string]TenantMetrics, len(g.metrics)), rateWindows: make(map[string]rateWindow, len(g.rateWindows)),
		traces: make(map[string]PlatformTrace, len(g.traces)), usedTokens: make(map[string]int64, len(g.usedTokens)), toolStarts: make(map[string]time.Time, len(g.toolStarts)),
		metricSamples: append([]MetricSample(nil), g.metricSamples...), auditPolicies: make(map[string]AuditPolicy, len(g.auditPolicies)),
	}
	for tenantID, policy := range g.auditPolicies {
		state.auditPolicies[tenantID] = normalizedAuditPolicy(policy)
	}
	for key, value := range g.policies {
		state.policies[key] = clonePolicy(value)
	}
	for key, value := range g.confirmations {
		state.confirmations[key] = value
	}
	for key, value := range g.executions {
		value.toolCosts = cloneToolCosts(value.toolCosts)
		state.executions[key] = value
	}
	for key, value := range g.metrics {
		state.metrics[key] = value
	}
	for key, value := range g.rateWindows {
		state.rateWindows[key] = value
	}
	for key, value := range g.traces {
		state.traces[key] = cloneTrace(value)
	}
	for key, value := range g.usedTokens {
		state.usedTokens[key] = value
	}
	for key, value := range g.toolStarts {
		state.toolStarts[key] = value
	}
	return state
}

func (g *GovernanceCenter) restoreLocked(state governanceState) {
	g.policies, g.audits, g.confirmations, g.executions = state.policies, state.audits, state.confirmations, state.executions
	g.metrics, g.rateWindows, g.traces, g.usedTokens = state.metrics, state.rateWindows, state.traces, state.usedTokens
	g.toolStarts = state.toolStarts
	g.metricSamples = state.metricSamples
	g.auditPolicies = state.auditPolicies
}

func (g *GovernanceCenter) recordTraceLocked(request GovernanceRequest, traceID, name, status string, now time.Time) {
	trace := g.traces[traceID]
	trace.TraceID = traceID
	trace.TenantID = request.TenantID
	trace.RequestID = request.RequestID
	if request.SessionID != "" {
		trace.SessionID = request.SessionID
	}
	if request.AgentAppID != "" {
		trace.AgentAppID = request.AgentAppID
	}
	parentID := ""
	if len(trace.Spans) > 0 {
		parentID = trace.Spans[len(trace.Spans)-1].SpanID
	}
	spanID := stableID(traceID + "\x00" + strconv.Itoa(len(trace.Spans)) + "\x00" + name)
	trace.Spans = append(trace.Spans, TraceSpan{SpanID: spanID, ParentSpanID: parentID, Name: name, Status: status, OccurredAt: now})
	g.traces[traceID] = trace
}

func auditForRequest(request GovernanceRequest, traceID, decision, errorType string, now time.Time) AuditEvent {
	return AuditEvent{TenantID: request.TenantID, Channel: request.Channel, UserID: request.UserID, SessionID: request.SessionID, AgentName: request.AgentAppID, Decision: decision, ErrorType: errorType, TraceID: traceID, RequestID: request.RequestID, PolicyRevision: request.PolicyRevision, OccurredAt: now}
}

func policyCheckpoint(decision string) string {
	if strings.HasPrefix(decision, "guardrail.input") {
		return "input"
	}
	if strings.HasPrefix(decision, "tool.") || strings.HasPrefix(decision, "mcp.") {
		return "tool.before_call"
	}
	if strings.HasPrefix(decision, "im_") {
		return "channel.authorization"
	}
	return "pre_execution"
}
func estimateTokens(value string) int64 {
	n := int64(len([]rune(value)))
	if n == 0 {
		return 1
	}
	return (n + 3) / 4
}
func redact(value string, patterns []string) string {
	for _, pattern := range patterns {
		value = strings.ReplaceAll(value, pattern, "[REDACTED]")
	}
	return value
}

var auditCredentialPattern = regexp.MustCompile(`(?i)(token|secret|password|api[_-]?key|authorization)\s*[:=]\s*[^,\s]+`)

func redactAuditContent(value string) string {
	if value == "" {
		return ""
	}
	return auditCredentialPattern.ReplaceAllString(value, "$1=[REDACTED]")
}
func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
func subset(values, allowed []string) bool {
	for _, value := range values {
		if !contains(allowed, value) {
			return false
		}
	}
	return true
}
func normalizedValues(values []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" && !seen[value] {
			seen[value] = true
			out = append(out, value)
		}
	}
	sort.Strings(out)
	return out
}
func normalizedPatterns(values []string) []string {
	out := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}
func clonePolicy(policy TenantPolicy) TenantPolicy {
	policy.AllowedTools = append([]string{}, policy.AllowedTools...)
	policy.AllowedMCP = append([]string{}, policy.AllowedMCP...)
	policy.DangerousTools = append([]string{}, policy.DangerousTools...)
	policy.DeniedInputPatterns = append([]string{}, policy.DeniedInputPatterns...)
	policy.DeniedOutputPatterns = append([]string{}, policy.DeniedOutputPatterns...)
	policy.RedactedPatterns = append([]string{}, policy.RedactedPatterns...)
	policy.ToolCosts = cloneToolCosts(policy.ToolCosts)
	policy.AllowedIMUsers = append([]string{}, policy.AllowedIMUsers...)
	policy.AllowedIMSubjects = append([]string{}, policy.AllowedIMSubjects...)
	policy.AllowedProviderAccounts = append([]string{}, policy.AllowedProviderAccounts...)
	policy.AllowedConversationTypes = append([]string{}, policy.AllowedConversationTypes...)
	return policy
}
func cloneToolCosts(costs map[string]float64) map[string]float64 {
	cloned := make(map[string]float64, len(costs))
	for key, value := range costs {
		cloned[key] = value
	}
	return cloned
}
func cloneTrace(trace PlatformTrace) PlatformTrace {
	trace.Spans = append([]TraceSpan(nil), trace.Spans...)
	return trace
}
func newTraceID() string {
	bytes := make([]byte, 16)
	_, _ = rand.Read(bytes)
	return hex.EncodeToString(bytes)
}
func stableID(value string) string      { sum := sha256Bytes(value); return hex.EncodeToString(sum[:8]) }
func sha256Bytes(value string) [32]byte { return sha256.Sum256([]byte(value)) }
func argumentSummary(arguments []byte) string {
	sum := sha256.Sum256(arguments)
	return "sha256:" + hex.EncodeToString(sum[:])
}
func confirmationForRequestLocked(items map[string]ToolConfirmation, tenantID, requestID string) string {
	for _, item := range items {
		if item.TenantID == tenantID && item.RequestID == requestID {
			return item.ID
		}
	}
	return ""
}

func confirmationTraceForRequestLocked(items map[string]ToolConfirmation, tenantID, requestID string) string {
	for _, item := range items {
		if item.TenantID == tenantID && item.RequestID == requestID {
			return item.TraceID
		}
	}
	return ""
}
func toolExecutionKey(tenantID, requestID, toolName string) string {
	return tenantID + "\x00" + requestID + "\x00" + toolName
}
func traceForRequestLocked(items map[string]governanceExecution, tenantID, requestID string) string {
	return items[executionKey(tenantID, requestID)].traceID
}
