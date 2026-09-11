package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	agentruntime "github.com/cyl6/trpc-agent-service/trpcservice/agent"
	"github.com/cyl6/trpc-agent-service/trpcservice/budget"
	"github.com/cyl6/trpc-agent-service/trpcservice/channels"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/contentsafety"
	"github.com/cyl6/trpc-agent-service/trpcservice/coordination"
	"github.com/cyl6/trpc-agent-service/trpcservice/delivery"
	"github.com/cyl6/trpc-agent-service/trpcservice/domain"
	auditlog "github.com/cyl6/trpc-agent-service/trpcservice/log"
	"github.com/cyl6/trpc-agent-service/trpcservice/metrics"
	"github.com/cyl6/trpc-agent-service/trpcservice/privacy"
	"github.com/cyl6/trpc-agent-service/trpcservice/sessionturn"
	"github.com/cyl6/trpc-agent-service/trpcservice/tenant/governance"
	"github.com/cyl6/trpc-agent-service/trpcservice/tooloperation"

	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

var tracer = otel.Tracer("trpc-agent-service/worker")

type Task struct {
	Tenant config.TenantConfig `json:"tenant"`
	// These fields are captured at ingress and persisted with the durable Inbox
	// payload. Refreshing or rolling back the control plane therefore cannot
	// change the configuration used by an in-flight task.
	ConfigRevision   string                  `json:"config_revision,omitempty"`
	ConfigGeneration int64                   `json:"config_generation,omitempty"`
	Binding          config.ChannelConfig    `json:"binding"`
	Message          domain.InboundMessage   `json:"message"`
	Deliver          bool                    `json:"deliver"`
	TraceCarrier     map[string]string       `json:"trace_carrier,omitempty"`
	Pipeline         DurablePipelineMetadata `json:"pipeline"`
	// TurnCommitParticipant is attached only after a durable PostgreSQL Inbox
	// is leased. It cannot be serialized into the persisted task payload.
	TurnCommitParticipant TurnCommitParticipant `json:"-"`
}

// TurnCommitParticipant contributes the durable queue writes to a strict SQL
// Session turn. CompleteInTransaction must use tx exclusively for database
// work. MarkCommitted is called only after the owner of tx successfully
// commits, allowing the relay to suppress its legacy second completion.
type TurnCommitParticipant interface {
	CompleteInTransaction(context.Context, pgx.Tx, AtomicDeliveryPlan) error
	MarkCommitted()
}

// AtomicDeliveryPlan is the canonical, replay-backed set of provider messages
// that must become Outbox operations beside a strict Session commit. Parts is
// empty only for a legacy replay that predates persisted plans.
type AtomicDeliveryPlan struct {
	Version  int
	Outbound domain.OutboundMessage
	Parts    []domain.OutboundMessage
}

// ProcessDisposition tells a durable relay whether an unsuccessful Process
// call should be retried, acknowledged without a reply, or parked for operator
// repair. The error remains available through errors.Is/errors.As so HTTP and
// synchronous callers keep their existing error semantics.
type ProcessDisposition string

const (
	ProcessRetryable       ProcessDisposition = "retryable"
	ProcessBlocked         ProcessDisposition = "blocked"
	ProcessTerminalIgnored ProcessDisposition = "terminal_ignored"
	ProcessDeadLetter      ProcessDisposition = "dead_letter"
)

// ProcessFailure attaches queue control metadata to an ordinary processing
// error without changing its public message or unwrap chain.
type ProcessFailure struct {
	cause       error
	disposition ProcessDisposition
	retryAfter  time.Duration
}

func (e *ProcessFailure) Error() string { return e.cause.Error() }
func (e *ProcessFailure) Unwrap() error { return e.cause }

// WithProcessDisposition marks an error for durable processing. It is exported
// so alternative Processor implementations can participate without changing
// the long-standing Process method signature.
func WithProcessDisposition(err error, disposition ProcessDisposition, retryAfter time.Duration) error {
	if err == nil {
		return nil
	}
	if disposition != ProcessTerminalIgnored && disposition != ProcessDeadLetter && disposition != ProcessBlocked {
		disposition = ProcessRetryable
	}
	if retryAfter < 0 {
		retryAfter = 0
	}
	return &ProcessFailure{cause: err, disposition: disposition, retryAfter: retryAfter}
}

// ProcessDispositionOf returns an explicit durable action. Unclassified errors
// default to retryable for compatibility with existing Processor implementations.
func ProcessDispositionOf(err error) (ProcessDisposition, time.Duration) {
	if err == nil {
		return "", 0
	}
	var failure *ProcessFailure
	if errors.As(err, &failure) {
		return failure.disposition, failure.retryAfter
	}
	return ProcessRetryable, 0
}

// StageTrace is one execution stage of a processed message, exported so the
// console frontend can render a real processing timeline.
type StageTrace struct {
	Name       string `json:"name"`
	StartMS    int64  `json:"start_ms"`
	DurationMS int64  `json:"duration_ms"`
}

// ToolTrace records one governance decision made for a tool call during the
// run. Arguments are intentionally reduced to a hash by the policy layer.
type ToolTrace struct {
	Name         string `json:"name"`
	Decision     string `json:"decision"`
	Reason       string `json:"reason,omitempty"`
	OperationKey string `json:"operation_key,omitempty"`
	Phase        string `json:"phase,omitempty"`
	Outcome      string `json:"outcome,omitempty"`
}

type Result struct {
	Text      string `json:"text"`
	RequestID string `json:"request_id"`
	SessionID string `json:"session_id"`
	Duplicate bool   `json:"duplicate"`
	// CacheHit marks replies served from the dedup result replay or the
	// completed-claim short circuit instead of a fresh model run.
	CacheHit         bool         `json:"cache_hit"`
	TraceID          string       `json:"trace_id,omitempty"`
	PromptTokens     int          `json:"prompt_tokens"`
	CompletionTokens int          `json:"completion_tokens"`
	CostUSD          float64      `json:"cost_usd"`
	UsageState       string       `json:"usage_state,omitempty"`
	UnknownCalls     int          `json:"unknown_calls,omitempty"`
	UnknownCostUSD   float64      `json:"unknown_cost_usd,omitempty"`
	LatencyMS        int64        `json:"latency_ms"`
	Tools            []ToolTrace  `json:"tools,omitempty"`
	Timeline         []StageTrace `json:"timeline,omitempty"`
	// Outbound is the reply that should be delivered for this message. It is
	// populated even when task.Deliver is false, so the durable pipeline can
	// persist it into the Outbox and deliver it independently of Agent
	// execution. It is nil when no reply should be sent (e.g. policy deny).
	// Outbound is an internal hand-off to the durable queue. It must not leak
	// channel routing metadata through the synchronous HTTP chat response.
	Outbound *domain.OutboundMessage `json:"-"`
}

type pendingResult struct {
	Outbound            domain.OutboundMessage   `json:"outbound"`
	AuditID             string                   `json:"audit_id,omitempty"`
	RequestID           string                   `json:"request_id"`
	PromptTokens        int                      `json:"prompt_tokens"`
	CompletionTokens    int                      `json:"completion_tokens"`
	CostUSD             float64                  `json:"cost_usd"`
	UsageState          string                   `json:"usage_state,omitempty"`
	UnknownCalls        int                      `json:"unknown_calls,omitempty"`
	UnknownCostUSD      float64                  `json:"unknown_cost_usd,omitempty"`
	DeliveryPlanVersion int                      `json:"delivery_plan_version,omitempty"`
	DeliveryParts       []domain.OutboundMessage `json:"delivery_parts,omitempty"`
}

const (
	turnReplaySchema        = "worker.turn-replay"
	turnReplayVersion       = 2
	legacyTurnReplayVersion = 1
	deliveryPlanVersion     = 1
	maxTurnReplayBytes      = 1 << 20
)

// turnReplayEnvelope is the durable, versioned hand-off between the strict
// Session turn transaction and Worker result replay. Routing identity is
// repeated deliberately: replay bytes are authoritative after a crash, so a
// corrupt or misrouted blob must fail closed instead of creating an Outbox for
// another tenant or binding.
type turnReplayEnvelope struct {
	Schema         string        `json:"schema"`
	Version        int           `json:"version"`
	TurnID         string        `json:"turn_id"`
	TenantID       string        `json:"tenant_id"`
	BindingID      string        `json:"binding_id"`
	Channel        string        `json:"channel"`
	ConfigRevision string        `json:"config_revision"`
	Result         pendingResult `json:"result"`
}

type toolTraceBuffer struct {
	mu       sync.Mutex
	traces   []ToolTrace
	auditErr error
}

type runtimeAcquireFunc func(context.Context, config.TenantConfig) (*agentruntime.Runtime, func(), error)

type Service struct {
	runtimes       *agentruntime.Manager
	acquireRuntime runtimeAcquireFunc
	coordinator    coordination.Coordinator
	channels       *channels.Registry
	filter         *governance.Filter
	approvals      governance.ApprovalBackend
	audit          auditlog.Sink
	metrics        *metrics.Metrics
	budget         budget.Ledger
	safety         contentsafety.Checker
	safetyPolicy   string
	toolOperations tooloperation.Ledger
	// toolTraces correlates per-request tool decisions with the finishing
	// Result. Entries are written from governance callbacks during the run and
	// deleted when the result is finalized, so the map stays bounded.
	toolTraces sync.Map
	lockTTL    time.Duration
	dedupTTL   time.Duration
	runTimeout time.Duration
}

var ErrTenantFrozen = errors.New("worker: tenant is frozen for backend migration")
var ErrTaskRevisionMismatch = errors.New("worker: task configuration revision mismatch")

type Options struct {
	LockTTL        time.Duration
	DedupTTL       time.Duration
	RunTimeout     time.Duration
	ToolOperations tooloperation.Ledger
	Approvals      governance.ApprovalBackend
	Budget         budget.Ledger
	Safety         contentsafety.Checker
	SafetyPolicy   string
}

func NewService(
	runtimes *agentruntime.Manager,
	coordinator coordination.Coordinator,
	channelRegistry *channels.Registry,
	filter *governance.Filter,
	auditSink auditlog.Sink,
	metricsExporter *metrics.Metrics,
	opts Options,
) *Service {
	if opts.LockTTL <= 0 {
		opts.LockTTL = 2 * time.Minute
	}
	if opts.DedupTTL <= 0 {
		opts.DedupTTL = 24 * time.Hour
	}
	if opts.RunTimeout <= 0 {
		opts.RunTimeout = 90 * time.Second
	}
	if runtimes == nil {
		runtimes = agentruntime.NewManager()
	}
	if coordinator == nil {
		coordinator = coordination.NewInMemory()
	}
	if channelRegistry == nil {
		channelRegistry = channels.NewRegistry()
	}
	if filter == nil {
		filter = governance.NewFilter()
	}
	if metricsExporter == nil {
		metricsExporter = metrics.NewMetrics()
	}
	if opts.ToolOperations == nil {
		opts.ToolOperations = tooloperation.NewMemory()
	}
	if opts.Approvals == nil {
		opts.Approvals = governance.NewApprovalStore()
	}
	if opts.SafetyPolicy == "" {
		opts.SafetyPolicy = "default-v1"
	}
	return &Service{
		runtimes: runtimes, acquireRuntime: runtimes.Acquire,
		coordinator: coordinator, channels: channelRegistry, filter: filter,
		approvals: opts.Approvals, audit: auditSink, metrics: metricsExporter,
		budget:         opts.Budget,
		safety:         opts.Safety,
		safetyPolicy:   opts.SafetyPolicy,
		toolOperations: opts.ToolOperations,
		lockTTL:        opts.LockTTL, dedupTTL: opts.DedupTTL, runTimeout: opts.RunTimeout,
	}
}

func (s *Service) Process(ctx context.Context, task Task) (result Result, err error) {
	started := time.Now()
	if task.ConfigRevision == "" {
		task.ConfigRevision = task.Tenant.Version
	}
	if task.ConfigRevision != "" && task.Tenant.Version != "" && task.ConfigRevision != task.Tenant.Version {
		return result, ErrTaskRevisionMismatch
	}
	msg := task.Message
	msg.TenantID = task.Tenant.TenantID
	msg.BindingID = task.Binding.BindingID
	msg.Channel = task.Binding.Type
	task.Message = msg
	requestID := uuid.NewString()
	result.RequestID = requestID
	principalID, sessionID := domain.Identity(msg, task.Tenant.App.Name)
	result.SessionID = sessionID
	auditUserID := domain.SenderIdentity(msg)
	dedupKey := messageKey(msg)
	labels := map[string]string{"tenant": task.Tenant.TenantID, "channel": msg.Channel}
	ctx, span := tracer.Start(ctx, "worker.process", trace.WithAttributes(
		attribute.String("tenant.id", task.Tenant.TenantID),
		attribute.String("channel", msg.Channel),
		attribute.String("config.revision", task.ConfigRevision),
	))
	stages := make([]StageTrace, 0, 8)
	stage := func(name string) func() {
		t0 := time.Now()
		return func() {
			stages = append(stages, StageTrace{
				Name: name, StartMS: t0.Sub(started).Milliseconds(), DurationMS: time.Since(t0).Milliseconds(),
			})
		}
	}
	defer func() {
		err = classifyProcessError(err)
		if err != nil {
			span.SetStatus(codes.Error, "request_failed:"+errorType(err))
		}
		span.End()
		result.LatencyMS = time.Since(started).Milliseconds()
		result.Timeline = stages
		if traceID := span.SpanContext().TraceID(); traceID.IsValid() {
			result.TraceID = traceID.String()
		}
		if held, ok := s.toolTraces.LoadAndDelete(requestID); ok {
			buffer := held.(*toolTraceBuffer)
			buffer.mu.Lock()
			result.Tools = append([]ToolTrace(nil), buffer.traces...)
			buffer.mu.Unlock()
		}
		s.metrics.Add("agent_requests_total", "Agent messages processed.", 1, mergeLabels(labels, "result", resultLabel(err)))
		s.metrics.Observe("agent_request_latency_seconds", "End-to-end worker latency.", time.Since(started).Seconds(), labels)
	}()

	if freezeReader, ok := s.coordinator.(coordination.TenantFreezeReader); ok {
		_, _, frozen, freezeErr := freezeReader.TenantFreeze(
			ctx, task.Tenant.TenantID, domain.AppNamespace(task.Tenant.TenantID, task.Tenant.App.Name),
		)
		if freezeErr != nil {
			return result, freezeErr
		}
		if frozen {
			return result, ErrTenantFrozen
		}
	}

	endLock := stage("acquire_session_lock")
	lockCtx, cancelLock := context.WithTimeout(ctx, s.processingTTL())
	lockLease, err := s.coordinator.Lock(lockCtx, domain.SessionPartitionKey(msg, task.Tenant.App.Name), s.lockTTL)
	cancelLock()
	endLock()
	if err != nil {
		return result, err
	}
	defer lockLease.Release()
	processCtx, cancelProcess := context.WithCancelCause(ctx)
	defer cancelProcess(nil)
	go func() {
		select {
		case lostErr := <-lockLease.Lost:
			if lostErr != nil {
				cancelProcess(lostErr)
			}
		case <-processCtx.Done():
		}
	}()
	ctx = processCtx

	endClaim := stage("dedup_claim")
	claimLease, err := s.coordinator.Claim(ctx, dedupKey, s.processingTTL())
	endClaim()
	if err != nil {
		return result, err
	}
	var pending pendingResult
	if claimLease.State == coordination.AlreadyCompleted {
		result.Duplicate = true
		result.CacheHit = true
		endReplay := stage("replay_lookup")
		found, loadErr := s.coordinator.LoadResult(ctx, dedupKey, &pending)
		endReplay()
		if loadErr != nil {
			return result, loadErr
		}
		if found && task.Pipeline.AtomicCommitMode != AtomicCommitRequired {
			if auditErr := s.writeReplayAudit(ctx, task, pending, auditUserID, sessionID); auditErr != nil {
				return result, auditErr
			}
			restorePendingResult(&result, pending)
			s.metrics.Add("result_cache_hits_total", "Replies served from dedup result cache.", 1, labels)
			return result, nil
		}
		// Keep the legacy/non-SQL completed-claim behavior unchanged. Those
		// backends have no durable turn replay to consult, while SQL runtimes are
		// required by Manager to expose the strict transactional capability.
		if task.Tenant.Data.Session.Type != "sql" {
			return result, nil
		}
		// A completed coordination claim without its short-lived result must not
		// become an empty successful reply. In strict mode the PostgreSQL turn is
		// the durable source of truth, so recover its canonical replay.
		endAcquire := stage("runtime_acquire")
		runtime, release, acquireErr := s.acquireRuntime(ctx, task.Tenant)
		endAcquire()
		if acquireErr != nil {
			return result, acquireErr
		}
		defer release()
		if validateErr := validateAtomicRuntime(task, runtime.SessionDatabaseIdentity); validateErr != nil {
			return result, validateErr
		}
		if runtime.TurnSession == nil {
			return result, corruptTurnReplay("completed message has no replayable result")
		}
		turnKey := session.Key{AppName: runtime.AppNamespace, UserID: principalID, SessionID: sessionID}
		endTurnBegin := stage("session_turn_begin")
		_, turn, turnID, beginErr := beginStrictTurn(ctx, runtime.TurnSession, turnKey, dedupKey)
		endTurnBegin()
		if beginErr != nil {
			return result, beginErr
		}
		defer turn.Abort()
		replay, replayed := turn.Replay()
		if !replayed {
			return result, corruptTurnReplay("completed message has no committed turn replay")
		}
		if replayErr := turn.Err(); replayErr != nil {
			return result, fmt.Errorf("read session turn replay: %w", replayErr)
		}
		canonical := replay
		if task.TurnCommitParticipant != nil {
			canonical, _, err = commitStrictTurn(ctx, turn, replay, turnID, task)
			if err != nil {
				return result, fmt.Errorf("complete replayed session turn: %w", err)
			}
		}
		pending, replayErr := decodeTurnReplay(canonical, turnID, task)
		if replayErr != nil {
			return result, replayErr
		}
		if auditErr := s.writeReplayAudit(ctx, task, pending, auditUserID, sessionID); auditErr != nil {
			return result, auditErr
		}
		restorePendingResult(&result, pending)
		s.metrics.Add("result_cache_hits_total", "Replies served from dedup result cache.", 1, labels)
		return result, nil
	}
	if claimLease.State == coordination.AlreadyProcessing {
		// The full session lane is already held, so this is an orphaned or
		// independently active claim rather than a completed duplicate.
		return result, coordination.ErrClaimInProgress
	}
	claimCompleted := false
	defer func() {
		if !claimCompleted {
			_ = s.coordinator.ReleaseClaim(context.Background(), dedupKey, claimLease.Token)
		}
	}()

	endReplay := stage("replay_lookup")
	found, loadErr := s.coordinator.LoadResult(ctx, dedupKey, &pending)
	endReplay()
	if loadErr != nil {
		return result, loadErr
	} else if found && task.Pipeline.AtomicCommitMode != AtomicCommitRequired {
		if auditErr := s.writeReplayAudit(ctx, task, pending, auditUserID, sessionID); auditErr != nil {
			return result, auditErr
		}
		restorePendingResult(&result, pending)
		result.CacheHit = true
		s.metrics.Add("result_cache_hits_total", "Replies served from dedup result cache.", 1, labels)
		if task.Deliver {
			if err := s.Deliver(ctx, task.Binding, pending.Outbound); err != nil {
				s.metrics.Add("im_delivery_total", "IM delivery attempts.", 1, mergeLabels(labels, "result", "error"))
				return result, err
			}
			s.metrics.Add("im_delivery_total", "IM delivery attempts.", 1, mergeLabels(labels, "result", "success"))
		}
		if err := s.finishClaim(ctx, dedupKey, claimLease.Token); err != nil {
			return result, err
		}
		claimCompleted = true
		return result, nil
	}

	endFilter := stage("inbound_policy")
	err = s.filter.CheckInboundContext(ctx, task.Tenant, task.Binding, msg)
	endFilter()
	if err != nil {
		classifiedErr := classifyProcessError(err)
		// Coordinator completion means a replayable Agent result exists. Policy
		// rejection has no Session replay or Outbound result, so leave the claim
		// releasable; the durable relay owns terminal Inbox acknowledgement. This
		// also preserves the same policy error for repeated synchronous calls.
		if auditErr := s.writeAudit(ctx, auditEntryForTenant(auditlog.Entry{
			Timestamp: time.Now().UTC(), TenantID: task.Tenant.TenantID, Channel: msg.Channel,
			BindingID: msg.BindingID, UserID: auditUserID, SessionID: sessionID,
			AgentName: task.Tenant.App.AgentName, Decision: "deny", Reason: err.Error(),
			LatencyMS: time.Since(started).Milliseconds(), ErrorType: errorType(err),
			AuditID:   auditlog.StableOperationAuditID(task.Tenant.TenantID, dedupKey, "deny", "deny"),
			RequestID: requestID, ConfigRevision: task.Tenant.Version, ContentHash: auditlog.ContentHash(msg.Text),
		}, task.Tenant)); auditErr != nil {
			return result, auditErr
		}
		return result, classifiedErr
	}
	if s.safety != nil {
		endSafety := stage("content_safety_input")
		decision, safetyErr := s.safety.Check(ctx, contentsafety.Request{
			TenantID: task.Tenant.TenantID, AppName: task.Tenant.App.Name, SessionID: sessionID,
			MessageKey: dedupKey, ConfigRevision: task.ConfigRevision, Phase: contentsafety.PhaseInput,
			PolicyVersion: s.safetyPolicy, ContentHash: contentsafety.HashContent(msg.Text), Text: msg.Text,
		})
		endSafety()
		if safetyErr != nil {
			if errors.Is(safetyErr, contentsafety.ErrBlocked) || decision.Status == contentsafety.StatusBlocked {
				return result, WithProcessDisposition(contentsafety.ErrBlocked, ProcessTerminalIgnored, 0)
			}
			return result, WithProcessDisposition(contentsafety.ErrUnavailable, ProcessRetryable, time.Second)
		}
	}

	endAcquire := stage("runtime_acquire")
	runtime, release, err := s.acquireRuntime(ctx, task.Tenant)
	endAcquire()
	if err != nil {
		return result, err
	}
	defer release()
	if validateErr := validateAtomicRuntime(task, runtime.SessionDatabaseIdentity); validateErr != nil {
		return result, validateErr
	}

	turnKey := session.Key{AppName: runtime.AppNamespace, UserID: principalID, SessionID: sessionID}
	turnID, err := sessionturn.DeriveTurnID(turnKey, dedupKey)
	if err != nil {
		return result, fmt.Errorf("derive stable session turn ID: %w", err)
	}
	runBaseCtx := ctx
	strictTurn := false
	var activeTurn sessionturn.Turn
	var turnErr func() error
	if runtime.TurnSession != nil {
		endTurnBegin := stage("session_turn_begin")
		turnCtx, turn, derivedTurnID, beginErr := beginStrictTurn(ctx, runtime.TurnSession, turnKey, dedupKey)
		endTurnBegin()
		if beginErr != nil {
			return result, beginErr
		}
		if derivedTurnID != turnID {
			turn.Abort()
			return result, errors.New("strict session turn identity mismatch")
		}
		defer turn.Abort()
		turnReplay, replayed := turn.Replay()
		if replayed {
			if replayErr := turn.Err(); replayErr != nil {
				return result, fmt.Errorf("read session turn replay: %w", replayErr)
			}
			canonical := turnReplay
			if task.TurnCommitParticipant != nil {
				canonical, _, err = commitStrictTurn(ctx, turn, turnReplay, turnID, task)
				if err != nil {
					return result, fmt.Errorf("complete replayed session turn: %w", err)
				}
			}
			pending, err = decodeTurnReplay(canonical, turnID, task)
			if err != nil {
				return result, err
			}
			if auditErr := s.writeReplayAudit(ctx, task, pending, auditUserID, sessionID); auditErr != nil {
				return result, auditErr
			}
			restorePendingResult(&result, pending)
			result.Duplicate = true
			result.CacheHit = true
			s.metrics.Add("result_cache_hits_total", "Replies served from dedup result cache.", 1, labels)
			endPersist := stage("result_persist")
			saveErr := s.coordinator.SaveResult(ctx, dedupKey, claimLease.Token, pending, s.dedupTTL)
			endPersist()
			if saveErr != nil {
				return result, saveErr
			}
			if task.Deliver {
				endDeliver := stage("im_deliver")
				deliverErr := s.Deliver(ctx, task.Binding, pending.Outbound)
				endDeliver()
				if deliverErr != nil {
					s.metrics.Add("im_delivery_total", "IM delivery attempts.", 1, mergeLabels(labels, "result", "error"))
					return result, deliverErr
				}
				s.metrics.Add("im_delivery_total", "IM delivery attempts.", 1, mergeLabels(labels, "result", "success"))
			}
			if err := s.finishClaim(ctx, dedupKey, claimLease.Token); err != nil {
				return result, err
			}
			claimCompleted = true
			return result, nil
		}
		runBaseCtx = turnCtx
		strictTurn = true
		activeTurn = turn
		turnErr = turn.Err
	}

	cleanText, approvalNonce := governance.ExtractApproval(msg.Text)
	userMessage := buildUserMessage(cleanText, msg.Attachments, msg.Scope, auditUserID)
	userMessage.Content, err = s.applyPrivacy(ctx, task, userMessage.Content, "input", requestID, sessionID, auditUserID)
	if err != nil {
		return result, err
	}
	runCtx, cancel := context.WithTimeout(runBaseCtx, s.runTimeout)
	defer cancel()
	runCtx = governance.WithRequestContext(runCtx, governance.RequestContext{
		TenantID: task.Tenant.TenantID, ConfigVersion: task.Tenant.Version,
		AppNamespace: runtime.AppNamespace, UserID: auditUserID, SessionID: sessionID,
		TurnID: turnID, DedupKey: dedupKey, SenderID: auditUserID,
		Channel: msg.Channel, BindingID: msg.BindingID, AgentName: task.Tenant.App.AgentName,
		RequestID: requestID, ApprovalNonce: approvalNonce,
		AuditPolicy: task.Tenant.Audit, AuditSecrets: task.Tenant.SecretEnvNames(),
	})
	usageRun := budget.NewRun(budget.RunMetadata{
		TenantID: task.Tenant.TenantID, AppNamespace: runtime.AppNamespace,
		SessionID: sessionID, DedupKey: dedupKey, RequestID: requestID,
		RunID: requestID, MonthlyLimitUnits: budget.USDUnits(task.Tenant.Budget.MonthlyCostUSD),
		Ledger: s.budget, Observer: s.observeUsage,
	})
	runCtx = budget.WithRun(runCtx, usageRun)
	toolGuard, err := tooloperation.NewRunGuard(s.toolOperations, tooloperation.GuardScope{
		TenantID: task.Tenant.TenantID, AppNamespace: runtime.AppNamespace,
		SessionID: sessionID, TurnID: turnID, ConfigRevision: task.Tenant.Version,
		SideEffects: append([]string(nil), task.Tenant.Tools.SideEffects...),
	},
		tooloperation.WithGuardLeaseTTL(2*s.runTimeout),
		tooloperation.WithGuardObserver(s.observeToolOperation),
	)
	if err != nil {
		return result, fmt.Errorf("create tool side-effect guard: %w", err)
	}
	baseToolPolicy := governance.PermissionPolicyObserved(
		task.Tenant.Tools, s.approvals, s.observeToolDecision,
	)
	endRun := stage("agent_run")
	events, err := runtime.Runner.Run(
		runCtx,
		principalID,
		sessionID,
		userMessage,
		agent.WithAppName(runtime.AppNamespace),
		agent.WithRequestID(requestID),
		agent.WithRuntimeState(map[string]any{
			"tenant_id": task.Tenant.TenantID, "config_revision": task.Tenant.Version,
			"channel": msg.Channel, "sender_id": auditUserID,
			budget.RuntimeStateKey: usageRun,
		}),
		agent.WithKnowledgeFilter(map[string]any{"tenant_id": task.Tenant.TenantID, "app_name": task.Tenant.App.Name}),
		agent.WithToolFilter(governance.ToolFilter(task.Tenant.Tools)),
		agent.WithToolPermissionPolicy(toolGuard.WrapPermissionPolicy(baseToolPolicy)),
		plugin.WithPlugins(toolGuard),
		agent.WithMaxRunDuration(s.runTimeout),
		agent.WithSpanAttributes(
			attribute.String("tenant.id", task.Tenant.TenantID),
			attribute.String("channel", msg.Channel),
		),
	)
	if err != nil {
		endRun()
		return result, fmt.Errorf("run agent: %w", err)
	}
	text, promptTokens, completionTokens, err := collect(events)
	endRun()
	if err != nil {
		return result, err
	}
	if auditErr := s.toolAuditError(requestID); auditErr != nil {
		return result, auditErr
	}
	if cause := context.Cause(processCtx); cause != nil {
		return result, cause
	}
	// A runner may detach work from its deadline and still close the event
	// stream with a completion. Never commit that result after the run budget
	// expired. Commit itself intentionally uses runBaseCtx below: it remains
	// fenced by the caller/session-lock context without inheriting an already
	// expired model-run deadline.
	if cause := context.Cause(runCtx); cause != nil {
		return result, cause
	}
	if strictTurn {
		if bufferErr := turnErr(); bufferErr != nil {
			return result, fmt.Errorf("buffer session turn: %w", bufferErr)
		}
	}
	if strings.TrimSpace(text) == "" {
		text = "消息已处理，但 Agent 没有返回可显示的文本。"
	}
	text, err = s.applyPrivacy(ctx, task, text, "output", requestID, sessionID, auditUserID)
	if err != nil {
		return result, err
	}
	if s.safety != nil {
		endSafety := stage("content_safety_output")
		decision, safetyErr := s.safety.Check(ctx, contentsafety.Request{
			TenantID: task.Tenant.TenantID, AppName: task.Tenant.App.Name, SessionID: sessionID,
			MessageKey: contentsafety.OutputCandidateKey(dedupKey, task.Tenant.Version, text), ConfigRevision: task.ConfigRevision, Phase: contentsafety.PhaseOutput,
			PolicyVersion: s.safetyPolicy, ContentHash: contentsafety.HashContent(text), Text: text,
		})
		endSafety()
		if safetyErr != nil {
			if errors.Is(safetyErr, contentsafety.ErrBlocked) || decision.Status == contentsafety.StatusBlocked {
				return result, WithProcessDisposition(contentsafety.ErrBlocked, ProcessTerminalIgnored, 0)
			}
			return result, WithProcessDisposition(contentsafety.ErrUnavailable, ProcessRetryable, time.Second)
		}
	}
	usageSummary := usageRun.Summary()
	if usageSummary.RecordedCalls > 0 {
		promptTokens = usageSummary.PromptTokens
		completionTokens = usageSummary.CompletionTokens
	}
	result.Text = text
	if usageSummary.RecordedCalls > 0 {
		result.PromptTokens = usageSummary.PromptTokens
		result.CompletionTokens = usageSummary.CompletionTokens
		result.CostUSD = usageSummary.CostUSD()
		result.UnknownCalls = usageSummary.UnknownCalls
		result.UnknownCostUSD = budget.UnitsUSD(usageSummary.UnknownUnits)
		result.UsageState = usageState(usageSummary)
	} else if s.budget == nil {
		// Compatibility-only path for injected test/demo Runners that predate
		// the provider model wrapper. Production service construction always
		// supplies a ledger, so collect's aggregate usage is never a billing
		// source there.
		result.PromptTokens = promptTokens
		result.CompletionTokens = completionTokens
		result.CostUSD = (float64(promptTokens)*task.Tenant.Model.InputPrice + float64(completionTokens)*task.Tenant.Model.OutputPrice) / 1_000_000
		result.UsageState = "legacy"
	} else {
		result.UsageState = "none"
	}
	cost := result.CostUSD
	outbound := domain.OutboundMessage{
		TenantID: task.Tenant.TenantID, BindingID: task.Binding.BindingID, Channel: task.Binding.Type,
		Target: msg.ReplyTarget, ThreadID: msg.ThreadID, Scope: msg.Scope, Text: text,
	}
	pending = pendingResult{
		Outbound:     outbound,
		AuditID:      auditlog.StableOperationAuditID(task.Tenant.TenantID, dedupKey, "allow", "allow"),
		RequestID:    requestID,
		PromptTokens: result.PromptTokens, CompletionTokens: result.CompletionTokens, CostUSD: cost,
		UsageState: result.UsageState, UnknownCalls: result.UnknownCalls, UnknownCostUSD: result.UnknownCostUSD,
	}
	if task.TurnCommitParticipant != nil {
		endPlan := stage("delivery_plan")
		parts, planErr := s.PlanDelivery(task.Binding, outbound)
		endPlan()
		if planErr != nil {
			return result, planErr
		}
		if len(parts) == 0 {
			return result, errors.New("delivery plan is empty")
		}
		pending.DeliveryPlanVersion = deliveryPlanVersion
		pending.DeliveryParts = make([]domain.OutboundMessage, 0, len(parts))
		for _, part := range parts {
			pending.DeliveryParts = append(pending.DeliveryParts, part.Message)
		}
	}
	commitReplayed := false
	if strictTurn {
		replay, encodeErr := encodeTurnReplay(turnID, task, pending)
		if encodeErr != nil {
			return result, encodeErr
		}
		endCommit := stage("session_turn_commit")
		canonical, replayed, commitErr := commitStrictTurn(runBaseCtx, activeTurn, replay, turnID, task)
		endCommit()
		if commitErr != nil {
			return result, fmt.Errorf("commit session turn: %w", commitErr)
		}
		pending, err = decodeTurnReplay(canonical, turnID, task)
		if err != nil {
			return result, err
		}
		if replayed {
			result.Duplicate = true
			result.CacheHit = true
			commitReplayed = true
			s.metrics.Add("result_cache_hits_total", "Replies served from dedup result cache.", 1, labels)
		}
	}
	restorePendingResult(&result, pending)
	cost = pending.CostUSD
	if !commitReplayed {
		if auditErr := s.writeAudit(ctx, auditEntryForTenant(auditlog.Entry{
			Timestamp: time.Now().UTC(), TenantID: task.Tenant.TenantID, Channel: msg.Channel,
			BindingID: msg.BindingID, UserID: auditUserID, SessionID: sessionID,
			AgentName: task.Tenant.App.AgentName, Decision: "allow", LatencyMS: time.Since(started).Milliseconds(),
			CostUSD: pending.CostUSD, AuditID: pending.AuditID,
			RequestID: pending.RequestID, ConfigRevision: task.Tenant.Version,
			ContentHash: auditlog.ContentHash(msg.Text),
		}, task.Tenant)); auditErr != nil {
			return result, auditErr
		}
	}
	endPersist := stage("result_persist")
	saveErr := s.coordinator.SaveResult(ctx, dedupKey, claimLease.Token, pending, s.dedupTTL)
	endPersist()
	if saveErr != nil {
		return result, saveErr
	}
	// Actual provider calls have already updated usage metrics through the
	// ledger observer. The compatibility metrics below are only for injected
	// legacy/demo Runners without a ledger-backed model wrapper. Audit remains
	// independent of IM delivery and is written for both paths.
	if !commitReplayed {
		if s.budget == nil && usageRun.Summary().RecordedCalls == 0 {
			s.metrics.Add("model_tokens_total", "Model tokens consumed.", float64(pending.PromptTokens+pending.CompletionTokens), labels)
			s.metrics.Add("tenant_cost_usd_total", "Estimated model cost in USD.", pending.CostUSD, labels)
		}
	}
	if task.Deliver {
		endDeliver := stage("im_deliver")
		deliverErr := s.Deliver(ctx, task.Binding, pending.Outbound)
		endDeliver()
		if deliverErr != nil {
			s.metrics.Add("im_delivery_total", "IM delivery attempts.", 1, mergeLabels(labels, "result", "error"))
			return result, deliverErr
		}
		s.metrics.Add("im_delivery_total", "IM delivery attempts.", 1, mergeLabels(labels, "result", "success"))
	}
	if err := s.finishClaim(ctx, dedupKey, claimLease.Token); err != nil {
		return result, err
	}
	claimCompleted = true
	return result, nil
}

func commitStrictTurn(
	ctx context.Context,
	turn sessionturn.Turn,
	replay []byte,
	turnID string,
	task Task,
) ([]byte, bool, error) {
	if task.TurnCommitParticipant == nil {
		return turn.Commit(ctx, replay)
	}
	atomicTurn, ok := turn.(sessionturn.AtomicTurn)
	if !ok {
		return nil, false, errors.New("strict session turn does not support durable transaction participant")
	}
	canonical, replayed, err := atomicTurn.CommitWithParticipant(
		ctx,
		replay,
		func(participantCtx context.Context, tx pgx.Tx, canonicalReplay []byte) error {
			pending, decodeErr := decodeTurnReplay(canonicalReplay, turnID, task)
			if decodeErr != nil {
				return decodeErr
			}
			return task.TurnCommitParticipant.CompleteInTransaction(
				participantCtx,
				tx,
				AtomicDeliveryPlan{
					Version:  pending.DeliveryPlanVersion,
					Outbound: pending.Outbound,
					Parts:    append([]domain.OutboundMessage(nil), pending.DeliveryParts...),
				},
			)
		},
	)
	if err != nil {
		return nil, false, err
	}
	task.TurnCommitParticipant.MarkCommitted()
	return canonical, replayed, nil
}

func buildUserMessage(text string, attachments []domain.Attachment, scope domain.Scope, senderID string) model.Message {
	// Runner uses a stable group principal. UserPrompt preserves the
	// pseudonymous human sender and safe attachment metadata without forwarding
	// provider URLs or file IDs.
	return model.NewUserMessage(domain.UserPrompt(text, attachments, scope, senderID))
}

// Each permission evaluation is a distinct audit event, including evaluations
// during a real execution retry. The generated ID is stored with the immutable
// payload by the SQL/spool sink; replay of that record retains both. Canonical
// turn replay does not re-evaluate permissions or create another tool audit.
func (s *Service) observeToolDecision(ctx context.Context, decision governance.ToolDecision) {
	s.metrics.Add("tool_permission_total", "Tool permission decisions.", 1, map[string]string{
		"tenant": decision.Request.TenantID, "component": decision.ToolName, "result": decision.Decision,
	})
	if decision.Request.RequestID != "" {
		traced := ToolTrace{Name: decision.ToolName, Decision: decision.Decision, Reason: decision.Reason}
		held, _ := s.toolTraces.LoadOrStore(decision.Request.RequestID, &toolTraceBuffer{})
		buffer := held.(*toolTraceBuffer)
		buffer.mu.Lock()
		buffer.traces = append(buffer.traces, traced)
		buffer.mu.Unlock()
	}
	policy := decision.Request.AuditPolicy
	if err := s.writeAudit(ctx, auditlog.Entry{
		Timestamp: time.Now().UTC(), TenantID: decision.Request.TenantID,
		Channel: decision.Request.Channel, BindingID: decision.Request.BindingID,
		UserID: decision.Request.UserID, SessionID: decision.Request.SessionID,
		AgentName: decision.Request.AgentName, ToolName: decision.ToolName,
		Decision: decision.Decision, Reason: decision.Reason,
		AuditID: auditlog.StableOperationAuditID(decision.Request.TenantID,
			decision.Request.TurnID+"\x1f"+decision.Request.ConfigVersion+"\x1f"+uuid.NewString(),
			"permission", decision.Decision),
		RequestID: decision.Request.RequestID, ConfigRevision: decision.Request.ConfigVersion,
		ToolArgsHash:   decision.ArgumentsHash,
		PolicySnapshot: &policy, SecretEnvNames: append([]string(nil), decision.Request.AuditSecrets...),
	}); err != nil {
		s.recordToolAuditError(decision.Request.RequestID, err)
	}
}

func (s *Service) recordToolAuditError(requestID string, err error) {
	if s == nil || requestID == "" || err == nil {
		return
	}
	held, _ := s.toolTraces.LoadOrStore(requestID, &toolTraceBuffer{})
	buffer := held.(*toolTraceBuffer)
	buffer.mu.Lock()
	if buffer.auditErr == nil {
		buffer.auditErr = err
	}
	buffer.mu.Unlock()
}

func (s *Service) toolAuditError(requestID string) error {
	if s == nil || requestID == "" {
		return nil
	}
	held, ok := s.toolTraces.Load(requestID)
	if !ok {
		return nil
	}
	buffer := held.(*toolTraceBuffer)
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.auditErr
}

func (s *Service) observeToolOperation(ctx context.Context, operation tooloperation.GuardEvent) {
	s.metrics.Add("tool_side_effect_operations_total", "Durable side-effect operation transitions.", 1, map[string]string{
		"tenant":    operation.TenantID,
		"component": operation.ToolName,
		"result":    operation.Phase + "_" + operation.Outcome,
	})
	rc, _ := governance.RequestContextFrom(ctx)
	if rc.RequestID != "" {
		trace := ToolTrace{
			Name: operation.ToolName, Decision: "side_effect", OperationKey: operation.OperationKey,
			Phase: operation.Phase, Outcome: operation.Outcome,
		}
		held, _ := s.toolTraces.LoadOrStore(rc.RequestID, &toolTraceBuffer{})
		buffer := held.(*toolTraceBuffer)
		buffer.mu.Lock()
		buffer.traces = append(buffer.traces, trace)
		buffer.mu.Unlock()
	}
	policy := rc.AuditPolicy
	if err := s.writeAudit(ctx, auditlog.Entry{
		Timestamp: time.Now().UTC(), TenantID: operation.TenantID,
		Channel: rc.Channel, BindingID: rc.BindingID, UserID: rc.UserID,
		SessionID: rc.SessionID, AgentName: rc.AgentName, ToolName: operation.ToolName,
		Decision: "side_effect_" + operation.Phase, RequestID: rc.RequestID,
		AuditID: auditlog.StableOperationAuditID(operation.TenantID, operation.OperationKey,
			operation.Phase, operation.Outcome),
		ConfigRevision: rc.ConfigVersion, OperationKey: operation.OperationKey,
		OperationPhase: operation.Phase, OperationState: operation.Outcome,
		PolicySnapshot: &policy, SecretEnvNames: append([]string(nil), rc.AuditSecrets...),
	}); err != nil {
		s.recordToolAuditError(rc.RequestID, err)
	}
}

func (s *Service) finishClaim(ctx context.Context, key, ownerToken string) error {
	// Keep the result for the same TTL as the completed dedup claim. The
	// durable Inbox transaction happens after Process returns; retaining this
	// value lets a retry reconstruct the Outbox if the process crashes or that
	// transaction fails in the intervening window.
	return s.coordinator.Complete(ctx, key, ownerToken, s.dedupTTL)
}

func (s *Service) observeUsage(record budget.UsageRecord) {
	if s == nil || s.metrics == nil {
		return
	}
	labels := map[string]string{"tenant": record.TenantID}
	s.metrics.Add("model_calls_total", "Actual provider model calls.", 1,
		mergeLabels(labels, "result", string(record.State)))
	switch record.State {
	case budget.StateSettled:
		s.metrics.Add("model_tokens_total", "Model tokens consumed.",
			float64(record.PromptTokens+record.CompletionTokens), labels)
		s.metrics.Add("tenant_cost_usd_total", "Actual settled model cost in USD.",
			budget.UnitsUSD(record.ActualUnits), labels)
	case budget.StateUnknown:
		s.metrics.Add("model_usage_unknown_total", "Model calls whose provider cost is unknown.",
			1, labels)
		s.metrics.Add("tenant_unknown_cost_usd_total", "Reserved cost retained for unknown model calls.",
			budget.UnitsUSD(record.EstimatedUnits), labels)
	}
}

func usageState(summary budget.Summary) string {
	switch {
	case summary.UnknownCalls > 0 && summary.SettledUnits > 0:
		return "mixed"
	case summary.UnknownCalls > 0:
		return "unknown"
	case summary.RecordedCalls > 0:
		return "settled"
	default:
		return "none"
	}
}

func restorePendingResult(result *Result, pending pendingResult) {
	result.Text = pending.Outbound.Text
	outbound := pending.Outbound
	result.Outbound = &outbound
	result.PromptTokens = pending.PromptTokens
	result.CompletionTokens = pending.CompletionTokens
	result.CostUSD = pending.CostUSD
	result.UsageState = pending.UsageState
	result.UnknownCalls = pending.UnknownCalls
	result.UnknownCostUSD = pending.UnknownCostUSD
	if pending.RequestID != "" {
		result.RequestID = pending.RequestID
	}
}

func beginStrictTurn(
	ctx context.Context,
	service sessionturn.TransactionalService,
	key session.Key,
	idempotencyKey string,
) (context.Context, sessionturn.Turn, string, error) {
	turnID, err := sessionturn.DeriveTurnID(key, idempotencyKey)
	if err != nil {
		return ctx, nil, "", fmt.Errorf("derive session turn ID: %w", err)
	}
	turnCtx, turn, err := service.BeginTurn(ctx, key, idempotencyKey)
	if err != nil {
		if turn != nil {
			turn.Abort()
		}
		return ctx, nil, "", fmt.Errorf("begin session turn: %w", err)
	}
	if turn == nil {
		return ctx, nil, "", errors.New("begin session turn returned no turn")
	}
	if turnCtx == nil {
		turn.Abort()
		return ctx, nil, "", errors.New("begin session turn returned no context")
	}
	return turnCtx, turn, turnID, nil
}

func encodeTurnReplay(turnID string, task Task, pending pendingResult) ([]byte, error) {
	envelope := turnReplayEnvelope{
		Schema:         turnReplaySchema,
		Version:        turnReplayVersion,
		TurnID:         turnID,
		TenantID:       task.Tenant.TenantID,
		BindingID:      task.Binding.BindingID,
		Channel:        task.Binding.Type,
		ConfigRevision: task.Tenant.Version,
		Result:         pending,
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("encode session turn replay: %w", err)
	}
	if len(encoded) > maxTurnReplayBytes {
		return nil, fmt.Errorf("session turn replay exceeds %d bytes", maxTurnReplayBytes)
	}
	return encoded, nil
}

func decodeTurnReplay(encoded []byte, turnID string, task Task) (pendingResult, error) {
	if len(encoded) == 0 {
		return pendingResult{}, corruptTurnReplay("session turn replay is empty")
	}
	if len(encoded) > maxTurnReplayBytes {
		return pendingResult{}, corruptTurnReplay("session turn replay exceeds %d bytes", maxTurnReplayBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var envelope turnReplayEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return pendingResult{}, corruptTurnReplay("decode session turn replay: %v", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return pendingResult{}, corruptTurnReplay("decode session turn replay: %v", err)
	}
	if envelope.Schema != turnReplaySchema ||
		(envelope.Version != legacyTurnReplayVersion && envelope.Version != turnReplayVersion) {
		return pendingResult{}, corruptTurnReplay("unsupported session turn replay schema %q version %d", envelope.Schema, envelope.Version)
	}
	if envelope.TurnID != turnID {
		return pendingResult{}, corruptTurnReplay("session turn replay turn identity mismatch")
	}
	if envelope.TenantID != task.Tenant.TenantID ||
		envelope.BindingID != task.Binding.BindingID ||
		envelope.Channel != task.Binding.Type ||
		envelope.ConfigRevision != task.Tenant.Version {
		return pendingResult{}, corruptTurnReplay("session turn replay routing identity mismatch")
	}
	pending := envelope.Result
	if pending.RequestID == "" {
		return pendingResult{}, corruptTurnReplay("session turn replay request ID is empty")
	}
	if pending.AuditID == "" {
		pending.AuditID = auditlog.StableOperationAuditID(task.Tenant.TenantID, messageKey(task.Message), "allow", "allow")
	}
	if pending.Outbound.TenantID != task.Tenant.TenantID ||
		pending.Outbound.BindingID != task.Binding.BindingID ||
		pending.Outbound.Channel != task.Binding.Type ||
		pending.Outbound.Target != task.Message.ReplyTarget ||
		pending.Outbound.ThreadID != task.Message.ThreadID ||
		pending.Outbound.Scope != task.Message.Scope {
		return pendingResult{}, corruptTurnReplay("session turn replay outbound identity mismatch")
	}
	if pending.PromptTokens < 0 || pending.CompletionTokens < 0 || pending.CostUSD < 0 ||
		pending.UnknownCalls < 0 || pending.UnknownCostUSD < 0 {
		return pendingResult{}, corruptTurnReplay("session turn replay contains negative usage")
	}
	if pending.DeliveryPlanVersion != 0 && pending.DeliveryPlanVersion != deliveryPlanVersion {
		return pendingResult{}, corruptTurnReplay("unsupported delivery plan version %d", pending.DeliveryPlanVersion)
	}
	if pending.DeliveryPlanVersion == 0 && len(pending.DeliveryParts) != 0 {
		return pendingResult{}, corruptTurnReplay("session turn replay has parts without a delivery plan version")
	}
	if pending.DeliveryPlanVersion != 0 && len(pending.DeliveryParts) == 0 {
		return pendingResult{}, corruptTurnReplay("session turn replay delivery plan is empty")
	}
	for i, part := range pending.DeliveryParts {
		if part.TenantID != pending.Outbound.TenantID ||
			part.BindingID != pending.Outbound.BindingID ||
			part.Channel != pending.Outbound.Channel ||
			part.Target != pending.Outbound.Target ||
			part.ThreadID != pending.Outbound.ThreadID ||
			part.Scope != pending.Outbound.Scope {
			return pendingResult{}, corruptTurnReplay("session turn replay delivery part %d identity mismatch", i)
		}
	}
	return pending, nil
}

func corruptTurnReplay(format string, args ...any) error {
	return fmt.Errorf("%w: %s", sessionturn.ErrCorruptData, fmt.Sprintf(format, args...))
}

func (s *Service) processingTTL() time.Duration {
	ttl := 2 * s.runTimeout
	if lockWindow := 2 * s.lockTTL; ttl < lockWindow {
		ttl = lockWindow
	}
	if ttl <= 0 {
		return 3 * time.Minute
	}
	return ttl
}

// ListUnknownToolOperations returns a redacted tenant-scoped projection for
// operator reconciliation. Expired executing leases are first promoted to
// unknown; they are never made automatically retryable.
func (s *Service) ListUnknownToolOperations(
	ctx context.Context,
	tenantID string,
	limit int,
) ([]tooloperation.Record, error) {
	if s == nil || s.toolOperations == nil {
		return nil, errors.New("tool operation ledger is unavailable")
	}
	if _, err := s.toolOperations.ReclaimExpired(ctx, time.Now().UTC(), 100); err != nil {
		return nil, err
	}
	return s.toolOperations.ListUnknown(ctx, tenantID, limit)
}

// ResolveToolOperation applies one audited CAS decision. Only an explicit
// retry_not_applied resolution can make an unknown side effect executable.
func (s *Service) ResolveToolOperation(
	ctx context.Context,
	request tooloperation.ResolveRequest,
) (*tooloperation.Record, error) {
	if s == nil || s.toolOperations == nil {
		return nil, errors.New("tool operation ledger is unavailable")
	}
	record, err := s.toolOperations.ResolveUnknown(ctx, request, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	if s.metrics != nil {
		s.metrics.Add("tool_side_effect_operations_total", "Durable side-effect operation transitions.", 1, map[string]string{
			"tenant": request.TenantID, "component": record.Metadata.ToolName,
			"result": "resolve_" + string(record.State),
		})
	}
	s.writeAudit(ctx, auditlog.Entry{
		Timestamp: time.Now().UTC(), TenantID: request.TenantID,
		UserID: request.ActorHash, ToolName: record.Metadata.ToolName,
		Decision: "side_effect_resolve", Reason: request.ReasonCode,
		RequestID: request.ResolutionID, OperationKey: request.OperationKey,
		OperationPhase: "resolve", OperationState: string(record.State),
	})
	return record, nil
}

// PlanDelivery deterministically splits one reply into provider-sized
// operations without reading secrets or performing network I/O.
func (s *Service) PlanDelivery(binding config.ChannelConfig, outbound domain.OutboundMessage) ([]delivery.Part, error) {
	adapter, err := s.channels.Get(binding.Type)
	if err != nil {
		return nil, err
	}
	return adapter.Plan(binding, outbound)
}

// DeliverOperation sends exactly one planned message operation and returns a
// structured outcome. Unknown results must never be retried automatically.
func (s *Service) DeliverOperation(ctx context.Context, binding config.ChannelConfig, request delivery.Request) delivery.Result {
	adapter, err := s.channels.Get(binding.Type)
	if err != nil {
		return delivery.Result{Outcome: delivery.PermanentRejected, ErrorType: "adapter_unavailable", Err: err}
	}
	ctx, span := tracer.Start(ctx, "im.send", trace.WithAttributes(attribute.String("channel", binding.Type)))
	defer span.End()
	result := adapter.Deliver(ctx, binding, request)
	if err := result.Validate(); err != nil {
		span.SetStatus(codes.Error, "invalid delivery result")
		return delivery.Result{Outcome: delivery.Unknown, ErrorType: "adapter_invalid_result", Err: err}
	}
	if result.Outcome != delivery.Confirmed {
		span.SetStatus(codes.Error, "delivery "+string(result.Outcome))
	}
	return result
}

// Deliver is the compatibility path for the non-durable in-process queue. It
// plans and sends parts sequentially, but cannot provide the crash recovery or
// manual reconciliation guarantees of the durable operation ledger.
func (s *Service) Deliver(ctx context.Context, binding config.ChannelConfig, outbound domain.OutboundMessage) error {
	parts, err := s.PlanDelivery(binding, outbound)
	if err != nil {
		return err
	}
	operationPrefix := "sync:" + uuid.NewString()
	for index, part := range parts {
		result := s.DeliverOperation(ctx, binding, delivery.Request{
			OperationKey: operationPrefix + ":" + strconv.Itoa(index),
			AttemptNo:    1,
			Message:      part.Message,
		})
		if err := result.Failure(); err != nil {
			var failure *delivery.FailureError
			if index > 0 && errors.As(err, &failure) && failure.RetryWholeTask {
				// The current request is known not sent, but replaying the entire
				// non-durable task would duplicate earlier confirmed parts. Durable
				// mode retries this operation independently; the compatibility path
				// must stop instead of manufacturing a whole-task duplicate.
				failure.RetryWholeTask = false
				failure.Category = "partial_delivery_not_replayable"
			}
			return fmt.Errorf("deliver IM response: %w", err)
		}
	}
	return nil
}

func collect(events <-chan *event.Event) (text string, promptTokens, completionTokens int, err error) {
	var streamed strings.Builder
	var full string
	var completionText string
	var terminalErr error
	sawCompletion := false
	for evt := range events {
		if evt == nil {
			continue
		}
		isRoot := evt.ParentInvocationID == ""
		if isRoot && evt.IsTerminalError() && terminalErr == nil {
			terminalErr = collectResponseError(evt.Error)
		}
		if evt.Response != nil && evt.Usage != nil {
			if evt.IsRunnerCompletion() {
				// Runner completion carries the aggregate usage for the run.
				promptTokens = evt.Usage.PromptTokens
				completionTokens = evt.Usage.CompletionTokens
			} else {
				promptTokens += evt.Usage.PromptTokens
				completionTokens += evt.Usage.CompletionTokens
			}
		}
		if evt.Response != nil && isRoot {
			for _, choice := range evt.Choices {
				if !evt.IsRunnerCompletion() && choice.Delta.Content != "" {
					streamed.WriteString(choice.Delta.Content)
				}
				if choice.Message.Role == model.RoleAssistant && choice.Message.Content != "" {
					if evt.IsRunnerCompletion() {
						completionText = choice.Message.Content
					} else {
						full = choice.Message.Content
					}
				}
			}
		}
		if evt.IsRunnerCompletion() {
			sawCompletion = true
			if evt.Error != nil && terminalErr == nil {
				terminalErr = collectResponseError(evt.Error)
			}
		}
	}
	if terminalErr != nil {
		return "", promptTokens, completionTokens, terminalErr
	}
	if !sawCompletion {
		return "", promptTokens, completionTokens, errors.New("runner event stream closed before completion")
	}
	if completionText != "" {
		return completionText, promptTokens, completionTokens, nil
	}
	if streamed.Len() > 0 {
		return streamed.String(), promptTokens, completionTokens, nil
	}
	return full, promptTokens, completionTokens, nil
}

func collectResponseError(responseError *model.ResponseError) error {
	if responseError == nil {
		return nil
	}
	if responseError.Type == budget.ErrorTypeLedgerUnavailable ||
		(responseError.Code != nil && *responseError.Code == budget.ErrorTypeLedgerUnavailable) {
		return budget.ErrLedgerUnavailable
	}
	if responseError.Message == "" {
		return errors.New("model response failed")
	}
	return errors.New(responseError.Message)
}

// MessageDedupKey derives the stable dedup key for an inbound message. It is
// shared by the in-process coordinator claims and the durable Inbox so both
// pipelines reject the same platform redeliveries.
func MessageDedupKey(msg domain.InboundMessage) string {
	return messageKey(msg)
}

func messageKey(msg domain.InboundMessage) string {
	h := sha256.Sum256([]byte(strings.Join([]string{msg.TenantID, msg.BindingID, msg.Channel, msg.ExternalMessageID}, "\x1f")))
	return hex.EncodeToString(h[:])
}

func (s *Service) writeReplayAudit(ctx context.Context, task Task, pending pendingResult, userID, sessionID string) error {
	if pending.AuditID == "" {
		pending.AuditID = auditlog.StableOperationAuditID(task.Tenant.TenantID, messageKey(task.Message), "allow", "allow")
	}
	if checker, ok := s.audit.(auditlog.ReplayChecker); ok {
		exists, err := checker.AuditExists(ctx, pending.AuditID)
		if err != nil {
			return err
		}
		if exists {
			return nil
		}
	} else {
		return nil
	}
	return s.writeAudit(ctx, auditEntryForTenant(auditlog.Entry{
		Timestamp: time.Now().UTC(), TenantID: task.Tenant.TenantID,
		Channel: task.Binding.Type, BindingID: task.Binding.BindingID,
		UserID: userID, SessionID: sessionID, AgentName: task.Tenant.App.AgentName,
		Decision: "allow", CostUSD: pending.CostUSD, AuditID: pending.AuditID,
		RequestID: pending.RequestID, ConfigRevision: task.ConfigRevision,
		ContentHash: auditlog.ContentHash(task.Message.Text),
	}, task.Tenant))
}

func (s *Service) writeAudit(ctx context.Context, entry auditlog.Entry) error {
	if s.audit == nil {
		return nil
	}
	entry.AuditID = auditlog.StableAuditID(entry)
	spanContext := trace.SpanContextFromContext(ctx)
	if spanContext.IsValid() {
		entry.TraceID = spanContext.TraceID().String()
	}
	var writeErr error
	if contextual, ok := s.audit.(auditlog.ContextSink); ok {
		writeErr = contextual.WriteContext(ctx, entry)
	} else {
		writeErr = s.audit.Write(entry)
	}
	if writeErr != nil {
		s.metrics.Add("audit_write_failures_total", "Audit records that could not be written.", 1, map[string]string{
			"tenant": entry.TenantID,
		})
		return writeErr
	}
	return nil
}

func auditEntryForTenant(entry auditlog.Entry, tenantConfig config.TenantConfig) auditlog.Entry {
	policy := tenantConfig.Audit
	entry.PolicySnapshot = &policy
	entry.SecretEnvNames = tenantConfig.SecretEnvNames()
	return entry
}

func mergeLabels(base map[string]string, key, value string) map[string]string {
	result := make(map[string]string, len(base)+1)
	for k, v := range base {
		result[k] = v
	}
	result[key] = value
	return result
}

func classifyProcessError(err error) error {
	if err == nil {
		return nil
	}
	var classified *ProcessFailure
	if errors.As(err, &classified) {
		return err
	}
	switch {
	case errors.Is(err, governance.ErrUserDenied),
		errors.Is(err, governance.ErrInputTooLarge),
		errors.Is(err, governance.ErrBudgetExceeded):
		// Retrying the same denied/oversized message cannot make it valid. A
		// monthly budget denial also outlives the Inbox attempt window.
		return WithProcessDisposition(err, ProcessTerminalIgnored, 0)
	case errors.Is(err, contentsafety.ErrBlocked), privacy.IsBlocked(err):
		return WithProcessDisposition(err, ProcessTerminalIgnored, 0)
	case errors.Is(err, governance.ErrRateLimited):
		// The current limiter is a fixed UTC-minute window. Delay until the
		// next window rather than exhausting all Inbox attempts in a few seconds.
		now := time.Now()
		retryAfter := now.Truncate(time.Minute).Add(time.Minute).Sub(now)
		if retryAfter < time.Second {
			retryAfter = time.Second
		}
		return WithProcessDisposition(err, ProcessRetryable, retryAfter)
	case errors.Is(err, governance.ErrRateLimiterUnavailable),
		errors.Is(err, budget.ErrLedgerUnavailable):
		return WithProcessDisposition(err, ProcessBlocked, time.Minute)
	case errors.Is(err, ErrFuturePipelineVersion),
		errors.Is(err, ErrTenantFrozen),
		errors.Is(err, ErrAtomicDatabaseMismatch),
		errors.Is(err, ErrAtomicCommitUnavailable),
		errors.Is(err, tooloperation.ErrOutcomeUnknown),
		errors.Is(err, tooloperation.ErrUnknownRequiresResolution),
		errors.Is(err, tooloperation.ErrOperationInProgress),
		errors.Is(err, tooloperation.ErrReplayRequired):
		// A mixed-version fleet or a temporarily mismatched Session binding may
		// have another compatible replica. Park with a visible delay instead of
		// permanently discarding the durable message on first contact.
		return WithProcessDisposition(err, ProcessBlocked, time.Minute)
	case errors.Is(err, sessionturn.ErrCorruptData),
		errors.Is(err, sessionturn.ErrInvalidRequest),
		errors.Is(err, sessionturn.ErrTurnAborted),
		errors.Is(err, sessionturn.ErrTurnScopeMismatch),
		errors.Is(err, sessionturn.ErrScopedStateUnsupported),
		errors.Is(err, tooloperation.ErrConflict),
		errors.Is(err, tooloperation.ErrInvalidRequest),
		errors.Is(err, tooloperation.ErrOperationRejected),
		errors.Is(err, ErrUnsupportedPipeline):
		// These are deterministic for the stable turn/message identity and need
		// repair or migration, not an automatic replay loop.
		return WithProcessDisposition(err, ProcessDeadLetter, 0)
	default:
		// Transient infrastructure failures and Session fencing/version conflicts
		// retain the historical retry behavior.
		return WithProcessDisposition(err, ProcessRetryable, 0)
	}
}

func resultLabel(err error) string {
	if err != nil {
		return "error"
	}
	return "success"
}

func errorType(err error) string {
	if err == nil {
		return ""
	}
	if privacy.IsBlocked(err) {
		return "privacy_blocked"
	}
	if errors.Is(err, privacy.ErrUnavailable) {
		return "privacy_unavailable"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, governance.ErrUserDenied) {
		return "permission_denied"
	}
	if errors.Is(err, governance.ErrRateLimited) {
		return "rate_limited"
	}
	if errors.Is(err, governance.ErrBudgetExceeded) {
		return "budget_exceeded"
	}
	if errors.Is(err, governance.ErrRateLimiterUnavailable) {
		return "rate_limiter_unavailable"
	}
	if errors.Is(err, budget.ErrLedgerUnavailable) {
		return "budget_ledger_unavailable"
	}
	if errors.Is(err, governance.ErrInputTooLarge) {
		return "input_too_large"
	}
	return "internal"
}

func (s *Service) applyPrivacy(ctx context.Context, task Task, text, phase, requestID, sessionID, userID string) (string, error) {
	mode := task.Tenant.Privacy.Input
	if phase == "output" {
		mode = task.Tenant.Privacy.Output
	}
	clean, changed, err := privacy.Apply(mode, text, task.Tenant)
	if changed || err != nil {
		decision := "redact"
		if err != nil {
			decision = "deny"
		}
		s.metrics.Add("privacy_decisions_total", "Tenant text privacy decisions.", 1, map[string]string{
			"tenant": task.Tenant.TenantID, "component": phase, "result": decision,
		})
		if auditErr := s.writeAudit(ctx, auditEntryForTenant(auditlog.Entry{
			AuditID: uuid.NewString(), Timestamp: time.Now().UTC(), TenantID: task.Tenant.TenantID,
			Channel: task.Message.Channel, UserID: userID, SessionID: sessionID,
			AgentName: task.Tenant.App.AgentName, Decision: decision, Reason: "privacy_" + phase,
			RequestID: requestID, ConfigRevision: task.Tenant.Version, ContentHash: auditlog.ContentHash(text),
		}, task.Tenant)); auditErr != nil {
			return "", auditErr
		}
	}
	return clean, err
}
