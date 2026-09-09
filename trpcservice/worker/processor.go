// Package worker connects durable gateway work to fenced tRPC-Agent runtimes.
package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
	servicelog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	servicemetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/policy"
	serviceruntime "github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessioncoord"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	servicetool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
)

// Processor executes one already-claimed Inbox message.
type Processor struct {
	WorkerID      string
	Inbox         idempotency.Store
	Coordinator   sessioncoord.LeaseCoordinator
	Writes        sessioncoord.WriteStore
	Runtimes      *serviceruntime.Manager
	Snapshots     gateway.SnapshotResolver
	Publisher     gateway.EventPublisher
	LeaseTTL      time.Duration
	RetryDelay    time.Duration
	Policy        *policy.Engine
	Audit         audit.Store
	Redactor      *servicelog.Redactor
	Telemetry     *servicemetrics.Telemetry
	Cancellations CancellationStore
	RunLimiter    RunLimiter
	QuotaWait     time.Duration
	activeMu      sync.Mutex
	active        map[string]context.CancelFunc
	canceled      map[string]bool
}

var errCanceledWhileWaitingForQuota = errors.New("worker: run canceled while waiting for tenant quota")
var errTenantQuotaBusy = errors.New("worker: tenant run quota is busy")

// RuntimeFactory creates tenant services and gates every session mutation with the current fence.
type PlatformStore interface {
	sessioncoord.WriteStore
	sessioncoord.FenceValidator
}

// CancellationStore is the durable cross-node cancellation intent queried by
// a Worker before and while it owns a run.
type CancellationStore interface {
	Requested(context.Context, string, string) bool
}

// TestRuntimeFactory constructs the deterministic fixture used by protocol and
// worker tests. Service entrypoints use RuntimeFactoryWithServices.
func TestRuntimeFactory(writes PlatformStore) serviceruntime.Factory {
	return RuntimeFactoryWithServices(writes, func(snapshot config.RuntimeSnapshot) (*storage.Services, error) {
		return storage.NewTestServices(snapshot.App().Storage)
	})
}

// ServicesFactory resolves the storage services owned by one immutable Bundle.
type ServicesFactory func(config.RuntimeSnapshot) (*storage.Services, error)

// ToolCatalogFactory resolves version-pinned external tools for one Bundle.
type ToolCatalogFactory func(config.RuntimeSnapshot) (*servicetool.Catalog, error)

// RuntimeFactoryWithServices builds fenced runtimes from an injected backend.
func RuntimeFactoryWithServices(writes PlatformStore, servicesFactory ServicesFactory) serviceruntime.Factory {
	return RuntimeFactoryWithServicesAndTools(writes, servicesFactory, nil)
}

// RuntimeFactoryWithServicesAndTools builds fenced storage and external tools
// as one immutable lifecycle unit.
func RuntimeFactoryWithServicesAndTools(writes PlatformStore, servicesFactory ServicesFactory, toolFactory ToolCatalogFactory) serviceruntime.Factory {
	return func(snapshot config.RuntimeSnapshot) (serviceruntime.Runtime, error) {
		if writes == nil || servicesFactory == nil {
			return nil, errors.New("worker: platform store and services factory are required")
		}
		services, err := servicesFactory(snapshot)
		if err != nil {
			return nil, err
		}
		fenced, err := sessioncoord.NewFencedSessionService(services.Session, writes)
		if err != nil {
			_ = services.Close()
			return nil, err
		}
		services.Session = fenced
		fencedMemory, err := sessioncoord.NewFencedMemoryService(services.Memory, writes)
		if err != nil {
			_ = services.Close()
			return nil, err
		}
		fencedArtifact, err := sessioncoord.NewFencedArtifactService(services.Artifact, writes)
		if err != nil {
			_ = services.Close()
			return nil, err
		}
		services.Memory = fencedMemory
		services.Artifact = fencedArtifact
		var catalog *servicetool.Catalog
		if toolFactory != nil {
			catalog, err = toolFactory(snapshot)
			if err != nil {
				_ = services.Close()
				return nil, err
			}
		}
		var externalTools map[string]trpctool.Tool
		var closeTools func() error
		if catalog != nil {
			externalTools = catalog.Tools()
			closeTools = catalog.Close
		}
		bundle, err := serviceruntime.NewBundleWithServicesAndTools(snapshot, services, externalTools, closeTools, serviceruntime.NewGovernancePlugin(snapshot.Audit().RedactFields))
		if err != nil {
			if catalog != nil {
				_ = catalog.Close()
			}
			_ = services.Close()
			return nil, err
		}
		return bundle, nil
	}
}

// Process follows inbox -> event/state -> summary/memory -> outbox -> inbox complete.
func (processor *Processor) Process(ctx context.Context, request gateway.RunRequest) (runErr error) {
	if err := processor.validate(request); err != nil {
		return err
	}
	ctx = processor.Telemetry.Extract(ctx, request.TraceContext)
	ctx, runSpan := processor.Telemetry.Start(ctx, "worker.run", processor.spanFields(request))
	defer runSpan.End()
	started := time.Now()
	completed := false
	var projection *eventProjection
	governanceDecision := "allow"
	redact := processor.redact
	auditEnabled, auditStoreContent, auditFailClosed := false, false, false
	auditAppended := false
	tenantRedactor := servicelog.NewRedactor(nil, nil)
	defer func() {
		decision, errorType := governanceDecision, ""
		details := map[string]any{}
		if runErr != nil {
			if decision == "allow" {
				decision = "error"
			}
			errorType = fmt.Sprintf("%T", runErr)
			if auditStoreContent {
				details["error"] = redact(runErr.Error())
			}
		}
		tokens := int64(0)
		costMicros := int64(0)
		toolName := ""
		if projection != nil {
			tokens = int64(projection.totalTokens)
			costMicros = projection.costMicros
			toolName = projection.lastTool
			if projection.pricingVersion != "" {
				details["pricing_version"] = projection.pricingVersion
				if projection.usageObserved {
					details["cost_basis"] = "provider_usage"
				} else {
					details["cost_basis"] = "reserved_estimate"
				}
			}
		}
		processor.Telemetry.Request(ctx, servicemetrics.Labels{TenantID: request.TenantID, AppID: request.AppID, Channel: request.BindingID, Operation: "runner", Status: decision}, time.Since(started), tokens, costMicros)
		if auditEnabled && processor.Audit != nil && !auditAppended {
			auditErr := processor.appendAudit(request, tenantRedactor, decision, errorType, details, time.Since(started), toolName, costMicros)
			if auditErr != nil {
				processor.Telemetry.Request(ctx, servicemetrics.Labels{TenantID: request.TenantID, AppID: request.AppID, Channel: request.BindingID, Operation: "audit", Status: "failed"}, 0, 0, 0)
			}
		}
		if projection != nil && projection.pricingVersion != "" && !projection.usageObserved {
			processor.Telemetry.ModelUsageMissing(ctx, servicemetrics.Labels{TenantID: request.TenantID, AppID: request.AppID, Channel: request.BindingID, Operation: "model", Status: "missing"})
		}
	}()
	defer func() {
		if runErr != nil {
			// A stale stream copy is expected after another node reclaims the
			// Inbox/session lease. It must not overwrite the new owner's shared
			// status. Other recoverable failures remain non-terminal while the
			// Inbox schedules a retry; policy rejection is terminal.
			if errors.Is(runErr, idempotency.ErrClaimOwner) || errors.Is(runErr, sessioncoord.ErrStaleFence) {
				return
			}
			eventType, terminal := "run.retrying", false
			if completed {
				eventType, terminal = "run.error", true
			}
			processor.publish(request, gateway.RunEvent{Type: eventType, RequestID: request.InboxID, SessionID: request.SessionID, TraceID: request.TraceID, Error: redact(runErr.Error()), Terminal: terminal})
		}
	}()
	claim := idempotency.Claim{InboxID: request.InboxID, Owner: request.ClaimOwner, ClaimToken: request.ClaimToken, Attempt: request.ClaimAttempt, InboxSeq: request.InboxSeq, LeaseUntil: request.ClaimLeaseUntil, Message: gateway.InboundMessage{TenantID: request.TenantID, BindingID: request.BindingID, ExternalMessageID: request.ExternalMessageID}}
	// A request can wait behind earlier work from the same session long enough
	// for its Inbox lease to be reclaimed. Validate and extend ownership before
	// acquiring the session lease so a stale queued copy never calls the model or
	// a tool after another node has taken over the durable message.
	claim, err := processor.Inbox.Renew(ctx, claim, processor.leaseTTL())
	if err != nil {
		return err
	}
	request.ClaimLeaseUntil = claim.LeaseUntil
	defer func() {
		if !completed && runErr != nil {
			failCtx, cancelFail := context.WithTimeout(context.Background(), 2*time.Second)
			_ = processor.Inbox.Fail(failCtx, claim, errors.New(redact(runErr.Error())), time.Now().UTC().Add(processor.retryDelay()))
			cancelFail()
		}
	}()
	if processor.cancellationRequested(ctx, request) {
		if err := processor.Inbox.Cancel(ctx, claim); err != nil {
			return err
		}
		completed = true
		processor.publish(request, gateway.RunEvent{Type: "run.canceled", RequestID: request.InboxID, SessionID: request.SessionID, TraceID: request.TraceID, Terminal: true})
		return nil
	}
	snapshot, err := processor.Snapshots.Resolve(ctx, request.TenantID, request.AppID, request.ConfigVersion)
	if err != nil {
		return err
	}
	quotaStarted := time.Now()
	permit, claim, err := processor.waitForRunPermit(ctx, request, claim, snapshot.Runtime().ConcurrentRunLimit())
	quotaStatus := "success"
	if errors.Is(err, errTenantQuotaBusy) {
		quotaStatus = "deferred"
	} else if err != nil {
		quotaStatus = "failed"
	}
	processor.observeOperationStatus(ctx, request, "tenant_run_quota", quotaStatus, quotaStarted)
	if errors.Is(err, errCanceledWhileWaitingForQuota) {
		if cancelErr := processor.Inbox.Cancel(context.Background(), claim); cancelErr != nil {
			return cancelErr
		}
		completed = true
		processor.publish(request, gateway.RunEvent{Type: "run.canceled", RequestID: request.InboxID, SessionID: request.SessionID, TraceID: request.TraceID, Terminal: true})
		return nil
	}
	if errors.Is(err, errTenantQuotaBusy) {
		if deferErr := processor.Inbox.Defer(ctx, claim, time.Now().UTC().Add(processor.retryDelay())); deferErr != nil {
			return deferErr
		}
		processor.publish(request, gateway.RunEvent{Type: "run.queued", RequestID: request.InboxID, SessionID: request.SessionID, TraceID: request.TraceID, Stage: "tenant_quota"})
		return nil
	}
	if err != nil {
		return err
	}
	if processor.RunLimiter != nil {
		defer processor.RunLimiter.Release(permit)
	}
	leaseCtx, leaseSpan := processor.Telemetry.Start(ctx, "session.lease", processor.spanFields(request))
	leaseStarted := time.Now()
	lease, err := processor.Coordinator.Acquire(leaseCtx, request.Key(), processor.WorkerID, processor.leaseTTL())
	leaseSpan.End()
	processor.observeOperation(leaseCtx, request, "session_lease", leaseStarted, err)
	if err != nil {
		return err
	}
	defer processor.Coordinator.Release(lease)
	turnOrderStarted := time.Now()
	err = processor.Writes.ValidateTurn(ctx, request.Key(), request.InboxSeq)
	processor.observeOperation(ctx, request, "session_turn_order", turnOrderStarted, err)
	if errors.Is(err, sessioncoord.ErrOutOfOrder) {
		if deferErr := processor.Inbox.Defer(ctx, claim, time.Now().UTC().Add(processor.retryDelay())); deferErr != nil {
			return deferErr
		}
		processor.publish(request, gateway.RunEvent{Type: "run.queued", RequestID: request.InboxID, SessionID: request.SessionID, TraceID: request.TraceID, Stage: "session_order"})
		return nil
	}
	if err != nil {
		return err
	}
	auditPolicy := snapshot.Audit()
	auditEnabled, auditStoreContent, auditFailClosed = auditPolicy.Enabled, auditPolicy.StoreContent, auditPolicy.FailClosed
	if auditEnabled && auditFailClosed && processor.Audit == nil {
		return errors.New("worker: fail-closed audit store is unavailable")
	}
	tenantRedactor = servicelog.NewRedactor(auditPolicy.RedactFields, nil)
	redact = func(value string) string { return tenantRedactor.RedactString(processor.redact(value)) }
	estimatedTokens := int64(len(request.Text)/4 + 1)
	binding, ok := channelBinding(snapshot, request.BindingID)
	if !ok {
		governanceDecision = "deny"
		if rejectErr := processor.Inbox.Reject(context.Background(), claim); rejectErr != nil {
			return errors.Join(policy.ErrIdentityDenied, rejectErr)
		}
		completed = true
		return policy.ErrIdentityDenied
	}
	app := snapshot.App()
	estimatedCost := policy.EstimateModelCost(app.Model.Pricing, estimatedTokens, int64(app.Model.MaxTokens))
	policyRequest := policy.Request{TenantID: request.TenantID, AppID: request.AppID, UserID: request.UserID, RequestID: request.InboxID, ExternalUserID: request.ExternalUserID, ConversationID: request.ConversationID, AllowedUsers: binding.AllowedUsers, AllowedChats: binding.AllowedChats, Policy: app.Tools, EstimatedTokens: estimatedTokens + int64(app.Model.MaxTokens), EstimatedCostMicros: estimatedCost}
	controls, err := processor.Policy.Evaluate(ctx, policyRequest)
	if err != nil {
		if isGovernanceDenial(err) {
			governanceDecision = "deny"
			if rejectErr := processor.Inbox.Reject(context.Background(), claim); rejectErr != nil {
				return errors.Join(err, rejectErr)
			}
			completed = true
		}
		return err
	}
	runtimeLease, err := processor.Runtimes.Acquire(snapshot)
	if err != nil {
		return err
	}
	defer runtimeLease.Release()
	ownershipStarted := time.Now()
	lease, err = processor.Coordinator.Renew(ctx, lease, processor.leaseTTL())
	if err == nil {
		claim, err = processor.Inbox.Renew(ctx, claim, processor.leaseTTL())
	}
	if err == nil && processor.RunLimiter != nil {
		err = processor.RunLimiter.Renew(ctx, permit, processor.leaseTTL())
	}
	processor.observeOperation(ctx, request, "execution_ownership_preflight", ownershipStarted, err)
	if err != nil {
		return fmt.Errorf("worker: execution ownership preflight: %w", err)
	}
	processor.publish(request, gateway.RunEvent{Type: "run.started", RequestID: request.InboxID, SessionID: request.SessionID, TraceID: request.TraceID})
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	runCtx = policy.WithRequest(runCtx, processor.Policy, policyRequest)
	runCtx = servicelog.WithRedactor(runCtx, tenantRedactor)
	runCtx = servicemetrics.WithTelemetry(runCtx, processor.Telemetry, processor.spanFields(request))
	projection = &eventProjection{publisher: processor.Publisher, request: request, workerID: processor.WorkerID, redact: redact, costMicros: estimatedCost, pricingVersion: app.Model.Pricing.Version}
	executionStore, _ := processor.Inbox.(idempotency.ExecutionStore)
	recovered := false
	if executionStore != nil {
		execution, readErr := executionStore.GetExecution(ctx, claim)
		if readErr != nil {
			return fmt.Errorf("worker: recover execution state: %w", readErr)
		}
		if execution.Stage != idempotency.ExecutionNone && execution.Reply != "" {
			projection.reply = execution.Reply
			recovered = true
		}
	}
	if !recovered {
		if reader, ok := processor.Writes.(sessioncoord.CommittedTurnReader); ok {
			committed, found, readErr := reader.CommittedTurn(ctx, request.Key(), request.InboxID)
			if readErr != nil {
				return fmt.Errorf("worker: recover committed turn: %w", readErr)
			}
			if found {
				projection.reply = committed.Payload
				recovered = true
			}
		}
	}
	projection.onUsage = func(promptTokens, completionTokens int64) error {
		actualTokens := promptTokens + completionTokens
		actualCost := policy.EstimateModelCost(app.Model.Pricing, promptTokens, completionTokens)
		projection.costMicros = actualCost
		projection.usageObserved = true
		return processor.Policy.Reconcile(runCtx, policyRequest, actualTokens, actualCost)
	}
	projection.cancel = cancelRun
	processor.register(request.TenantID, request.InboxID, cancelRun)
	defer processor.unregister(request.TenantID, request.InboxID)
	renewDone := make(chan struct{})
	renewFailure := make(chan error, 1)
	go processor.renew(runCtx, cancelRun, request, lease, claim, permit, renewDone, renewFailure)
	if !recovered {
		runnerCtx, runnerSpan := processor.Telemetry.Start(sessioncoord.WithLease(runCtx, lease), "runner.execute", processor.spanFields(request))
		runInput := serviceruntime.RunInput{RequestID: request.InboxID, UserID: request.UserID, SessionID: request.SessionID, Text: request.Text, Attachments: request.Attachments, Observer: projection.Observe, ToolFilter: controls.Visibility, ToolExecutionFilter: controls.Execution, ToolPermissionPolicy: controls.Permission}
		_, err = runtimeLease.Runtime.Run(runnerCtx, runInput)
		if err == nil && projection.pendingTool != "" {
			processor.publish(request, gateway.RunEvent{Type: "run.approval_required", RequestID: request.InboxID, SessionID: request.SessionID, TraceID: request.TraceID, Stage: "approval_required", ToolName: projection.pendingTool, ToolCallID: projection.pendingCall})
			if approvalErr := processor.Policy.WaitApproval(runnerCtx, policyRequest, projection.pendingTool); approvalErr != nil {
				err = approvalErr
			} else if resumer, ok := runtimeLease.Runtime.(serviceruntime.ToolResumer); !ok {
				err = errors.New("worker: runtime does not support approved tool resume")
			} else {
				_, err = resumer.ResumeTool(runnerCtx, serviceruntime.ToolResume{Input: runInput, ToolName: projection.pendingTool, ToolCallID: projection.pendingCall, Arguments: projection.pendingArgs})
			}
		}
		if projection.policyErr != nil {
			err = errors.Join(err, projection.policyErr)
		}
		if err == nil && executionStore != nil && projection.reply != "" {
			if saveErr := executionStore.SaveExecution(ctx, claim, idempotency.ExecutionRecord{Stage: idempotency.ExecutionRunnerCommitted, Reply: projection.reply, EventID: "agent:" + request.InboxID}); saveErr != nil {
				err = fmt.Errorf("worker: persist runner result: %w", saveErr)
			}
		}
		runnerSpan.End()
	} else {
		processor.publish(request, gateway.RunEvent{Type: "run.recovered", RequestID: request.InboxID, SessionID: request.SessionID, TraceID: request.TraceID})
	}
	cancelRun()
	<-renewDone
	select {
	case renewErr := <-renewFailure:
		if renewErr != nil {
			err = errors.Join(err, renewErr)
		}
	default:
	}
	if err != nil {
		if isGovernanceDenial(err) {
			governanceDecision = "deny"
			if rejectErr := processor.Inbox.Reject(context.Background(), claim); rejectErr != nil {
				return errors.Join(err, rejectErr)
			}
			completed = true
			return err
		}
		if errors.Is(err, context.Canceled) && processor.takeCanceled(request.TenantID, request.InboxID) {
			if cancelErr := processor.Inbox.Cancel(context.Background(), claim); cancelErr != nil {
				return cancelErr
			}
			completed = true
			processor.publish(request, gateway.RunEvent{Type: "run.canceled", RequestID: request.InboxID, SessionID: request.SessionID, TraceID: request.TraceID, Terminal: true})
			return nil
		}
		return err
	}
	reply := projection.reply
	if reply == "" {
		return errors.New("worker: runner produced no final message")
	}
	reply = processor.redact(reply)
	eventID := "agent:" + request.InboxID
	writeCtx, writeSpan := processor.Telemetry.Start(ctx, "session.write", processor.spanFields(request))
	writeStarted := time.Now()
	seq, err := processor.Writes.CommitTurn(writeCtx, sessioncoord.TurnWrite{Key: request.Key(), Fence: lease.Token, InboxSeq: request.InboxSeq, InboxID: request.InboxID, EventID: eventID, EventType: "assistant", Payload: reply, TraceID: request.TraceID, StateDelta: map[string]string{"last_inbox_id": request.InboxID}})
	writeSpan.End()
	processor.observeOperation(writeCtx, request, "session_write", writeStarted, err)
	if err != nil {
		return err
	}
	derivedCtx, derivedSpan := processor.Telemetry.Start(ctx, "memory.summary.write", processor.spanFields(request))
	derivedStarted := time.Now()
	if err := processor.Writes.PublishSummary(derivedCtx, request.Key(), lease.Token, sessioncoord.Summary{Version: seq, CutoffEventSeq: seq, Content: reply}); err != nil {
		derivedSpan.End()
		processor.observeOperation(derivedCtx, request, "memory_summary", derivedStarted, err)
		return err
	}
	if err := processor.Writes.UpsertMemory(derivedCtx, request.Key(), lease.Token, sessioncoord.Memory{MemoryID: "memory:" + eventID, SourceEventID: eventID, SourceEventSeq: seq, Version: 1, Status: "active", Content: reply}); err != nil {
		derivedSpan.End()
		processor.observeOperation(derivedCtx, request, "memory_summary", derivedStarted, err)
		return err
	}
	derivedSpan.End()
	processor.observeOperation(derivedCtx, request, "memory_summary", derivedStarted, nil)
	if executionStore != nil {
		if saveErr := executionStore.SaveExecution(ctx, claim, idempotency.ExecutionRecord{Stage: idempotency.ExecutionDerivedCommitted, Reply: reply, EventID: eventID}); saveErr != nil {
			return fmt.Errorf("worker: persist derived execution state: %w", saveErr)
		}
	}
	if auditEnabled && processor.Audit != nil {
		if auditErr := processor.appendAudit(request, tenantRedactor, governanceDecision, "", map[string]any{"cost_basis": "reserved_estimate"}, time.Since(started), projection.lastTool, projection.costMicros); auditErr != nil {
			processor.Telemetry.Request(ctx, servicemetrics.Labels{TenantID: request.TenantID, AppID: request.AppID, Channel: request.BindingID, Operation: "audit", Status: "failed"}, 0, 0, 0)
			if auditFailClosed {
				return fmt.Errorf("worker: audit persistence failed: %w", auditErr)
			}
		} else {
			auditAppended = true
		}
	}
	outboxCtx, outboxSpan := processor.Telemetry.Start(ctx, "outbox.write", processor.spanFields(request))
	outbound := gateway.OutboundMessage{TenantID: request.TenantID, AppID: request.AppID, BindingID: request.BindingID, ConfigVersion: request.ConfigVersion, OutboxID: "outbox:" + request.InboxID, DedupeKey: "reply:" + request.InboxID, UserID: request.UserID, SessionID: request.SessionID, ExternalUserID: request.ExternalUserID, ConversationID: request.ConversationID, Text: reply, ReplyFormat: replyFormat(snapshot, request.BindingID), TraceID: request.TraceID, TraceContext: processor.Telemetry.Inject(outboxCtx), SourceInboxID: request.InboxID, SourceEventID: eventID}
	outboxStarted := time.Now()
	if err := processor.Writes.PublishOutbox(outboxCtx, request.Key(), lease.Token, outbound); err != nil {
		outboxSpan.End()
		processor.observeOperation(outboxCtx, request, "outbox_write", outboxStarted, err)
		return err
	}
	outboxSpan.End()
	processor.observeOperation(outboxCtx, request, "outbox_write", outboxStarted, nil)
	if executionStore != nil {
		if saveErr := executionStore.SaveExecution(ctx, claim, idempotency.ExecutionRecord{Stage: idempotency.ExecutionOutboxCommitted, Reply: reply, EventID: eventID}); saveErr != nil {
			return fmt.Errorf("worker: persist outbox execution state: %w", saveErr)
		}
	}
	if err := processor.Inbox.Complete(ctx, claim); err != nil {
		return err
	}
	completed = true
	processor.publish(request, gateway.RunEvent{Type: "run.completed", RequestID: request.InboxID, SessionID: request.SessionID, TraceID: request.TraceID, Message: reply, Terminal: true})
	return nil
}

func (processor *Processor) appendAudit(request gateway.RunRequest, redactor *servicelog.Redactor, decision, errorType string, details map[string]any, latency time.Duration, toolName string, costMicros int64) error {
	if processor == nil || processor.Audit == nil {
		return errors.New("worker: audit store is unavailable")
	}
	record := audit.Record{TenantID: request.TenantID, Channel: request.BindingID, UserID: request.UserID, SessionID: request.SessionID, AgentName: request.AppID, ToolName: toolName, Decision: decision, Latency: latency, ErrorType: errorType, CostMicros: costMicros, ConfigVersion: request.ConfigVersion, PolicyVersion: request.ConfigVersion, TraceID: request.TraceID, RequestID: request.InboxID, Details: details}
	if redactor != nil {
		record.Channel = redactor.RedactField("channel", record.Channel)
		record.UserID = redactor.RedactField("user_id", record.UserID)
		record.SessionID = redactor.RedactField("session_id", record.SessionID)
		record.ToolName = redactor.RedactField("tool_name", record.ToolName)
	}
	auditCtx, cancelAudit := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelAudit()
	return processor.Audit.Append(auditCtx, record)
}

func isGovernanceDenial(err error) bool {
	return errors.Is(err, policy.ErrIdentityDenied) || errors.Is(err, policy.ErrBudgetExceeded) || errors.Is(err, policy.ErrToolDenied)
}

func replyFormat(snapshot config.RuntimeSnapshot, bindingID string) string {
	if binding, ok := channelBinding(snapshot, bindingID); ok {
		return binding.ReplyFormat
	}
	return ""
}

func channelBinding(snapshot config.RuntimeSnapshot, bindingID string) (tenant.ChannelBinding, bool) {
	for _, binding := range snapshot.App().Channels {
		if binding.ID == bindingID && binding.Enabled {
			return binding, true
		}
	}
	return tenant.ChannelBinding{}, false
}

// Cancel cancels an active Runner request. Queued cancellation belongs to the durable queue.
func (processor *Processor) Cancel(tenantID, requestID string) bool {
	key := activeRequestKey(tenantID, requestID)
	processor.activeMu.Lock()
	cancel := processor.active[key]
	if cancel != nil {
		if processor.canceled == nil {
			processor.canceled = make(map[string]bool)
		}
		processor.canceled[key] = true
	}
	processor.activeMu.Unlock()
	if cancel == nil {
		return false
	}
	cancel()
	return true
}

func (processor *Processor) markCanceled(tenantID, requestID string) {
	processor.activeMu.Lock()
	if processor.canceled == nil {
		processor.canceled = make(map[string]bool)
	}
	processor.canceled[activeRequestKey(tenantID, requestID)] = true
	processor.activeMu.Unlock()
}
func (processor *Processor) register(tenantID, requestID string, cancel context.CancelFunc) {
	processor.activeMu.Lock()
	if processor.active == nil {
		processor.active = make(map[string]context.CancelFunc)
	}
	processor.active[activeRequestKey(tenantID, requestID)] = cancel
	processor.activeMu.Unlock()
}
func (processor *Processor) unregister(tenantID, requestID string) {
	processor.activeMu.Lock()
	delete(processor.active, activeRequestKey(tenantID, requestID))
	processor.activeMu.Unlock()
}
func (processor *Processor) takeCanceled(tenantID, requestID string) bool {
	key := activeRequestKey(tenantID, requestID)
	processor.activeMu.Lock()
	defer processor.activeMu.Unlock()
	value := processor.canceled[key]
	delete(processor.canceled, key)
	return value
}

func activeRequestKey(tenantID, requestID string) string {
	return tenantID + "\x00" + requestID
}

func (processor *Processor) validate(request gateway.RunRequest) error {
	if processor == nil || processor.WorkerID == "" || processor.Inbox == nil || processor.Coordinator == nil || processor.Writes == nil || processor.Runtimes == nil || processor.Snapshots == nil || processor.Policy == nil {
		return errors.New("worker: processor dependencies are incomplete")
	}
	if request.InboxID == "" || request.InboxSeq == 0 || request.TenantID == "" || request.AppID == "" || request.BindingID == "" || request.ExternalMessageID == "" || request.UserID == "" || request.SessionID == "" || request.Text == "" || request.ClaimOwner == "" || request.ClaimToken == "" || request.ConfigVersion == 0 {
		return errors.New("worker: request routing, claim, config version, and text are required")
	}
	return nil
}

func (processor *Processor) leaseTTL() time.Duration {
	if processor.LeaseTTL > 0 {
		return processor.LeaseTTL
	}
	return 30 * time.Second
}
func (processor *Processor) retryDelay() time.Duration {
	if processor.RetryDelay > 0 {
		return processor.RetryDelay
	}
	return time.Second
}
func (processor *Processor) quotaWait() time.Duration {
	if processor.QuotaWait > 0 {
		return processor.QuotaWait
	}
	return 500 * time.Millisecond
}
func (processor *Processor) publish(request gateway.RunRequest, event gateway.RunEvent) {
	if processor.Publisher != nil {
		event.TenantID = request.TenantID
		event.BindingID = request.BindingID
		event.WorkerID = processor.WorkerID
		processor.Publisher.Publish(event)
	}
}

func (processor *Processor) waitForRunPermit(ctx context.Context, request gateway.RunRequest, claim idempotency.Claim, limit int) (RunPermit, idempotency.Claim, error) {
	if processor.RunLimiter == nil {
		return RunPermit{}, claim, nil
	}
	ttl := processor.leaseTTL()
	retryEvery := 200 * time.Millisecond
	if ttl/6 < retryEvery {
		retryEvery = ttl / 6
	}
	if retryEvery <= 0 {
		retryEvery = time.Millisecond
	}
	nextRenew := time.Now().Add(ttl / 3)
	deadline := time.Now().Add(processor.quotaWait())
	for {
		permit, acquired, err := processor.RunLimiter.TryAcquire(ctx, request.TenantID, request.InboxID, request.ClaimToken, limit, ttl)
		if err != nil {
			return RunPermit{}, claim, err
		}
		if acquired {
			return permit, claim, nil
		}
		if processor.cancellationRequested(ctx, request) {
			return RunPermit{}, claim, errCanceledWhileWaitingForQuota
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return RunPermit{}, claim, errTenantQuotaBusy
		}
		wait := min(retryEvery, remaining)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return RunPermit{}, claim, ctx.Err()
		case <-timer.C:
		}
		if !time.Now().Before(nextRenew) {
			claim, err = processor.Inbox.Renew(ctx, claim, ttl)
			if err != nil {
				return RunPermit{}, claim, fmt.Errorf("worker: renew inbox while waiting for tenant quota: %w", err)
			}
			nextRenew = time.Now().Add(ttl / 3)
		}
	}
}

func (processor *Processor) renew(ctx context.Context, cancel context.CancelFunc, request gateway.RunRequest, lease sessioncoord.Lease, claim idempotency.Claim, permit RunPermit, done chan<- struct{}, failure chan<- error) {
	defer close(done)
	interval := processor.leaseTTL() / 3
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if processor.cancellationRequested(ctx, request) {
				processor.markCanceled(request.TenantID, request.InboxID)
				cancel()
				return
			}
			var err error
			lease, err = processor.Coordinator.Renew(ctx, lease, processor.leaseTTL())
			if err != nil {
				failure <- fmt.Errorf("worker: renew session lease: %w", err)
				cancel()
				return
			}
			claim, err = processor.Inbox.Renew(ctx, claim, processor.leaseTTL())
			if err != nil {
				failure <- fmt.Errorf("worker: renew inbox claim: %w", err)
				cancel()
				return
			}
			if processor.RunLimiter != nil {
				if err := processor.RunLimiter.Renew(ctx, permit, processor.leaseTTL()); err != nil {
					failure <- fmt.Errorf("worker: renew tenant run quota: %w", err)
					cancel()
					return
				}
			}
		}
	}
}

func (processor *Processor) cancellationRequested(ctx context.Context, request gateway.RunRequest) bool {
	return processor.Cancellations != nil && processor.Cancellations.Requested(ctx, request.TenantID, request.InboxID)
}

type eventProjection struct {
	publisher        gateway.EventPublisher
	request          gateway.RunRequest
	workerID         string
	reply            string
	lastTool         string
	totalTokens      int
	promptTokens     int
	completionTokens int
	costMicros       int64
	pricingVersion   string
	usageObserved    bool
	redact           func(string) string
	pendingTool      string
	pendingCall      string
	pendingArgs      []byte
	onUsage          func(int64, int64) error
	cancel           context.CancelFunc
	policyErr        error
	usageByResponse  map[string]usageTotals
}

type usageTotals struct{ prompt, completion int }

func (projection *eventProjection) Observe(item *event.Event) {
	if item == nil || item.Response == nil {
		return
	}
	base := gateway.RunEvent{TenantID: projection.request.TenantID, BindingID: projection.request.BindingID, WorkerID: projection.workerID, RequestID: projection.request.InboxID, SessionID: projection.request.SessionID, TraceID: projection.request.TraceID}
	if item.Usage != nil {
		base.PromptTokens = item.Usage.PromptTokens
		base.CompletionTokens = item.Usage.CompletionTokens
		base.TotalTokens = item.Usage.TotalTokens
		if projection.usageByResponse == nil {
			projection.usageByResponse = make(map[string]usageTotals)
		}
		usageKey := item.Response.ID
		if usageKey == "" {
			usageKey = "<unknown-response>"
		}
		current := usageTotals{prompt: item.Usage.PromptTokens, completion: item.Usage.CompletionTokens}
		previous := projection.usageByResponse[usageKey]
		if current.prompt >= previous.prompt && current.completion >= previous.completion {
			projection.promptTokens += current.prompt - previous.prompt
			projection.completionTokens += current.completion - previous.completion
			projection.usageByResponse[usageKey] = current
		}
		projection.totalTokens = projection.promptTokens + projection.completionTokens
		if (item.Usage.PromptTokens > 0 || item.Usage.CompletionTokens > 0) && projection.onUsage != nil && projection.policyErr == nil {
			if err := projection.onUsage(int64(projection.promptTokens), int64(projection.completionTokens)); err != nil {
				projection.policyErr = err
				if projection.cancel != nil {
					projection.cancel()
				}
			}
		}
	}
	if item.Response.IsToolCallResponse() {
		base.Type, base.Stage, base.ToolStatus = "run.progress", "running_tool", "running"
		for _, choice := range item.Choices {
			calls := choice.Message.ToolCalls
			if len(calls) == 0 {
				calls = choice.Delta.ToolCalls
			}
			if len(calls) > 0 {
				base.ToolName = calls[0].Function.Name
				base.ToolCallID = calls[0].ID
				projection.pendingTool = base.ToolName
				projection.pendingCall = base.ToolCallID
				projection.pendingArgs = append([]byte(nil), calls[0].Function.Arguments...)
				break
			}
		}
		projection.publish(base)
		projection.lastTool = base.ToolName
		return
	}
	if item.Response.IsToolResultResponse() {
		projection.pendingTool, projection.pendingCall, projection.pendingArgs = "", "", nil
		base.Type, base.Stage, base.ToolStatus = "run.progress", "tool_completed", "completed"
		for _, choice := range item.Choices {
			if choice.Message.ToolID != "" {
				base.ToolCallID = choice.Message.ToolID
				break
			}
			if choice.Delta.ToolID != "" {
				base.ToolCallID = choice.Delta.ToolID
				break
			}
		}
		projection.publish(base)
		return
	}
	if item.IsError() {
		base.Type = "run.progress"
		base.Stage = "runner_error"
		if item.Error != nil {
			base.Error = item.Error.Error()
		}
		projection.publish(base)
		return
	}
	for _, choice := range item.Choices {
		delta := choice.Delta.Content
		if item.Object != model.ObjectTypeChatCompletionChunk || strings.TrimSpace(delta) == "" {
			delta = choice.Message.Content
		}
		delta = strings.TrimSpace(delta)
		if delta == "" {
			continue
		}
		if item.Response.IsFinalResponse() {
			projection.reply = delta
			base.Type = "message.completed"
			base.Message = delta
		} else {
			base.Type = "message.delta"
			base.Delta = delta
		}
		projection.publish(base)
	}
}
func (projection *eventProjection) publish(event gateway.RunEvent) {
	if projection.redact != nil {
		event.Delta = projection.redact(event.Delta)
		event.Message = projection.redact(event.Message)
		event.Error = projection.redact(event.Error)
	}
	if projection.publisher != nil {
		projection.publisher.Publish(event)
	}
}

func (processor *Processor) redact(value string) string {
	if processor == nil || processor.Redactor == nil {
		return servicelog.NewRedactor(nil, nil).RedactString(value)
	}
	return processor.Redactor.RedactString(value)
}
func (processor *Processor) spanFields(request gateway.RunRequest) servicemetrics.SpanFields {
	return servicemetrics.SpanFields{TenantID: request.TenantID, AppID: request.AppID, Channel: request.BindingID, RequestID: request.InboxID, TraceID: request.TraceID}
}
func (processor *Processor) observeOperation(ctx context.Context, request gateway.RunRequest, operation string, started time.Time, err error) {
	status := "success"
	if err != nil {
		status = "failed"
	}
	processor.observeOperationStatus(ctx, request, operation, status, started)
}

func (processor *Processor) observeOperationStatus(ctx context.Context, request gateway.RunRequest, operation, status string, started time.Time) {
	processor.Telemetry.Request(ctx, servicemetrics.Labels{TenantID: request.TenantID, AppID: request.AppID, Channel: request.BindingID, Operation: operation, Status: status}, time.Since(started), 0, 0)
}
