// Package worker consumes durable Agent tasks and records their outcomes.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	agentruntime "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/approval"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/background"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
)

// Runtime is the tenant-scoped Agent execution boundary used by a Worker.
type Runtime interface {
	ChatWithScope(ctx context.Context, input agentruntime.ChatInput) (agentruntime.ChatResult, error)
}

type Options struct {
	WorkerID    string
	MaxAttempts int
	RetryDelay  time.Duration
	Audit       audit.Writer
	Metrics     *platformmetrics.Recorder
	Approvals   approval.Repository
	Jobs        background.Repository
	Quota       *tenant.Guard
}

// Worker processes at-least-once queue deliveries. Durable idempotency makes
// reprocessing safe after completion-before-ack crashes.
type Worker struct {
	queue   workqueue.Queue
	journal gateway.Journal
	runtime Runtime
	opts    Options
}

func New(
	queue workqueue.Queue,
	journal gateway.Journal,
	runtime Runtime,
	opts Options,
) (*Worker, error) {
	if queue == nil || journal == nil || runtime == nil {
		return nil, fmt.Errorf("Worker queue, journal and runtime are required")
	}
	if opts.WorkerID == "" || opts.MaxAttempts <= 0 || opts.RetryDelay <= 0 {
		return nil, fmt.Errorf("Worker options are invalid")
	}
	return &Worker{queue: queue, journal: journal, runtime: runtime, opts: opts}, nil
}

// ProcessOne receives and handles a single delivery.
func (w *Worker) ProcessOne(ctx context.Context) (bool, error) {
	delivery, err := w.queue.Receive(ctx)
	if err != nil {
		if errors.Is(err, workqueue.ErrNoMessage) {
			return false, nil
		}
		return false, err
	}
	task := delivery.Task()
	ctx = taskContext(ctx, task)
	ctx, span := otel.Tracer("trpc-agent-service/worker").Start(ctx, "worker.agent.run")
	span.SetAttributes(
		attribute.String("tenant.id", task.Scope.TenantID),
		attribute.String("agent.app.id", task.Scope.AppID),
		attribute.String("agent.revision.id", task.Scope.RevisionID),
		attribute.String("messaging.message.id", task.MessageID),
		attribute.String("gen_ai.request.id", task.RequestID),
	)
	defer span.End()
	started := time.Now()
	if err := task.Scope.Validate(); err != nil {
		_ = delivery.Ack(ctx)
		return true, fmt.Errorf("reject invalid task scope: %w", err)
	}
	if err := w.journal.MarkRunRunning(ctx, task.RequestID, w.opts.WorkerID); err != nil {
		return true, w.retryOrAck(ctx, delivery, task, err)
	}
	releaseQuota := func() {}
	if w.opts.Quota != nil {
		release, quotaErr := w.opts.Quota.AcquireRun(ctx, task.Scope.TenantID)
		if quotaErr != nil {
			failErr := w.journal.FailRun(ctx, task.RequestID, "tenant_quota", quotaErr)
			auditErr := w.recordAudit(
				ctx, task, gateway.RunResult{}, "run_rejected", "tenant_quota", started,
			)
			return true, w.retryOrAck(ctx, delivery, task, errors.Join(quotaErr, failErr, auditErr))
		}
		releaseQuota = release
	}
	defer releaseQuota()
	result, runErr := w.runtime.ChatWithScope(ctx, agentruntime.ChatInput{
		Scope:             task.Scope,
		MessageID:         task.MessageID,
		UserID:            task.UserID,
		SessionID:         task.SessionID,
		Text:              task.Text,
		RequestID:         task.RequestID,
		ApprovedTools:     append([]string(nil), task.ApprovedTools...),
		ApprovedToolCalls: append([]governance.ApprovedToolCall(nil), task.ApprovedToolCalls...),
		ReplyTarget:       task.ReplyTarget,
	})
	if runErr != nil {
		w.opts.Metrics.RecordRun(ctx, task.Scope.TenantID, "failed", time.Since(started))
		failErr := w.journal.FailRun(ctx, task.RequestID, "agent_execution", runErr)
		auditErr := w.recordAudit(ctx, task, gateway.RunResult{}, "run_failed", "agent_execution", started)
		return true, w.retryOrAck(ctx, delivery, task, errors.Join(runErr, failErr, auditErr))
	}
	if w.opts.Approvals != nil {
		pending, approvalErr := w.opts.Approvals.ListPendingByRequest(
			ctx, task.Scope.TenantID, task.RequestID,
		)
		if approvalErr != nil {
			return true, w.retryOrAck(ctx, delivery, task, approvalErr)
		}
		result.Reply = appendApprovalInstructions(result.Reply, pending)
	}
	if err := w.journal.CompleteRun(ctx, task, gateway.RunResult{
		Reply:            result.Reply,
		AgentName:        result.AgentName,
		FencingToken:     result.FencingToken,
		EventCount:       result.EventCount,
		PromptTokens:     result.PromptTokens,
		CompletionTokens: result.CompletionTokens,
		Cost:             result.Cost,
		TraceID:          audit.TraceID(ctx),
	}); err != nil {
		return true, w.retryOrAck(ctx, delivery, task, err)
	}
	if err := w.recordAudit(ctx, task, gateway.RunResult{
		Reply: result.Reply, AgentName: result.AgentName,
		FencingToken: result.FencingToken, EventCount: result.EventCount,
		PromptTokens: result.PromptTokens, CompletionTokens: result.CompletionTokens,
		Cost: result.Cost, TraceID: audit.TraceID(ctx),
	}, "run_completed", "", started); err != nil {
		return true, w.retryOrAck(ctx, delivery, task, err)
	}
	if w.opts.Quota != nil {
		if err := w.opts.Quota.RecordUsage(
			ctx, task.Scope.TenantID, task.RequestID,
			result.PromptTokens, result.CompletionTokens, result.Cost,
		); err != nil {
			return true, w.retryOrAck(ctx, delivery, task, err)
		}
	}
	if err := w.enqueueSessionJobs(ctx, task); err != nil {
		return true, w.retryOrAck(ctx, delivery, task, err)
	}
	w.opts.Metrics.RecordRun(ctx, task.Scope.TenantID, "completed", time.Since(started))
	w.opts.Metrics.RecordUsage(
		ctx, task.Scope.TenantID, result.PromptTokens, result.CompletionTokens, result.Cost,
	)
	if err := delivery.Ack(ctx); err != nil {
		return true, err
	}
	return true, nil
}

func (w *Worker) enqueueSessionJobs(ctx context.Context, task workqueue.AgentTask) error {
	if w.opts.Jobs == nil {
		return nil
	}
	payload, err := json.Marshal(background.SessionJobPayload{
		StorageScope: task.Scope.StorageScope,
		UserID:       task.UserID,
		SessionID:    task.SessionID,
		TurnSeq:      task.TurnSeq,
	})
	if err != nil {
		return err
	}
	dedupeKey := task.ConversationID + ":" + fmt.Sprint(task.TurnSeq)
	for _, jobType := range []string{background.JobSummary, background.JobMemoryExtract} {
		if _, err := w.opts.Jobs.Enqueue(ctx, background.EnqueueRequest{
			TenantID:    task.Scope.TenantID,
			AppID:       task.Scope.AppID,
			RevisionID:  task.Scope.RevisionID,
			Type:        jobType,
			DedupeKey:   dedupeKey,
			Payload:     payload,
			TraceParent: background.TraceParent(ctx),
		}); err != nil {
			return fmt.Errorf("enqueue %s job: %w", jobType, err)
		}
	}
	return nil
}

func appendApprovalInstructions(reply string, records []approval.Record) string {
	if len(records) == 0 {
		return reply
	}
	var builder strings.Builder
	builder.WriteString(strings.TrimSpace(reply))
	for _, record := range records {
		builder.WriteString("\n\n需要人工审批工具：")
		builder.WriteString(record.ToolName)
		builder.WriteString("\n批准请回复：批准 ")
		builder.WriteString(record.ApprovalID)
		builder.WriteString("\n拒绝请回复：拒绝 ")
		builder.WriteString(record.ApprovalID)
	}
	return builder.String()
}

func (w *Worker) recordAudit(
	ctx context.Context,
	task workqueue.AgentTask,
	result gateway.RunResult,
	decision string,
	errorType string,
	started time.Time,
) error {
	if w.opts.Audit == nil {
		return nil
	}
	return w.opts.Audit.Record(ctx, audit.Event{
		TenantID:         task.Scope.TenantID,
		Channel:          task.Scope.ChannelType,
		ChannelBindingID: task.Scope.ChannelBindingID,
		UserID:           task.UserID,
		SessionID:        task.SessionID,
		MessageID:        task.MessageID,
		RequestID:        task.RequestID,
		TraceID:          audit.TraceID(ctx),
		AgentName:        result.AgentName,
		RevisionID:       task.Scope.RevisionID,
		Decision:         decision,
		Latency:          time.Since(started),
		ErrorType:        errorType,
		Cost:             result.Cost,
		Details: map[string]any{
			"fencing_token":     result.FencingToken,
			"event_count":       result.EventCount,
			"attempt":           task.Attempt,
			"prompt_tokens":     result.PromptTokens,
			"completion_tokens": result.CompletionTokens,
		},
	})
}

func taskContext(ctx context.Context, task workqueue.AgentTask) context.Context {
	carrier := propagation.MapCarrier{}
	if task.TraceParent != "" {
		carrier.Set("traceparent", task.TraceParent)
	}
	if task.TraceState != "" {
		carrier.Set("tracestate", task.TraceState)
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

func (w *Worker) retryOrAck(
	ctx context.Context,
	delivery workqueue.Delivery,
	task workqueue.AgentTask,
	cause error,
) error {
	if task.Attempt+1 < w.opts.MaxAttempts {
		if w.opts.RetryDelay > 0 {
			timer := time.NewTimer(w.opts.RetryDelay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return errors.Join(cause, context.Cause(ctx))
			}
		}
		return errors.Join(cause, delivery.Retry(ctx))
	}
	return errors.Join(cause, delivery.Ack(ctx))
}

// Run keeps consuming until cancellation. Individual task errors do not stop
// the worker because their run state and retry decision are already durable.
func (w *Worker) Run(ctx context.Context) error {
	for {
		_, err := w.ProcessOne(ctx)
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		if err != nil {
			continue
		}
	}
}
