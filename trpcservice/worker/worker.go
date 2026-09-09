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
	"github.com/liuzengh/trpc-agent-service/trpcservice/modelops"
	"github.com/liuzengh/trpc-agent-service/trpcservice/routing"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/toolexec"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"golang.org/x/sync/errgroup"
)

// Runtime is the tenant-scoped Agent execution boundary used by a Worker.
type Runtime interface {
	ChatWithScope(ctx context.Context, input agentruntime.ChatInput) (agentruntime.ChatResult, error)
}

type Options struct {
	Authorize   func(context.Context, workqueue.AgentTask) error
	Concurrency int
	Attachments interface {
		Import(context.Context, workqueue.AgentTask) (string, error)
	}
	WorkerID          string
	MaxAttempts       int
	RetryDelay        time.Duration
	Audit             audit.Writer
	Metrics           *platformmetrics.Recorder
	Approvals         approval.Repository
	ToolJournal       toolexec.Journal
	Jobs              background.Repository
	Quota             *tenant.Guard
	ModelUsageManaged bool
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
	if opts.Concurrency < 0 || opts.Concurrency > 64 {
		return nil, errors.New("Worker concurrency must be between 0 and 64")
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
	if leased, ok := delivery.(workqueue.LeasedDelivery); ok {
		defer leased.Close()
		ctx = leased.Context()
	}
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
	if task.Attempt >= w.opts.MaxAttempts {
		if admission, ok := w.journal.(gateway.RunAdmission); ok {
			skip, err := admission.SkipDelivery(ctx, task)
			if err != nil {
				return true, err
			}
			if skip {
				return true, delivery.Ack(ctx)
			}
		}
		return true, w.retryOrAck(ctx, delivery, task, gateway.ErrRunTerminal)
	}
	var admissionErr error
	if admission, ok := w.journal.(gateway.RunAdmission); ok {
		skip, err := admission.StartRun(ctx, task, w.opts.WorkerID)
		admissionErr = err
		if err == nil && skip {
			w.opts.Metrics.RecordRun(ctx, task.Scope.TenantID, "skipped_delivery", time.Since(started))
			return true, delivery.Ack(ctx)
		}
	} else {
		admissionErr = w.journal.MarkRunRunning(ctx, task.RequestID, w.opts.WorkerID)
	}
	if err := admissionErr; err != nil {
		if errors.Is(err, gateway.ErrEarlierTurn) {
			return true, w.deferRun(ctx, delivery, task, "session_order", time.Now().Add(2*time.Second))
		}
		if errors.Is(err, gateway.ErrRunTerminal) {
			return true, delivery.Ack(ctx)
		}
		return true, w.retryOrAck(ctx, delivery, task, err)
	}
	if recovery, ok := w.journal.(gateway.RunRecovery); ok && task.Media == nil {
		at, err := recovery.DependencyReadyAt(ctx, task)
		if err != nil {
			return true, err
		}
		if at.After(time.Now()) {
			return true, w.deferRun(ctx, delivery, task, "model_unavailable", at)
		}
	}
	if w.opts.Authorize != nil {
		if err := w.opts.Authorize(ctx, task); err != nil {
			if errors.Is(err, routing.ErrRouteDisabled) || errors.Is(err, routing.ErrBindingChanged) {
				if e := w.journal.FailRun(ctx, task.RequestID, "authorization_changed", err, w.opts.WorkerID); e != nil {
					return true, e
				}
				_, e := w.journal.TerminalFailRun(ctx, task, gateway.RunResult{WorkerID: w.opts.WorkerID, ErrorType: "authorization_changed", Reply: "接入配置或授权已变更，本请求已停止自动执行。此前操作不会自动回滚，请先核对执行记录。请求编号：" + task.RequestID, TraceID: audit.TraceID(ctx), TraceParent: background.TraceParent(ctx)})
				if e != nil {
					return true, e
				}
				return true, delivery.Ack(ctx)
			}
			// Control-plane unavailability is not evidence of revoked access.
			return true, err
		}
	}
	releaseQuota := func() {}
	if w.opts.Quota != nil && task.Media == nil {
		lease, quotaErr := w.opts.Quota.AcquireRunLease(ctx, task.Scope.TenantID)
		if quotaErr != nil {
			if errors.Is(quotaErr, tenant.ErrConcurrencyLimited) {
				return true, w.deferRun(ctx, delivery, task, "tenant_capacity", time.Now().Add(2*time.Second))
			}
			failErr := w.journal.FailRun(ctx, task.RequestID, "tenant_quota", quotaErr, w.opts.WorkerID)
			auditErr := w.recordAudit(
				ctx, task, gateway.RunResult{}, "run_rejected", "tenant_quota", started,
			)
			return true, w.retryOrAck(ctx, delivery, task, errors.Join(quotaErr, failErr, auditErr))
		}
		releaseQuota = lease.Release
		ctx = lease.Context()
	}
	defer releaseQuota()
	var result agentruntime.ChatResult
	var runErr error
	if task.Media != nil {
		if w.opts.Attachments == nil {
			runErr = errors.New("attachment import unavailable")
		} else {
			result.Reply, runErr = w.opts.Attachments.Import(ctx, task)
		}
		result.RequestID = task.RequestID
		result.AgentName = "platform-attachment"
	} else {
		result, runErr = w.runtime.ChatWithScope(ctx, agentruntime.ChatInput{
			ChatType:          task.ChatType,
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
	}
	if runErr != nil {
		if errors.Is(runErr, modelops.ErrUnavailable) && task.Media == nil && ctx.Err() == nil {
			safe := true
			if w.opts.ToolJournal != nil {
				executions, err := w.opts.ToolJournal.ListByRequest(ctx, task.Scope.TenantID, task.RequestID)
				if err != nil {
					return true, err
				}
				safe = len(executions) == 0
			}
			if safe {
				delay := min(30*time.Second, 5*time.Second*time.Duration(1<<min(task.DeferredCount, 3)))
				return true, w.deferRun(ctx, delivery, task, "model_unavailable", time.Now().Add(delay))
			}
		}
		failureType := "agent_execution"
		if task.Media != nil {
			failureType = "attachment_import"
		}
		// Export only a stable category, never arbitrary provider errors, file
		// content or credential-bearing URLs in trace status descriptions.
		span.SetAttributes(attribute.String("error.type", failureType))
		span.SetStatus(codes.Error, failureType)
		w.opts.Metrics.RecordRun(ctx, task.Scope.TenantID, "failed", time.Since(started))
		failErr := w.journal.FailRun(ctx, task.RequestID, failureType, runErr, w.opts.WorkerID)
		auditErr := w.recordAudit(ctx, task, gateway.RunResult{}, "run_failed", failureType, started)
		return true, w.retryOrAck(ctx, delivery, task, errors.Join(runErr, failErr, auditErr))
	}
	if task.ApprovalID != "" {
		if w.opts.ToolJournal == nil {
			return true, w.retryOrAck(ctx, delivery, task, fmt.Errorf("approval result journal is unavailable"))
		}
		executions, err := w.opts.ToolJournal.ListByRequest(ctx, task.Scope.TenantID, task.RequestID)
		if err != nil {
			return true, w.retryOrAck(ctx, delivery, task, err)
		}
		result.Reply = approvalExecutionReply(task.ApprovalID, executions)
	}
	if w.opts.Approvals != nil {
		// Also cover free-form replies such as "never mind" about an earlier
		// approval: a model's cancellation prose cannot override pending state.
		pending, approvalErr := w.opts.Approvals.ListPendingBySession(
			ctx, task.Scope.TenantID, task.Scope.ChannelBindingID, task.UserID, task.SessionID,
		)
		if approvalErr != nil {
			return true, w.retryOrAck(ctx, delivery, task, approvalErr)
		}
		result.Reply = appendApprovalInstructions(result.Reply, pending)
	}
	if err := w.journal.CompleteRun(ctx, task, gateway.RunResult{
		ErrorType:        result.PlatformCode,
		WorkerID:         w.opts.WorkerID,
		Reply:            result.Reply,
		AgentName:        result.AgentName,
		FencingToken:     result.FencingToken,
		EventCount:       result.EventCount,
		PromptTokens:     result.PromptTokens,
		CompletionTokens: result.CompletionTokens,
		Cost:             result.Cost,
		TraceID:          audit.TraceID(ctx),
		TraceParent:      background.TraceParent(ctx),
	}); err != nil {
		return true, w.retryOrAck(ctx, delivery, task, err)
	}
	if err := w.recordAudit(ctx, task, gateway.RunResult{
		Reply: result.Reply, AgentName: result.AgentName,
		FencingToken: result.FencingToken, EventCount: result.EventCount,
		PromptTokens: result.PromptTokens, CompletionTokens: result.CompletionTokens,
		Cost: result.Cost, TraceID: audit.TraceID(ctx),
		TraceParent: background.TraceParent(ctx),
	}, "run_completed", result.PlatformCode, started); err != nil {
		return true, w.retryOrAck(ctx, delivery, task, err)
	}
	if w.opts.Quota != nil && !w.opts.ModelUsageManaged && task.Media == nil {
		if err := w.opts.Quota.RecordUsage(
			ctx, task.Scope.TenantID, task.RequestID,
			result.PromptTokens, result.CompletionTokens, result.Cost,
		); err != nil {
			return true, w.retryOrAck(ctx, delivery, task, err)
		}
	}
	if task.Media == nil {
		if err := w.enqueueSessionJobs(ctx, task); err != nil {
			return true, w.retryOrAck(ctx, delivery, task, err)
		}
	}
	w.opts.Metrics.RecordRun(ctx, task.Scope.TenantID, "completed", time.Since(started))
	if !w.opts.ModelUsageManaged {
		w.opts.Metrics.RecordUsage(
			ctx, task.Scope.TenantID, result.PromptTokens, result.CompletionTokens, result.Cost,
		)
	}
	if err := delivery.Ack(ctx); err != nil {
		return true, err
	}
	return true, nil
}

func (w *Worker) deferRun(ctx context.Context, delivery workqueue.Delivery, task workqueue.AgentTask, reason string, at time.Time) error {
	recovery, ok := w.journal.(gateway.RunRecovery)
	if !ok {
		return errors.New("durable run recovery unavailable")
	}
	if err := recovery.DeferRun(ctx, task, w.opts.WorkerID, reason, at); err != nil {
		return err
	}
	w.opts.Metrics.RecordRun(ctx, task.Scope.TenantID, "waiting", 0)
	if err := w.recordAudit(ctx, task, gateway.RunResult{}, "run_deferred", reason, time.Now()); err != nil {
		return err
	}
	return delivery.Ack(ctx)
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
			SourceRequestID: task.RequestID,
			TenantID:        task.Scope.TenantID,
			AppID:           task.Scope.AppID,
			RevisionID:      task.Scope.RevisionID,
			Type:            jobType,
			DedupeKey:       dedupeKey,
			Payload:         payload,
			TraceParent:     background.TraceParent(ctx),
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
	// Do not prepend the model's prose: it may claim cancellation/success even
	// though these rows prove the tools are still waiting for approval.
	builder.WriteString("平台提示：以下工具尚未执行，正在等待你的审批。")
	for _, record := range records {
		builder.WriteString("\n\n需要人工审批工具：")
		builder.WriteString(record.ToolName)
		builder.WriteString("\n请在原会话操作。Telegram 群聊请使用“回复”此消息，再发送以下命令，不要额外添加 @ 或其他文字。")
		builder.WriteString("\n请仅复制下面其中一行命令：\n\n批准 ")
		builder.WriteString(record.ApprovalID)
		builder.WriteString("\n\n拒绝 ")
		builder.WriteString(record.ApprovalID)
	}
	return builder.String()
}

func approvalExecutionReply(approvalID string, executions []toolexec.Execution) string {
	var result strings.Builder
	result.WriteString("平台执行结果（审批 " + approvalID + "）：")
	if len(executions) == 0 {
		result.WriteString("本轮没有工具执行记录，不能认定执行成功。")
		return result.String()
	}
	for _, execution := range executions {
		status := "执行结果尚未确认，请勿重复发起操作"
		switch execution.Status {
		case toolexec.StatusSucceeded:
			status = "执行成功"
		case toolexec.StatusFailed:
			status = "执行失败"
		}
		result.WriteString("\n" + execution.ToolName + "：" + status)
		if execution.OperationID != "" {
			result.WriteString("（业务操作编号：" + execution.OperationID + "）")
		}
	}
	return result.String()
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
	if errors.Is(cause, gateway.ErrRunSuperseded) {
		return delivery.Ack(ctx)
	}
	if task.Attempt+1 < w.opts.MaxAttempts {
		return errors.Join(cause, w.retryDelivery(ctx, delivery))
	}
	if ctx.Err() != nil {
		return errors.Join(cause, context.Cause(ctx))
	}
	dead, err := w.journal.TerminalFailRun(ctx, task, gateway.RunResult{
		WorkerID: w.opts.WorkerID,
		Reply: "平台提示：本次处理未能完成，已停止自动执行重试。请求编号：" + task.RequestID +
			"。如果涉及工具操作，请先按请求编号核对执行记录；这条提示不代表工具已经回滚，请勿直接重复发起有副作用的操作。",
		TraceID: audit.TraceID(ctx), TraceParent: background.TraceParent(ctx),
	})
	if errors.Is(err, gateway.ErrRunSuperseded) {
		return delivery.Ack(ctx)
	}
	if err != nil {
		return errors.Join(cause, err, w.retryDelivery(ctx, delivery))
	}
	if dead {
		if err := w.recordAudit(ctx, task, gateway.RunResult{}, "run_dead", "retry_exhausted", time.Now()); err != nil {
			return errors.Join(cause, err, w.retryDelivery(ctx, delivery))
		}
	}
	return errors.Join(cause, delivery.Ack(ctx))
}

func (w *Worker) retryDelivery(ctx context.Context, delivery workqueue.Delivery) error {
	timer := time.NewTimer(w.opts.RetryDelay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return delivery.Retry(ctx)
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// Run keeps consuming until cancellation. Individual task errors do not stop
// the worker because their run state and retry decision are already durable.
func (w *Worker) Run(ctx context.Context) error {
	if w.opts.Concurrency <= 1 {
		return w.run(ctx)
	}
	group, ctx := errgroup.WithContext(ctx)
	for i := 0; i < w.opts.Concurrency; i++ {
		lane := *w
		lane.opts.WorkerID = fmt.Sprintf("%s-%d", w.opts.WorkerID, i)
		if i == 0 {
			if q, ok := w.queue.(interface{ ForegroundQueue() workqueue.Queue }); ok {
				lane.queue = q.ForegroundQueue()
			}
		}
		group.Go(func() error { return lane.run(ctx) })
	}
	return group.Wait()
}

func (w *Worker) run(ctx context.Context) error {
	for {
		processed, err := w.ProcessOne(ctx)
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		if err != nil || !processed {
			// Receive may fail immediately while a backend is unavailable. Do
			// not spin, including for non-blocking empty queue implementations.
			delay := max(w.opts.RetryDelay, 100*time.Millisecond)
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return context.Cause(ctx)
			}
		}
	}
}
