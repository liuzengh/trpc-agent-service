package console

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	agentruntime "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/toolexec"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
)

type Engine struct {
	Probe               func(context.Context) []Observation
	probeMu             sync.RWMutex
	observations        []Observation
	Repository          controlplane.Repository
	Cleanup             func(context.Context, string, string, string, string) error
	Store               *Store
	Runtime             Runtime
	Approvals           *Approvals
	Tools               *ToolJournal
	Audit               audit.Writer
	Quota               *tenant.Guard
	WorkerID, ModelName string
	SandboxEnabled      bool
	working             atomic.Bool
}

func (e *Engine) Run(ctx context.Context) error {
	if e.WorkerID == "" {
		return errors.New("debug worker identity required")
	}
	var heartbeats sync.WaitGroup
	heartbeats.Add(1)
	go func() {
		defer heartbeats.Done()
		timer := time.NewTicker(15 * time.Second)
		defer timer.Stop()
		for {
			e.observe(ctx)
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
		}
	}()
	heartbeats.Add(1)
	go func() {
		defer heartbeats.Done()
		timer := time.NewTicker(time.Minute)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				e.maintenance(ctx)
			}
		}
	}()
	heartbeats.Add(1)
	go func() {
		defer heartbeats.Done()
		timer := time.NewTicker(5 * time.Second)
		defer timer.Stop()
		for {
			e.heartbeat(ctx)
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
		}
	}()
	defer heartbeats.Wait()
	timer := time.NewTicker(500 * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		}
		records, err := e.Store.List(ctx, Filter{Kind: "run", AllTenants: true, Status: "queued", Limit: 20, Ascending: true})
		if err != nil {
			continue
		}
		for _, record := range records {
			if ctx.Err() != nil {
				return nil
			}
			var run Run
			if json.Unmarshal(record.Data, &run) != nil {
				continue
			}
			run.WorkerID = e.WorkerID
			run.StartedAt = time.Now().UTC()
			run.LeaseUntil = time.Now().UTC().Add(20 * time.Second)
			record.Data, _ = json.Marshal(run)
			record.Status = "running"
			claimed, err := e.Store.Update(ctx, record, record.Version)
			if err != nil {
				continue
			}
			e.process(ctx, claimed, run)
			break
		}
		e.recoverStale(ctx)
	}
}

func (e *Engine) heartbeat(ctx context.Context) {
	id := "worker-" + Hash(e.WorkerID)[:32]
	e.probeMu.RLock()
	checks := append([]Observation(nil), e.observations...)
	e.probeMu.RUnlock()
	payload, _ := json.Marshal(map[string]any{"worker_id": e.WorkerID, "model_name": e.ModelName, "sandbox_enabled": e.SandboxEnabled, "busy": e.working.Load(), "capabilities": []string{"console_debug"}, "checks": checks})
	record, err := e.Store.Get(ctx, "worker", "", id)
	if errors.Is(err, ErrNotFound) {
		// Expiring heartbeats must not accumulate one permanent row per
		// process restart; cleanup also permits this process to return after
		// a storage outage lasting longer than its heartbeat lifetime.
		_ = e.Store.Cleanup(ctx, "worker")
		_, _ = e.Store.Create(ctx, Record{Kind: "worker", ID: id, OwnerID: e.WorkerID, Status: "ready", Data: payload, ExpiresAt: time.Now().UTC().Add(time.Minute)})
		return
	}
	if err != nil {
		return
	}
	record.Data = payload
	record.Status = "ready"
	record.ExpiresAt = time.Now().UTC().Add(time.Minute)
	_, _ = e.Store.Update(ctx, record, record.Version)
}

func (e *Engine) event(ctx context.Context, record Record, eventType string) {
	id, err := uuid.NewV7()
	if err != nil {
		return
	}
	raw, _ := json.Marshal(map[string]any{"type": eventType, "run_id": record.ID, "status": record.Status})
	_, _ = e.Store.Create(ctx, Record{Kind: "event", TenantID: record.TenantID, AppID: record.ID, ID: id.String(), OwnerID: record.OwnerID, Status: eventType, Data: raw, ExpiresAt: record.ExpiresAt})
}

func (e *Engine) process(parent context.Context, record Record, run Run) {
	e.working.Store(true)
	defer e.working.Store(false)
	if e.Repository != nil {
		tenant, err := e.Repository.GetTenant(parent, record.TenantID)
		if err != nil || tenant.Status != controlplane.StatusActive {
			run.ErrorType = "tenant_unavailable"
			run.Reply = "当前租户不可执行调试，请检查租户状态。"
			e.finish(parent, record, run, "failed")
			return
		}
	}
	ctx := otel.GetTextMapPropagator().Extract(parent, propagation.MapCarrier{"traceparent": run.TraceParent})
	ctx, span := otel.Tracer("trpc-agent-service/console").Start(ctx, "console.debug.run")
	defer span.End()
	span.SetAttributes(attribute.String("tenant.id", record.TenantID), attribute.String("agent.app.id", record.AppID), attribute.String("request.id", record.ID))
	run.TraceID = audit.TraceID(ctx)
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	run.TraceParent = carrier.Get("traceparent")
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	ctx = WithDebugScope(ctx, DebugScope{TenantID: record.TenantID, AppID: record.AppID, SnapshotID: run.SnapshotID, SessionID: run.SessionID, UserID: run.UserID, OwnerID: record.OwnerID})
	var release = func() {}
	if e.Quota != nil {
		lease, err := e.Quota.AcquireRunLease(ctx, record.TenantID)
		if err != nil {
			run.ErrorType = "tenant_quota"
			run.Reply = "当前租户并发或预算限制阻止了调试。"
			e.finish(parent, record, run, "failed")
			return
		}
		ctx = lease.Context()
		release = lease.Release
	}
	defer release()
	e.event(ctx, record, "started")
	done := make(chan struct{})
	var monitor sync.WaitGroup
	monitor.Add(1)
	go func() {
		defer monitor.Done()
		timer := time.NewTicker(2 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-timer.C:
				current, err := e.Store.Get(ctx, "run", record.TenantID, record.ID)
				if err != nil {
					cancel()
					return
				}
				var live Run
				if json.Unmarshal(current.Data, &live) != nil || live.WorkerID != e.WorkerID || current.Status != "running" {
					cancel()
					return
				}
				live.LeaseUntil = time.Now().UTC().Add(20 * time.Second)
				current.Data, _ = json.Marshal(live)
				if _, err = e.Store.Update(ctx, current, current.Version); err != nil {
					cancel()
					return
				}
			}
		}
	}()
	scope, err := runtimecontext.NewScope(record.TenantID, record.AppID, run.SnapshotID, "console", DebugBinding)
	var result agentruntime.ChatResult
	if err == nil {
		result, err = e.Runtime.ChatWithScope(ctx, agentruntime.ChatInput{Scope: scope, ChatType: "direct", UserID: run.UserID, SessionID: run.SessionID, MessageID: run.MessageID, RequestID: record.ID, Text: run.Input, ApprovedToolCalls: run.ApprovedCalls, ReplyTarget: run.SessionID})
	}
	close(done)
	monitor.Wait()
	finalCtx, finalCancel := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
	defer finalCancel()
	state := "completed"
	run.Reply = result.Reply
	run.PromptTokens = result.PromptTokens
	run.CompletionTokens = result.CompletionTokens
	run.Cost = result.Cost
	if len(run.Reply) > 64<<10 {
		run.Reply = run.Reply[:64<<10] + "\n[调试输出达到大小限制]"
	}
	if err != nil {
		state = "failed"
		run.ErrorType = "agent_execution_failed"
		run.Reply = "本次 Agent 调试未能完成，请查看运行状态和工具记录。"
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			run.ErrorType = "run_timeout"
		}
		if current, getErr := e.Store.Get(finalCtx, "run", record.TenantID, record.ID); getErr == nil && current.Status == "cancel_requested" {
			state = "cancelled"
			run.ErrorType = "cancelled"
			run.Reply = "已结束本次执行。取消不代表此前工具操作已经回滚。"
		}
	}
	tools, toolErr := e.Tools.ListByRequest(finalCtx, record.TenantID, record.ID)
	if toolErr != nil {
		state = "unknown"
		run.ErrorType = "journal_unavailable"
	}
	for _, tool := range tools {
		if tool.Status == toolexec.StatusUnknown || tool.Status == toolexec.StatusRunning {
			state = "unknown"
			run.ErrorType = "tool_outcome_unknown"
			run.Reply = "工具结果尚不能确认，平台不会自动重试。请先核对执行记录。"
			break
		}
		if tool.Status == toolexec.StatusFailed && state == "completed" {
			state = "failed"
			run.ErrorType = tool.ErrorType
		}
	}
	if state == "completed" && result.PlatformCode != "" {
		state = "failed"
		run.ErrorType = result.PlatformCode
	}
	if state == "completed" {
		pending, approvalErr := e.Approvals.ListPendingByRequest(finalCtx, record.TenantID, record.ID)
		if approvalErr != nil {
			state = "unknown"
			run.ErrorType = "approval_state_unavailable"
		} else if len(pending) > 0 {
			state = "awaiting_approval"
			run.Reply = "需要你的批准后，才能执行以下工具。批准不等于执行成功，结果会单独返回。"
			for _, a := range pending {
				run.PendingApprovals = append(run.PendingApprovals, ApprovalView{ID: a.ApprovalID, Tool: a.ToolName, Status: a.Status, ArgumentsHash: a.ArgumentsHash, ExpiresAt: a.ExpiresAt})
			}
		}
	}
	e.finish(finalCtx, record, run, state)
}

func (e *Engine) finish(ctx context.Context, claimed Record, run Run, state string) {
	current, err := e.Store.Get(ctx, "run", claimed.TenantID, claimed.ID)
	if err != nil {
		return
	}
	var live Run
	if json.Unmarshal(current.Data, &live) != nil || live.WorkerID != e.WorkerID {
		return
	}
	if current.Status != "running" && current.Status != "cancel_requested" {
		return
	}
	current.Status = state
	if !run.StartedAt.IsZero() {
		ms := max(int64(0), time.Since(run.StartedAt).Milliseconds())
		run.LatencyMS = &ms
	}
	run.LeaseUntil = time.Time{}
	current.Data, _ = json.Marshal(run)
	current, err = e.Store.Update(ctx, current, current.Version)
	if err != nil {
		return
	}
	e.event(ctx, current, state)
	if e.Audit != nil {
		_ = e.Audit.Record(ctx, audit.Event{TenantID: current.TenantID, Channel: "console", ChannelBindingID: DebugBinding, UserID: current.OwnerID, SessionID: run.SessionID, RequestID: current.ID, RevisionID: run.SnapshotID, Decision: "debug_" + state, ErrorType: run.ErrorType, TraceID: run.TraceID, Cost: run.Cost})
	}
}

func (e *Engine) recoverStale(ctx context.Context) {
	for _, state := range []string{"running", "cancel_requested", "awaiting_approval"} {
		records, err := e.Store.List(ctx, Filter{Kind: "run", AllTenants: true, Status: state, Limit: 30})
		if err != nil {
			return
		}
		for _, record := range records {
			var run Run
			if json.Unmarshal(record.Data, &run) != nil {
				continue
			}
			expired := false
			if state == "awaiting_approval" {
				expired = len(run.PendingApprovals) > 0
				for _, a := range run.PendingApprovals {
					expired = expired && time.Now().After(a.ExpiresAt)
				}
			} else {
				expired = !run.LeaseUntil.IsZero() && time.Now().After(run.LeaseUntil.Add(10*time.Second))
			}
			if !expired {
				continue
			}
			if state == "awaiting_approval" {
				record.Status = "failed"
				run.ErrorType = "approval_expired"
				run.Reply = "审批已过期，没有创建后续执行任务。"
			} else {
				record.Status = "unknown"
				run.ErrorType = "worker_interrupted"
				run.Reply = "执行节点已中断，结果需要核对。为避免重复执行，平台没有自动重试。"
			}
			record.Data, _ = json.Marshal(run)
			if updated, err := e.Store.Update(ctx, record, record.Version); err == nil {
				e.event(ctx, updated, record.Status)
			}
		}
	}
}
