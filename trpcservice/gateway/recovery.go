package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
)

var ErrEarlierTurn = errors.New("an earlier conversation turn is pending")

// RunRecovery reuses queue_outbox for durable scheduling. Commit comes before
// transport ACK; the generation makes a redelivery of the old task harmless.
type RunRecovery interface {
	DeferRun(context.Context, workqueue.AgentTask, string, string, time.Time) error
	DependencyReadyAt(context.Context, workqueue.AgentTask) (time.Time, error)
}

func pendingRun(status string) bool {
	return status == "queued" || status == "running" || status == "failed" || status == "waiting"
}

func outboxLane(task workqueue.AgentTask) string {
	if task.Background || (!task.Lifetime.OccurredAt.IsZero() && time.Since(task.Lifetime.OccurredAt) > 2*time.Minute) {
		return "backlog"
	}
	return "recent"
}

func deferredTask(task workqueue.AgentTask, reason string) workqueue.AgentTask {
	task.Generation++
	task.Background = true
	if reason == "model_unavailable" {
		task.DeferredCount++
	}
	return task
}

const waitingNotice = "消息已收到，模型服务暂时不可用，平台会在恢复后继续处理，无需重复发送。"

// Caller holds the journal lock; sending/unknown notices cannot be unsent.
func (j *MemoryJournal) cancelWaitingNotice(id string) {
	if row := j.outbound[stableID("wait_", id)]; row != nil && row.status == "pending" {
		row.status = "cancelled"
	}
}

func (j *PostgresJournal) DeferRun(ctx context.Context, task workqueue.AgentTask, owner, reason string, at time.Time) error {
	if err := task.Scope.Validate(); err != nil {
		return err
	}
	if reason != "model_unavailable" && reason != "session_order" && reason != "tenant_capacity" {
		return errors.New("invalid deferral reason")
	}
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var status, worker string
	var generation int64
	if err = tx.QueryRowContext(ctx, `SELECT status,COALESCE(worker_id,''),schedule_generation FROM agent_run WHERE request_id=$1 AND tenant_id=$2 AND app_id=$3 FOR UPDATE`, task.RequestID, task.Scope.TenantID, task.Scope.AppID).Scan(&status, &worker, &generation); err != nil {
		return err
	}
	if generation != task.Generation || !pendingRun(status) {
		return nil
	}
	if status == "running" && worker != owner {
		return ErrRunSuperseded
	}
	next := deferredTask(task, reason)
	if _, err = tx.ExecContext(ctx, `UPDATE agent_run SET status='waiting',worker_id=NULL,schedule_generation=$2,next_attempt_at=$3,deferred_count=$4,error_type=$5,error_message=NULL,completed_at=NULL WHERE request_id=$1`, task.RequestID, next.Generation, at, next.DeferredCount, reason); err != nil {
		return err
	}
	payload, err := json.Marshal(next)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO queue_outbox(outbox_id,topic,partition_key,payload,next_attempt_at,lane) VALUES($1,'agent.run',$2,$3::jsonb,$4,'backlog')`, stableID("qout_", task.RequestID, strconv.FormatInt(next.Generation, 10)), task.Scope.StorageScope+"|"+task.UserID+"|"+task.SessionID, string(payload), at); err != nil {
		return err
	}
	if reason == "model_unavailable" {
		notice, _ := json.Marshal(map[string]any{"text": waitingNotice, "reply_target": task.ReplyTarget, "agent_name": "platform-control", "traceparent": task.TraceParent})
		if _, err = tx.ExecContext(ctx, `INSERT INTO outbound_message(outbound_id,tenant_id,app_id,channel_binding_id,request_id,conversation_id,payload,status,message_kind) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,'pending','waiting') ON CONFLICT(request_id,message_kind) DO NOTHING`, stableID("wait_", task.RequestID), task.Scope.TenantID, task.Scope.AppID, task.Scope.ChannelBindingID, task.RequestID, task.ConversationID, string(notice)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (j *PostgresJournal) DependencyReadyAt(ctx context.Context, task workqueue.AgentTask) (time.Time, error) {
	var at sql.NullTime
	err := j.db.QueryRowContext(ctx, `SELECT max(next_attempt_at) FROM agent_run WHERE tenant_id=$1 AND app_id=$2 AND revision_id=$3 AND status='waiting' AND error_type='model_unavailable' AND next_attempt_at>now() AND EXISTS(SELECT 1 FROM agent_run own WHERE own.request_id=$4 AND own.status='running' AND own.schedule_generation=$5)`, task.Scope.TenantID, task.Scope.AppID, task.Scope.RevisionID, task.RequestID, task.Generation).Scan(&at)
	return at.Time, err
}

func (j *MemoryJournal) DeferRun(ctx context.Context, task workqueue.AgentTask, owner, reason string, at time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if reason != "model_unavailable" && reason != "session_order" && reason != "tenant_capacity" {
		return errors.New("invalid deferral reason")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	r := j.runs[task.RequestID]
	if r == nil || r.tenantID != task.Scope.TenantID || r.appID != task.Scope.AppID {
		return ErrRunMissing
	}
	if r.generation != task.Generation || !pendingRun(r.status) {
		return nil
	}
	if r.status == "running" && r.workerID != owner {
		return ErrRunSuperseded
	}
	next := deferredTask(task, reason)
	r.status, r.workerID, r.errType = "waiting", "", reason
	r.generation, r.deferredCount, r.nextAttempt = next.Generation, next.DeferredCount, at
	r.completedAt = time.Time{}
	id := stableID("qout_", task.RequestID, strconv.FormatInt(next.Generation, 10))
	j.outbox[id] = &memoryQueueOutbox{id: id, task: next, status: "pending", nextAttempt: at}
	if reason == "model_unavailable" {
		id = stableID("wait_", task.RequestID)
		if j.outbound[id] == nil {
			j.outbound[id] = &memoryOutbound{item: OutboundItem{ID: id, RequestID: task.RequestID, TenantID: task.Scope.TenantID, ChannelBindingID: task.Scope.ChannelBindingID, Text: waitingNotice, ReplyTarget: task.ReplyTarget, TraceParent: task.TraceParent}, status: "pending", nextAttempt: time.Now()}
		}
	}
	return nil
}

func (j *MemoryJournal) DependencyReadyAt(ctx context.Context, task workqueue.AgentTask) (time.Time, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	var at time.Time
	if own := j.runs[task.RequestID]; own == nil || own.status != "running" || own.generation != task.Generation {
		return at, nil
	}
	for _, r := range j.runs {
		if r.tenantID == task.Scope.TenantID && r.appID == task.Scope.AppID && r.revisionID == task.Scope.RevisionID && r.status == "waiting" && r.errType == "model_unavailable" && r.nextAttempt.After(at) {
			at = r.nextAttempt
		}
	}
	return at, nil
}
