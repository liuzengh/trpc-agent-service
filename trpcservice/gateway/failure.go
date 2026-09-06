package gateway

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
)

// TerminalFailRun commits the dead state and its outbound notice together.
// It never fabricates a completed Agent result or overwrites a successful run.
func (j *PostgresJournal) TerminalFailRun(ctx context.Context, task workqueue.AgentTask, result RunResult) (bool, error) {
	if err := task.Scope.Validate(); err != nil {
		return false, err
	}
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	var status, owner string
	if err := tx.QueryRowContext(ctx, `SELECT status,COALESCE(worker_id,'') FROM agent_run WHERE request_id=$1 AND tenant_id=$2 AND app_id=$3 FOR UPDATE`,
		task.RequestID, task.Scope.TenantID, task.Scope.AppID).Scan(&status, &owner); err != nil {
		return false, err
	}
	if status == "completed" {
		return false, nil
	}
	if status == "running" && result.WorkerID != "" && owner != result.WorkerID {
		return false, ErrRunSuperseded
	}
	if _, err := tx.ExecContext(ctx, `UPDATE agent_run SET status='dead',error_type='retry_exhausted',error_message=NULL,
completed_at=COALESCE(completed_at,now()),trace_id=COALESCE(NULLIF($2,''),trace_id) WHERE request_id=$1`, task.RequestID, result.TraceID); err != nil {
		return false, err
	}
	payload, err := json.Marshal(map[string]any{"text": result.Reply, "reply_target": task.ReplyTarget, "agent_name": "platform-control", "traceparent": result.TraceParent})
	if err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO outbound_message(outbound_id,tenant_id,app_id,channel_binding_id,request_id,conversation_id,payload,status)
VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,'pending') ON CONFLICT(request_id) DO NOTHING`,
		stableID("out_", task.RequestID), task.Scope.TenantID, task.Scope.AppID, task.Scope.ChannelBindingID, task.RequestID, task.ConversationID, string(payload)); err != nil {
		return false, fmt.Errorf("persist terminal failure notice: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE inbound_message SET status='failed',processed_at=now() WHERE inbound_id=$1`, task.InboundID); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}
