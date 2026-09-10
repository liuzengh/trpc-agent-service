package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
)

// LoadCompletedRun restores only the matching immutable run and final reply.
// A waiting notice must never be mistaken for the model's completed result.
func (j *PostgresJournal) LoadCompletedRun(ctx context.Context, task workqueue.AgentTask) (RunResult, bool, error) {
	if err := task.Scope.Validate(); err != nil {
		return RunResult{}, false, err
	}
	var result RunResult
	var payload []byte
	err := j.db.QueryRowContext(ctx, `SELECT COALESCE(r.worker_id,''),COALESCE(r.agent_name,''),r.fencing_token,
 r.prompt_tokens,r.completion_tokens,r.cost,COALESCE(r.error_type,''),COALESCE(r.trace_id,''),r.finalized_at IS NOT NULL,o.payload
 FROM agent_run r LEFT JOIN outbound_message o ON o.tenant_id=r.tenant_id AND o.request_id=r.request_id AND o.message_kind='result'
 WHERE r.request_id=$1 AND r.tenant_id=$2 AND r.app_id=$3 AND r.revision_id=$4 AND r.conversation_id=$5
 AND r.turn_seq=$6 AND r.schedule_generation=$7 AND r.status='completed'`, task.RequestID, task.Scope.TenantID, task.Scope.AppID, task.Scope.RevisionID, task.ConversationID, task.TurnSeq, task.Generation).Scan(
		&result.WorkerID, &result.AgentName, &result.FencingToken, &result.PromptTokens, &result.CompletionTokens, &result.Cost, &result.ErrorType, &result.TraceID, &result.Finalized, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return RunResult{}, false, nil
	}
	if err != nil {
		return RunResult{}, false, errors.New("completed run result unavailable")
	}
	var reply struct {
		Text        string `json:"text"`
		EventCount  int    `json:"event_count"`
		TraceParent string `json:"traceparent"`
	}
	if json.Unmarshal(payload, &reply) != nil || reply.Text == "" {
		return RunResult{}, false, errors.New("completed run final reply missing")
	}
	result.Reply, result.EventCount, result.TraceParent = reply.Text, reply.EventCount, reply.TraceParent
	return result, true, nil
}

func (j *PostgresJournal) MarkRunFinalized(ctx context.Context, task workqueue.AgentTask) (bool, error) {
	if err := task.Scope.Validate(); err != nil {
		return false, err
	}
	result, err := j.db.ExecContext(ctx, `UPDATE agent_run SET finalized_at=now() WHERE request_id=$1 AND tenant_id=$2 AND app_id=$3
 AND revision_id=$4 AND conversation_id=$5 AND turn_seq=$6 AND schedule_generation=$7 AND status='completed' AND finalized_at IS NULL`,
		task.RequestID, task.Scope.TenantID, task.Scope.AppID, task.Scope.RevisionID, task.ConversationID, task.TurnSeq, task.Generation)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if n == 1 {
		return true, nil
	}
	stored, found, err := j.LoadCompletedRun(ctx, task)
	if err != nil {
		return false, err
	}
	if !found || !stored.Finalized {
		return false, ErrRunSuperseded
	}
	return false, nil
}

func (j *MemoryJournal) LoadCompletedRun(ctx context.Context, task workqueue.AgentTask) (RunResult, bool, error) {
	if err := ctx.Err(); err != nil {
		return RunResult{}, false, err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return RunResult{}, false, ErrJournalClosed
	}
	r := j.runs[task.RequestID]
	if r == nil || r.tenantID != task.Scope.TenantID || r.appID != task.Scope.AppID || r.revisionID != task.Scope.RevisionID || r.conversationID != task.ConversationID || r.turnSeq != task.TurnSeq || r.generation != task.Generation || r.status != "completed" {
		return RunResult{}, false, nil
	}
	if out := j.outbound[stableID("out_", task.RequestID)]; out == nil || out.item.Text == "" {
		return RunResult{}, false, errors.New("completed run final reply missing")
	}
	return r.result, true, nil
}

func (j *MemoryJournal) MarkRunFinalized(ctx context.Context, task workqueue.AgentTask) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return false, ErrJournalClosed
	}
	r := j.runs[task.RequestID]
	if r == nil || r.tenantID != task.Scope.TenantID || r.appID != task.Scope.AppID || r.revisionID != task.Scope.RevisionID || r.conversationID != task.ConversationID || r.turnSeq != task.TurnSeq || r.generation != task.Generation || r.status != "completed" {
		return false, ErrRunSuperseded
	}
	first := !r.result.Finalized
	r.result.Finalized = true
	return first, nil
}
