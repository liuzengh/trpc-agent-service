// Package management projects immutable Execution ledger facts for authorized
// operational reads. It never changes a Run and never reads SDK payload bodies.
package management

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	governancev1 "github.com/liuzengh/trpc-agent-service/api/runtime/governance/v1"
	managementv1 "github.com/liuzengh/trpc-agent-service/api/runtime/management/v1"
)

var ErrNotFound = errors.New("run record not found")

type Reader struct{ pool *pgxpool.Pool }

func NewReader(pool *pgxpool.Pool) *Reader { return &Reader{pool: pool} }

const summaryColumns = `r.run_id,r.session_id,r.status,r.wait_reason,r.attempts,
 r.accepted_at,r.execution_deadline,
 COALESCE(c.reason,''),COALESCE(c.memory_status,''),
 CASE WHEN o.intent_id IS NULL THEN 'NOT_CREATED' WHEN o.published_at IS NULL THEN 'PENDING' ELSE 'HANDED_OFF' END,
 COALESCE(u.input_tokens,0),COALESCE(u.output_tokens,0),COALESCE(u.total_tokens,0),COALESCE(u.usage_records,0)`

func scanSummary(row pgx.Row) (managementv1.RunSummary, error) {
	var out managementv1.RunSummary
	var usageRecords int64
	if err := row.Scan(&out.RunID, &out.SessionID, &out.Status, &out.WaitReason, &out.Attempts,
		&out.AcceptedAt, &out.ExecutionUntil, &out.FailureReason, &out.MemoryStatus,
		&out.ReplyStatus, &out.InputTokens, &out.OutputTokens, &out.TotalTokens, &usageRecords); err != nil {
		return out, err
	}
	out.Stage = stage(out)
	out.UsageStatus = usageStatus(out, usageRecords)
	return out, nil
}

func usageStatus(run managementv1.RunSummary, records int64) string {
	if records == 0 {
		return "UNAVAILABLE"
	}
	if run.Status == "SUCCEEDED" && records == int64(run.Attempts) {
		return "COMPLETE"
	}
	return "PARTIAL"
}

func stage(run managementv1.RunSummary) string {
	switch {
	case run.Status == "FAILED":
		return "FAILED"
	case run.MemoryStatus == "PENDING":
		return "MEMORY"
	case run.MemoryStatus == "FAILED":
		return "FAILED"
	case run.ReplyStatus == "PENDING":
		return "REPLY_PUBLISH"
	case run.ReplyStatus == "HANDED_OFF":
		return "REPLY_HANDOFF"
	case run.Status == "SUCCEEDED":
		return "COMPLETED"
	case run.Status == "RUNNING" || run.Status == "RETRY_WAIT":
		return "EXECUTION"
	default:
		return "QUEUED"
	}
}

func (r *Reader) List(ctx context.Context, tenant string, offset, limit int) (managementv1.RunPage, error) {
	page := managementv1.RunPage{Runs: []managementv1.RunSummary{}, Offset: offset, Limit: limit}
	if err := r.pool.QueryRow(ctx, `SELECT count(*) FROM execution_runs WHERE tenant_id=$1`, tenant).Scan(&page.Total); err != nil {
		return page, err
	}
	rows, err := r.pool.Query(ctx, `SELECT `+summaryColumns+`
 FROM execution_runs r
 LEFT JOIN execution_completions c ON c.tenant_id=r.tenant_id AND c.run_id=r.run_id
 LEFT JOIN execution_reply_outbox o ON o.tenant_id=r.tenant_id AND o.run_id=r.run_id
 LEFT JOIN LATERAL (SELECT count(*) usage_records,COALESCE(sum(input_tokens),0) input_tokens,COALESCE(sum(output_tokens),0) output_tokens,COALESCE(sum(total_tokens),0) total_tokens FROM execution_model_usage WHERE tenant_id=r.tenant_id AND run_id=r.run_id) u ON true
 WHERE r.tenant_id=$1 ORDER BY r.accepted_at DESC,r.run_id DESC OFFSET $2 LIMIT $3`, tenant, offset, limit)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	for rows.Next() {
		item, scanErr := scanSummary(rows)
		if scanErr != nil {
			return page, scanErr
		}
		page.Runs = append(page.Runs, item)
	}
	return page, rows.Err()
}

func (r *Reader) Get(ctx context.Context, tenant, runID string) (managementv1.RunDetail, error) {
	var out managementv1.RunDetail
	var acceptedHead, acceptedDigest string
	var usageRecords int64
	err := r.pool.QueryRow(ctx, `SELECT `+summaryColumns+`,r.admission_id,
 COALESCE(r.request_json->'Route'->>'ManifestRef',''),COALESCE(r.request_json->'Route'->>'ManifestDigest',''),
 s.accepted_ref,s.accepted_digest
 FROM execution_runs r
 JOIN execution_sessions s ON s.tenant_id=r.tenant_id AND s.session_id=r.session_id
 LEFT JOIN execution_completions c ON c.tenant_id=r.tenant_id AND c.run_id=r.run_id
 LEFT JOIN execution_reply_outbox o ON o.tenant_id=r.tenant_id AND o.run_id=r.run_id
 LEFT JOIN LATERAL (SELECT count(*) usage_records,COALESCE(sum(input_tokens),0) input_tokens,COALESCE(sum(output_tokens),0) output_tokens,COALESCE(sum(total_tokens),0) total_tokens FROM execution_model_usage WHERE tenant_id=r.tenant_id AND run_id=r.run_id) u ON true
 WHERE r.tenant_id=$1 AND r.run_id=$2`, tenant, runID).Scan(
		&out.RunID, &out.SessionID, &out.Status, &out.WaitReason, &out.Attempts,
		&out.AcceptedAt, &out.ExecutionUntil, &out.FailureReason, &out.MemoryStatus,
		&out.ReplyStatus, &out.InputTokens, &out.OutputTokens, &out.TotalTokens, &usageRecords,
		&out.AdmissionID, &out.ManifestRef, &out.ManifestDigest, &acceptedHead, &acceptedDigest)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, ErrNotFound
	}
	if err != nil {
		return out, err
	}
	out.Stage = stage(out.RunSummary)
	out.UsageStatus = usageStatus(out.RunSummary, usageRecords)
	if acceptedHead != "" {
		out.SessionHead = acceptedHead
	}
	out.AttemptsLog = []managementv1.Attempt{}
	rows, err := r.pool.Query(ctx, `SELECT attempt_id,worker_id,generation,status,reason,created_at,agent_started_at,ended_at FROM execution_attempts WHERE tenant_id=$1 AND run_id=$2 ORDER BY generation,attempt_id`, tenant, runID)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var attempt managementv1.Attempt
		if err = rows.Scan(&attempt.AttemptID, &attempt.WorkerID, &attempt.Generation, &attempt.Status, &attempt.Reason, &attempt.CreatedAt, &attempt.StartedAt, &attempt.EndedAt); err != nil {
			rows.Close()
			return out, err
		}
		out.AttemptsLog = append(out.AttemptsLog, attempt)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return out, err
	}
	rows.Close()
	out.Timeline = timeline(out)
	var completionStatus, completionReason, memoryStatus string
	var completedAt time.Time
	err = r.pool.QueryRow(ctx, `SELECT status,reason,memory_status,completed_at FROM execution_completions WHERE tenant_id=$1 AND run_id=$2`, tenant, runID).Scan(&completionStatus, &completionReason, &memoryStatus, &completedAt)
	if err == nil {
		out.Timeline = append(out.Timeline, managementv1.TimelineEvent{Source: "worker", Category: "RUN_COMPLETED", Status: completionStatus, Reason: completionReason, OccurredAt: completedAt})
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return out, err
	}
	// memory_status is a current-state projection. The ledger does not yet store
	// the time at which the memory finalizer reached that state, so it must not
	// be represented as a timestamped timeline event using completed_at.
	_ = memoryStatus
	var replyCreated time.Time
	var replyPublished *time.Time
	err = r.pool.QueryRow(ctx, `SELECT created_at,published_at FROM execution_reply_outbox WHERE tenant_id=$1 AND run_id=$2`, tenant, runID).Scan(&replyCreated, &replyPublished)
	if err == nil {
		out.Timeline = append(out.Timeline, managementv1.TimelineEvent{Source: "worker", Category: "REPLY_CREATED", Status: "PENDING", OccurredAt: replyCreated})
		if replyPublished != nil {
			out.Timeline = append(out.Timeline, managementv1.TimelineEvent{Source: "worker", Category: "REPLY_HANDED_OFF", Status: "SUCCEEDED", OccurredAt: *replyPublished})
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return out, err
	}
	sort.SliceStable(out.Timeline, func(i, j int) bool { return out.Timeline[i].OccurredAt.Before(out.Timeline[j].OccurredAt) })
	out.Coverage = []string{"worker.run", "worker.attempt", "worker.session_head", "worker.memory", "worker.model_usage", "worker.reply_handoff"}
	_ = acceptedDigest
	return out, nil
}

func timeline(run managementv1.RunDetail) []managementv1.TimelineEvent {
	events := []managementv1.TimelineEvent{{Source: "worker", Category: "RUN_ACCEPTED", Status: "SUCCEEDED", OccurredAt: run.AcceptedAt}}
	for _, attempt := range run.AttemptsLog {
		events = append(events, managementv1.TimelineEvent{Source: "worker", Category: "ATTEMPT_CREATED", Status: "CREATED", OccurredAt: attempt.CreatedAt, Attributes: map[string]any{"attempt_id": attempt.AttemptID, "generation": attempt.Generation, "worker_id": attempt.WorkerID}})
		if attempt.StartedAt != nil {
			events = append(events, managementv1.TimelineEvent{Source: "worker", Category: "AGENT_STARTED", Status: "RUNNING", OccurredAt: *attempt.StartedAt, Attributes: map[string]any{"attempt_id": attempt.AttemptID}})
		}
		if attempt.EndedAt != nil {
			events = append(events, managementv1.TimelineEvent{Source: "worker", Category: "ATTEMPT_ENDED", Status: attempt.Status, Reason: attempt.Reason, OccurredAt: *attempt.EndedAt, Attributes: map[string]any{"attempt_id": attempt.AttemptID}})
		}
	}
	sort.SliceStable(events, func(i, j int) bool { return events[i].OccurredAt.Before(events[j].OccurredAt) })
	return events
}

func (r *Reader) Audit(ctx context.Context, tenant string, offset, limit int) (managementv1.AuditPage, error) {
	var total int
	if err := r.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM execution_runs WHERE tenant_id=$1)+(SELECT count(*) FROM execution_attempts WHERE tenant_id=$1)+(SELECT count(*) FROM execution_completions WHERE tenant_id=$1)+(SELECT count(*) FROM execution_reply_outbox WHERE tenant_id=$1)`, tenant).Scan(&total); err != nil {
		return managementv1.AuditPage{}, err
	}
	rows, err := r.pool.Query(ctx, `SELECT event_id,category,action,outcome,resource_type,resource_id,reason,occurred_at FROM (
 SELECT 'run:'||run_id event_id,'execution' category,'RUN_ACCEPTED' action,'SUCCEEDED' outcome,'run' resource_type,run_id resource_id,'' reason,accepted_at occurred_at FROM execution_runs WHERE tenant_id=$1
 UNION ALL
 SELECT 'attempt:'||attempt_id,'execution','ATTEMPT_'||status,status,'attempt',attempt_id,reason,COALESCE(ended_at,agent_started_at,created_at) FROM execution_attempts WHERE tenant_id=$1
 UNION ALL
 SELECT 'completion:'||completion_id,'execution','RUN_COMPLETED',status,'run',run_id,reason,completed_at FROM execution_completions WHERE tenant_id=$1
 UNION ALL
 SELECT 'reply:'||intent_id,'delivery','REPLY_'||CASE WHEN published_at IS NULL THEN 'PENDING' ELSE 'HANDED_OFF' END,CASE WHEN published_at IS NULL THEN 'PENDING' ELSE 'SUCCEEDED' END,'reply_intent',intent_id,'',COALESCE(published_at,created_at) FROM execution_reply_outbox WHERE tenant_id=$1
 ) events ORDER BY occurred_at DESC,event_id DESC OFFSET $2 LIMIT $3`, tenant, offset, limit)
	if err != nil {
		return managementv1.AuditPage{}, err
	}
	defer rows.Close()
	page := managementv1.AuditPage{Events: []managementv1.AuditEvent{}, Offset: offset, Limit: limit, Total: total}
	for rows.Next() {
		var event managementv1.AuditEvent
		if err = rows.Scan(&event.EventID, &event.Category, &event.Action, &event.Outcome, &event.ResourceType, &event.ResourceID, &event.Reason, &event.OccurredAt); err != nil {
			return page, err
		}
		event.Source = "worker"
		page.Events = append(page.Events, event)
	}
	return page, rows.Err()
}

func (r *Reader) Usage(ctx context.Context, tenant string) (governancev1.UsageSummary, error) {
	out := governancev1.UsageSummary{TenantID: tenant}
	var start *time.Time
	err := r.pool.QueryRow(ctx, `SELECT COALESCE(max(policy_revision),0),max(period_start),COALESCE(max(period_seconds),0),COALESCE(max(token_limit),0),
 COALESCE(sum(CASE WHEN s.usage_known THEN s.total_tokens ELSE 0 END),0),
 COALESCE(sum(CASE WHEN s.usage_known THEN 0 ELSE x.reserved_tokens END),0),
 count(*) FILTER(WHERE s.usage_known=false),count(*) FILTER(WHERE s.attempt_id IS NULL),
 COALESCE(sum(CASE WHEN s.usage_known THEN s.estimated_cost_micros ELSE 0 END),0)
 FROM worker_tenant_usage_reservations_v1 x LEFT JOIN worker_tenant_usage_settlements_v1 s USING(tenant_id,attempt_id)
 WHERE x.tenant_id=$1 AND x.period_start=(SELECT max(period_start) FROM worker_tenant_usage_reservations_v1 WHERE tenant_id=$1)`, tenant).Scan(&out.PolicyRevision, &start, &out.PeriodSeconds, &out.TokenLimit, &out.UsedTokens, &out.ReservedTokens, &out.UnknownUsageCount, &out.PendingUsageCount, &out.EstimatedCostMicros)
	if err != nil {
		return out, err
	}
	if start != nil {
		out.PeriodStart = start.UTC().Format(time.RFC3339)
	}
	return out, nil
}
