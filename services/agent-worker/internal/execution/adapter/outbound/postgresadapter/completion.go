package postgresadapter

import (
	"context"
	"encoding/json"
	"errors"
	app "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"strings"

	"github.com/jackc/pgx/v5"
	codec "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const completionColumns = `tenant_id,run_id,completion_id,COALESCE(attempt_id,''),kind,status,candidate_ref,candidate_digest,final_intent_id,reply_disposition,reason,completed_at,result_digest,memory_digest,memory_status`

func scanCompletion(row pgx.Row) (domain.Completion, error) {
	var c domain.Completion
	err := row.Scan(&c.TenantID, &c.RunID, &c.CompletionID, &c.AttemptID, &c.Kind, &c.Status, &c.Candidate.Ref, &c.Candidate.Digest, &c.FinalIntentID, &c.ReplyDisposition, &c.Reason, &c.CompletedAt, &c.ResultDigest, &c.MemoryDigest, &c.MemoryStatus)
	return c, mapped(err)
}
func (l *Ledger) FindCompletion(ctx context.Context, tenant, run string) (found domain.Completion, resultErr error) {
	ctx, span := telemetrytrace.Start(l.Tracer, ctx, "worker.run.find_completion")
	defer func() {
		if resultErr == nil {
			span.SetAttributes(attribute.String("app.outcome", "accepted"), attribute.String("app.run.status", string(found.Status)))
		} else if errors.Is(resultErr, domain.ErrNotFound) {
			span.SetAttributes(attribute.String("app.outcome", "not_found"))
			span.End()
			return
		}
		telemetrytrace.End(span, resultErr)
	}()
	return scanCompletion(l.pool.QueryRow(ctx, `SELECT `+completionColumns+` FROM execution_completions WHERE tenant_id=$1 AND run_id=$2`, tenant, run))
}
func (l *Ledger) Complete(ctx context.Context, f domain.Finish) (completed domain.Completion, resultErr error) {
	ctx, span := telemetrytrace.Start(l.Tracer, ctx, "worker.run.complete", trace.WithAttributes(attribute.String("app.run.id", f.Grant.Run.Request.RunID), attribute.String("app.attempt.id", f.Grant.AttemptID), attribute.String("app.run.status", string(f.Status))))
	defer func() { telemetrytrace.End(span, resultErr) }()
	commitAttempted := false
	if f.Status != domain.Succeeded && f.Status != domain.Failed {
		return domain.Completion{}, domain.ErrInvalid
	}
	if (f.Status != domain.Succeeded && len(f.Attachments) != 0) || domain.ValidateAttachments(f.Attachments) != nil {
		return domain.Completion{}, domain.ErrInvalid
	}
	if f.MemoryDigest != "" && (f.Status != domain.Succeeded || !domain.DigestValid(f.MemoryDigest)) {
		return domain.Completion{}, domain.ErrInvalid
	}
	if len(f.Reason) > 128 || !f.Candidate.Parent.Valid() {
		return domain.Completion{}, domain.ErrInvalid
	}
	if f.Status == domain.Succeeded && (f.Candidate.Ref == "" || !domain.DigestValid(f.Candidate.Digest) || strings.TrimSpace(f.FinalText) == "") {
		return domain.Completion{}, domain.ErrInvalid
	}
	if f.Status == domain.Failed && (f.Candidate.Ref != "" || f.Candidate.Digest != "") {
		return domain.Completion{}, domain.ErrInvalid
	}
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return domain.Completion{}, err
	}
	defer rollback(tx)
	r, _, _, err := lockRun(ctx, tx, f.Grant.Run.Request.Route.TenantID, f.Grant.Run.Request.RunID)
	if err != nil {
		return domain.Completion{}, err
	}
	// Uncertain commit retry checks durable identity before today's expired
	// lease. It never calls the model again or accepts different late content.
	if old, e := scanCompletion(tx.QueryRow(ctx, `SELECT `+completionColumns+` FROM execution_completions WHERE tenant_id=$1 AND run_id=$2`, r.Request.Route.TenantID, r.Request.RunID)); e == nil {
		var digest string
		if e = tx.QueryRow(ctx, `SELECT result_digest FROM execution_completions WHERE tenant_id=$1 AND run_id=$2`, old.TenantID, old.RunID).Scan(&digest); e != nil {
			return domain.Completion{}, e
		}
		if digest != domain.FinishDigest(f) {
			return domain.Completion{}, domain.ErrConflict
		}
		return old, tx.Commit(ctx)
	} else if !errors.Is(e, domain.ErrNotFound) {
		return domain.Completion{}, e
	}
	r, head, now, err := fenced(ctx, tx, f.Grant)
	if err != nil {
		return domain.Completion{}, err
	}
	if f.Status == domain.Succeeded && f.Candidate.Parent != head {
		return domain.Completion{}, domain.ErrFenced
	}
	c := domain.Completion{TenantID: r.Request.Route.TenantID, RunID: r.Request.RunID, CompletionID: domain.StableID("cmp", r.Request.RunID), AttemptID: f.Grant.AttemptID, Kind: "ATTEMPT", Status: f.Status, ReplyDisposition: "NONE", Reason: f.Reason, CompletedAt: now}
	if f.Status == domain.Succeeded {
		c.Candidate = domain.Head{Ref: f.Candidate.Ref, Digest: f.Candidate.Digest}
	}
	if f.MemoryDigest != "" {
		c.MemoryDigest, c.MemoryStatus = f.MemoryDigest, "PENDING"
	}
	var payload []byte
	var digest string
	if f.FinalText != "" && now.Before(r.ReplyDeadline) {
		c.FinalIntentID = domain.StableID("fin", r.Request.RunID)
		c.ReplyDisposition = "FINAL"
		payload, digest, err = app.EncodeFinalIntent(r, c.AttemptID, f.Grant.Generation, f.FinalText, f.Attachments)
		if err != nil {
			return domain.Completion{}, err
		}
	} else if !now.Before(r.ReplyDeadline) && c.Reason == "" {
		c.Reason = "DEADLINE_EXPIRED"
	}
	c.ResultDigest = domain.FinishDigest(f)
	if err = insertCompletion(ctx, tx, r, c, c.ResultDigest); err != nil {
		return domain.Completion{}, err
	}
	if _, err = tx.Exec(ctx, `UPDATE execution_attempts SET status=$3,ended_at=$4,reason=$5 WHERE tenant_id=$1 AND attempt_id=$2`, c.TenantID, c.AttemptID, string(c.Status), now, f.Reason); err != nil {
		return domain.Completion{}, err
	}
	if c.Status == domain.Succeeded {
		commitCtx, commitSpan := telemetrytrace.Start(l.Tracer, ctx, "worker.session.commit", trace.WithAttributes(attribute.String("app.session.id", r.SessionID)))
		defer func() {
			outcome := completionOutcome(resultErr, commitAttempted)
			commitSpan.SetAttributes(attribute.String("app.outcome", outcome))
			telemetrytrace.End(commitSpan, resultErr)
		}()
		if _, err = tx.Exec(commitCtx, `INSERT INTO execution_session_commits(tenant_id,run_id,session_id,candidate_ref,candidate_digest) VALUES($1,$2,$3,$4,$5)`, c.TenantID, c.RunID, r.SessionID, c.Candidate.Ref, c.Candidate.Digest); err != nil {
			return domain.Completion{}, err
		}
		if _, err = tx.Exec(commitCtx, `UPDATE execution_sessions SET accepted_ref=$3,accepted_digest=$4 WHERE tenant_id=$1 AND session_id=$2`, c.TenantID, r.SessionID, c.Candidate.Ref, c.Candidate.Digest); err != nil {
			return domain.Completion{}, err
		}
	}
	if c.FinalIntentID != "" {
		createCtx, createSpan := telemetrytrace.Start(l.Tracer, ctx, "create execution.reply-intent.v1", trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(attribute.String("app.intent.id", c.FinalIntentID)))
		defer func() {
			createSpan.SetAttributes(attribute.String("app.outcome", completionOutcome(resultErr, commitAttempted)))
			telemetrytrace.End(createSpan, resultErr)
		}()
		carrier := tracecontext.Capture(createCtx)
		if _, err = tx.Exec(createCtx, `INSERT INTO execution_reply_outbox(intent_id,tenant_id,run_id,digest,payload,created_at,traceparent,tracestate,ready) VALUES($1,$2,$3,$4,$5,$6,NULLIF($7,''),NULLIF($8,''),$9)`, c.FinalIntentID, c.TenantID, c.RunID, digest, payload, now, carrier.Traceparent, carrier.Tracestate, c.MemoryStatus != "PENDING"); err != nil {
			return domain.Completion{}, err
		}
	}
	commitAttempted = true
	if err = tx.Commit(ctx); err != nil {
		return domain.Completion{}, err
	}
	return c, nil
}
func insertCompletion(ctx context.Context, tx pgx.Tx, r domain.Run, c domain.Completion, resultDigest string) error {
	_, err := tx.Exec(ctx, `INSERT INTO execution_completions(tenant_id,run_id,completion_id,attempt_id,kind,status,candidate_ref,candidate_digest,final_intent_id,reply_disposition,reason,result_digest,completed_at,memory_digest,memory_status) VALUES($1,$2,$3,NULLIF($4,''),$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`, c.TenantID, c.RunID, c.CompletionID, c.AttemptID, c.Kind, string(c.Status), c.Candidate.Ref, c.Candidate.Digest, c.FinalIntentID, c.ReplyDisposition, c.Reason, resultDigest, c.CompletedAt, c.MemoryDigest, c.MemoryStatus)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE execution_runs SET status=$3,wait_reason='',retry_at=NULL WHERE tenant_id=$1 AND run_id=$2`, c.TenantID, c.RunID, string(c.Status)); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE execution_sessions SET settled_sequence=$3 WHERE tenant_id=$1 AND session_id=$2 AND settled_sequence=$3-1`, c.TenantID, r.SessionID, r.Sequence)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return domain.ErrFenced
	}
	return nil
}
func (l *Ledger) FailAttempt(ctx context.Context, g domain.Grant, reason string, retry bool) error {
	if !retry {
		_, err := l.Complete(ctx, domain.Finish{Grant: g, Status: domain.Failed, Reason: reason, FinalText: "本次执行未完成，请稍后重试。"})
		return err
	}
	if reason == "" || len(reason) > 128 {
		return domain.ErrInvalid
	}
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	r, _, now, err := fenced(ctx, tx, g)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE execution_attempts SET status='FAILED',ended_at=$3,reason=$4 WHERE tenant_id=$1 AND attempt_id=$2`, r.Request.Route.TenantID, g.AttemptID, now, reason); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE execution_runs SET status='RETRY_WAIT',wait_reason='RETRY_BACKOFF',retry_at=$3 WHERE tenant_id=$1 AND run_id=$2`, r.Request.Route.TenantID, r.Request.RunID, now.Add(r.Policy.RetryBackoff)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (l *Ledger) Terminalize(ctx context.Context, tenant, run, reason string) (changed bool, resultErr error) {
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer rollback(tx)
	var carrier tracecontext.Carrier
	r, _, settled, err := lockRun(ctx, tx, tenant, run, &carrier.Traceparent, &carrier.Tracestate)
	if err != nil {
		return false, err
	}
	if r.Status == domain.Succeeded || r.Status == domain.Failed || r.Sequence != settled+1 {
		return false, nil
	}
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return false, err
	}
	if r.CurrentAttemptID != "" {
		live, err := currentLive(ctx, tx, r, now)
		if err != nil {
			return false, err
		}
		if live {
			return false, nil
		}
	}
	expired := !now.Before(r.RunDeadline) || (r.ExecutionDeadline != nil && !now.Before(*r.ExecutionDeadline))
	permanent := reason == "MANIFEST_INVALID" || reason == "UNSUPPORTED_MANIFEST" || r.WaitReason == "INVALID_CLOCK"
	if !expired && !permanent && r.Attempts < r.Policy.MaxAttempts {
		return false, nil
	}
	stored := trace.SpanContextFromContext(carrier.Restore(context.Background()))
	ambient := trace.SpanContextFromContext(ctx)
	options := []trace.SpanStartOption{trace.WithAttributes(attribute.String("app.run.id", run), attribute.String("app.session.id", r.SessionID), attribute.String("app.run.status", "FAILED"))}
	if stored.IsValid() && (!ambient.IsValid() || ambient.TraceID() != stored.TraceID()) {
		if ambient.IsValid() {
			options = append(options, trace.WithLinks(trace.Link{SpanContext: ambient}))
		}
		ctx = carrier.Restore(ctx)
	}
	ctx, terminalSpan := telemetrytrace.Start(l.Tracer, ctx, "worker.run.terminalize", options...)
	defer func() { telemetrytrace.End(terminalSpan, resultErr) }()
	if expired {
		reason = "DEADLINE_EXPIRED"
	} else if r.WaitReason == "INVALID_CLOCK" {
		reason = "INVALID_CLOCK"
	} else if !permanent {
		reason = "ATTEMPTS_EXHAUSTED"
	}
	if r.CurrentAttemptID != "" {
		if _, err = tx.Exec(ctx, `UPDATE execution_attempts SET status='ABORTED',ended_at=$3,reason=$4 WHERE tenant_id=$1 AND attempt_id=$2 AND status IN ('PREPARING','EXECUTING')`, tenant, r.CurrentAttemptID, now, reason); err != nil {
			return false, err
		}
	}
	c := domain.Completion{TenantID: tenant, RunID: run, CompletionID: domain.StableID("cmp", run), AttemptID: r.CurrentAttemptID, Kind: "SYSTEM_TERMINATION", Status: domain.Failed, ReplyDisposition: "NONE", Reason: reason, CompletedAt: now}
	if err = insertCompletion(ctx, tx, r, c, domain.Digest([]byte("SYSTEM_TERMINATION:"+reason))); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

// PendingReplies is the business-only inspection view. Relay uses the same query
// through PendingTracedReplies, with observation metadata outside the domain.
func (l *Ledger) PendingReplies(ctx context.Context, limit int) ([]domain.OutboxItem, error) {
	rows, err := l.PendingTracedReplies(ctx, limit)
	out := make([]domain.OutboxItem, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.OutboxItem)
	}
	return out, err
}
func (l *Ledger) PendingTracedReplies(ctx context.Context, limit int) ([]app.TracedReply, error) {
	if limit < 1 {
		return nil, domain.ErrInvalid
	}
	rows, err := l.pool.Query(ctx, `SELECT intent_id,digest,payload,COALESCE(traceparent,''),COALESCE(tracestate,'') FROM execution_reply_outbox WHERE published_at IS NULL AND ready ORDER BY created_at,intent_id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []app.TracedReply
	for rows.Next() {
		var r app.TracedReply
		if err = rows.Scan(&r.IntentID, &r.Digest, &r.Payload, &r.Carrier.Traceparent, &r.Carrier.Tracestate); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}
func (l *Ledger) MarkReplyPublished(ctx context.Context, id, digest string) error {
	r, err := l.pool.Exec(ctx, `UPDATE execution_reply_outbox SET published_at=COALESCE(published_at,clock_timestamp()) WHERE intent_id=$1 AND digest=$2 AND ready`, id, digest)
	if err != nil {
		return err
	}
	if r.RowsAffected() != 1 {
		return domain.ErrConflict
	}
	return nil
}
func (l *Ledger) Final(ctx context.Context, id string) (domain.Final, error) {
	var f domain.Final
	var raw []byte
	err := l.pool.QueryRow(ctx, `SELECT o.tenant_id,o.digest,o.payload,r.request_json FROM execution_reply_outbox o JOIN execution_runs r ON r.tenant_id=o.tenant_id AND r.run_id=o.run_id JOIN execution_completions c ON c.tenant_id=o.tenant_id AND c.run_id=o.run_id AND c.final_intent_id=o.intent_id WHERE o.intent_id=$1 AND o.ready`, id).Scan(&f.TenantID, &f.Digest, &f.Payload, &raw)
	if err != nil {
		return f, mapped(err)
	}
	var req domain.Requested
	if err = json.Unmarshal(raw, &req); err != nil {
		return f, err
	}
	e, err := codec.DecodeReplyIntent(f.Payload)
	if err != nil {
		return f, err
	}
	d, err := codec.ReplyIntentDigest(e)
	if err != nil {
		return f, err
	}
	if d != f.Digest {
		return f, domain.ErrConflict
	}
	f.IntentID = e.IntentID
	f.RunID = e.RunID
	f.AdmissionID = e.AdmissionID
	f.AttemptID = e.Execution.AttemptID
	f.CompletionID = e.Execution.CompletionID
	f.ExecutionGeneration = e.Execution.Generation
	f.Sequence = e.Sequence
	f.ManifestDigest = req.Route.ManifestDigest
	return f, nil
}
