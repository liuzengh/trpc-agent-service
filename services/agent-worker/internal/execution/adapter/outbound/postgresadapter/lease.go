package postgresadapter

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (l *Ledger) Claim(ctx context.Context, req domain.ClaimRequest) (grant domain.Grant, resultErr error) {
	parent := trace.SpanFromContext(ctx)
	ctx, span := telemetrytrace.Start(l.Tracer, ctx, "worker.run.claim")
	defer func() {
		if resultErr == nil {
			span.SetAttributes(attribute.String("app.attempt.id", grant.AttemptID))
			parent.SetAttributes(attribute.String("app.attempt.id", grant.AttemptID))
		}
		telemetrytrace.End(span, resultErr)
	}()
	if req.TenantID == "" || req.RunID == "" || req.WorkerID == "" || req.MaxRunSeconds <= 0 || req.MaxRunSeconds > int64((1<<63-1)/time.Second) || req.MaxActive < 1 {
		return domain.Grant{}, domain.ErrInvalid
	}
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return domain.Grant{}, err
	}
	defer rollback(tx)
	// A single short capacity lock bounds claims across processes, not merely
	// within one worker's goroutine pool. No external work holds this lock.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(731004286)`); err != nil {
		return domain.Grant{}, err
	}
	r, head, settled, err := lockRun(ctx, tx, req.TenantID, req.RunID)
	if err != nil {
		return domain.Grant{}, err
	}
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return domain.Grant{}, err
	}
	if r.Status == domain.Succeeded || r.Status == domain.Failed || r.Sequence != settled+1 {
		return domain.Grant{}, domain.ErrNotReady
	}
	// Complete has accepted the prior Session snapshot, but its Memory result
	// is not visible until this same-session local gate is finalized.
	var memoryPending bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM execution_completions c JOIN execution_runs prior ON prior.tenant_id=c.tenant_id AND prior.run_id=c.run_id WHERE prior.tenant_id=$1 AND prior.session_id=$2 AND c.memory_status='PENDING')`, req.TenantID, r.SessionID).Scan(&memoryPending); err != nil {
		return domain.Grant{}, err
	}
	if memoryPending {
		return domain.Grant{}, domain.ErrNotReady
	}
	if r.WaitReason == "INVALID_CLOCK" || !now.Before(r.RunDeadline) || (r.ExecutionDeadline != nil && !now.Before(*r.ExecutionDeadline)) || r.Attempts >= r.Policy.MaxAttempts {
		return domain.Grant{}, domain.ErrNotReady
	}
	if r.CurrentAttemptID != "" {
		live, err := currentLive(ctx, tx, r, now)
		if err != nil {
			return domain.Grant{}, err
		}
		if live {
			return domain.Grant{}, domain.ErrNotReady
		}
		if _, err = tx.Exec(ctx, `UPDATE execution_attempts SET status='ABORTED',ended_at=$3,reason='LEASE_EXPIRED' WHERE tenant_id=$1 AND attempt_id=$2 AND status IN ('PREPARING','EXECUTING')`, req.TenantID, r.CurrentAttemptID, now); err != nil {
			return domain.Grant{}, err
		}
	}
	var retryAt *time.Time
	if err = tx.QueryRow(ctx, `SELECT retry_at FROM execution_runs WHERE tenant_id=$1 AND run_id=$2`, req.TenantID, req.RunID).Scan(&retryAt); err != nil {
		return domain.Grant{}, err
	}
	if retryAt != nil && now.Before(*retryAt) {
		return domain.Grant{}, domain.ErrNotReady
	}
	var active int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM execution_attempts WHERE status IN ('PREPARING','EXECUTING') AND lease_until>clock_timestamp()`).Scan(&active); err != nil {
		return domain.Grant{}, err
	}
	if active >= req.MaxActive {
		return domain.Grant{}, domain.ErrCapacity
	}
	if r.Request.UsagePolicy.Enabled {
		// Claims lock different Run rows, so serialize quota decisions by tenant.
		// Every Worker replica shares this transaction-scoped lock.
		if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,812764913))`, req.TenantID); err != nil {
			return domain.Grant{}, err
		}
		var tenantActive int
		if err = tx.QueryRow(ctx, `SELECT count(*) FROM execution_attempts WHERE tenant_id=$1 AND status IN ('PREPARING','EXECUTING') AND lease_until>clock_timestamp()`, req.TenantID).Scan(&tenantActive); err != nil {
			return domain.Grant{}, err
		}
		if tenantActive >= r.Request.UsagePolicy.MaxConcurrentRuns {
			return domain.Grant{}, domain.ErrCapacity
		}
		periodStart := time.Unix((now.Unix()/r.Request.UsagePolicy.TokenPeriodSeconds)*r.Request.UsagePolicy.TokenPeriodSeconds, 0).UTC()
		var committed int64
		if err = tx.QueryRow(ctx, `SELECT COALESCE(sum(CASE WHEN s.usage_known THEN s.total_tokens ELSE r.reserved_tokens END),0)
FROM worker_tenant_usage_reservations_v1 r LEFT JOIN worker_tenant_usage_settlements_v1 s USING(tenant_id,attempt_id)
WHERE r.tenant_id=$1 AND r.created_at >= $2`, req.TenantID, periodStart).Scan(&committed); err != nil {
			return domain.Grant{}, err
		}
		if committed > r.Request.UsagePolicy.TokenLimit-r.Request.UsagePolicy.TokenReservationPerRun {
			return domain.Grant{}, domain.ErrCapacity
		}
	}
	if r.ExecutionDeadline == nil {
		deadline := now.Add(time.Duration(req.MaxRunSeconds) * time.Second)
		if r.RunDeadline.Before(deadline) {
			deadline = r.RunDeadline
		}
		r.ExecutionDeadline = &deadline
	}
	buf := make([]byte, 32)
	if _, err = rand.Read(buf); err != nil {
		return domain.Grant{}, err
	}
	token := hex.EncodeToString(buf)
	idBytes := make([]byte, 16)
	if _, err = rand.Read(idBytes); err != nil {
		return domain.Grant{}, err
	}
	attempt := "att_" + hex.EncodeToString(idBytes)
	r.Attempts++
	r.Generation++
	r.LeaseEpoch++
	r.CurrentAttemptID = attempt
	r.Status = domain.Running
	r.WaitReason = ""
	until := now.Add(r.Policy.LeaseTTL)
	if r.ExecutionDeadline.Before(until) {
		until = *r.ExecutionDeadline
	}
	_, err = tx.Exec(ctx, `INSERT INTO execution_attempts(tenant_id,attempt_id,run_id,worker_id,generation,lease_epoch,token_hash,lease_until,status,parent_ref,parent_digest,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'PREPARING',$9,$10,$11)`, req.TenantID, attempt, req.RunID, req.WorkerID, r.Generation, r.LeaseEpoch, domain.Digest([]byte(token)), until, head.Ref, head.Digest, now)
	if err != nil {
		return domain.Grant{}, err
	}
	if r.Request.UsagePolicy.Enabled {
		p := r.Request.UsagePolicy
		periodStart := time.Unix((now.Unix()/p.TokenPeriodSeconds)*p.TokenPeriodSeconds, 0).UTC()
		_, err = tx.Exec(ctx, `INSERT INTO worker_tenant_usage_reservations_v1(tenant_id,attempt_id,run_id,policy_revision,period_start,period_seconds,reserved_tokens,token_limit,input_micros_per_million_tokens,output_micros_per_million_tokens) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, req.TenantID, attempt, req.RunID, p.Revision, periodStart, p.TokenPeriodSeconds, p.TokenReservationPerRun, p.TokenLimit, p.InputMicrosPerMillionTokens, p.OutputMicrosPerMillionTokens)
		if err != nil {
			return domain.Grant{}, err
		}
	}
	_, err = tx.Exec(ctx, `UPDATE execution_runs SET status='RUNNING',wait_reason='',retry_at=NULL,execution_deadline=$3,attempts=$4,generation=$5,lease_epoch=$6,current_attempt_id=$7 WHERE tenant_id=$1 AND run_id=$2`, req.TenantID, req.RunID, r.ExecutionDeadline, r.Attempts, r.Generation, r.LeaseEpoch, attempt)
	if err != nil {
		return domain.Grant{}, err
	}
	g := domain.Grant{Run: r, AttemptID: attempt, WorkerID: req.WorkerID, Token: token, Generation: r.Generation, LeaseEpoch: r.LeaseEpoch, LeaseUntil: until, Parent: head}
	if err = tx.Commit(ctx); err != nil {
		return domain.Grant{}, err
	}
	return g, nil
}

func currentLive(ctx context.Context, tx pgx.Tx, r domain.Run, now time.Time) (bool, error) {
	var live bool
	err := tx.QueryRow(ctx, `SELECT status IN ('PREPARING','EXECUTING') AND lease_until>$3 FROM execution_attempts WHERE tenant_id=$1 AND attempt_id=$2 FOR UPDATE`, r.Request.Route.TenantID, r.CurrentAttemptID, now).Scan(&live)
	return live, err
}

// fenced rechecks time after obtaining the business locks, not at transaction
// start. An expired claimant waiting on a row lock cannot commit a late result.
func fenced(ctx context.Context, tx pgx.Tx, g domain.Grant) (domain.Run, domain.Head, time.Time, error) {
	r, head, settled, err := lockRun(ctx, tx, g.Run.Request.Route.TenantID, g.Run.Request.RunID)
	if err != nil {
		return r, head, time.Time{}, err
	}
	var worker, token, status string
	var epoch, generation int64
	var until time.Time
	err = tx.QueryRow(ctx, `SELECT worker_id,token_hash,status,lease_epoch,generation,lease_until FROM execution_attempts WHERE tenant_id=$1 AND attempt_id=$2 FOR UPDATE`, r.Request.Route.TenantID, g.AttemptID).Scan(&worker, &token, &status, &epoch, &generation, &until)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, head, time.Time{}, domain.ErrFenced
	}
	if err != nil {
		return r, head, time.Time{}, err
	}
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return r, head, now, err
	}
	if r.Status != domain.Running || r.CurrentAttemptID != g.AttemptID || r.LeaseEpoch != g.LeaseEpoch || r.Generation != g.Generation ||
		epoch != g.LeaseEpoch || generation != g.Generation || worker != g.WorkerID || token != domain.Digest([]byte(g.Token)) ||
		(status != "PREPARING" && status != "EXECUTING") || !now.Before(until) || !now.Before(r.RunDeadline) ||
		r.ExecutionDeadline == nil || !now.Before(*r.ExecutionDeadline) || r.Sequence != settled+1 || head != g.Parent {
		return r, head, now, domain.ErrFenced
	}
	return r, head, now, nil
}
func (l *Ledger) Check(ctx context.Context, g domain.Grant) error {
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	_, _, _, err = fenced(ctx, tx, g)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (l *Ledger) Renew(ctx context.Context, g domain.Grant) (domain.Grant, error) {
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return domain.Grant{}, err
	}
	defer rollback(tx)
	r, _, now, err := fenced(ctx, tx, g)
	if err != nil {
		return domain.Grant{}, err
	}
	until := now.Add(r.Policy.LeaseTTL)
	if r.ExecutionDeadline.Before(until) {
		until = *r.ExecutionDeadline
	}
	if _, err = tx.Exec(ctx, `UPDATE execution_attempts SET lease_until=$3 WHERE tenant_id=$1 AND attempt_id=$2`, r.Request.Route.TenantID, g.AttemptID, until); err != nil {
		return domain.Grant{}, err
	}
	g.LeaseUntil = until
	if err = tx.Commit(ctx); err != nil {
		return domain.Grant{}, err
	}
	return g, nil
}
func (l *Ledger) MarkExecuting(ctx context.Context, g domain.Grant) error {
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	r, _, now, err := fenced(ctx, tx, g)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE execution_attempts SET status='EXECUTING',agent_started_at=$3 WHERE tenant_id=$1 AND attempt_id=$2 AND status='PREPARING' AND agent_started_at IS NULL`, r.Request.Route.TenantID, g.AttemptID, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrFenced
	}
	return tx.Commit(ctx)
}

// ActiveToken is an online authorization query. It has no dependency on a
// worker process' in-memory grant and never returns the token itself.
func (l *Ledger) ActiveToken(ctx context.Context, worker, token, manifestID, manifestDigest string) (domain.Grant, error) {
	if worker == "" || token == "" {
		return domain.Grant{}, domain.ErrFenced
	}
	var tenant, run, attempt string
	err := l.pool.QueryRow(ctx, `SELECT a.tenant_id,a.run_id,a.attempt_id FROM execution_attempts a JOIN execution_runs r ON r.tenant_id=a.tenant_id AND r.run_id=a.run_id WHERE a.token_hash=$1 AND a.worker_id=$2 AND r.current_attempt_id=a.attempt_id AND r.status='RUNNING' AND a.status IN ('PREPARING','EXECUTING') AND a.lease_until>clock_timestamp() AND r.run_deadline>clock_timestamp() AND r.execution_deadline>clock_timestamp()`, domain.Digest([]byte(token)), worker).Scan(&tenant, &run, &attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Grant{}, domain.ErrFenced
	}
	if err != nil {
		return domain.Grant{}, err
	}
	r, err := l.FindRun(ctx, tenant, run)
	if err != nil {
		return domain.Grant{}, err
	}
	if r.Request.Route.ManifestRef != manifestID || r.Request.Route.ManifestDigest != manifestDigest {
		return domain.Grant{}, domain.ErrFenced
	}
	g := domain.Grant{Run: r, AttemptID: attempt, WorkerID: worker, Token: token, Generation: r.Generation, LeaseEpoch: r.LeaseEpoch}
	err = l.pool.QueryRow(ctx, `SELECT parent_ref,parent_digest,lease_until FROM execution_attempts WHERE tenant_id=$1 AND attempt_id=$2`, tenant, attempt).Scan(&g.Parent.Ref, &g.Parent.Digest, &g.LeaseUntil)
	if err != nil {
		return domain.Grant{}, err
	}
	if err = l.Check(ctx, g); err != nil {
		return domain.Grant{}, err
	}
	g.Token = ""
	return g, nil
}
