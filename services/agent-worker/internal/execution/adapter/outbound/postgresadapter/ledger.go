// Package postgresadapter implements Execution's atomic, tenant-scoped ledger.
package postgresadapter

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"go.opentelemetry.io/otel/trace"
)

type Ledger struct {
	pool   *pgxpool.Pool
	Tracer trace.Tracer
}

var _ application.Ledger = (*Ledger)(nil)

func New(pool *pgxpool.Pool) *Ledger { return &Ledger{pool: pool} }
func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
func databaseTime(ctx context.Context, tx pgx.Tx) (time.Time, error) {
	var now time.Time
	err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now)
	return now, err
}
func mapped(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound
	}
	return err
}

const runColumns = `request_json,session_id,session_sequence,status,wait_reason,policy_json,accepted_at,run_deadline,reply_deadline,execution_deadline,attempts,generation,lease_epoch,current_attempt_id`

func prefixColumns(prefix, columns string) string {
	return prefix + "." + strings.ReplaceAll(columns, ",", ","+prefix+".")
}
func scanRun(row pgx.Row, extra ...any) (domain.Run, error) {
	var r domain.Run
	var raw, policy []byte
	err := row.Scan(append([]any{&raw, &r.SessionID, &r.Sequence, &r.Status, &r.WaitReason, &policy, &r.AcceptedAt, &r.RunDeadline, &r.ReplyDeadline, &r.ExecutionDeadline, &r.Attempts, &r.Generation, &r.LeaseEpoch, &r.CurrentAttemptID}, extra...)...)
	if err != nil {
		return r, mapped(err)
	}
	if err = json.Unmarshal(raw, &r.Request); err != nil {
		return r, err
	}
	err = json.Unmarshal(policy, &r.Policy)
	return r, err
}
func (l *Ledger) FindRun(ctx context.Context, tenant, id string) (domain.Run, error) {
	return scanRun(l.pool.QueryRow(ctx, `SELECT `+runColumns+` FROM execution_runs WHERE tenant_id=$1 AND run_id=$2`, tenant, id))
}

// lockRun uses the same Session -> Run order in Claim, Renew, Complete and
// recovery. The first read only locates the immutable session identity.
func lockRun(ctx context.Context, tx pgx.Tx, tenant, id string, extra ...any) (domain.Run, domain.Head, int64, error) {
	var session string
	if err := tx.QueryRow(ctx, `SELECT session_id FROM execution_runs WHERE tenant_id=$1 AND run_id=$2`, tenant, id).Scan(&session); err != nil {
		return domain.Run{}, domain.Head{}, 0, mapped(err)
	}
	var head domain.Head
	var settled int64
	if err := tx.QueryRow(ctx, `SELECT accepted_ref,accepted_digest,settled_sequence FROM execution_sessions WHERE tenant_id=$1 AND session_id=$2 FOR UPDATE`, tenant, session).Scan(&head.Ref, &head.Digest, &settled); err != nil {
		return domain.Run{}, head, 0, err
	}
	columns := runColumns
	if len(extra) > 0 {
		columns += `,COALESCE(traceparent,''),COALESCE(tracestate,'')`
	}
	r, err := scanRun(tx.QueryRow(ctx, `SELECT `+columns+` FROM execution_runs WHERE tenant_id=$1 AND run_id=$2 FOR UPDATE`, tenant, id), extra...)
	return r, head, settled, err
}
func rejected(ctx context.Context, tx pgx.Tx, source, digest, reason string) error {
	_, err := tx.Exec(ctx, `INSERT INTO execution_rejections(rejection_id,source_identity,digest,reason) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, domain.StableID("rej", source+"\x00"+digest), source, digest, reason)
	return err
}
func (l *Ledger) Reject(ctx context.Context, source, digest, reason string) error {
	if source == "" || !domain.DigestValid(digest) || reason == "" {
		return domain.ErrInvalid
	}
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if err = rejected(ctx, tx, source, digest, reason); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func receipt(ctx context.Context, tx pgx.Tx, eventID string) (domain.Receipt, string, error) {
	var r domain.Receipt
	var digest string
	var carrier tracecontext.Carrier
	err := tx.QueryRow(ctx, `SELECT e.event_id,e.event_digest,e.tenant_id,e.run_id,e.outcome,r.session_id,r.session_sequence,COALESCE(r.traceparent,''),COALESCE(r.tracestate,'') FROM execution_receipts e JOIN execution_runs r ON r.tenant_id=e.tenant_id AND r.run_id=e.run_id WHERE e.event_id=$1`, eventID).Scan(&r.EventID, &digest, &r.TenantID, &r.RunID, &r.Outcome, &r.SessionID, &r.Sequence, &carrier.Traceparent, &carrier.Tracestate)
	if err == nil {
		if sc := trace.SpanContextFromContext(carrier.Restore(context.Background())); sc.IsValid() {
			trace.SpanFromContext(ctx).AddLink(trace.Link{SpanContext: sc})
		}
	}
	return r, digest, mapped(err)
}

func (l *Ledger) Accept(ctx context.Context, req domain.Requested, policy domain.Policy, limits domain.IntakeLimits) (domain.Receipt, error) {
	if req.Validate() != nil || policy.Validate() != nil || limits.Validate() != nil {
		return domain.Receipt{}, domain.ErrInvalid
	}
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return domain.Receipt{}, err
	}
	defer rollback(tx)
	// Admission identity locks precede all business row locks. Sorted identities
	// serialize collisions even when event IDs, tenant or Session differ.
	keys := []string{"event:" + req.EventID, "run:" + req.RunID, "admission:" + req.AdmissionID}
	sort.Strings(keys)
	for _, key := range keys {
		if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,731004284))`, key); err != nil {
			return domain.Receipt{}, err
		}
	}
	if old, digest, e := receipt(ctx, tx, req.EventID); e == nil {
		if digest == req.EventDigest {
			return old, tx.Commit(ctx)
		}
		if e = rejected(ctx, tx, "event:"+req.EventID, req.EventDigest, "EVENT_CONFLICT"); e != nil {
			return domain.Receipt{}, e
		}
		if e = tx.Commit(ctx); e != nil {
			return domain.Receipt{}, e
		}
		return domain.Receipt{}, domain.ErrConflict
	} else if !errors.Is(e, domain.ErrNotFound) {
		return domain.Receipt{}, e
	}
	var tenant, id, digest, admission string
	err = tx.QueryRow(ctx, `SELECT tenant_id,run_id,request_digest,admission_id FROM execution_runs WHERE run_id=$1 OR admission_id=$2 ORDER BY run_id LIMIT 1`, req.RunID, req.AdmissionID).Scan(&tenant, &id, &digest, &admission)
	if err == nil {
		if tenant != req.Route.TenantID || id != req.RunID || admission != req.AdmissionID || digest != req.RunDigest {
			if err = rejected(ctx, tx, "event:"+req.EventID, req.EventDigest, "RUN_CONFLICT"); err != nil {
				return domain.Receipt{}, err
			}
			if err = tx.Commit(ctx); err != nil {
				return domain.Receipt{}, err
			}
			return domain.Receipt{}, domain.ErrConflict
		}
		if _, err = tx.Exec(ctx, `INSERT INTO execution_receipts(event_id,event_digest,tenant_id,run_id,outcome) VALUES($1,$2,$3,$4,'ACCEPTED')`, req.EventID, req.EventDigest, tenant, id); err != nil {
			return domain.Receipt{}, err
		}
		r, _, e := receipt(ctx, tx, req.EventID)
		if e != nil {
			return r, e
		}
		return r, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.Receipt{}, err
	}
	// A tenant backend migration holds the matching exclusive advisory lock
	// while it verifies quiescence and copies current Memory heads. Existing
	// receipt replays above remain available, but a new Run cannot cross the
	// migration snapshot/cutover boundary.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended($1,731004286))`, req.Route.TenantID); err != nil {
		return domain.Receipt{}, err
	}
	// Capacity admission is globally serialized, but only for new identities.
	// Retries replay their receipt even while the queue is full.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(731004285)`); err != nil {
		return domain.Receipt{}, err
	}
	// Count all retained Run identities under the same cross-process admission
	// lock. Completed history still occupies storage, but this gate never blocks
	// receipt replay, an alias for an existing Run, or that Run's recovery writes.
	var queued, retained int64
	if err = tx.QueryRow(ctx, `SELECT count(*) FILTER(WHERE status IN ('QUEUED','RUNNING','RETRY_WAIT')),count(*) FROM execution_runs`).Scan(&queued, &retained); err != nil {
		return domain.Receipt{}, err
	}
	if queued >= int64(limits.MaxQueuedRuns) || retained >= int64(limits.MaxRetainedRuns) {
		return domain.Receipt{}, domain.ErrCapacity
	}
	session := req.SessionID()
	scope, err := json.Marshal(req.Scope())
	if err != nil {
		return domain.Receipt{}, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO execution_sessions(tenant_id,session_id,scope_json) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, req.Route.TenantID, session, scope); err != nil {
		return domain.Receipt{}, err
	}
	// Identity, session association, Run and Receipt are one intake transaction.
	// Replay and capacity checks above produce no new identity or last-seen writes.
	identity := req.SocialIdentityID()
	if _, err = tx.Exec(ctx, `INSERT INTO execution_social_identities(tenant_id,identity_id,provider,account_id,external_user_id) VALUES($1,$2,$3,$4,$5) ON CONFLICT(tenant_id,identity_id) DO UPDATE SET last_seen_at=clock_timestamp()`, req.Route.TenantID, identity, req.Route.Provider, req.Route.AccountID, req.Input.SenderID); err != nil {
		return domain.Receipt{}, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO execution_session_identities(tenant_id,session_id,identity_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, req.Route.TenantID, session, identity); err != nil {
		return domain.Receipt{}, err
	}
	var linked bool
	if err = tx.QueryRow(ctx, `SELECT identity_id=$3 FROM execution_session_identities WHERE tenant_id=$1 AND session_id=$2`, req.Route.TenantID, session, identity).Scan(&linked); err != nil || !linked {
		if err != nil {
			return domain.Receipt{}, err
		}
		return domain.Receipt{}, domain.ErrConflict
	}
	var seq int64
	var same bool
	if err = tx.QueryRow(ctx, `SELECT next_sequence,scope_json=$3::jsonb FROM execution_sessions WHERE tenant_id=$1 AND session_id=$2 FOR UPDATE`, req.Route.TenantID, session, scope).Scan(&seq, &same); err != nil {
		return domain.Receipt{}, err
	}
	if !same {
		return domain.Receipt{}, domain.ErrConflict
	}
	seq++
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return domain.Receipt{}, err
	}
	wait := "MANIFEST"
	if req.Input.ReceivedAt.After(now.Add(policy.MaxFutureSkew)) {
		wait = "INVALID_CLOCK"
	}
	raw, err := json.Marshal(req)
	if err != nil {
		return domain.Receipt{}, err
	}
	p, err := json.Marshal(policy)
	if err != nil {
		return domain.Receipt{}, err
	}
	carrier := tracecontext.Capture(ctx)
	_, err = tx.Exec(ctx, `INSERT INTO execution_runs(tenant_id,run_id,admission_id,request_digest,request_json,session_id,session_sequence,status,wait_reason,policy_json,accepted_at,run_deadline,reply_deadline,traceparent,tracestate) VALUES($1,$2,$3,$4,$5,$6,$7,'QUEUED',$8,$9,$10,$11,$12,NULLIF($13,''),NULLIF($14,''))`, req.Route.TenantID, req.RunID, req.AdmissionID, req.RunDigest, raw, session, seq, wait, p, now, req.Input.ReceivedAt.Add(policy.MaxRunAge), req.Input.ReceivedAt.Add(policy.MaxReplyAge), carrier.Traceparent, carrier.Tracestate)
	if err != nil {
		return domain.Receipt{}, err
	}
	if _, err = tx.Exec(ctx, `UPDATE execution_sessions SET next_sequence=$3 WHERE tenant_id=$1 AND session_id=$2`, req.Route.TenantID, session, seq); err != nil {
		return domain.Receipt{}, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO execution_receipts(event_id,event_digest,tenant_id,run_id,outcome) VALUES($1,$2,$3,$4,'ACCEPTED')`, req.EventID, req.EventDigest, req.Route.TenantID, req.RunID); err != nil {
		return domain.Receipt{}, err
	}
	r := domain.Receipt{EventID: req.EventID, RunID: req.RunID, TenantID: req.Route.TenantID, SessionID: session, Sequence: seq, Outcome: "ACCEPTED"}
	return r, tx.Commit(ctx)
}

func (l *Ledger) Ready(ctx context.Context, limit int) ([]domain.Run, error) {
	rows, err := l.Scheduled(ctx, limit)
	if err != nil {
		return nil, err
	}
	out := make([]domain.Run, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.Run)
	}
	return out, nil
}

func (l *Ledger) Scheduled(ctx context.Context, limit int) ([]application.ScheduledRun, error) {
	if limit < 1 {
		return nil, domain.ErrInvalid
	}
	rows, err := l.pool.Query(ctx, `SELECT `+prefixColumns("r", runColumns)+`,COALESCE(r.traceparent,''),COALESCE(r.tracestate,'') FROM execution_runs r JOIN execution_sessions s ON s.tenant_id=r.tenant_id AND s.session_id=r.session_id WHERE r.status IN ('QUEUED','RUNNING','RETRY_WAIT') AND r.session_sequence=s.settled_sequence+1 AND NOT EXISTS (SELECT 1 FROM execution_completions c JOIN execution_runs prior ON prior.tenant_id=c.tenant_id AND prior.run_id=c.run_id WHERE prior.tenant_id=r.tenant_id AND prior.session_id=r.session_id AND c.memory_status='PENDING') AND NOT EXISTS (SELECT 1 FROM tool_approval_operations o WHERE o.tenant_id=r.tenant_id AND o.run_id=r.run_id AND o.status='UNKNOWN') AND NOT EXISTS (SELECT 1 FROM execution_attempts a WHERE a.tenant_id=r.tenant_id AND a.attempt_id=r.current_attempt_id AND a.status IN ('PREPARING','EXECUTING') AND a.lease_until>clock_timestamp()) AND (r.retry_at IS NULL OR r.retry_at<=clock_timestamp() OR r.run_deadline<=clock_timestamp() OR r.execution_deadline<=clock_timestamp()) ORDER BY r.accepted_at,r.tenant_id,r.session_id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []application.ScheduledRun
	for rows.Next() {
		var carrier tracecontext.Carrier
		r, err := scanRun(rows, &carrier.Traceparent, &carrier.Tracestate)
		if err != nil {
			return nil, err
		}
		out = append(out, application.ScheduledRun{Run: r, Carrier: carrier.Normalize()})
	}
	return out, rows.Err()
}
