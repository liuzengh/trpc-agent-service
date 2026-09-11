// Package jobs is the consumer of outbox_events: the durable work queue the
// commit transaction writes into, and the only place background work runs
// from.
//
// It generalises the pattern reply_outbox already uses for delivery — claim
// one row with FOR UPDATE SKIP LOCKED, run it under a lease, finish with a
// conditional update — because the alternative (a second queue table per
// job family) would reproduce the same claim loop for every kind. Handlers
// must be idempotent: a lease that expires mid-run lets a second worker run
// the same job, which is the safe direction to fail.
package jobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

// ErrNothingToDo is the idle answer; a sweep loop sleeps on it.
var ErrNothingToDo = errors.New("jobs: no job is claimable")

// Job is one claimed unit of background work.
type Job struct {
	ID          int64
	TenantID    string
	Kind        string
	Payload     json.RawMessage
	Attempts    int
	MaxAttempts int
}

// Handler runs one job. Returning an error schedules a retry with backoff
// until MaxAttempts, after which the row is 'failed' (visible to the DLQ
// listing and to /admin/v2/dead-letters).
type Handler func(ctx context.Context, job Job) error

// Runner claims and runs jobs for one worker identity.
type Runner struct {
	db       *controlplane.DB
	workerID string
	handlers map[string]Handler
	log      *slog.Logger
}

// DefaultLease bounds one run; handlers that can exceed it must chunk their
// work, because a second worker may claim the row after it expires (running
// it twice is safe by contract, but doing so is wasteful).
const DefaultLease = 5 * time.Minute

// New builds a Runner.
func New(db *controlplane.DB, workerID string, log *slog.Logger) *Runner {
	if log == nil {
		log = slog.Default()
	}
	return &Runner{db: db, workerID: workerID, handlers: map[string]Handler{}, log: log}
}

// Register attaches the handler for one kind. Re-registering a kind is a bug
// (two answers to "what does doc_index do"), so the second registration wins
// loudly in the log rather than silently.
func (r *Runner) Register(kind string, h Handler) {
	if _, dup := r.handlers[kind]; dup {
		r.log.Warn("jobs: handler re-registered", "kind", kind)
	}
	r.handlers[kind] = h
}

// Kinds lists what this runner can execute — used by deployment checks and
// tests to notice a registered-but-unreachable job family.
func (r *Runner) Kinds() []string {
	out := make([]string, 0, len(r.handlers))
	for k := range r.handlers {
		out = append(out, k)
	}
	return out
}

// RunOne claims and runs at most one job of any registered kind.
func (r *Runner) RunOne(ctx context.Context, tenantID string) (bool, error) {
	job, err := r.claim(ctx, tenantID)
	switch {
	case errors.Is(err, ErrNothingToDo):
		return false, nil
	case err != nil:
		return false, err
	}
	h, ok := r.handlers[job.Kind]
	if !ok {
		// A kind nobody registered is not an error condition for this
		// deployment to retry forever: it belongs to another role. The row
		// is parked as failed with the reason, visible in the DLQ listing.
		return true, r.finish(ctx, job, false, fmt.Errorf("no handler registered for kind %q in this deployment", job.Kind))
	}
	runCtx, cancel := context.WithTimeout(ctx, DefaultLease)
	defer cancel()
	runErr := h(runCtx, job)
	if runErr != nil {
		r.log.Warn("jobs: handler failed", "kind", job.Kind, "job", job.ID,
			"attempt", job.Attempts, "err", runErr)
	}
	return true, r.finish(ctx, job, runErr == nil, runErr)
}

// claim takes one pending job under SKIP LOCKED, bumps its fence and opens a
// lease in the same transaction, and also reclaims jobs whose lease expired
// (a worker that died mid-run) — the reclaim is what makes a crash a delay
// instead of a stuck queue.
func (r *Runner) claim(ctx context.Context, tenantID string) (Job, error) {
	scope, err := r.db.Scope(tenantID)
	if err != nil {
		return Job{}, err
	}
	var out Job
	err = scope.WithTx(ctx, func(tx *controlplane.TxScope) error {
		if _, err := tx.Exec(ctx, `
			UPDATE outbox_events
			SET status = 'pending', lease_owner = NULL, lease_until = NULL
			WHERE tenant_id = ? AND status = 'running' AND lease_until < UTC_TIMESTAMP(6)`,
			tenantID); err != nil {
			return fmt.Errorf("jobs: reclaim expired leases: %w", err)
		}
		row := tx.QueryRow(ctx, `
			SELECT job_id, kind, payload, attempts, max_attempts
			FROM outbox_events
			WHERE tenant_id = ?
			  AND status = 'pending'
			  AND next_attempt_at <= UTC_TIMESTAMP(6)
			ORDER BY job_id
			LIMIT 1
			FOR UPDATE SKIP LOCKED`, tenantID)
		switch err := row.Scan(&out.ID, &out.Kind, &out.Payload, &out.Attempts, &out.MaxAttempts); {
		case errors.Is(err, sql.ErrNoRows):
			return ErrNothingToDo
		case err != nil:
			return fmt.Errorf("jobs: claim: %w", err)
		}
		out.TenantID = tenantID
		if _, err := tx.Exec(ctx, `
			UPDATE outbox_events
			SET status = 'running', attempts = attempts + 1, fencing_token = fencing_token + 1,
			    lease_owner = ?, lease_until = UTC_TIMESTAMP(6) + INTERVAL ? SECOND
			WHERE job_id = ?`,
			r.workerID, int(DefaultLease.Seconds()), out.ID); err != nil {
			return fmt.Errorf("jobs: open lease: %w", err)
		}
		out.Attempts++
		return nil
	})
	if err != nil {
		return Job{}, err
	}
	return out, nil
}

// finish closes the claimed row: done on success, back to pending with a
// backoff while attempts remain, failed when they do not.
func (r *Runner) finish(ctx context.Context, job Job, ok bool, runErr error) error {
	scope, err := r.db.Scope(job.TenantID)
	if err != nil {
		return err
	}
	detail := ""
	if runErr != nil {
		detail = truncate(runErr.Error(), 512)
	}
	if ok {
		_, err = scope.Exec(ctx, `
			UPDATE outbox_events
			SET status = 'done', lease_owner = NULL, lease_until = NULL, last_error = ?
			WHERE job_id = ? AND status = 'running'`, detail, job.ID)
		if err != nil {
			return fmt.Errorf("jobs: close job: %w", err)
		}
		return nil
	}
	if job.Attempts >= job.MaxAttempts {
		_, err = scope.Exec(ctx, `
			UPDATE outbox_events
			SET status = 'failed', lease_owner = NULL, lease_until = NULL, last_error = ?
			WHERE job_id = ? AND status = 'running'`, detail, job.ID)
	} else {
		_, err = scope.Exec(ctx, `
			UPDATE outbox_events
			SET status = 'pending', lease_owner = NULL, lease_until = NULL,
			    next_attempt_at = UTC_TIMESTAMP(6) + INTERVAL ? SECOND, last_error = ?
			WHERE job_id = ? AND status = 'running'`,
			backoffSeconds(job.Attempts), detail, job.ID)
	}
	if err != nil {
		return fmt.Errorf("jobs: reschedule job: %w", err)
	}
	return nil
}

// backoffSeconds grows with the attempt number, capped: a sick dependency
// should be retried rarely, and a transient one quickly.
func backoffSeconds(attempt int) int {
	seconds := 2 << uint(min(attempt, 6)) // 4s … 128s
	return seconds
}

// Enqueue writes one job. It is safe to call inside a caller's transaction
// (pass its TxScope) or standalone (pass nil): the idempotency key is the
// deduplication, exactly as reply_outbox uses its (tenant, execution,
// part_seq) key — re-enqueuing a job that already exists is a no-op, not a
// duplicate run.
func Enqueue(ctx context.Context, scope controlplane.Scope, tx *controlplane.TxScope, tenantID, kind, idempotencyKey string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("jobs: encode payload: %w", err)
	}
	const q = `
		INSERT INTO outbox_events (tenant_id, kind, idempotency_key, payload)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE job_id = job_id`
	var execErr error
	if tx != nil {
		_, execErr = tx.Exec(ctx, q, tenantID, kind, idempotencyKey, string(raw))
	} else {
		_, execErr = scope.Exec(ctx, q, tenantID, kind, idempotencyKey, string(raw))
	}
	if execErr != nil {
		return fmt.Errorf("jobs: enqueue %s: %w", kind, execErr)
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := s[:n]
	for len(cut) > 0 && cut[len(cut)-1]&0xC0 == 0x80 {
		cut = cut[:len(cut)-1]
	}
	return cut
}
