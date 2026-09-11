// Package execution runs one accepted message to completion: claim a session
// under a lease, run its agent against a private sessionstore workspace, and
// commit the result only if this worker still holds the fence.
//
// Everything in here is a short transaction. The model call and any tool
// calls happen in between, holding no database lock at all — a claim that
// lasted as long as a Runner takes would turn a slow upstream into a
// table-wide stall, which is the failure mode the whole "claim, then run,
// then commit conditionally" shape exists to avoid (approved plan, "领取与执行"
// and "模型执行不持有 SQL 长事务").
package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/event"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionstore"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Defaults for the lease, taken from the approved plan's numbers rather than
// invented here: a claim lasts thirty seconds and is renewed every ten, so a
// worker that dies is taken over within half a minute and a worker that merely
// stalls on one slow model call is not.
const (
	DefaultLeaseTTL      = 30 * time.Second
	DefaultRenewInterval = 10 * time.Second
)

// ErrStaleFence means this worker's lease was taken over by someone else
// while it was running. It is not retried: the correct response is to stop,
// because another worker already owns the outcome of this execution now.
var ErrStaleFence = errors.New("execution: this worker no longer holds the session fence")

// ErrNothingToClaim is returned when no session is currently available. It is
// not an error condition in the ordinary sense — an idle cluster is the
// normal state — and callers loop on it with a backoff.
var ErrNothingToClaim = errors.New("execution: no session is claimable")

// Claim is one session this worker now owns, plus the head message it is
// allowed to execute and the committed session state to run against.
type Claim struct {
	WorkerID    string
	TenantID    string
	SessionPK   int64
	AppID       int64
	BindingID   int64
	ActorKey    string
	IsGroup     bool
	Generation  uint32
	RevisionID  int64
	ExecutionID string
	InSeq       uint32
	FenceToken  uint64
	LeaseTTL    time.Duration

	// BaseSessionVersion is the committed session version the private
	// workspace was built from. The commit step re-checks it: an unchanged
	// version is what proves no one else moved the conversation forward while
	// this execution was running.
	BaseSessionVersion uint64

	State          map[string][]byte
	Events         []event.Event
	Summary        string
	SummaryCovered uint32
	UserText       string
	Traceparent    string
	Guardrails     tenant.Guardrails

	// claimedAt is when this row became ours, not when the inbox message
	// arrived: the latency an execution records is "how long this worker's
	// attempt took", and a queue that sat idle for an hour before a worker
	// got to it must not make the model look slow.
	claimedAt time.Time
}

// Service claims and commits executions against the control-plane database.
type Service struct {
	db       *controlplane.DB
	leaseTTL time.Duration
}

// NewService wires execution to the control plane's database.
func NewService(db *controlplane.DB, leaseTTL time.Duration) *Service {
	if leaseTTL <= 0 {
		leaseTTL = DefaultLeaseTTL
	}
	return &Service{db: db, leaseTTL: leaseTTL}
}

// DB exposes the control-plane database to collaborators that own their own
// tables — the tool ledger in trpcservice/tool writes tool_calls, and the
// worker needs the handle to build one per execution without this package
// gaining a ledger of its own.
func (s *Service) DB() *controlplane.DB { return s.db }

// LeaseTTL reports the interval Claim.Renew and Claim.StopRenewing assume.
func (s *Service) LeaseTTL() time.Duration { return s.leaseTTL }

// ClaimNext finds one claimable session and takes it. It is called under a
// scope whose tenant is allowed to own the session — a worker that serves
// every tenant iterates tenants first, then claims within each.
//
// "FOR UPDATE SKIP LOCKED" is the whole reason two workers do not fight: a
// session another worker already holds does not block this query, it is
// skipped, so a busy cluster divides sessions between itself with no
// application-side coordination and no retry-on-lock-timeout loop.
//
// head_seq <= in_seq is the "there is work waiting" condition: both counters
// live on the session row so this stays one statement, and only the head
// message is ever taken (approved plan, "队头重试期间后续消息等待").
func (s *Service) ClaimNext(ctx context.Context, tenantID, workerID string) (*Claim, error) {
	scope, err := s.db.Scope(tenantID)
	if err != nil {
		return nil, err
	}

	var out *Claim
	err = scope.WithTx(ctx, func(tx *controlplane.TxScope) error {
		var (
			c           Claim
			bindingID   sql.NullInt64
			stateRaw    sql.NullString
			summaryRaw  sql.NullString
			userTextRaw sql.NullString
		)
		row := tx.QueryRow(ctx, `
			SELECT s.session_pk, s.app_id, s.binding_id, s.actor_key, s.is_group, s.generation,
			       s.revision_id, s.session_version, s.state, s.summary,
			       s.summary_covered_seq, s.head_seq, s.fencing_token
			FROM sessions s
			WHERE s.tenant_id = ?
			  AND s.blocked_reason IS NULL
			  AND s.head_seq <= s.in_seq
			  AND (s.lease_until IS NULL OR s.lease_until < UTC_TIMESTAMP(6))
			ORDER BY s.session_pk
			LIMIT 1
			FOR UPDATE SKIP LOCKED`, tenantID)
		switch err := row.Scan(&c.SessionPK, &c.AppID, &bindingID, &c.ActorKey, &c.IsGroup, &c.Generation,
			&c.RevisionID, &c.BaseSessionVersion, &stateRaw,
			&summaryRaw, &c.SummaryCovered, &c.InSeq, &c.FenceToken); {
		case errors.Is(err, sql.ErrNoRows):
			return ErrNothingToClaim
		case err != nil:
			return fmt.Errorf("execution: claim: %w", err)
		}
		c.TenantID = tenantID
		c.BindingID = bindingID.Int64

		nextFence := c.FenceToken + 1
		if _, err := tx.Exec(ctx, `
			UPDATE sessions
			SET fencing_token = ?, lease_owner = ?, lease_until = UTC_TIMESTAMP(6) + INTERVAL ? MICROSECOND
			WHERE tenant_id = ? AND session_pk = ? AND fencing_token = ?`,
			nextFence, workerID, c.leaseUntilMicros(s.leaseTTL), tenantID, c.SessionPK, c.FenceToken); err != nil {
			return fmt.Errorf("execution: take lease: %w", err)
		}
		c.FenceToken = nextFence
		c.WorkerID = workerID
		c.LeaseTTL = s.leaseTTL

		// The head message, now guaranteed unlocked because this session is
		// ours for the next lease window.
		msgRow := tx.QueryRow(ctx, `
			SELECT im.execution_id, im.in_seq, im.traceparent, im.text, im.status,
			       e.revision_id, e.model_profile_id, e.backend_profile_id
			FROM inbox_messages im
			JOIN executions e ON e.execution_id = im.execution_id
			WHERE im.tenant_id = ? AND im.session_pk = ? AND im.in_seq = ?
			FOR UPDATE`,
			tenantID, c.SessionPK, c.InSeq)
		var (
			execStatus  string
			executionID string
			traceparent string
			revID       int64
			modelID     int64
			backendID   int64
		)
		if err := msgRow.Scan(&executionID, &c.InSeq, &traceparent, &userTextRaw, &execStatus,
			&revID, &modelID, &backendID); err != nil {
			return fmt.Errorf("execution: load head message: %w", err)
		}
		if execStatus == "done" {
			// Should be unreachable while head ordering holds, but a repair
			// that moved head_seq without moving in_seq would show up here as
			// a no-op rather than a re-run of completed work.
			return ErrNothingToClaim
		}
		c.ExecutionID = executionID
		c.Traceparent = traceparent
		c.claimedAt = time.Now().UTC()
		if userTextRaw.Valid {
			c.UserText = userTextRaw.String
		}

		// The guardrail policy that applies to this message is the one that
		// was published with the revision this session is fixed to — not
		// whatever an admin is running by the time a worker gets to it.
		var guardrailsRaw sql.NullString
		if err := tx.QueryRow(ctx,
			"SELECT guardrails FROM agent_revisions WHERE tenant_id = ? AND revision_id = ?",
			tenantID, revID).Scan(&guardrailsRaw); err != nil {
			return fmt.Errorf("execution: load revision guardrails: %w", err)
		}
		if guardrailsRaw.Valid && guardrailsRaw.String != "" {
			if err := json.Unmarshal([]byte(guardrailsRaw.String), &c.Guardrails); err != nil {
				return fmt.Errorf("execution: decode revision guardrails: %w", err)
			}
		}

		if _, err := tx.Exec(ctx,
			"UPDATE executions SET status = 'running', attempts = attempts + 1 WHERE execution_id = ? AND tenant_id = ?",
			executionID, tenantID); err != nil {
			return fmt.Errorf("execution: mark running: %w", err)
		}
		if _, err := tx.Exec(ctx,
			"UPDATE inbox_messages SET status = 'running' WHERE tenant_id = ? AND session_pk = ? AND in_seq = ?",
			tenantID, c.SessionPK, c.InSeq); err != nil {
			return fmt.Errorf("execution: mark inbox running: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO execution_attempts (execution_id, worker_id, fencing_token, status, base_session_version)
			VALUES (?, ?, ?, 'running', ?)`,
			executionID, workerID, c.FenceToken, c.BaseSessionVersion); err != nil {
			return fmt.Errorf("execution: open attempt: %w", err)
		}

		if stateRaw.Valid {
			if err := json.Unmarshal([]byte(stateRaw.String), &c.State); err != nil {
				return fmt.Errorf("execution: decode committed state: %w", err)
			}
		}
		if c.State == nil {
			c.State = map[string][]byte{}
		}
		if summaryRaw.Valid {
			c.Summary = summaryRaw.String
		}

		// Committed history, oldest first: the private workspace is seeded
		// from this, and the Runner must not see an empty conversation just
		// because a second worker picked up where the first left off.
		events, err := loadCommittedEvents(ctx, tx, tenantID, c.SessionPK)
		if err != nil {
			return err
		}
		c.Events = events

		out = &c
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c Claim) leaseUntilMicros(ttl time.Duration) int64 {
	return ttl.Microseconds()
}

// Workspace builds the private session service the Runner is given: a
// sessionstore seeded from the committed snapshot this claim read. Nothing the
// Runner writes through it can reach MySQL until Commit is called.
func (c *Claim) Workspace() *sessionstore.Workspace {
	base := sessionForClaim(c)
	return sessionstore.NewWorkspace(sessionstore.Options{Base: base})
}

// Renew extends the lease. It must be called on a timer while the model runs;
// a failed renewal is the worker's signal to stop, because it means someone
// else already took over (see StopRenewing and Commit's own fence check).
func (s *Service) Renew(ctx context.Context, c *Claim) error {
	scope, err := s.db.Scope(c.TenantID)
	if err != nil {
		return err
	}
	res, err := scope.Exec(ctx, `
		UPDATE sessions
		SET lease_until = UTC_TIMESTAMP(6) + INTERVAL ? MICROSECOND
		WHERE tenant_id = ? AND session_pk = ? AND lease_owner = ? AND fencing_token = ?
		  AND lease_until >= UTC_TIMESTAMP(6)`,
		c.LeaseTTL.Microseconds(), c.TenantID, c.SessionPK, c.WorkerID, c.FenceToken)
	if err != nil {
		return fmt.Errorf("execution: renew lease: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrStaleFence
	}
	return nil
}

// StopRenewing releases the lease early (a graceful shutdown). It never
// returns ErrStaleFence: losing a lease while trying to hand it back is the
// normal, fine case — the point was to stop claiming with this fence, which
// is already true.
func (s *Service) StopRenewing(ctx context.Context, c *Claim) error {
	scope, err := s.db.Scope(c.TenantID)
	if err != nil {
		return err
	}
	_, err = scope.Exec(ctx, `
		UPDATE sessions
		SET lease_owner = NULL, lease_until = NULL
		WHERE tenant_id = ? AND session_pk = ? AND lease_owner = ? AND fencing_token = ?`,
		c.TenantID, c.SessionPK, c.WorkerID, c.FenceToken)
	if err != nil {
		return fmt.Errorf("execution: release lease: %w", err)
	}
	return nil
}
