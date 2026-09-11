package execution

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/artifact"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

// BlockForReview parks a session for a human decision. It is the commit that
// deliberately commits nothing the user will see:
//
//   - the workspace is discarded (events, state — the run's partial work is
//     not authoritative, because the tool outcome it depends on is unknown);
//   - head_seq does not move, so the message is not lost and not skipped —
//     it is waiting;
//   - no reply is enqueued: the one thing the platform may not do here is
//     tell the user something it is not sure about (approved plan, IM
//     "unknown、无自动补发" and 工具 "unknown 阻断后续调用");
//   - the session records why, and the lease is released, so a human's
//     disposition — not a retry loop — is what moves this conversation.
//
// The conditional re-check is the same one Commit performs, and for the same
// reason: whether this worker still owns the session cannot be checked
// before the transaction that is about to act on that fact.
func (s *Service) BlockForReview(ctx context.Context, c *Claim, reason, errorType string) error {
	scope, err := s.db.Scope(c.TenantID)
	if err != nil {
		return err
	}
	return scope.WithTx(ctx, func(tx *controlplane.TxScope) error {
		var (
			currentFence   uint64
			currentVersion uint64
			currentHead    uint32
			leaseOwner     sql.NullString
			leaseUntil     sql.NullTime
			blockedReason  sql.NullString
		)
		err := tx.QueryRow(ctx, `
			SELECT fencing_token, session_version, head_seq, lease_owner, lease_until, blocked_reason
			FROM sessions
			WHERE tenant_id = ? AND session_pk = ?
			FOR UPDATE`, c.TenantID, c.SessionPK).
			Scan(&currentFence, &currentVersion, &currentHead, &leaseOwner, &leaseUntil, &blockedReason)
		if err != nil {
			return fmt.Errorf("execution: re-check session at block: %w", err)
		}
		if currentFence != c.FenceToken ||
			leaseOwner.String != c.WorkerID ||
			!leaseUntil.Valid || time.Now().UTC().After(leaseUntil.Time) ||
			blockedReason.Valid ||
			currentVersion != c.BaseSessionVersion ||
			currentHead != c.InSeq {
			return ErrCommitConditionFailed
		}

		if _, err := tx.Exec(ctx, `
			UPDATE execution_attempts
			SET status = 'blocked', error = ?
			WHERE execution_id = ? AND fencing_token = ?`,
			truncateRunes(reason, 512), c.ExecutionID, c.FenceToken); err != nil {
			return fmt.Errorf("execution: close attempt for review: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE executions
			SET status = 'unknown', last_error = ?
			WHERE execution_id = ? AND tenant_id = ?`,
			truncateRunes(reason, 512), c.ExecutionID, c.TenantID); err != nil {
			return fmt.Errorf("execution: mark execution unknown: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO audit_events
				(trace_id, event, tenant_id, session_id, execution_id, agent_name,
				 decision, stage, error_type, detail, tool_name)
			VALUES (?, 'tool_unknown', ?, ?, ?, ?, 'blocked', 'tool', ?, ?, ?)`,
			traceIDFrom(c.Traceparent), c.TenantID, SessionID(c), c.ExecutionID, agent.AgentName,
			errorType, truncateRunes(reason, 512), blockedToolName(reason)); err != nil {
			return fmt.Errorf("execution: audit tool block: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE inbox_messages SET status = 'unknown'
			WHERE tenant_id = ? AND session_pk = ? AND in_seq = ?`,
			c.TenantID, c.SessionPK, c.InSeq); err != nil {
			return fmt.Errorf("execution: mark inbox unknown: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE sessions
			SET blocked_reason = ?, lease_owner = NULL, lease_until = NULL
			WHERE tenant_id = ? AND session_pk = ?`,
			truncateRunes(reason, 255), c.TenantID, c.SessionPK); err != nil {
			return fmt.Errorf("execution: block session: %w", err)
		}
		// A blocked run never becomes history, so its staged artifacts must
		// never become downloadable either (see artifact.AbandonExecution).
		if err := artifact.AbandonExecution(ctx, tx, c.TenantID, c.ExecutionID); err != nil {
			return err
		}
		return enqueueProjectionFor(ctx, tx, c.TenantID, c.SessionPK,
			fmt.Sprintf("project:block:%s:%d", c.ExecutionID, c.FenceToken))
	})
}

// blockedToolName pulls the tool name out of the block reason for the audit
// row's tool_name column. Reasons produced by the governor read
// "tool call <id> (<tool>) outcome is unknown: ..." and
// "previous attempt left tool call <id> (<tool>, side_effect=write) ...";
// when the shape does not match, the column keeps its empty default rather
// than a guess.
func blockedToolName(reason string) string {
	start := strings.IndexByte(reason, '(')
	if start < 0 {
		return ""
	}
	end := strings.IndexByte(reason[start:], ')')
	if end < 0 {
		return ""
	}
	name := reason[start+1 : start+end]
	if i := strings.Index(name, ", "); i >= 0 {
		name = name[:i]
	}
	return name
}
