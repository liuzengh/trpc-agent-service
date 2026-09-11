package execution

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

// Disposition is what a human decided about the parked message.
type Disposition string

const (
	// DispositionConfirmed: the side effect happened (or the work should be
	// considered done). The head message is retired with a receipt to the
	// user; nothing is re-run.
	DispositionConfirmed Disposition = "confirmed"
	// DispositionCancelled: the side effect did not happen. The head message
	// goes back to pending and the next worker re-runs it — the resolution
	// rows are what let the ledger's recovery rule pass this time.
	DispositionCancelled Disposition = "cancelled"
)

// ResolveOutcome reports what one disposition request did.
type ResolveOutcome struct {
	// Unblocked is true when this call moved the session out of blocked.
	Unblocked bool
	// Remaining is how many unresolved calls still hold the session.
	Remaining int
	// Disposition is the applied disposition, or "" when the session was
	// still waiting for other resolutions.
	Disposition Disposition
	// HeadExecutionID is the execution the disposition acted on.
	HeadExecutionID string
}

// ErrNotToolBlocked means the session is parked for a reason this path does
// not own (a reply-delivery unknown, say). Disposing of the wrong kind of
// block is worse than refusing: the two have different safe actions.
var ErrNotToolBlocked = errors.New("execution: this session is not blocked by a tool call")

// ToolBlockPrefix is the marker every governor-authored block reason starts
// with. It is the only thing that authorises TryUnblock to act.
const ToolBlockPrefix = "tool "

// TryUnblock closes out a session parked by a tool-call unknown, once every
// unresolved call of its head execution has a resolution. Everything — the
// re-check of what is blocked, the count that decides whether it is still
// blocked, and the disposition itself — happens in one transaction, because
// "the last call" is exactly the state two concurrent resolutions race over.
func (s *Service) TryUnblock(ctx context.Context, tenantID string, sessionPK int64) (ResolveOutcome, error) {
	scope, err := s.db.Scope(tenantID)
	if err != nil {
		return ResolveOutcome{}, err
	}
	var out ResolveOutcome
	err = scope.WithTx(ctx, func(tx *controlplane.TxScope) error {
		var (
			blockedReason sql.NullString
			headSeq       uint32
			appID         int64
			bindingID     sql.NullInt64
			actorKey      string
			generation    uint32
		)
		err := tx.QueryRow(ctx, `
			SELECT blocked_reason, head_seq, app_id, binding_id, actor_key, generation
			FROM sessions
			WHERE tenant_id = ? AND session_pk = ?
			FOR UPDATE`, tenantID, sessionPK).Scan(&blockedReason, &headSeq,
			&appID, &bindingID, &actorKey, &generation)
		if errors.Is(err, sql.ErrNoRows) {
			return controlplane.ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("execution: lock session for resolution: %w", err)
		}
		if !blockedReason.Valid {
			// Already unblocked by someone else: the state the caller wanted
			// is the state that exists.
			return nil
		}
		if !strings.HasPrefix(blockedReason.String, ToolBlockPrefix) {
			return fmt.Errorf("%w: blocked because %q", ErrNotToolBlocked, blockedReason.String)
		}

		var headExecutionID string
		err = tx.QueryRow(ctx, `
			SELECT execution_id FROM inbox_messages
			WHERE tenant_id = ? AND session_pk = ? AND in_seq = ?
			FOR UPDATE`, tenantID, sessionPK, headSeq).Scan(&headExecutionID)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("execution: head message %d is missing; the session is blocked but has no head", headSeq)
		}
		if err != nil {
			return fmt.Errorf("execution: load head execution: %w", err)
		}
		out.HeadExecutionID = headExecutionID

		rows, err := tx.Query(ctx, `
			SELECT resolution, COUNT(*) FROM tool_calls
			WHERE tenant_id = ? AND execution_id = ?
			GROUP BY resolution`, tenantID, headExecutionID)
		if err != nil {
			return fmt.Errorf("execution: count call resolutions: %w", err)
		}
		var confirmed, unresolved int
		for rows.Next() {
			var res sql.NullString
			var n int
			if err := rows.Scan(&res, &n); err != nil {
				rows.Close()
				return fmt.Errorf("execution: scan call resolutions: %w", err)
			}
			switch {
			case !res.Valid:
				unresolved += n
			case res.String == string(DispositionConfirmed):
				confirmed += n
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()

		if unresolved > 0 {
			// Something is still undecided. The session stays blocked; the
			// caller is told how much review is left.
			out.Remaining = unresolved
			return nil
		}

		// Any confirmed side effect wins over re-running: re-running would
		// repeat work a human has already stated happened.
		disposition := DispositionCancelled
		if confirmed > 0 {
			disposition = DispositionConfirmed
		}
		out.Unblocked = true
		out.Disposition = disposition

		if _, err := tx.Exec(ctx, `
			UPDATE sessions SET blocked_reason = NULL, lease_owner = NULL, lease_until = NULL
			WHERE tenant_id = ? AND session_pk = ?`, tenantID, sessionPK); err != nil {
			return fmt.Errorf("execution: unblock session: %w", err)
		}

		switch disposition {
		case DispositionConfirmed:
			if _, err := tx.Exec(ctx, `
				UPDATE inbox_messages SET status = 'done'
				WHERE tenant_id = ? AND session_pk = ? AND in_seq = ?`,
				tenantID, sessionPK, headSeq); err != nil {
				return fmt.Errorf("execution: retire confirmed message: %w", err)
			}
			if _, err := tx.Exec(ctx, `
				UPDATE executions SET status = 'committed', last_error = ''
				WHERE execution_id = ? AND tenant_id = ?`, headExecutionID, tenantID); err != nil {
				return fmt.Errorf("execution: close confirmed execution: %w", err)
			}
			if err := enqueueResolutionReceipt(ctx, tx, tenantID, sessionPK, headExecutionID); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
				UPDATE sessions SET head_seq = head_seq + 1
				WHERE tenant_id = ? AND session_pk = ?`, tenantID, sessionPK); err != nil {
				return fmt.Errorf("execution: advance head after confirmation: %w", err)
			}
		case DispositionCancelled:
			if _, err := tx.Exec(ctx, `
				UPDATE inbox_messages SET status = 'pending'
				WHERE tenant_id = ? AND session_pk = ? AND in_seq = ?`,
				tenantID, sessionPK, headSeq); err != nil {
				return fmt.Errorf("execution: requeue cancelled message: %w", err)
			}
			if _, err := tx.Exec(ctx, `
				UPDATE executions SET status = 'pending', last_error = ''
				WHERE execution_id = ? AND tenant_id = ?`, headExecutionID, tenantID); err != nil {
				return fmt.Errorf("execution: requeue cancelled execution: %w", err)
			}
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO audit_events
				(trace_id, event, tenant_id, session_id, execution_id, agent_name,
				 decision, stage, detail)
			VALUES ('', 'tool_resolved', ?, ?, ?, ?, ?, 'tool', ?)`,
			tenantID, sessionIDFromColumns(tenantID, appID, bindingID.Int64, actorKey, generation),
			headExecutionID, agent.AgentName, string(disposition),
			fmt.Sprintf("human disposition: %s, %d calls resolved", disposition, confirmed)); err != nil {
			return fmt.Errorf("execution: audit resolution: %w", err)
		}
		return enqueueProjectionFor(ctx, tx, tenantID, sessionPK,
			fmt.Sprintf("project:resolve:%s:%s", headExecutionID, disposition))
	})
	if err != nil {
		return ResolveOutcome{}, err
	}
	return out, nil
}

// enqueueResolutionReceipt is the one reply a confirmation is allowed to
// send: it states what the platform knows (a human confirmed the earlier
// work), not what the model would have said. Its idempotency key cannot
// collide with execution parts because a blocked execution enqueued none.
func enqueueResolutionReceipt(ctx context.Context, tx *controlplane.TxScope, tenantID string, sessionPK int64, executionID string) error {
	const text = "这条请求的处理结果已由人工核对完成，无需重试。"
	_, err := tx.Exec(ctx, `
		INSERT INTO reply_outbox
			(tenant_id, execution_id, session_pk, part_seq, channel_type, binding_id,
			 target, text, is_done, idempotency_key)
		SELECT ?, ?, ?, 1, s.channel_type, s.binding_id, s.actor_key, ?, 1, ?
		FROM sessions s
		WHERE s.tenant_id = ? AND s.session_pk = ?
		ON DUPLICATE KEY UPDATE outbox_id = outbox_id`,
		tenantID, executionID, sessionPK, text,
		"resolve:"+executionID+":confirmed", tenantID, sessionPK)
	if err != nil {
		return fmt.Errorf("execution: enqueue resolution receipt: %w", err)
	}
	return nil
}
