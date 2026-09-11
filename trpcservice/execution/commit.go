package execution

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/artifact"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionstore"
	"github.com/liuzengh/trpc-agent-service/trpcservice/summary"
)

// ReplyPart is one piece of the answer to send after this execution commits.
// It is a row in reply_outbox the moment it commits, not something a
// goroutine holds in memory: the delivery role may be a different process
// that has never seen this execution, and a reply that only exists in the
// sender's stack disappears when the sender does.
type ReplyPart struct {
	Text   string
	IsDone bool
	// SideEffectFree is false for anything that is not "one whole message the
	// user has not seen yet" — the distinction delivery uses to decide
	// whether a retry is safe at all (approved plan, "投递、重试与人工处置").
	SideEffectFree bool
}

// Result is everything a completed run contributes to the commit. Prepared is
// the frozen workspace; the rest are this execution's own facts (tokens,
// latency, guardrail decision) that belong in the same transaction so a
// query can never see a committed reply with no audit row behind it.
type Result struct {
	Prepared *sessionstore.Prepared
	Parts    []ReplyPart
	Decision string
	// ModelCalled distinguishes "the model ran and its output was cut by an
	// output guardrail" from "an input guardrail rejected this before the
	// model was ever reached." Both are decision=block; only one may claim a
	// model_call audit row and token counts, or an operator cannot tell a
	// blocked-at-the-door message apart from one that reached the upstream.
	ModelCalled      bool
	ErrorType        string
	Detail           string
	Latency          time.Duration
	PromptTokens     int
	CompletionTokens int
}

// ErrCommitConditionFailed means the fence check inside the commit
// transaction did not hold: another worker already took this session over.
// The prepared write set is not applied and is not retried automatically.
var ErrCommitConditionFailed = errors.New("execution: the commit condition no longer holds")

// Commit freezes `res` as the session's next committed version.
//
// This is the one transaction the approved plan calls "原子提交与崩溃恢复":
// events, state, execution outcome, audit rows, the reply to send, the inbox
// row being marked done, and the head advancing all succeed together or none
// do. It re-checks owner, fence, lease expiry, base version, generation and
// head, because every one of those is a fact that a competing worker could
// have changed while this execution was running and none of them can be
// checked before the transaction starts (that would be a check-then-act race
// on the exact fields the transaction is about to modify).
func (s *Service) Commit(ctx context.Context, c *Claim, res *Result) error {
	if res.Prepared == nil {
		return errors.New("execution: commit needs a prepared write set")
	}
	scope, err := s.db.Scope(c.TenantID)
	if err != nil {
		return err
	}

	preparedJSON, preparedHash, err := encodePrepared(res.Prepared)
	if err != nil {
		return err
	}

	return scope.WithTx(ctx, func(tx *controlplane.TxScope) error {
		// Re-read the session row under the lock this transaction will
		// modify: every "still mine?" condition in the approved plan's commit
		// section is checked against this one read, not against the copy the
		// claim made minutes ago.
		var (
			currentFence   uint64
			currentVersion uint64
			currentHead    uint32
			leaseOwner     sql.NullString
			leaseUntil     sql.NullTime
			blockedReason  sql.NullString
			summaryVersion uint32
			summaryCovered uint32
		)
		err := tx.QueryRow(ctx, `
			SELECT fencing_token, session_version, head_seq, lease_owner, lease_until, blocked_reason,
			       summary_version, summary_covered_seq
			FROM sessions
			WHERE tenant_id = ? AND session_pk = ?
			FOR UPDATE`, c.TenantID, c.SessionPK).
			Scan(&currentFence, &currentVersion, &currentHead, &leaseOwner, &leaseUntil, &blockedReason,
				&summaryVersion, &summaryCovered)
		if err != nil {
			return fmt.Errorf("execution: re-check session at commit: %w", err)
		}
		if currentFence != c.FenceToken ||
			leaseOwner.String != c.WorkerID ||
			!leaseUntil.Valid || time.Now().UTC().After(leaseUntil.Time) ||
			blockedReason.Valid ||
			currentVersion != c.BaseSessionVersion ||
			currentHead != c.InSeq {
			return ErrCommitConditionFailed
		}

		// Persist the prepared write set before it becomes history: if any of
		// the statements below fail, a later recovery has the exact bytes this
		// attempt produced and does not need to re-run the model to know what
		// happened here.
		if _, err := tx.Exec(ctx, `
			UPDATE execution_attempts
			SET status = 'prepared', prepared = ?, prepared_hash = ?, base_session_version = ?
			WHERE execution_id = ? AND fencing_token = ?`,
			string(preparedJSON), preparedHash, c.BaseSessionVersion, c.ExecutionID, c.FenceToken); err != nil {
			return fmt.Errorf("execution: persist prepared set: %w", err)
		}

		if err := applyPreparedToSession(ctx, tx, c, res.Prepared, currentVersion); err != nil {
			return err
		}

		if err := recordExecutionOutcome(ctx, tx, c, res); err != nil {
			return err
		}

		if err := writeAuditForCommit(ctx, tx, c, res); err != nil {
			return err
		}

		if err := enqueueReplyParts(ctx, tx, c, res.Parts); err != nil {
			return err
		}

		// Artifacts a tool staged become downloadable exactly when the run
		// they belong to becomes history — the plan's "提交前不可下载" as a
		// property of this one transaction.
		if err := artifact.PromoteExecution(ctx, tx, c.TenantID, c.ExecutionID); err != nil {
			return err
		}

		// A session with enough uncovered events gets a summary job, queued
		// in the same transaction that grew the log (summary.EnqueueIfDue
		// counts under this lock; the job itself CASes when it applies).
		if err := summary.EnqueueIfDue(ctx, tx, c.TenantID, c.SessionPK, summaryVersion, summaryCovered); err != nil {
			return err
		}

		if _, err := tx.Exec(ctx,
			"UPDATE inbox_messages SET status = 'done' WHERE tenant_id = ? AND session_pk = ? AND in_seq = ?",
			c.TenantID, c.SessionPK, c.InSeq); err != nil {
			return fmt.Errorf("execution: close inbox row: %w", err)
		}
		if _, err := tx.Exec(ctx,
			"UPDATE sessions SET head_seq = ? WHERE tenant_id = ? AND session_pk = ?",
			c.InSeq+1, c.TenantID, c.SessionPK); err != nil {
			return fmt.Errorf("execution: advance head: %w", err)
		}
		// The Redis projection is itself an outbox job, not a direct write:
		// committing to MySQL and then failing to reach Redis must not turn a
		// committed execution into an error or an un-rollbackable half-state.
		if err := enqueueProjectionJob(ctx, tx, c); err != nil {
			return err
		}
		return nil
	})
}

func applyPreparedToSession(
	ctx context.Context,
	tx *controlplane.TxScope,
	c *Claim,
	prepared *sessionstore.Prepared,
	baseVersion uint64,
) error {
	seq := uint32(0)
	if len(prepared.Events) > 0 {
		// Events continue the session's own numbering; seq 0 is unused by
		// design so a NULL head and a real first event are distinguishable.
		if err := tx.QueryRow(ctx,
			"SELECT COALESCE(MAX(seq), 0) FROM session_events WHERE tenant_id = ? AND session_pk = ?",
			c.TenantID, c.SessionPK).Scan(&seq); err != nil {
			return fmt.Errorf("execution: next event seq: %w", err)
		}
	}
	for _, e := range prepared.Events {
		seq++
		payload, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("execution: encode event %s: %w", e.ID, err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO session_events (tenant_id, session_pk, seq, event_id, execution_id, author, payload)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
			c.TenantID, c.SessionPK, seq, e.ID, c.ExecutionID, e.Author, string(payload)); err != nil {
			return fmt.Errorf("execution: append event %s: %w", e.ID, err)
		}
	}

	stateJSON, err := json.Marshal(prepared.State)
	if err != nil {
		return fmt.Errorf("execution: encode state: %w", err)
	}
	// MySQL has no UPDATE ... RETURNING (that is Postgres syntax, and using it
	// here was measured to fail at parse time, not at runtime). The row is
	// already locked FOR UPDATE by Commit's re-check above, so a separate
	// read of the new version cannot race anything.
	res, err := tx.Exec(ctx, `
		UPDATE sessions
		SET state = ?, session_version = session_version + 1,
		    lease_owner = NULL, lease_until = NULL
		WHERE tenant_id = ? AND session_pk = ? AND fencing_token = ? AND session_version = ?`,
		string(stateJSON), c.TenantID, c.SessionPK, c.FenceToken, baseVersion)
	if err != nil {
		return fmt.Errorf("execution: commit state: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		// The WHERE clause is the fence check; zero rows means the version or
		// token moved since Commit read it a statement ago — the same answer
		// as the earlier explicit check, reached a different way.
		return ErrCommitConditionFailed
	}
	return nil
}

func recordExecutionOutcome(ctx context.Context, tx *controlplane.TxScope, c *Claim, res *Result) error {
	status := "committed"
	if res.Decision == "error" || res.ErrorType != "" {
		status = "failed"
	}
	if _, err := tx.Exec(ctx,
		"UPDATE executions SET status = ?, last_error = ? WHERE execution_id = ? AND tenant_id = ?",
		status, truncateRunes(res.Detail, 512), c.ExecutionID, c.TenantID); err != nil {
		return fmt.Errorf("execution: close execution row: %w", err)
	}
	if _, err := tx.Exec(ctx,
		"UPDATE execution_attempts SET status = 'committed' WHERE execution_id = ? AND fencing_token = ?",
		c.ExecutionID, c.FenceToken); err != nil {
		return fmt.Errorf("execution: close attempt row: %w", err)
	}
	return nil
}

func writeAuditForCommit(ctx context.Context, tx *controlplane.TxScope, c *Claim, res *Result) error {
	latency := res.Latency.Milliseconds()
	rows := []struct {
		event    string
		decision string
		stage    string
	}{
		{"reply", res.Decision, ""},
	}
	if res.ModelCalled {
		rows = append([]struct {
			event    string
			decision string
			stage    string
		}{{"model_call", res.Decision, ""}}, rows...)
	}
	for _, row := range rows {
		if _, err := tx.Exec(ctx, `
			INSERT INTO audit_events
				(trace_id, event, tenant_id, session_id, execution_id, agent_name,
				 decision, stage, latency_ms, error_type, detail, prompt_tokens, completion_tokens)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			traceIDFrom(c.Traceparent), row.event, c.TenantID, SessionID(c), c.ExecutionID, agent.AgentName,
			row.decision, row.stage, latency, res.ErrorType, truncateRunes(res.Detail, 512),
			res.PromptTokens, res.CompletionTokens); err != nil {
			return fmt.Errorf("execution: audit %s: %w", row.event, err)
		}
	}
	return nil
}

// enqueueReplyParts is what makes "a reply was decided" and "a reply still
// needs to be sent" the same committed fact. If this insert fails, the whole
// commit rolls back, and the execution is still owned by this fence for a
// retry; it can never be that a user is told something and the platform has
// no record that it promised to say it.
func enqueueReplyParts(ctx context.Context, tx *controlplane.TxScope, c *Claim, parts []ReplyPart) error {
	for i, p := range parts {
		text := p.Text
		if text == "" && !p.IsDone {
			continue
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO reply_outbox
				(tenant_id, execution_id, session_pk, part_seq, channel_type, binding_id,
				 target, text, is_done, idempotency_key)
			SELECT ?, ?, ?, ?, s.channel_type, s.binding_id, s.actor_key, ?, ?, ?
			FROM sessions s
			WHERE s.tenant_id = ? AND s.session_pk = ?`,
			c.TenantID, c.ExecutionID, c.SessionPK, i+1, text, boolToInt(p.IsDone),
			idempotencyKey(c, i+1), c.TenantID, c.SessionPK); err != nil {
			return fmt.Errorf("execution: enqueue reply part %d: %w", i+1, err)
		}
	}
	return nil
}

func enqueueProjectionJob(ctx context.Context, tx *controlplane.TxScope, c *Claim) error {
	return enqueueProjectionFor(ctx, tx, c.TenantID, c.SessionPK,
		fmt.Sprintf("project:%s:%d", c.ExecutionID, c.FenceToken))
}

// enqueueProjectionFor is the same job with explicit arguments, for code
// paths that act on a session without holding a Claim (a human disposition,
// say). The idempotency key is the caller's: two different reasons to
// re-project the same session must not collapse into one job by accident.
func enqueueProjectionFor(ctx context.Context, tx *controlplane.TxScope, tenantID string, sessionPK int64, idem string) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox_events (tenant_id, kind, idempotency_key, payload)
		VALUES (?, 'session_project', ?, ?)
		ON DUPLICATE KEY UPDATE job_id = job_id`,
		tenantID, idem, fmt.Sprintf(`{"session_pk":%d}`, sessionPK)); err != nil {
		return fmt.Errorf("execution: enqueue redis projection: %w", err)
	}
	return nil
}

func encodePrepared(p *sessionstore.Prepared) ([]byte, string, error) {
	raw, err := json.Marshal(p)
	if err != nil {
		return nil, "", fmt.Errorf("execution: encode prepared set: %w", err)
	}
	sum := sha256.Sum256(raw)
	return raw, hex.EncodeToString(sum[:]), nil
}

func idempotencyKey(c *Claim, partSeq int) string {
	return fmt.Sprintf("%s:%d:%d", c.ExecutionID, c.FenceToken, partSeq)
}

// truncateRunes cuts to at most n runes, not bytes: an error detail in
// Chinese must not be sliced mid-codepoint.
func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// traceIDFrom extracts the 32-hex-character trace id from a W3C traceparent
// header ("00-<trace-id>-<span-id>-<flags>"). It is stored already parsed
// because audit_events.trace_id is indexed and an operator is going to search
// by it, and re-parsing a whole column of strings in SQL would make that
// index useless.
func traceIDFrom(traceparent string) string {
	parts := strings.Split(traceparent, "-")
	if len(parts) < 2 || len(parts[1]) != 32 {
		return ""
	}
	return parts[1]
}
