package postgresadapter

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	codec "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

const memoryFailureFinal = "本次执行未完成，请稍后重试。"

// FinalizeMemory is a short, local visibility transaction. It never calls the
// Memory backend, changes the accepted Session, or reopens a terminal Attempt.
// The same decision is idempotent, including a lost commit response. A different
// decision conflicts instead of rewriting an already observable Final.
func (l *Ledger) FinalizeMemory(ctx context.Context, accepted domain.Completion, success bool) (resultErr error) {
	ctx, span := telemetrytrace.Start(l.Tracer, ctx, "worker.memory.finalize")
	defer func() { telemetrytrace.End(span, resultErr) }()
	if accepted.Kind != "ATTEMPT" || accepted.Status != domain.Succeeded || !domain.DigestValid(accepted.MemoryDigest) || !domain.DigestValid(accepted.ResultDigest) {
		return domain.ErrInvalid
	}
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	// Keep the existing Session -> Run -> Completion ordering. Claim checks the
	// pending gate while holding the same Session lock.
	if _, _, _, err = lockRun(ctx, tx, accepted.TenantID, accepted.RunID); err != nil {
		return err
	}
	c, err := scanCompletion(tx.QueryRow(ctx, `SELECT `+completionColumns+` FROM execution_completions WHERE tenant_id=$1 AND run_id=$2 FOR UPDATE`, accepted.TenantID, accepted.RunID))
	if err != nil {
		return err
	}
	if c.CompletionID != accepted.CompletionID || c.AttemptID != accepted.AttemptID || c.Kind != accepted.Kind || c.Status != accepted.Status || c.ResultDigest != accepted.ResultDigest || c.MemoryDigest != accepted.MemoryDigest || c.Candidate != accepted.Candidate || c.FinalIntentID != accepted.FinalIntentID || c.ReplyDisposition != accepted.ReplyDisposition || c.Reason != accepted.Reason || !c.CompletedAt.Equal(accepted.CompletedAt) {
		return domain.ErrConflict
	}
	decision := "FAILED"
	if success {
		decision = "APPLIED"
	}
	if c.MemoryStatus == decision {
		return tx.Commit(ctx)
	}
	if c.MemoryStatus != "PENDING" {
		return domain.ErrConflict
	}
	if c.FinalIntentID != "" {
		var payload []byte
		var digest string
		var ready, published bool
		err = tx.QueryRow(ctx, `SELECT payload,digest,ready,published_at IS NOT NULL FROM execution_reply_outbox WHERE intent_id=$1 AND tenant_id=$2 AND run_id=$3 FOR UPDATE`, c.FinalIntentID, c.TenantID, c.RunID).Scan(&payload, &digest, &ready, &published)
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrConflict
		}
		if err != nil {
			return err
		}
		if ready || published {
			return domain.ErrConflict
		}
		event, e := codec.DecodeReplyIntent(payload)
		if e != nil {
			return domain.ErrConflict
		}
		actual, e := codec.ReplyIntentDigest(event)
		if e != nil || actual != digest || event.IntentID != c.FinalIntentID || event.RunID != c.RunID || event.Execution.AttemptID != c.AttemptID || event.Execution.CompletionID != c.CompletionID {
			return domain.ErrConflict
		}
		if !success {
			event.Content.Text = memoryFailureFinal
			event.Content.Attachments = nil
			payload, err = codec.EncodeReplyIntent(event)
			if err != nil {
				return err
			}
			digest, err = codec.ReplyIntentDigest(event)
			if err != nil {
				return err
			}
		}
		if _, err = tx.Exec(ctx, `UPDATE execution_reply_outbox SET payload=$2,digest=$3,ready=true WHERE intent_id=$1`, c.FinalIntentID, payload, digest); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE execution_completions SET memory_status=$3 WHERE tenant_id=$1 AND run_id=$2`, c.TenantID, c.RunID, decision); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
