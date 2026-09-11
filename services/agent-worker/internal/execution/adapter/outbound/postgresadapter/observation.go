package postgresadapter

import (
	"context"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
)

// ObserveStorage reads only outstanding work using partial indexes. It does not
// count or scan retained completed history and never opens a write transaction.
func (l *Ledger) ObserveStorage(ctx context.Context) (application.StorageObservation, error) {
	var s application.StorageObservation
	err := l.pool.QueryRow(ctx, `SELECT
 count(*) FILTER(WHERE status='QUEUED'),
 count(*) FILTER(WHERE status='RUNNING'),
 count(*) FILTER(WHERE status='RETRY_WAIT'),
 COALESCE(GREATEST(0,extract(epoch FROM clock_timestamp()-min(accepted_at))),0)
 FROM execution_runs WHERE status IN ('QUEUED','RUNNING','RETRY_WAIT')`).Scan(&s.Queued, &s.Running, &s.RetryWait, &s.OldestRunSeconds)
	if err != nil {
		return s, err
	}
	err = l.pool.QueryRow(ctx, `SELECT count(*),COALESCE(GREATEST(0,extract(epoch FROM clock_timestamp()-min(created_at))),0) FROM execution_reply_outbox WHERE published_at IS NULL`).Scan(&s.ReplyPending, &s.OldestReplySeconds)
	return s, err
}
