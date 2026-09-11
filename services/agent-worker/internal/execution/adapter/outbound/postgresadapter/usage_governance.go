package postgresadapter

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

func (l *Ledger) RecordUsage(ctx context.Context, g domain.Grant, usage domain.ModelUsage) error {
	if !g.Run.Request.UsagePolicy.Enabled {
		return nil
	}
	if usage.InputTokens < 0 || usage.OutputTokens < 0 || usage.TotalTokens < 0 || usage.Known && usage.InputTokens+usage.OutputTokens != usage.TotalTokens || !usage.Known && (usage.InputTokens != 0 || usage.OutputTokens != 0 || usage.TotalTokens != 0) {
		return domain.ErrInvalid
	}
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if _, _, _, err = fenced(ctx, tx, g); err != nil {
		return err
	}
	var known bool
	var in, out, total *int64
	err = tx.QueryRow(ctx, `SELECT usage_known,input_tokens,output_tokens,total_tokens FROM worker_tenant_usage_settlements_v1 WHERE tenant_id=$1 AND attempt_id=$2`, g.Run.Request.Route.TenantID, g.AttemptID).Scan(&known, &in, &out, &total)
	if err == nil {
		if known != usage.Known || usage.Known && (in == nil || out == nil || total == nil || *in != usage.InputTokens || *out != usage.OutputTokens || *total != usage.TotalTokens) {
			return domain.ErrConflict
		}
		return tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if usage.Known {
		_, err = tx.Exec(ctx, `INSERT INTO worker_tenant_usage_settlements_v1(tenant_id,attempt_id,usage_known,input_tokens,output_tokens,total_tokens,estimated_cost_micros)
SELECT tenant_id,attempt_id,true,$3,$4,$5,(($3::numeric*input_micros_per_million_tokens+$4::numeric*output_micros_per_million_tokens)/1000000)::bigint
FROM worker_tenant_usage_reservations_v1 WHERE tenant_id=$1 AND attempt_id=$2`, g.Run.Request.Route.TenantID, g.AttemptID, usage.InputTokens, usage.OutputTokens, usage.TotalTokens)
	} else {
		_, err = tx.Exec(ctx, `INSERT INTO worker_tenant_usage_settlements_v1(tenant_id,attempt_id,usage_known) VALUES($1,$2,false)`, g.Run.Request.Route.TenantID, g.AttemptID)
	}
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
