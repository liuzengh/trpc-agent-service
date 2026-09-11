package postgresadapter

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
)

func chargeUsageRate(ctx context.Context, tx pgx.Tx, tenant, user string, tenantLimit, userLimit int) error {
	if tenantLimit < 1 || userLimit < 1 || userLimit > tenantLimit {
		return domain.ErrUnavailable
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,731004287))`, tenant); err != nil {
		return err
	}
	for _, item := range []struct {
		kind, id string
		limit    int
	}{{"tenant", tenant, tenantLimit}, {"user", user, userLimit}} {
		var count int64
		err := tx.QueryRow(ctx, `INSERT INTO gateway_usage_rate_windows(tenant_id,subject_kind,subject_id,window_start,request_count)
VALUES($1,$2,$3,date_trunc('minute',clock_timestamp()),1)
ON CONFLICT(tenant_id,subject_kind,subject_id,window_start) DO UPDATE
SET request_count=gateway_usage_rate_windows.request_count+1
WHERE gateway_usage_rate_windows.request_count < $4
RETURNING request_count`, tenant, item.kind, item.id, item.limit).Scan(&count)
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrRateLimited
		}
		if err != nil {
			return err
		}
	}
	return nil
}
