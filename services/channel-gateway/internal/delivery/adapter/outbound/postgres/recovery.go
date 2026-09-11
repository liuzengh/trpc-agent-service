package postgresadapter

import (
	"context"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
)

func (s *Store) RecoverExpiredClaims(ctx context.Context, limit int) (int, error) {
	return s.recover(ctx, limit, false)
}
func (s *Store) RecoverStaleCalling(ctx context.Context, limit int) (int, error) {
	return s.recover(ctx, limit, true)
}
func (s *Store) recover(ctx context.Context, limit int, calling bool) (int, error) {
	if limit < 1 || limit > 1000 {
		return 0, domain.ErrInvalid
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, databaseError(ctx, err)
	}
	defer rollback(tx)
	query := `SELECT part_id FROM gateway_delivery_parts WHERE state='CLAIMED' AND claim_until<=clock_timestamp() ORDER BY part_id LIMIT $1 FOR UPDATE SKIP LOCKED`
	if calling {
		query = `SELECT part_id FROM gateway_delivery_parts WHERE state='CALLING' AND calling_until<=clock_timestamp() ORDER BY part_id LIMIT $1 FOR UPDATE SKIP LOCKED`
	}
	rows, err := tx.Query(ctx, query, limit)
	if err != nil {
		return 0, databaseError(ctx, err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return 0, databaseError(ctx, err)
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, databaseError(ctx, err)
	}
	now, err := dbNow(ctx, tx)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, id := range ids {
		p, err := readPart(ctx, tx, id, false)
		if err != nil {
			return 0, err
		}
		if calling {
			if p.claim.Part.State != domain.Calling || p.callingUntil == nil || now.Before(*p.callingUntil) {
				continue
			}
			a, err := readAttempt(ctx, tx, p.currentAttempt)
			if err != nil {
				return 0, err
			}
			if a.result != nil {
				return 0, domain.ErrUnavailable
			}
			raw, _ := jsonResult(domain.Result{Certainty: domain.CertaintyUnknown})
			if _, err = tx.Exec(ctx, `UPDATE gateway_delivery_attempts SET result=$2,finished_at=$3 WHERE attempt_id=$1`, p.currentAttempt, raw, now); err != nil {
				return 0, databaseError(ctx, err)
			}
			if _, err = tx.Exec(ctx, `UPDATE gateway_delivery_parts SET state='UNKNOWN',next_attempt_at=NULL,updated_at=$2 WHERE part_id=$1`, id, now); err != nil {
				return 0, databaseError(ctx, err)
			}
		} else {
			if p.claim.Part.State != domain.Claimed || now.Before(p.claim.ExpiresAt) {
				continue
			}
			state := domain.Pending
			if !now.Before(p.deadline) {
				state = domain.Expired
			}
			if _, err = tx.Exec(ctx, `UPDATE gateway_delivery_parts SET state=$2,claim_token=NULL,instance_id=NULL,owner_fence=NULL,claim_until=NULL,next_attempt_at=CASE WHEN $2='PENDING' THEN $3::timestamptz ELSE NULL END,updated_at=$3 WHERE part_id=$1`, id, string(state), now); err != nil {
				return 0, databaseError(ctx, err)
			}
		}
		n++
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, databaseError(ctx, err)
	}
	return n, nil
}
