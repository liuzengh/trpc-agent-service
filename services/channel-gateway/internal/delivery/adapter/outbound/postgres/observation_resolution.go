package postgresadapter

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
)

// ResolveObserved is an explicit evidence-only recovery operation. It never
// authorizes another Provider call, and never edits a newer/currently-running
// attempt. The original send capability was already verified at Observe.
func (s *Store) ResolveObserved(ctx context.Context, id string) (bool, error) {
	if id == "" || len(id) > 128 {
		return false, domain.ErrInvalid
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, databaseError(ctx, err)
	}
	defer rollback(tx)
	var partID string
	if err = tx.QueryRow(ctx, `SELECT part_id FROM gateway_delivery_attempts WHERE attempt_id=$1`, id).Scan(&partID); errors.Is(err, pgx.ErrNoRows) {
		return false, domain.ErrNotFound
	} else if err != nil {
		return false, databaseError(ctx, err)
	}
	p, err := readPart(ctx, tx, partID, true)
	if err != nil {
		return false, err
	}
	if p.claim.Part.State != domain.Unknown || p.currentAttempt != id {
		return false, nil
	}
	a, err := readAttempt(ctx, tx, id)
	if err != nil {
		return false, err
	}
	if a.number != p.claim.Part.AttemptNumber || a.result == nil || a.result.Certainty != domain.CertaintyUnknown {
		return false, nil
	}
	rows, err := tx.Query(ctx, `SELECT observation_id,request_id,request_digest,result FROM gateway_delivery_observations WHERE attempt_id=$1 ORDER BY observed_at,observation_id LIMIT 17`, id)
	if err != nil {
		return false, databaseError(ctx, err)
	}
	var candidate *domain.Result
	chosenID := ""
	count := 0
	conflict, notSent := false, false
	for rows.Next() {
		count++
		var observationID, req, digest string
		var raw []byte
		if err = rows.Scan(&observationID, &req, &digest, &raw); err != nil {
			rows.Close()
			return false, databaseError(ctx, err)
		}
		var r domain.Result
		if json.Unmarshal(raw, &r) != nil || r.Validate() != nil || req != a.requestID || digest != a.digest {
			rows.Close()
			return false, domain.ErrUnavailable
		}
		switch r.Certainty {
		case domain.CertaintyAccepted, domain.CertaintyRejected:
			if candidate == nil {
				copy := r
				candidate = &copy
				chosenID = observationID
			} else if *candidate != r {
				conflict = true
			}
		case domain.CertaintyNotSent:
			notSent = true
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return false, databaseError(ctx, err)
	}
	if count > 16 {
		return false, domain.ErrUnavailable
	}
	if candidate == nil || conflict || notSent {
		return false, nil
	}
	now, err := dbNow(ctx, tx)
	if err != nil {
		return false, err
	}
	raw, _ := jsonResult(*candidate)
	if _, err = tx.Exec(ctx, `UPDATE gateway_delivery_attempts SET result=$2,resolved_at=$3,resolved_observation_id=$4 WHERE attempt_id=$1`, id, raw, now, chosenID); err != nil {
		return false, databaseError(ctx, err)
	}
	if _, err = tx.Exec(ctx, `UPDATE gateway_delivery_parts SET state=$2,next_attempt_at=NULL,updated_at=$3 WHERE part_id=$1`, partID, string(candidate.Certainty), now); err != nil {
		return false, databaseError(ctx, err)
	}
	if err = tx.Commit(ctx); err != nil {
		return false, databaseError(ctx, err)
	}
	return true, nil
}
