package postgresadapter

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	"time"
)

func jsonResult(r domain.Result) ([]byte, error) { return json.Marshal(r) }

type attemptRow struct {
	partID, claimToken, instance, requestID, digest, evidenceHash string
	number                                                        int64
	owner                                                         *domain.OwnerFence
	until                                                         time.Time
	result                                                        *domain.Result
}

func readAttempt(ctx context.Context, tx pgx.Tx, id string) (attemptRow, error) {
	var a attemptRow
	var owner, result []byte
	err := tx.QueryRow(ctx, `SELECT part_id,claim_token,instance_id,request_id,request_digest,evidence_hash,attempt_number,owner_fence,calling_until,result FROM gateway_delivery_attempts WHERE attempt_id=$1`, id).Scan(&a.partID, &a.claimToken, &a.instance, &a.requestID, &a.digest, &a.evidenceHash, &a.number, &owner, &a.until, &result)
	if errors.Is(err, pgx.ErrNoRows) {
		return a, domain.ErrNotFound
	}
	if err != nil {
		return a, databaseError(ctx, err)
	}
	if len(owner) > 0 && string(owner) != "null" && json.Unmarshal(owner, &a.owner) != nil {
		return a, domain.ErrUnavailable
	}
	if len(result) > 0 {
		if json.Unmarshal(result, &a.result) != nil || a.result == nil || a.result.Validate() != nil {
			return a, domain.ErrUnavailable
		}
	}
	return a, nil
}
func evidenceMatches(a attemptRow, token, req, digest string) bool {
	return token != "" && a.requestID == req && a.digest == digest && subtle.ConstantTimeCompare([]byte(a.evidenceHash), []byte(capabilityHash(token))) == 1
}
func (s *Store) Finish(ctx context.Context, attempt domain.Attempt, result domain.Result) error {
	if result.Validate() != nil || attempt.ID == "" || attempt.PartID == "" || len(attempt.EvidenceToken) > 256 {
		return domain.ErrInvalid
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return databaseError(ctx, err)
	}
	defer rollback(tx)
	p, err := readPart(ctx, tx, attempt.PartID, true)
	if err != nil {
		return err
	}
	// The part lock serializes all state/attempt mutations. Read the immutable
	// attempt for authentication before an optional current-owner guard.
	a, err := readAttempt(ctx, tx, attempt.ID)
	if err != nil {
		return err
	}
	if a.partID != p.claim.Part.ID || a.claimToken != attempt.ClaimToken || a.instance != attempt.InstanceID || a.number != attempt.Number || !sameOwner(a.owner, attempt.Owner) || attempt.IntentID != p.claim.Intent.ID || !evidenceMatches(a, attempt.EvidenceToken, attempt.RequestID, attempt.RequestDigest) {
		return domain.ErrClaimLost
	}
	if a.result != nil {
		if *a.result != result {
			return domain.ErrConflict
		}
		return databaseError(ctx, tx.Commit(ctx))
	}
	if p.claim.Part.State != domain.Calling || p.currentAttempt != attempt.ID || p.claim.Part.AttemptNumber != a.number {
		return domain.ErrClaimLost
	}
	if err = s.verifyOwner(ctx, tx, p.claim); err != nil {
		return err
	}
	now, err := dbNow(ctx, tx)
	if err != nil {
		return err
	}
	if p.callingUntil == nil || !now.Before(*p.callingUntil) || !now.Before(a.until) || !now.Before(p.deadline) {
		return domain.ErrClaimLost
	}
	var next *time.Time
	if a.number < int64(s.options.MaxAttempts) {
		if delay, ok := domain.RetryDelay(result, a.number, now, p.deadline); ok {
			at := now.Add(delay)
			next = &at
		}
	}
	raw, _ := jsonResult(result)
	if _, err = tx.Exec(ctx, `UPDATE gateway_delivery_attempts SET result=$2,finished_at=$3 WHERE attempt_id=$1`, attempt.ID, raw, now); err != nil {
		return databaseError(ctx, err)
	}
	if _, err = tx.Exec(ctx, `UPDATE gateway_delivery_parts SET state=$2,next_attempt_at=$3,updated_at=$4 WHERE part_id=$1`, attempt.PartID, string(result.Certainty), next, now); err != nil {
		return databaseError(ctx, err)
	}
	return databaseError(ctx, tx.Commit(ctx))
}
func (s *Store) Observe(ctx context.Context, o domain.Observation) error {
	if err := o.Validate(); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return databaseError(ctx, err)
	}
	defer rollback(tx)
	// This lookup is intentionally unlocked. A permanent Attempt provides its
	// parent key; only then acquire the same part->attempt order as Finish/recovery.
	var partID string
	if err = tx.QueryRow(ctx, `SELECT part_id FROM gateway_delivery_attempts WHERE attempt_id=$1`, o.AttemptID).Scan(&partID); errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound
	} else if err != nil {
		return databaseError(ctx, err)
	}
	if _, err = readPart(ctx, tx, partID, true); err != nil {
		return err
	}
	a, err := readAttempt(ctx, tx, o.AttemptID)
	if err != nil {
		return err
	}
	if !evidenceMatches(a, o.EvidenceToken, o.ProviderRequestID, o.RequestDigest) {
		return domain.ErrUnauthorized
	}
	var existingAttempt, req, digest string
	var result []byte
	err = tx.QueryRow(ctx, `SELECT attempt_id,request_id,request_digest,result FROM gateway_delivery_observations WHERE observation_id=$1`, o.ID).Scan(&existingAttempt, &req, &digest, &result)
	if err == nil {
		var r domain.Result
		if json.Unmarshal(result, &r) != nil {
			return domain.ErrUnavailable
		}
		if existingAttempt != o.AttemptID || req != o.ProviderRequestID || digest != o.RequestDigest || r != o.Result {
			return domain.ErrConflict
		}
		return databaseError(ctx, tx.Commit(ctx))
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return databaseError(ctx, err)
	}
	var count int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM gateway_delivery_observations WHERE attempt_id=$1`, o.AttemptID).Scan(&count); err != nil {
		return databaseError(ctx, err)
	}
	if count >= 16 {
		return domain.ErrCapacity
	}
	raw, _ := jsonResult(o.Result)
	if _, err = tx.Exec(ctx, `INSERT INTO gateway_delivery_observations(observation_id,attempt_id,request_id,request_digest,result) VALUES($1,$2,$3,$4,$5) ON CONFLICT(observation_id) DO NOTHING`, o.ID, o.AttemptID, o.ProviderRequestID, o.RequestDigest, raw); err != nil {
		return databaseError(ctx, err)
	}
	// Different parent attempts may race on a global observation ID; verify the
	// unique winner inside this fresh statement, rather than silently accepting it.
	if err = tx.QueryRow(ctx, `SELECT attempt_id,request_id,request_digest,result FROM gateway_delivery_observations WHERE observation_id=$1`, o.ID).Scan(&existingAttempt, &req, &digest, &result); err != nil {
		return databaseError(ctx, err)
	}
	var r domain.Result
	if json.Unmarshal(result, &r) != nil {
		return domain.ErrUnavailable
	}
	if existingAttempt != o.AttemptID || req != o.ProviderRequestID || digest != o.RequestDigest || r != o.Result {
		return domain.ErrConflict
	}
	return databaseError(ctx, tx.Commit(ctx))
}
func (s *Store) Observations(ctx context.Context, id string) ([]domain.StoredObservation, error) {
	if id == "" || len(id) > 128 {
		return nil, domain.ErrInvalid
	}
	rows, err := s.pool.Query(ctx, `SELECT observation_id,attempt_id,request_id,request_digest,result,observed_at FROM gateway_delivery_observations WHERE attempt_id=$1 ORDER BY observed_at,observation_id LIMIT 16`, id)
	if err != nil {
		return nil, databaseError(ctx, err)
	}
	defer rows.Close()
	out := []domain.StoredObservation{}
	for rows.Next() {
		var o domain.StoredObservation
		var raw []byte
		if err = rows.Scan(&o.ID, &o.AttemptID, &o.ProviderRequestID, &o.RequestDigest, &raw, &o.ObservedAt); err != nil {
			return nil, databaseError(ctx, err)
		}
		if json.Unmarshal(raw, &o.Result) != nil || o.Result.Validate() != nil {
			return nil, domain.ErrUnavailable
		}
		out = append(out, o)
	}
	return out, databaseError(ctx, rows.Err())
}
