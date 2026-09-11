package postgresadapter

import (
	"context"
	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/application"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	"time"
)

func (s *Store) ClaimDue(ctx context.Context, r domain.ClaimRequest) ([]domain.Claim, error) {
	rows, err := s.ClaimTraced(ctx, r)
	out := make([]domain.Claim, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.Claim)
	}
	return out, err
}
func (s *Store) ClaimTraced(ctx context.Context, r domain.ClaimRequest) ([]app.TracedClaim, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, databaseError(ctx, err)
	}
	defer rollback(tx)
	var useBinding string
	if s.accountGuard != nil {
		bounded, stop, e := s.accountGuard.AccountContext(ctx)
		if e != nil {
			return nil, domain.ErrUnauthorized
		}
		defer stop()
		ctx = bounded
		useBinding, err = s.accountGuard.VerifyAccount(ctx, tx, r.Provider, r.AccountID, "", nil)
		if err != nil {
			return nil, domain.ErrUnauthorized
		}
	}
	rows, err := tx.Query(ctx, `SELECT p.part_id FROM gateway_delivery_parts p JOIN gateway_delivery_intents i ON i.intent_id=p.intent_id
 WHERE i.provider=$1 AND i.account_id=$2 AND p.state IN ('PENDING','NOT_SENT') AND p.next_attempt_at<=clock_timestamp() AND p.attempt_number<$3 AND p.preparation_attempts<3
 AND NOT EXISTS(SELECT 1 FROM gateway_delivery_parts prev WHERE prev.intent_id=p.intent_id AND prev.part_index<p.part_index AND prev.state<>'ACCEPTED')
 ORDER BY i.created_at,i.intent_id,p.part_index LIMIT $4 FOR UPDATE OF p SKIP LOCKED`, r.Provider, r.AccountID, s.options.MaxAttempts, r.Limit)
	if err != nil {
		return nil, databaseError(ctx, err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, databaseError(ctx, err)
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, databaseError(ctx, err)
	}
	out := make([]app.TracedClaim, 0, len(ids))
	if len(ids) > 0 {
		if err = s.verifyOwner(ctx, tx, domain.Claim{Target: domain.Target{Provider: r.Provider, AccountID: r.AccountID}, InstanceID: r.InstanceID, Owner: r.Owner}); err != nil {
			return nil, err
		}
	}
	now, err := dbNow(ctx, tx)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		p, err := readPart(ctx, tx, id, false)
		if err != nil {
			return nil, err
		}
		if s.accountGuard != nil {
			proof, e := s.accountGuard.VerifyAccount(ctx, tx, r.Provider, r.AccountID, p.claim.Target.TenantID, nil)
			if e != nil || proof != useBinding {
				return nil, domain.ErrUnauthorized
			}
		}
		if !now.Before(p.deadline) {
			if _, err = tx.Exec(ctx, `UPDATE gateway_delivery_parts SET state='EXPIRED',next_attempt_at=NULL,updated_at=$2 WHERE part_id=$1`, id, now); err != nil {
				return nil, databaseError(ctx, err)
			}
			continue
		}
		token, err := randomID()
		if err != nil {
			return nil, err
		}
		until := now.Add(r.Lease)
		if p.deadline.Before(until) {
			until = p.deadline
		}
		_, err = tx.Exec(ctx, `UPDATE gateway_delivery_parts SET state='CLAIMED',claim_token=$2,instance_id=$3,owner_fence=$4,claim_until=$5,next_attempt_at=NULL,updated_at=$6,claim_use_binding=$7 WHERE part_id=$1`, id, token, r.InstanceID, ownerJSON(r.Owner), until, now, useBinding)
		if err != nil {
			return nil, databaseError(ctx, err)
		}
		p.claim.Part.State = domain.Claimed
		p.claim.Token = token
		p.claim.UseBinding = useBinding
		p.claim.InstanceID = r.InstanceID
		p.claim.ExpiresAt = until
		p.claim.Owner = r.Owner
		out = append(out, app.TracedClaim{Claim: p.claim, Carrier: p.carrier})
	}
	if s.accountGuard != nil {
		if err = s.accountGuard.RecheckAccount(ctx, tx); err != nil {
			return nil, domain.ErrUnauthorized
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, databaseError(ctx, err)
	}
	return out, nil
}
func (s *Store) MarkCalling(ctx context.Context, r domain.CallingRequest) (domain.Attempt, error) {
	var zero domain.Attempt
	if err := r.Validate(); err != nil {
		return zero, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return zero, databaseError(ctx, err)
	}
	defer rollback(tx)
	if s.accountGuard != nil {
		bounded, stop, e := s.accountGuard.AccountContext(ctx)
		if e != nil {
			return zero, domain.ErrUnauthorized
		}
		defer stop()
		ctx = bounded
		proof, e := s.accountGuard.VerifyAccount(ctx, tx, r.Claim.Target.Provider, r.Claim.Target.AccountID, r.Claim.Target.TenantID, nil)
		if e != nil || proof == "" || proof != r.Claim.UseBinding {
			return zero, domain.ErrUnauthorized
		}
	}
	p, err := readPart(ctx, tx, r.Claim.Part.ID, true)
	if err != nil {
		return zero, err
	}
	if p.claim.Target.Provider != r.Claim.Target.Provider || p.claim.Target.AccountID != r.Claim.Target.AccountID || p.claim.Target.TenantID != r.Claim.Target.TenantID {
		return zero, domain.ErrUnauthorized
	}
	if err = s.verifyOwner(ctx, tx, p.claim); err != nil {
		return zero, err
	}
	now, err := dbNow(ctx, tx)
	if err != nil {
		return zero, err
	}
	if err = checkClaim(p, r.Claim, now); err != nil {
		return zero, err
	}
	digest, err := domain.RequestDigest(p.claim)
	if err != nil {
		return zero, domain.ErrUnavailable
	}
	if digest != r.RequestDigest {
		return zero, domain.ErrConflict
	}
	if p.claim.Target.Provider == "wecom" && r.RequestID != p.claim.Target.CallbackRequestID {
		return zero, domain.ErrInvalid
	}
	if p.claim.Part.AttemptNumber >= int64(s.options.MaxAttempts) {
		return zero, domain.ErrClaimLost
	}
	id, err := randomID()
	if err != nil {
		return zero, err
	}
	capability, err := randomID()
	if err != nil {
		return zero, err
	}
	until := now.Add(r.Timeout)
	if p.deadline.Before(until) {
		until = p.deadline
	}
	a := domain.Attempt{UseBinding: p.claim.UseBinding, ID: id, PartID: p.claim.Part.ID, IntentID: p.claim.Intent.ID, ClaimToken: p.claim.Token, InstanceID: p.claim.InstanceID, Number: p.claim.Part.AttemptNumber + 1, Owner: p.claim.Owner, RequestID: r.RequestID, RequestDigest: digest, EvidenceToken: capability, CallingUntil: until, Intent: p.claim.Intent, Target: p.claim.Target, Text: p.claim.Part.Text}
	_, err = tx.Exec(ctx, `INSERT INTO gateway_delivery_attempts(attempt_id,part_id,attempt_number,claim_token,instance_id,owner_fence,request_id,request_digest,evidence_hash,calling_until,account_use_binding) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, a.ID, a.PartID, a.Number, a.ClaimToken, a.InstanceID, ownerJSON(a.Owner), a.RequestID, a.RequestDigest, capabilityHash(a.EvidenceToken), a.CallingUntil, a.UseBinding)
	if err != nil {
		return zero, databaseError(ctx, err)
	}
	_, err = tx.Exec(ctx, `UPDATE gateway_delivery_parts SET state='CALLING',attempt_number=$2,current_attempt_id=$3,calling_until=$4,updated_at=$5 WHERE part_id=$1`, a.PartID, a.Number, a.ID, until, now)
	if err != nil {
		return zero, databaseError(ctx, err)
	}
	// A caller receives neither a capability nor a send grant after an uncertain
	// commit. Retrying A2 cannot reacquire this already-entered CALLING gate.
	if s.accountGuard != nil {
		if err = s.accountGuard.RecheckAccount(ctx, tx); err != nil {
			return zero, domain.ErrUnauthorized
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return zero, databaseError(ctx, err)
	}
	return a, nil
}
func (s *Store) FinishPreparation(ctx context.Context, c domain.Claim, result domain.Result) error {
	if result.Validate() != nil || result.Certainty != domain.CertaintyNotSent || c.Part.ID == "" || c.Token == "" {
		return domain.ErrInvalid
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return databaseError(ctx, err)
	}
	defer rollback(tx)
	p, err := readPart(ctx, tx, c.Part.ID, true)
	if err != nil {
		return err
	}
	if err = s.verifyOwner(ctx, tx, p.claim); err != nil {
		return err
	}
	now, err := dbNow(ctx, tx)
	if err != nil {
		return err
	}
	if err = checkClaim(p, c, now); err != nil {
		return err
	}
	nextCount := p.prepAttempts + 1
	var next *time.Time
	if delay, ok := domain.RetryDelay(result, nextCount, now, p.deadline); ok {
		at := now.Add(delay)
		next = &at
	}
	raw, _ := jsonResult(result)
	state := domain.NotSent
	if result.ErrorClass == domain.ErrorDeadline {
		state = domain.Expired
		next = nil
	}
	_, err = tx.Exec(ctx, `UPDATE gateway_delivery_parts SET state=$6,preparation_attempts=$2,preparation_result=$3,next_attempt_at=$4,claim_until=NULL,updated_at=$5 WHERE part_id=$1`, c.Part.ID, nextCount, raw, next, now, string(state))
	if err != nil {
		return databaseError(ctx, err)
	}
	return databaseError(ctx, tx.Commit(ctx))
}
