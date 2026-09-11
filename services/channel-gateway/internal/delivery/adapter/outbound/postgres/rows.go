package postgresadapter

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	"time"
)

func dbNow(ctx context.Context, tx pgx.Tx) (time.Time, error) {
	var now time.Time
	err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now)
	return now, databaseError(ctx, err)
}
func randomID() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", domain.ErrUnavailable
	}
	return hex.EncodeToString(b[:]), nil
}
func capabilityHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
func ownerJSON(owner *domain.OwnerFence) []byte {
	if owner == nil {
		return nil
	}
	b, _ := json.Marshal(owner)
	return b
}
func sameOwner(a, b *domain.OwnerFence) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}
func (s *Store) verifyOwner(ctx context.Context, tx pgx.Tx, claim domain.Claim) error {
	if claim.Target.Provider == "telegram" {
		if claim.Owner != nil {
			return domain.ErrInvalid
		}
		return nil
	}
	if s.guard == nil || claim.Owner == nil {
		return domain.ErrClaimLost
	}
	o := claim.Owner
	if o.InstanceID != claim.InstanceID {
		return domain.ErrClaimLost
	}
	if err := s.guard.VerifyOwner(ctx, tx, claim.Target.AccountID, o.InstanceID, o.Epoch, o.Revision); err != nil {
		return domain.ErrClaimLost
	}
	return nil
}

type partRow struct {
	carrier        tracecontext.Carrier
	claim          domain.Claim
	deadline       time.Time
	callingUntil   *time.Time
	currentAttempt string
	next           *time.Time
	prepAttempts   int64
}

func readPart(ctx context.Context, tx pgx.Tx, id string, lock bool) (partRow, error) {
	var p partRow
	var intent, target, owner []byte
	var token, instance, current *string
	var expires *time.Time
	query := `SELECT p.part_id,p.intent_id,p.part_index,p.body,p.state,p.attempt_number,i.intent,i.target,p.claim_token,p.instance_id,p.owner_fence,p.claim_until,i.deadline,p.calling_until,p.current_attempt_id,p.next_attempt_at,p.preparation_attempts,p.claim_use_binding,COALESCE(i.traceparent,''),COALESCE(i.tracestate,'') FROM gateway_delivery_parts p JOIN gateway_delivery_intents i ON i.intent_id=p.intent_id WHERE p.part_id=$1`
	if lock {
		query += ` FOR UPDATE OF p`
	}
	err := tx.QueryRow(ctx, query, id).Scan(&p.claim.Part.ID, &p.claim.Part.IntentID, &p.claim.Part.Index, &p.claim.Part.Text, &p.claim.Part.State, &p.claim.Part.AttemptNumber, &intent, &target, &token, &instance, &owner, &expires, &p.deadline, &p.callingUntil, &current, &p.next, &p.prepAttempts, &p.claim.UseBinding, &p.carrier.Traceparent, &p.carrier.Tracestate)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, domain.ErrNotFound
	}
	if err != nil {
		return p, databaseError(ctx, err)
	}
	if json.Unmarshal(intent, &p.claim.Intent) != nil || json.Unmarshal(target, &p.claim.Target) != nil {
		return p, domain.ErrUnavailable
	}
	if p.claim.Intent.Validate() != nil || p.claim.Target.Validate() != nil {
		return p, domain.ErrUnavailable
	}
	if token != nil {
		p.claim.Token = *token
	}
	if instance != nil {
		p.claim.InstanceID = *instance
	}
	if expires != nil {
		p.claim.ExpiresAt = *expires
	}
	if current != nil {
		p.currentAttempt = *current
	}
	if len(owner) > 0 && string(owner) != "null" {
		if json.Unmarshal(owner, &p.claim.Owner) != nil || p.claim.Owner == nil {
			return p, domain.ErrUnavailable
		}
	}
	return p, nil
}
func checkClaim(p partRow, c domain.Claim, now time.Time) error {
	if p.claim.Part.State != domain.Claimed || p.claim.Token == "" || p.claim.Token != c.Token || p.claim.UseBinding != c.UseBinding || p.claim.InstanceID != c.InstanceID || !sameOwner(p.claim.Owner, c.Owner) || !now.Before(p.claim.ExpiresAt) || !now.Before(p.deadline) {
		return domain.ErrClaimLost
	}
	return nil
}
