package delivery

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
	"unicode"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

// SQLStore implements multi-node PostgreSQL Outbox claims.
type SQLStore struct {
	DB          *sql.DB
	Now         func() time.Time
	MaxAttempts int
}

var _ Store = (*SQLStore)(nil)

// ClaimReady atomically claims eligible pending, retry, or abandoned sending
// rows. Binding keys prevent this worker from consuming another channel type.
func (store *SQLStore) ClaimReady(ctx context.Context, bindings []BindingKey, owner string, ttl time.Duration, limit int) ([]Claim, error) {
	if store == nil || store.DB == nil {
		return nil, errors.New("delivery: nil SQL database")
	}
	if owner == "" || ttl <= 0 || limit <= 0 {
		return nil, errors.New("delivery: owner, positive ttl, and limit are required")
	}
	if err := validateBindings(bindings); err != nil {
		return nil, err
	}
	tx, err := store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	now := store.now().UTC()
	ready := `((candidate.status='pending') OR (candidate.status='retry' AND (candidate.retry_at IS NULL OR candidate.retry_at<=$1)) OR (candidate.status='claimed' AND (candidate.lease_until IS NULL OR candidate.lease_until<=$1)))`

	uncertainValues, uncertainArgs := bindingValues(bindings, 2, []any{now})
	if _, err := tx.ExecContext(ctx, `UPDATE outbox_messages AS candidate SET status='uncertain',lease_until=NULL,retry_at=NULL,completed_at=$1,last_error='delivery lease expired after send began'
		FROM (VALUES `+uncertainValues+`) AS eligible(tenant_id,binding_id)
		WHERE candidate.tenant_id=eligible.tenant_id AND candidate.binding_id=eligible.binding_id
		AND candidate.status='sending' AND (candidate.lease_until IS NULL OR candidate.lease_until<=$1)`, uncertainArgs...); err != nil {
		return nil, err
	}
	updateValues, updateArgs := bindingValues(bindings, 3, []any{now, store.maxAttempts()})
	if _, err := tx.ExecContext(ctx, `UPDATE outbox_messages AS candidate SET status='dlq',lease_until=NULL,retry_at=NULL,completed_at=$1
		FROM (VALUES `+updateValues+`) AS eligible(tenant_id,binding_id)
		WHERE candidate.tenant_id=eligible.tenant_id AND candidate.binding_id=eligible.binding_id
		AND `+ready+` AND candidate.attempts>=$2`, updateArgs...); err != nil {
		return nil, err
	}

	selectValues, selectArgs := bindingValues(bindings, 4, []any{now, store.maxAttempts(), limit})
	rows, err := tx.QueryContext(ctx, `SELECT candidate.tenant_id,candidate.outbox_id,candidate.status,candidate.attempts,candidate.payload_json,candidate.last_error
		FROM outbox_messages AS candidate
		JOIN (VALUES `+selectValues+`) AS eligible(tenant_id,binding_id)
		ON candidate.tenant_id=eligible.tenant_id AND candidate.binding_id=eligible.binding_id
		WHERE `+ready+` AND candidate.attempts<$2
		ORDER BY candidate.created_at,candidate.outbox_id
		FOR UPDATE OF candidate SKIP LOCKED LIMIT $3`, selectArgs...)
	if err != nil {
		return nil, err
	}
	type candidate struct {
		tenantID, outboxID string
		status             Status
		attempt            int
		message            gateway.OutboundMessage
		lastError          sql.NullString
	}
	var candidates []candidate
	for rows.Next() {
		var item candidate
		var payload []byte
		if err := rows.Scan(&item.tenantID, &item.outboxID, &item.status, &item.attempt, &payload, &item.lastError); err != nil {
			rows.Close()
			return nil, err
		}
		if err := json.Unmarshal(payload, &item.message); err != nil {
			rows.Close()
			return nil, fmt.Errorf("delivery: decode Outbox %q: %w", item.outboxID, err)
		}
		if item.message.TenantID != item.tenantID || item.message.OutboxID != item.outboxID {
			rows.Close()
			return nil, fmt.Errorf("delivery: Outbox %q payload scope mismatch", item.outboxID)
		}
		candidates = append(candidates, item)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	leaseUntil := now.Add(ttl)
	claims := make([]Claim, 0, len(candidates))
	for _, item := range candidates {
		attempt := item.attempt + 1
		token := claimToken(item.tenantID, item.outboxID, owner, attempt, now)
		result, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET status='claimed',attempts=$3,claim_owner=$4,claim_token=$5,lease_until=$6,retry_at=NULL,last_error=NULL
			WHERE tenant_id=$1 AND outbox_id=$2`, item.tenantID, item.outboxID, attempt, owner, token, leaseUntil)
		if err != nil {
			return nil, err
		}
		if err := exactResult(result, nil); err != nil {
			return nil, err
		}
		claims = append(claims, Claim{Message: item.message, Owner: owner, ClaimToken: token, Attempt: attempt, Status: StatusClaimed, LeaseUntil: leaseUntil, LastFailure: item.lastError.String})
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return claims, nil
}

// Renew extends only an unexpired exact sending claim.
func (store *SQLStore) Renew(ctx context.Context, claim Claim, ttl time.Duration) (Claim, error) {
	if err := store.validateClaim(claim); err != nil {
		return Claim{}, err
	}
	if ttl <= 0 {
		return Claim{}, errors.New("delivery: positive ttl is required")
	}
	now := store.now().UTC()
	until := now.Add(ttl)
	result, err := store.DB.ExecContext(ctx, `UPDATE outbox_messages SET lease_until=$6 WHERE tenant_id=$1 AND outbox_id=$2 AND claim_owner=$3 AND claim_token=$4 AND status IN ('claimed','sending') AND lease_until>$5`, claim.Message.TenantID, claim.Message.OutboxID, claim.Owner, claim.ClaimToken, now, until)
	if err := exactResult(result, err); err != nil {
		return Claim{}, err
	}
	claim.LeaseUntil = until
	return claim, nil
}

// BeginSend records the point after which an expired lease has an unknown
// provider outcome and must not be blindly retried.
func (store *SQLStore) BeginSend(ctx context.Context, claim Claim) error {
	if err := store.validateClaim(claim); err != nil {
		return err
	}
	now := store.now().UTC()
	result, err := store.DB.ExecContext(ctx, `UPDATE outbox_messages SET status='sending' WHERE tenant_id=$1 AND outbox_id=$2 AND claim_owner=$3 AND claim_token=$4 AND status='claimed' AND lease_until>$5`, claim.Message.TenantID, claim.Message.OutboxID, claim.Owner, claim.ClaimToken, now)
	return exactResult(result, err)
}

// MarkSent completes only an unexpired exact sending claim.
func (store *SQLStore) MarkSent(ctx context.Context, claim Claim) error {
	if err := store.validateClaim(claim); err != nil {
		return err
	}
	now := store.now().UTC()
	result, err := store.DB.ExecContext(ctx, `UPDATE outbox_messages SET status='sent',sent_at=$6,completed_at=$6,lease_until=NULL WHERE tenant_id=$1 AND outbox_id=$2 AND claim_owner=$3 AND claim_token=$4 AND status='sending' AND lease_until>$5`, claim.Message.TenantID, claim.Message.OutboxID, claim.Owner, claim.ClaimToken, now, now)
	return exactResult(result, err)
}

// Fail retries transient errors and moves permanent or exhausted work to DLQ.
func (store *SQLStore) Fail(ctx context.Context, claim Claim, cause error, retryAt time.Time, retryable bool) (Status, error) {
	if err := store.validateClaim(claim); err != nil {
		return "", err
	}
	status := StatusDLQ
	var nextAttempt any
	if retryable && claim.Attempt < store.maxAttempts() {
		status = StatusRetry
		nextAttempt = retryAt.UTC()
	}
	now := store.now().UTC()
	result, err := store.DB.ExecContext(ctx, `UPDATE outbox_messages SET status=$6,retry_at=$7,last_error=$8,lease_until=NULL,completed_at=CASE WHEN $6='dlq' THEN $5 ELSE NULL END WHERE tenant_id=$1 AND outbox_id=$2 AND claim_owner=$3 AND claim_token=$4 AND status IN ('claimed','sending') AND lease_until>$5`, claim.Message.TenantID, claim.Message.OutboxID, claim.Owner, claim.ClaimToken, now, status, nextAttempt, sanitizeError(cause))
	if err := exactResult(result, err); err != nil {
		return "", err
	}
	return status, nil
}

// MarkUncertain prevents an ambiguous provider outcome from being resent.
func (store *SQLStore) MarkUncertain(ctx context.Context, claim Claim, cause error) error {
	if err := store.validateClaim(claim); err != nil {
		return err
	}
	now := store.now().UTC()
	result, err := store.DB.ExecContext(ctx, `UPDATE outbox_messages SET status='uncertain',last_error=$5,lease_until=NULL,retry_at=NULL,completed_at=$6 WHERE tenant_id=$1 AND outbox_id=$2 AND claim_owner=$3 AND claim_token=$4 AND status='sending'`, claim.Message.TenantID, claim.Message.OutboxID, claim.Owner, claim.ClaimToken, sanitizeError(cause), now)
	return exactResult(result, err)
}

func (store *SQLStore) validateClaim(claim Claim) error {
	if store == nil || store.DB == nil {
		return errors.New("delivery: nil SQL database")
	}
	if claim.Message.TenantID == "" || claim.Message.OutboxID == "" || claim.Owner == "" || claim.ClaimToken == "" {
		return errors.New("delivery: tenant-scoped claim identity is required")
	}
	return nil
}

func validateBindings(bindings []BindingKey) error {
	if len(bindings) == 0 {
		return errors.New("delivery: at least one binding is required")
	}
	seen := make(map[BindingKey]struct{}, len(bindings))
	for _, binding := range bindings {
		if binding.TenantID == "" || binding.BindingID == "" {
			return errors.New("delivery: complete tenant binding is required")
		}
		if _, exists := seen[binding]; exists {
			return fmt.Errorf("delivery: duplicate binding %q/%q", binding.TenantID, binding.BindingID)
		}
		seen[binding] = struct{}{}
	}
	return nil
}

func bindingValues(bindings []BindingKey, start int, args []any) (string, []any) {
	values := make([]string, 0, len(bindings))
	for _, binding := range bindings {
		values = append(values, fmt.Sprintf("($%d,$%d)", start, start+1))
		start += 2
		args = append(args, binding.TenantID, binding.BindingID)
	}
	return strings.Join(values, ","), args
}

func claimToken(tenantID, outboxID, owner string, attempt int, now time.Time) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%s\x00%d\x00%d", tenantID, outboxID, owner, attempt, now.UnixNano())))
	return hex.EncodeToString(digest[:])
}

func exactResult(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return ErrClaimOwner
	}
	return nil
}

func sanitizeError(cause error) string {
	if cause == nil {
		return ""
	}
	value := strings.Map(func(char rune) rune {
		if unicode.IsControl(char) {
			return ' '
		}
		return char
	}, cause.Error())
	value = strings.TrimSpace(value)
	if len(value) > 1024 {
		value = value[:1024]
	}
	return value
}

func (store *SQLStore) now() time.Time {
	if store.Now != nil {
		return store.Now()
	}
	return time.Now()
}

func (store *SQLStore) maxAttempts() int {
	if store.MaxAttempts > 0 {
		return store.MaxAttempts
	}
	return 8
}
