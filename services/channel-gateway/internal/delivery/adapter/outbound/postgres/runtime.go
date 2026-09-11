package postgresadapter

import (
	"context"
	"regexp"
	"time"

	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/application"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
)

const runtimeQueryTimeout = 5 * time.Second

var runtimeIdentifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

func validRuntimeCursor(id string) bool { return id == "" || runtimeIdentifier.MatchString(id) }
func (s *Store) ListDueAccounts(ctx context.Context, q app.DueAccountQuery) (app.DueAccountPage, error) {
	if ctx == nil || q.Limit < 1 || q.Limit > 1000 || (q.Provider != "telegram" && q.Provider != "wecom") || !validRuntimeCursor(q.AfterAccountID) {
		return app.DueAccountPage{}, domain.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, runtimeQueryTimeout)
	defer cancel()
	rows, err := s.pool.Query(ctx, dueAccountsSQL, q.Provider, q.AfterAccountID, s.options.MaxAttempts, q.Limit+1)
	if err != nil {
		return app.DueAccountPage{}, databaseError(ctx, err)
	}
	defer rows.Close()
	page := app.DueAccountPage{Exhausted: true}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return app.DueAccountPage{}, databaseError(ctx, err)
		}
		if len(page.Accounts) == q.Limit {
			page.Exhausted = false
			break
		}
		if !runtimeIdentifier.MatchString(id) {
			return app.DueAccountPage{}, domain.ErrUnavailable
		}
		page.Accounts = append(page.Accounts, app.AccountKey{Provider: q.Provider, AccountID: id})
		page.NextAccountID = id
	}
	if err = rows.Err(); err != nil {
		return app.DueAccountPage{}, databaseError(ctx, err)
	}
	return page, nil
}

// This is discovery, not send authority. The claim transaction independently
// rechecks due state, predecessor completion, time and (for WeCom) current owner.
const dueAccountsSQL = `SELECT DISTINCT i.account_id COLLATE "C" FROM gateway_delivery_intents i
 WHERE i.provider=$1 AND i.account_id COLLATE "C">$2 AND i.deadline>statement_timestamp()
 AND EXISTS(SELECT 1 FROM gateway_delivery_parts p WHERE p.intent_id=i.intent_id
  AND p.state IN ('PENDING','NOT_SENT') AND p.next_attempt_at<=statement_timestamp()
  AND p.attempt_number<$3 AND p.preparation_attempts<3
  AND NOT EXISTS(SELECT 1 FROM gateway_delivery_parts prev WHERE prev.intent_id=p.intent_id
   AND prev.part_index<p.part_index AND prev.state<>'ACCEPTED'))
 ORDER BY i.account_id COLLATE "C" LIMIT $4`

func (s *Store) ExpirePending(ctx context.Context, limit int) (int, error) {
	if ctx == nil || limit < 1 || limit > 1000 {
		return 0, domain.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, runtimeQueryTimeout)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, databaseError(ctx, err)
	}
	defer rollback(tx)
	rows, err := tx.Query(ctx, expirePendingSQL, limit, s.options.MaxAttempts)
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
	// Selection is only a hint: sample the authoritative clock after row locks,
	// then repeat both the state and deadline predicates in the CAS update.
	now, err := dbNow(ctx, tx)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, id := range ids {
		result, err := tx.Exec(ctx, `UPDATE gateway_delivery_parts p SET state='EXPIRED',next_attempt_at=NULL,updated_at=$2
 WHERE p.part_id=$1 AND (p.state='PENDING' OR (p.state='NOT_SENT' AND p.next_attempt_at IS NOT NULL AND p.attempt_number<$3 AND p.preparation_attempts<3))
 AND EXISTS(SELECT 1 FROM gateway_delivery_intents i WHERE i.intent_id=p.intent_id AND i.deadline<=$2)`, id, now, s.options.MaxAttempts)
		if err != nil {
			return 0, databaseError(ctx, err)
		}
		count += int(result.RowsAffected())
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, databaseError(ctx, err)
	}
	return count, nil
}

// No owner or predecessor check belongs here. Expiry authorizes no side effect,
// does not collect rows, and must also reach a later part blocked by UNKNOWN.
const expirePendingSQL = `SELECT p.part_id FROM gateway_delivery_intents i
 JOIN gateway_delivery_parts p ON p.intent_id=i.intent_id
 WHERE i.deadline<=statement_timestamp()
 AND (p.state='PENDING' OR (p.state='NOT_SENT' AND p.next_attempt_at IS NOT NULL AND p.attempt_number<$2 AND p.preparation_attempts<3))
 ORDER BY i.deadline,p.part_id LIMIT $1 FOR UPDATE OF p SKIP LOCKED`

func (s *Store) ListResolvableObserved(ctx context.Context, q app.ObservedAttemptQuery) (app.ObservedAttemptPage, error) {
	if ctx == nil || q.Limit < 1 || q.Limit > 1000 || !validRuntimeCursor(q.AfterAttemptID) {
		return app.ObservedAttemptPage{}, domain.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, runtimeQueryTimeout)
	defer cancel()
	rows, err := s.pool.Query(ctx, observedAttemptsSQL, q.AfterAttemptID, q.Limit+1)
	if err != nil {
		return app.ObservedAttemptPage{}, databaseError(ctx, err)
	}
	defer rows.Close()
	page := app.ObservedAttemptPage{Exhausted: true}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return app.ObservedAttemptPage{}, databaseError(ctx, err)
		}
		if len(page.AttemptIDs) == q.Limit {
			page.Exhausted = false
			break
		}
		if !runtimeIdentifier.MatchString(id) {
			return app.ObservedAttemptPage{}, domain.ErrUnavailable
		}
		page.AttemptIDs = append(page.AttemptIDs, id)
		page.NextAttemptID = id
	}
	if err = rows.Err(); err != nil {
		return app.ObservedAttemptPage{}, databaseError(ctx, err)
	}
	return page, nil
}

// Discovery deliberately includes conflicting and non-final evidence. Only the
// existing ResolveObserved transaction decides whether evidence can resolve an
// UNKNOWN. Keyset pagination lets maintenance move beyond an unresolved poison
// candidate without filtering away its audit evidence or changing any state.
const observedAttemptsSQL = `SELECT a.attempt_id FROM gateway_delivery_attempts a
 JOIN gateway_delivery_parts p ON p.part_id=a.part_id AND p.current_attempt_id=a.attempt_id
 WHERE a.attempt_id COLLATE "C">$1 AND p.state='UNKNOWN'
 AND EXISTS(SELECT 1 FROM gateway_delivery_observations o WHERE o.attempt_id=a.attempt_id)
 ORDER BY a.attempt_id COLLATE "C" LIMIT $2`

var _ app.DueAccountReader = (*Store)(nil)
var _ app.MaintenanceStore = (*Store)(nil)
