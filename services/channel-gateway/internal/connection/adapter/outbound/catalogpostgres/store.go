// Package catalogpostgres owns the non-secret account directory and transactional
// account-use guard. Consumers pass their pgx.Tx; no business SQL crosses owners.
package catalogpostgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
)

type Store struct {
	pool                         *pgxpool.Pool
	scope, epoch, instance, boot string
}
type Poll = c.Poll
type Qualification = c.Qualification

func New(pool *pgxpool.Pool, scope, epoch, instance, boot string) (*Store, error) {
	if pool == nil || !c.ValidID(scope) || !c.ValidEpoch(epoch) || !c.ValidID(instance) || !c.ValidEpoch(boot) {
		return nil, c.ErrInvalid
	}
	return &Store{pool, scope, epoch, instance, boot}, nil
}
func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
func dbError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return c.ErrExpired
	}
	return c.ErrUnavailable
}
func (s *Store) BeginPoll(ctx context.Context) (Poll, error) {
	p := Poll{LocalStarted: time.Now(), ScopeID: s.scope, SourceEpoch: s.epoch, InstanceID: s.instance, InstanceEpoch: s.boot}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return p, dbError(err)
	}
	defer rollback(tx)
	_, err = tx.Exec(ctx, `INSERT INTO gateway_account_catalogs(scope_id,source_epoch) VALUES($1,$2) ON CONFLICT DO NOTHING`, s.scope, s.epoch)
	if err != nil {
		return p, dbError(err)
	}
	var ep string
	var blocked bool
	err = tx.QueryRow(ctx, `SELECT source_epoch,revision,digest,blocked,clock_timestamp() FROM gateway_account_catalogs WHERE scope_id=$1`, s.scope).Scan(&ep, &p.Revision, &p.Digest, &blocked, &p.StartedAt)
	if err != nil {
		return p, dbError(err)
	}
	if ep != s.epoch || blocked {
		return p, c.ErrIntegrity
	}
	if err = tx.Commit(ctx); err != nil {
		return p, dbError(err)
	}
	return p, nil
}
func (s *Store) validPoll(p Poll) bool {
	return p.ScopeID == s.scope && p.SourceEpoch == s.epoch && p.InstanceID == s.instance && p.InstanceEpoch == s.boot && !p.LocalStarted.IsZero() && time.Since(p.LocalStarted) >= 0 && time.Since(p.LocalStarted) < 5*time.Second && !p.StartedAt.IsZero()
}
func (s *Store) Apply(ctx context.Context, p Poll, snap c.Snapshot) (c.Classification, Qualification, error) {
	var q Qualification
	if !s.validPoll(p) {
		return "", q, c.ErrExpired
	}
	if snap.ScopeID != s.scope || snap.SourceEpoch != s.epoch {
		return "", q, c.ErrIntegrity
	}
	if err := snap.Validate(); err != nil {
		return "", q, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", q, dbError(err)
	}
	defer rollback(tx)
	var ep, digest string
	var current int64
	var blocked bool
	err = tx.QueryRow(ctx, `SELECT source_epoch,revision,digest,blocked FROM gateway_account_catalogs WHERE scope_id=$1 FOR UPDATE`, s.scope).Scan(&ep, &current, &digest, &blocked)
	if err != nil {
		return "", q, dbError(err)
	}
	if ep != s.epoch || blocked {
		return "", q, c.ErrIntegrity
	}
	known := ""
	if snap.Revision == current {
		known = digest
	}
	if snap.Revision == p.Revision {
		if known != "" && known != p.Digest {
			return "", q, s.quarantine(ctx, tx)
		}
		known = p.Digest
	}
	var receipt string
	err = tx.QueryRow(ctx, `SELECT digest FROM gateway_account_snapshot_receipts WHERE scope_id=$1 AND source_epoch=$2 AND revision=$3`, s.scope, s.epoch, snap.Revision).Scan(&receipt)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return "", q, dbError(err)
	}
	if err == nil {
		if known != "" && known != receipt {
			return "", q, s.quarantine(ctx, tx)
		}
		known = receipt
	}
	classification, err := c.Classify(p.Revision, current, snap.Revision, known, snap.Digest)
	if err != nil {
		return "", q, s.quarantine(ctx, tx)
	}
	if !s.validPoll(p) {
		return "", q, c.ErrExpired
	}
	_, err = tx.Exec(ctx, `INSERT INTO gateway_account_snapshot_receipts(scope_id,source_epoch,revision,digest) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, s.scope, s.epoch, snap.Revision, snap.Digest)
	if err != nil {
		return "", q, dbError(err)
	}
	if classification == c.Advance {
		if err = s.applyAccounts(ctx, tx, snap.Accounts); err != nil {
			if errors.Is(err, c.ErrIntegrity) {
				return "", q, s.quarantine(ctx, tx)
			}
			return "", q, err
		}
		raw, _ := json.Marshal(snap)
		_, err = tx.Exec(ctx, `UPDATE gateway_account_catalogs SET revision=$2,digest=$3,snapshot_json=$4 WHERE scope_id=$1`, s.scope, snap.Revision, snap.Digest, raw)
		if err != nil {
			return "", q, dbError(err)
		}
	}
	if classification != c.Superseded {
		// Clock is captured at request start, not arrival. An old or delayed response
		// can never grant a fresh 30 seconds from transaction completion.
		err = tx.QueryRow(ctx, `INSERT INTO gateway_account_qualifications(scope_id,instance_id,instance_epoch,generation,source_revision,valid_until,enabled,source_epoch) VALUES($1,$2,$3,1,$4,$5,true,$6)
   ON CONFLICT(scope_id,instance_id,instance_epoch) DO UPDATE SET source_revision=EXCLUDED.source_revision,valid_until=EXCLUDED.valid_until,enabled=true
   RETURNING generation,source_revision,valid_until`, s.scope, s.instance, s.boot, snap.Revision, p.StartedAt.Add(30*time.Second), s.epoch).Scan(&q.Generation, &q.Revision, &q.ValidUntil)
		if err != nil {
			return "", q, dbError(err)
		}
	}
	_, err = tx.Exec(ctx, `DELETE FROM gateway_account_snapshot_receipts WHERE scope_id=$1 AND received_at<clock_timestamp()-interval '90 seconds' AND revision<>$2`, s.scope, max(current, snap.Revision))
	if err != nil {
		return "", q, dbError(err)
	}
	if !s.validPoll(p) {
		return "", Qualification{}, c.ErrExpired
	}
	if err = tx.Commit(ctx); err != nil {
		return "", Qualification{}, dbError(err)
	}
	return classification, q, nil
}
func (s *Store) quarantine(ctx context.Context, tx pgx.Tx) error {
	// Called before applying content, or after a savepoint-protected account pass.
	_, err := tx.Exec(ctx, `UPDATE gateway_account_catalogs SET blocked=true WHERE scope_id=$1`, s.scope)
	if err != nil {
		return dbError(err)
	}
	// Guards do not acquire a catalog lock (avoids a broad hot lock), therefore
	// lock/update every account in the same fixed order as ordinary snapshots.
	rows, err := tx.Query(ctx, `SELECT account_id FROM gateway_account_directory WHERE scope_id=$1 ORDER BY account_id FOR UPDATE`, s.scope)
	if err != nil {
		return dbError(err)
	}
	rows.Close()
	if rows.Err() != nil {
		return dbError(rows.Err())
	}
	_, err = tx.Exec(ctx, `UPDATE gateway_account_directory SET enabled=false WHERE scope_id=$1`, s.scope)
	if err != nil {
		return dbError(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return dbError(err)
	}
	return c.ErrIntegrity
}
func (s *Store) applyAccounts(ctx context.Context, tx pgx.Tx, accounts []c.Account) error {
	rows, err := tx.Query(ctx, `SELECT account_id,account_json,present FROM gateway_account_directory WHERE scope_id=$1 ORDER BY account_id FOR UPDATE`, s.scope)
	if err != nil {
		return dbError(err)
	}
	type stored struct {
		account c.Account
		present bool
	}
	old := map[string]stored{}
	for rows.Next() {
		var id string
		var raw []byte
		var x stored
		if err = rows.Scan(&id, &raw, &x.present); err != nil {
			rows.Close()
			return dbError(err)
		}
		if json.Unmarshal(raw, &x.account) != nil || x.account.Validate() != nil {
			rows.Close()
			return c.ErrIntegrity
		}
		old[id] = x
	}
	rows.Close()
	if rows.Err() != nil {
		return dbError(rows.Err())
	}
	// Validate the complete candidate before any account writes; quarantine can
	// then commit without publishing a partially updated directory.
	for _, a := range accounts {
		if o, ok := old[a.ID]; ok {
			if err = c.CheckSuccessor(o.account, a, o.present); err != nil {
				return err
			}
		}
	}
	present := make(map[string]bool, len(accounts))
	for _, a := range accounts {
		present[a.ID] = true
	}
	if len(old)+countNew(oldKeys(old), present) > c.MaxAccounts {
		return c.ErrIntegrity
	}
	// New rows have no consumers yet. Existing rows were locked in account order.
	for _, a := range accounts {
		// Offsets and registered URLs belong to a Telegram server, not merely
		// a Bot ID. Never carry an official cursor into the local simulator.
		if o, ok := old[a.ID]; ok && o.account.Config.EndpointProfile != a.Config.EndpointProfile {
			_, err = tx.Exec(ctx, `UPDATE gateway_telegram_receivers SET next_offset=0,last_update_at='epoch',last_poll_at='epoch',managed_url='',pending_url='',managed_revision=0,next_due='epoch' WHERE scope_id=$1 AND account_id=$2`, s.scope, a.ID)
			if err != nil {
				return dbError(err)
			}
		}
		raw, _ := json.Marshal(a)
		_, err = tx.Exec(ctx, `INSERT INTO gateway_account_directory(scope_id,account_id,tenant_id,provider,provider_account_id,connection_revision,min_route_generation,enabled,present,account_json,source_epoch)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,true,$9,$10) ON CONFLICT(scope_id,account_id) DO UPDATE SET connection_revision=EXCLUDED.connection_revision,min_route_generation=EXCLUDED.min_route_generation,enabled=EXCLUDED.enabled,present=true,account_json=EXCLUDED.account_json`, s.scope, a.ID, a.TenantID, a.Provider, a.ProviderAccountID, a.ConnectionRevision, a.MinRouteGeneration, a.Enabled, raw, s.epoch)
		if err != nil {
			return dbError(err)
		}
	}
	for id := range old {
		if !present[id] {
			_, err = tx.Exec(ctx, `UPDATE gateway_account_directory SET enabled=false,present=false WHERE scope_id=$1 AND account_id=$2`, s.scope, id)
			if err != nil {
				return dbError(err)
			}
		}
	}
	return nil
}
func oldKeys[T any](m map[string]T) map[string]bool {
	o := map[string]bool{}
	for k := range m {
		o[k] = true
	}
	return o
}
func countNew(old, next map[string]bool) int {
	n := 0
	for id := range next {
		if !old[id] {
			n++
		}
	}
	return n
}

// Invalidate only affects this process incarnation. Other healthy replicas are
// not revoked merely because this replica cannot contact Control.
func (s *Store) Invalidate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `UPDATE gateway_account_qualifications SET enabled=false,generation=generation+1 WHERE scope_id=$1 AND instance_id=$2 AND instance_epoch=$3 AND enabled AND source_epoch=$4`, s.scope, s.instance, s.boot, s.epoch)
	return dbError(err)
}
func (s *Store) ReadAccount(ctx context.Context, id string) (c.Account, error) {
	var a c.Account
	var raw []byte
	var enabled, present bool
	err := s.pool.QueryRow(ctx, `SELECT account_json,enabled,present FROM gateway_account_directory WHERE scope_id=$1 AND account_id=$2`, s.scope, id).Scan(&raw, &enabled, &present)
	if errors.Is(err, pgx.ErrNoRows) {
		return a, c.ErrUnauthorized
	}
	if err != nil {
		return a, dbError(err)
	}
	if json.Unmarshal(raw, &a) != nil || a.Validate() != nil {
		return c.Account{}, c.ErrIntegrity
	}
	if !enabled || !present {
		return c.Account{}, c.ErrUnauthorized
	}
	return a, nil
}
