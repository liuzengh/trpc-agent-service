// Package postgresadapter owns Connection's account and lease SQL. All lease
// decisions use the database clock after taking the relevant account row lock.
package postgresadapter

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
)

type Store struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

type accountRow struct {
	account         domain.Account
	instanceID      string
	epoch           int64
	leaseUntil      *time.Time
	blockedRevision int64
}

// ApplyAndAcquire commits a valid newer account revision even when an existing
// lease prevents acquisition. Old owners immediately lose Renew/Check authority,
// while their existing lease still prevents overlap until release or expiry.
func (s *Store) ApplyAndAcquire(ctx context.Context, a domain.Account, instanceID string, ttl time.Duration) (domain.OwnerGrant, error) {
	if err := a.Validate(); err != nil {
		return domain.OwnerGrant{}, err
	}
	if err := domain.ValidateInstanceID(instanceID); err != nil {
		return domain.OwnerGrant{}, err
	}
	if err := domain.ValidateLeaseTTL(ttl); err != nil {
		return domain.OwnerGrant{}, err
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return domain.OwnerGrant{}, err
	}
	defer rollbackBounded(tx)
	_, err = tx.Exec(ctx, `INSERT INTO gateway_connection_accounts(account_id,bot_id,credential_ref,revision,enabled)
 VALUES($1,$2,$3,$4,$5) ON CONFLICT(account_id) DO NOTHING`, a.ID, a.BotID, a.CredentialRef, a.Revision, a.Enabled)
	if err != nil {
		return domain.OwnerGrant{}, classifyDatabaseError(err)
	}
	r, err := readAccount(ctx, tx, a.ID, true)
	if err != nil {
		return domain.OwnerGrant{}, err
	}
	if r.account.BotID != a.BotID {
		return domain.OwnerGrant{}, domain.ErrConflict
	}
	if a.Revision < r.account.Revision {
		return domain.OwnerGrant{}, domain.ErrStaleRevision
	}
	if a.Revision == r.account.Revision && (a.CredentialRef != r.account.CredentialRef || a.Enabled != r.account.Enabled) {
		return domain.OwnerGrant{}, domain.ErrConflict
	}
	if a.Revision > r.account.Revision {
		_, err = tx.Exec(ctx, `UPDATE gateway_connection_accounts SET credential_ref=$2,revision=$3,enabled=$4,blocked_revision=0,updated_at=clock_timestamp() WHERE account_id=$1`, a.ID, a.CredentialRef, a.Revision, a.Enabled)
		if err != nil {
			return domain.OwnerGrant{}, err
		}
		r.account = a
		r.blockedRevision = 0
	}
	if !r.account.Enabled {
		return domain.OwnerGrant{}, commitOutcome(ctx, tx, domain.ErrDisabled)
	}
	if r.blockedRevision == r.account.Revision {
		return domain.OwnerGrant{}, commitOutcome(ctx, tx, domain.ErrReplaced)
	}
	now, until, err := leaseWindow(ctx, tx, ttl)
	if err != nil {
		return domain.OwnerGrant{}, err
	}
	if r.leaseUntil != nil && now.Before(*r.leaseUntil) {
		return domain.OwnerGrant{}, commitOutcome(ctx, tx, domain.ErrHeld)
	}
	if r.epoch == math.MaxInt64 {
		return domain.OwnerGrant{}, commitOutcome(ctx, tx, domain.ErrConflict)
	}
	epoch := r.epoch + 1
	_, err = tx.Exec(ctx, `UPDATE gateway_connection_accounts SET instance_id=$2,epoch=$3,lease_until=$4,updated_at=$5 WHERE account_id=$1`, a.ID, instanceID, epoch, until, now)
	if err != nil {
		return domain.OwnerGrant{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return domain.OwnerGrant{}, err
	}
	return domain.OwnerGrant{AccountID: a.ID, BotID: a.BotID, InstanceID: instanceID, Epoch: epoch, Revision: a.Revision, LeaseUntil: until, ObservedAt: now}, nil
}

func (s *Store) Renew(ctx context.Context, g domain.OwnerGrant, ttl time.Duration) (domain.OwnerGrant, error) {
	if err := g.Validate(); err != nil {
		return domain.OwnerGrant{}, err
	}
	if err := domain.ValidateLeaseTTL(ttl); err != nil {
		return domain.OwnerGrant{}, err
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return domain.OwnerGrant{}, err
	}
	defer rollbackBounded(tx)
	r, err := readAccount(ctx, tx, g.AccountID, true)
	if err != nil {
		return domain.OwnerGrant{}, err
	}
	now, until, err := leaseWindow(ctx, tx, ttl)
	if err != nil {
		return domain.OwnerGrant{}, err
	}
	if r.account.BotID != g.BotID {
		return domain.OwnerGrant{}, domain.ErrConflict
	}
	if err = checkOwner(r, g.InstanceID, g.Epoch, g.Revision, now); err != nil {
		return domain.OwnerGrant{}, err
	}
	_, err = tx.Exec(ctx, `UPDATE gateway_connection_accounts SET lease_until=$2,updated_at=$3 WHERE account_id=$1`, g.AccountID, until, now)
	if err != nil {
		return domain.OwnerGrant{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return domain.OwnerGrant{}, err
	}
	g.LeaseUntil = until
	g.ObservedAt = now
	return g, nil
}

// Release deliberately ignores account revision: a rotated/disabled old client
// must finish closing and then release its own epoch. Retaining the old instance
// and epoch makes an exact repeated Release idempotent without deleting identity.
func (s *Store) Release(ctx context.Context, g domain.OwnerGrant) error {
	if err := g.Validate(); err != nil {
		return err
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer rollbackBounded(tx)
	r, err := readAccount(ctx, tx, g.AccountID, true)
	if err != nil {
		return err
	}
	if r.account.BotID != g.BotID {
		return domain.ErrConflict
	}
	if r.instanceID != g.InstanceID || r.epoch != g.Epoch {
		return domain.ErrLost
	}
	if _, err = tx.Exec(ctx, `UPDATE gateway_connection_accounts SET lease_until=NULL,updated_at=clock_timestamp() WHERE account_id=$1`, g.AccountID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) Check(ctx context.Context, g domain.OwnerGrant) error {
	if err := g.Validate(); err != nil {
		return err
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer rollbackBounded(tx)
	r, err := readAccount(ctx, tx, g.AccountID, false)
	if err != nil {
		return err
	}
	if r.account.BotID != g.BotID {
		return domain.ErrConflict
	}
	now, _, err := leaseWindow(ctx, tx, 0)
	if err != nil {
		return err
	}
	if err = checkOwner(r, g.InstanceID, g.Epoch, g.Revision, now); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// VerifyOwner is a read-only Adapter seam for an Admission transaction. It
// retains the row SHARE lock until the caller commits/rolls back. The clock is
// sampled after obtaining that lock, not before potentially waiting for it.
func (s *Store) VerifyOwner(ctx context.Context, tx pgx.Tx, accountID, instanceID string, epoch, revision int64) error {
	if err := domain.ValidateOwnerIdentity(accountID, instanceID, epoch, revision); err != nil {
		return err
	}
	if tx == nil {
		return domain.ErrInvalid
	}
	r, err := readAccount(ctx, tx, accountID, false)
	if err != nil {
		return err
	}
	now, _, err := leaseWindow(ctx, tx, 0)
	if err != nil {
		return err
	}
	return checkOwner(r, instanceID, epoch, revision, now)
}

// MarkReplaced fences this desired revision across all replicas. An old client
// cannot quarantine a newer revision/owner; only an active matching grant may
// install the block. A strictly newer account revision is required for recovery.
func (s *Store) MarkReplaced(ctx context.Context, g domain.OwnerGrant) error {
	if err := g.Validate(); err != nil {
		return err
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer rollbackBounded(tx)
	r, err := readAccount(ctx, tx, g.AccountID, true)
	if err != nil {
		return err
	}
	if r.account.BotID != g.BotID {
		return domain.ErrConflict
	}
	now, _, err := leaseWindow(ctx, tx, 0)
	if err != nil {
		return err
	}
	if err = checkOwner(r, g.InstanceID, g.Epoch, g.Revision, now); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE gateway_connection_accounts SET blocked_revision=revision,instance_id='',lease_until=NULL,updated_at=$2 WHERE account_id=$1`, g.AccountID, now)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) begin(ctx context.Context) (pgx.Tx, error) {
	if s.pool == nil {
		return nil, domain.ErrInvalid
	}
	return s.pool.Begin(ctx)
}

func readAccount(ctx context.Context, tx pgx.Tx, accountID string, exclusive bool) (accountRow, error) {
	query := `SELECT account_id,bot_id,credential_ref,revision,enabled,instance_id,epoch,lease_until,blocked_revision FROM gateway_connection_accounts WHERE account_id=$1`
	if exclusive {
		query += " FOR UPDATE"
	} else {
		query += " FOR SHARE"
	}
	var r accountRow
	err := tx.QueryRow(ctx, query, accountID).Scan(&r.account.ID, &r.account.BotID, &r.account.CredentialRef, &r.account.Revision, &r.account.Enabled, &r.instanceID, &r.epoch, &r.leaseUntil, &r.blockedRevision)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, domain.ErrLost
	}
	return r, err
}

func checkOwner(r accountRow, instanceID string, epoch, revision int64, now time.Time) error {
	if !r.account.Enabled {
		return domain.ErrDisabled
	}
	if r.account.Revision != revision {
		return domain.ErrStaleRevision
	}
	if r.blockedRevision == r.account.Revision {
		return domain.ErrReplaced
	}
	if r.instanceID != instanceID || r.epoch != epoch || r.leaseUntil == nil || !now.Before(*r.leaseUntil) {
		return domain.ErrLost
	}
	return nil
}

func leaseWindow(ctx context.Context, tx pgx.Tx, ttl time.Duration) (time.Time, time.Time, error) {
	var observed, until time.Time
	err := tx.QueryRow(ctx, `WITH clock AS MATERIALIZED (SELECT clock_timestamp() AS observed)
 SELECT observed,observed+($1::bigint*interval '1 microsecond') FROM clock`, ttl.Microseconds()).Scan(&observed, &until)
	return observed, until, err
}

func commitOutcome(ctx context.Context, tx pgx.Tx, outcome error) error {
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return outcome
}

func classifyDatabaseError(err error) error {
	var pgerr *pgconn.PgError
	if errors.As(err, &pgerr) && pgerr.Code == "23505" {
		return domain.ErrConflict
	}
	return err
}

func rollbackBounded(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
