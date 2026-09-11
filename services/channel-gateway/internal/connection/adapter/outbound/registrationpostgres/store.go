// Package registrationpostgres owns the Telegram registration fence and facts.
package registrationpostgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/telegramruntime"
	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
	"time"
)

type Guard interface {
	Guard(context.Context, pgx.Tx, *c.Permit, *int64) error
	RecheckExpiry(context.Context, pgx.Tx, *c.Permit) error
}
type Store struct {
	pool  *pgxpool.Pool
	guard Guard
}

func New(p *pgxpool.Pool, g Guard) *Store { return &Store{p, g} }
func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

// State is diagnostic only; it cannot authorize credentials or a call.
func (s *Store) State(ctx context.Context, p *c.Permit) (string, error) {
	if p.Check() != nil {
		return "", c.ErrUnauthorized
	}
	b := p.Binding()
	var state string
	e := s.pool.QueryRow(ctx, `SELECT CASE WHEN state='READY' AND next_due>clock_timestamp() THEN 'READY' ELSE 'PENDING' END FROM gateway_telegram_registrations WHERE scope_id=$1 AND account_id=$2 AND connection_revision=$3`, b.ScopeID, b.AccountID, b.ConnectionRevision).Scan(&state)
	if e != nil {
		return "", c.ErrUnavailable
	}
	return state, nil
}
func (s *Store) Acquire(ctx context.Context, p *c.Permit) (app.Operation, bool, error) {
	var z app.Operation
	if p.Check() != nil || p.Binding().Kind != "telegram_registration" {
		return z, false, c.ErrUnauthorized
	}
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return z, false, c.ErrUnavailable
	}
	defer rollback(tx)
	if e = s.guard.Guard(ctx, tx, p, nil); e != nil {
		return z, false, e
	}
	b := p.Binding()
	_, e = tx.Exec(ctx, `INSERT INTO gateway_telegram_registrations(scope_id,account_id,connection_revision) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, b.ScopeID, b.AccountID, b.ConnectionRevision)
	if e != nil {
		return z, false, c.ErrUnavailable
	}
	var rev, epoch int64
	var available bool
	e = tx.QueryRow(ctx, `SELECT connection_revision,epoch,lease_until<=clock_timestamp() AND (connection_revision<>$3 OR next_due<=clock_timestamp()) FROM gateway_telegram_registrations WHERE scope_id=$1 AND account_id=$2 FOR UPDATE`, b.ScopeID, b.AccountID, b.ConnectionRevision).Scan(&rev, &epoch, &available)
	if e != nil {
		return z, false, c.ErrUnavailable
	}
	if !available {
		return z, false, nil
	}
	// Expired in-flight operations retain UNKNOWN facts; they are not retried
	// under the old configuration or silently treated as never called.
	_, e = tx.Exec(ctx, `UPDATE gateway_telegram_registration_attempts SET state='UNKNOWN' WHERE scope_id=$1 AND account_id=$2 AND result IS NULL AND state IN ('PREPARING','CALLING')`, b.ScopeID, b.AccountID)
	if e != nil {
		return z, false, c.ErrUnavailable
	}
	var id [24]byte
	if _, e = rand.Read(id[:]); e != nil {
		return z, false, c.ErrUnavailable
	}
	z = app.Operation{ID: hex.EncodeToString(id[:]), AccountID: b.AccountID, InstanceID: b.InstanceID, InstanceEpoch: b.InstanceEpoch, Revision: b.ConnectionRevision, Epoch: epoch + 1}
	_, e = tx.Exec(ctx, `UPDATE gateway_telegram_registrations SET connection_revision=$3,epoch=$4,instance_id=$5,instance_epoch=$6,lease_until=clock_timestamp()+interval '25 seconds',operation_id=$7,state='PREPARING' WHERE scope_id=$1 AND account_id=$2`, b.ScopeID, b.AccountID, z.Revision, z.Epoch, z.InstanceID, z.InstanceEpoch, z.ID)
	if e != nil {
		return app.Operation{}, false, c.ErrUnavailable
	}
	_, e = tx.Exec(ctx, `INSERT INTO gateway_telegram_registration_attempts(operation_id,scope_id,account_id,connection_revision,epoch,instance_id,instance_epoch) VALUES($1,$2,$3,$4,$5,$6,$7)`, z.ID, b.ScopeID, z.AccountID, z.Revision, z.Epoch, z.InstanceID, z.InstanceEpoch)
	if e != nil {
		return app.Operation{}, false, c.ErrUnavailable
	}
	if e = s.guard.RecheckExpiry(ctx, tx, p); e != nil {
		return app.Operation{}, false, e
	}
	if e = tx.Commit(ctx); e != nil {
		return app.Operation{}, false, c.ErrUnavailable
	}
	return z, true, nil
}
func (s *Store) check(ctx context.Context, tx pgx.Tx, p *c.Permit, o app.Operation) error {
	b := p.Binding()
	if p.Check() != nil || b.Kind != "telegram_registration" || b.AccountID != o.AccountID || b.ConnectionRevision != o.Revision || b.InstanceID != o.InstanceID || b.InstanceEpoch != o.InstanceEpoch {
		return c.ErrUnauthorized
	}
	var yes bool
	e := tx.QueryRow(ctx, `SELECT operation_id=$3 AND epoch=$4 AND connection_revision=$5 AND instance_id=$6 AND instance_epoch=$7 AND lease_until>clock_timestamp() FROM gateway_telegram_registrations WHERE scope_id=$1 AND account_id=$2 FOR UPDATE`, b.ScopeID, o.AccountID, o.ID, o.Epoch, o.Revision, o.InstanceID, o.InstanceEpoch).Scan(&yes)
	if e != nil || !yes {
		return c.ErrUnauthorized
	}
	return nil
}
func (s *Store) gate(ctx context.Context, p *c.Permit, o app.Operation, begin bool) error {
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return c.ErrUnavailable
	}
	defer rollback(tx)
	if e = s.guard.Guard(ctx, tx, p, nil); e != nil {
		return e
	}
	if e = s.check(ctx, tx, p, o); e != nil {
		return e
	}
	if begin {
		tag, e := tx.Exec(ctx, `UPDATE gateway_telegram_registration_attempts SET state='CALLING',calling_at=clock_timestamp() WHERE operation_id=$1 AND state='PREPARING' AND result IS NULL`, o.ID)
		if e != nil || tag.RowsAffected() != 1 {
			return c.ErrUnauthorized
		}
	}
	if e = s.guard.RecheckExpiry(ctx, tx, p); e != nil {
		return e
	}
	if e = tx.Commit(ctx); e != nil {
		return c.ErrUnavailable
	}
	return nil
}
func (s *Store) Check(ctx context.Context, p *c.Permit, o app.Operation) error {
	return s.gate(ctx, p, o, false)
}
func (s *Store) BeginCall(ctx context.Context, p *c.Permit, o app.Operation) error {
	return s.gate(ctx, p, o, true)
}
func (s *Store) Finish(ctx context.Context, p *c.Permit, o app.Operation, result string) error {
	if result != "READY" && result != "UNKNOWN" && result != "NOT_SENT" && result != "IDENTITY_MISMATCH" {
		return c.ErrInvalid
	}
	b := p.Binding()
	// Record the original fact regardless of current account qualification.
	tag, e := s.pool.Exec(ctx, `UPDATE gateway_telegram_registration_attempts SET result=$7,state='FINISHED',finished_at=clock_timestamp() WHERE operation_id=$1 AND scope_id=$2 AND account_id=$3 AND epoch=$4 AND instance_id=$5 AND instance_epoch=$6 AND result IS NULL AND (calling_at IS NOT NULL OR $7<>'READY')`, o.ID, b.ScopeID, o.AccountID, o.Epoch, o.InstanceID, o.InstanceEpoch, result)
	if e != nil || tag.RowsAffected() != 1 {
		return c.ErrUnauthorized
	}
	// Any late old fact asks the CURRENT holder to reconcile, never promotes it.
	_, e = s.pool.Exec(ctx, `UPDATE gateway_telegram_registrations SET next_due=clock_timestamp() WHERE scope_id=$1 AND account_id=$2 AND operation_id<>$3`, b.ScopeID, o.AccountID, o.ID)
	if e != nil {
		return c.ErrUnavailable
	}
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return c.ErrUnavailable
	}
	defer rollback(tx)
	if e = s.guard.Guard(ctx, tx, p, nil); e != nil {
		return e
	}
	if e = s.check(ctx, tx, p, o); e != nil {
		return e
	}
	delay := 5
	if result == "READY" {
		delay = 60
	}
	_, e = tx.Exec(ctx, `UPDATE gateway_telegram_registrations SET state=$3,lease_until=clock_timestamp(),next_due=clock_timestamp()+$4*interval '1 second' WHERE scope_id=$1 AND account_id=$2`, b.ScopeID, o.AccountID, result, delay)
	if e != nil {
		return c.ErrUnavailable
	}
	if e = s.guard.RecheckExpiry(ctx, tx, p); e != nil {
		return e
	}
	if e = tx.Commit(ctx); e != nil {
		return c.ErrUnavailable
	}
	return nil
}
