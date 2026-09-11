// Package telegramreceptionpostgres owns physical-Bot leases, in-flight call
// fences, and durable offsets. No transaction spans an external HTTP request.
package telegramreceptionpostgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/telegramreception"
)

type Guard interface {
	Guard(context.Context, pgx.Tx, *c.Permit, *int64) error
	RecheckExpiry(context.Context, pgx.Tx, *c.Permit) error
}
type Store struct {
	pool  *pgxpool.Pool
	guard Guard
}

func New(pool *pgxpool.Pool, guard Guard) *Store { return &Store{pool, guard} }
func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func (s *Store) transaction(ctx context.Context, p *c.Permit, fn func(pgx.Tx) error) error {
	if p.Check() != nil || p.Binding().Kind != "telegram_receiver" || s.pool == nil || s.guard == nil {
		return d.ErrOwnership
	}
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return c.ErrUnavailable
	}
	defer rollback(tx)
	if e = s.guard.Guard(ctx, tx, p, nil); e != nil {
		return d.ErrOwnership
	}
	if e = fn(tx); e != nil {
		return e
	}
	if e = s.guard.RecheckExpiry(ctx, tx, p); e != nil {
		return d.ErrOwnership
	}
	if e = tx.Commit(ctx); e != nil {
		return c.ErrUnavailable
	}
	return nil
}

func (s *Store) Acquire(ctx context.Context, p *c.Permit, botID string) (d.Lease, bool, error) {
	var l d.Lease
	acquired := false
	e := s.transaction(ctx, p, func(tx pgx.Tx) error {
		b := p.Binding()
		var physical string
		if e := tx.QueryRow(ctx, `SELECT provider_account_id FROM gateway_account_directory WHERE scope_id=$1 AND account_id=$2`, b.ScopeID, b.AccountID).Scan(&physical); e != nil {
			return c.ErrUnavailable
		}
		if physical != botID {
			return d.ErrOwnership
		}
		// During an explicit upgrade window, a legacy registration may still be
		// settling. Do not race its bounded lease with the new receiver.
		var legacyBusy bool
		if e := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM gateway_telegram_registrations WHERE scope_id=$1 AND account_id=$2 AND lease_until>clock_timestamp())`, b.ScopeID, b.AccountID).Scan(&legacyBusy); e != nil {
			return c.ErrUnavailable
		}
		if legacyBusy {
			return nil
		}
		_, e := tx.Exec(ctx, `INSERT INTO gateway_telegram_receivers(bot_id,scope_id,account_id,source_epoch,connection_revision) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, botID, b.ScopeID, b.AccountID, b.SourceEpoch, b.ConnectionRevision)
		if e != nil {
			return c.ErrUnavailable
		}
		var due, callDone, expired bool
		e = tx.QueryRow(ctx, `SELECT scope_id,account_id,source_epoch,connection_revision,owner_epoch,instance_id,instance_epoch,next_offset,last_update_at,managed_url,pending_url,next_due<=clock_timestamp(),call_until<=clock_timestamp(),lease_until<=clock_timestamp() FROM gateway_telegram_receivers WHERE bot_id=$1 FOR UPDATE`, botID).Scan(&l.ScopeID, &l.AccountID, &l.SourceEpoch, &l.Revision, &l.Epoch, &l.InstanceID, &l.InstanceEpoch, &l.NextOffset, &l.LastUpdateAt, &l.ManagedURL, &l.PendingURL, &due, &callDone, &expired)
		if e != nil {
			return c.ErrUnavailable
		}
		if l.ScopeID != b.ScopeID || l.AccountID != b.AccountID || l.SourceEpoch != b.SourceEpoch {
			return d.ErrOwnership
		}
		same := l.Revision == b.ConnectionRevision && l.InstanceID == b.InstanceID && l.InstanceEpoch == b.InstanceEpoch
		// A newer authoritative connection revision already fences all old
		// permits in Guard. It may replace the owner only after the recorded
		// remote call window has ended; disabled alone is never sufficient.
		changed := l.Revision != b.ConnectionRevision
		// A lease naming this instance but a different process epoch belongs to
		// a previous incarnation: one logical instance runs one process, so that
		// owner is gone. Waiting out its 30 second lease would stall reception
		// on every restart and hold the whole Gateway unready, because Ready()
		// fails while any enabled account is unhealthy.
		restarted := l.InstanceID == b.InstanceID && l.InstanceEpoch != b.InstanceEpoch
		if !callDone || !expired && !same && !changed && !restarted {
			return nil
		}
		if !due && l.Revision == b.ConnectionRevision {
			return nil
		}
		if !same || expired {
			if l.Epoch >= c.MaxRevision {
				return d.ErrOwnership
			}
			l.Epoch++
		}
		l.BotID = botID
		l.Revision = b.ConnectionRevision
		l.InstanceID = b.InstanceID
		l.InstanceEpoch = b.InstanceEpoch
		e = tx.QueryRow(ctx, `UPDATE gateway_telegram_receivers SET connection_revision=$2,owner_epoch=$3,instance_id=$4,instance_epoch=$5,lease_until=clock_timestamp()+interval '30 seconds',call_id='' WHERE bot_id=$1 RETURNING lease_until`, botID, l.Revision, l.Epoch, l.InstanceID, l.InstanceEpoch).Scan(&l.Until)
		if e != nil {
			return c.ErrUnavailable
		}
		acquired = true
		return nil
	})
	return l, acquired, e
}

func checkBinding(p *c.Permit, l d.Lease) error {
	b := p.Binding()
	if p.Check() != nil || b.Kind != "telegram_receiver" || b.ScopeID != l.ScopeID || b.SourceEpoch != l.SourceEpoch || b.AccountID != l.AccountID || b.ConnectionRevision != l.Revision || b.InstanceID != l.InstanceID || b.InstanceEpoch != l.InstanceEpoch || l.Epoch < 1 {
		return d.ErrOwnership
	}
	return nil
}
func (s *Store) check(ctx context.Context, tx pgx.Tx, p *c.Permit, l d.Lease) error {
	if checkBinding(p, l) != nil {
		return d.ErrOwnership
	}
	var yes bool
	e := tx.QueryRow(ctx, `SELECT scope_id=$2 AND account_id=$3 AND source_epoch=$4 AND connection_revision=$5 AND owner_epoch=$6 AND instance_id=$7 AND instance_epoch=$8 AND lease_until>clock_timestamp() FROM gateway_telegram_receivers WHERE bot_id=$1 FOR UPDATE`, l.BotID, l.ScopeID, l.AccountID, l.SourceEpoch, l.Revision, l.Epoch, l.InstanceID, l.InstanceEpoch).Scan(&yes)
	if e != nil || !yes {
		return d.ErrOwnership
	}
	return nil
}
func (s *Store) Check(ctx context.Context, p *c.Permit, l d.Lease) error {
	return s.transaction(ctx, p, func(tx pgx.Tx) error { return s.check(ctx, tx, p, l) })
}

func (s *Store) BeginCall(ctx context.Context, p *c.Permit, l d.Lease) (d.Call, error) {
	var call d.Call
	e := s.transaction(ctx, p, func(tx pgx.Tx) error {
		if e := s.check(ctx, tx, p, l); e != nil {
			return e
		}
		var id [16]byte
		if _, e := rand.Read(id[:]); e != nil {
			return c.ErrUnavailable
		}
		call.ID = hex.EncodeToString(id[:])
		// Store a remote uncertainty window beyond the local HTTP deadline. A
		// cancelled/failed caller cannot clear this window in FinishCall.
		e := tx.QueryRow(ctx, `UPDATE gateway_telegram_receivers SET call_id=$2,call_until=clock_timestamp()+interval '10 seconds' WHERE bot_id=$1 AND call_until<=clock_timestamp() AND lease_until>clock_timestamp()+interval '10 seconds' RETURNING call_until-interval '2 seconds'`, l.BotID, call.ID).Scan(&call.Deadline)
		if e != nil {
			return d.ErrOwnership
		}
		return nil
	})
	return call, e
}
func (s *Store) FinishCall(ctx context.Context, p *c.Permit, l d.Lease, call d.Call, completed bool) error {
	if !completed {
		return nil
	} // Keep the recorded deadline for uncertain calls.
	return s.transaction(ctx, p, func(tx pgx.Tx) error {
		if e := s.check(ctx, tx, p, l); e != nil {
			return e
		}
		tag, e := tx.Exec(ctx, `UPDATE gateway_telegram_receivers SET call_id='',call_until=clock_timestamp() WHERE bot_id=$1 AND call_id=$2`, l.BotID, call.ID)
		if e != nil || tag.RowsAffected() != 1 {
			return d.ErrOwnership
		}
		return nil
	})
}

// CommitCursor is called only after durable admission. expected is a cursor
// CAS, not the request offset (idle recovery intentionally polls from zero).
func (s *Store) CommitCursor(ctx context.Context, p *c.Permit, l d.Lease, expected, next int64) error {
	if expected < 0 || next < 1 || next > c.MaxRevision {
		return d.ErrCursor
	}
	return s.transaction(ctx, p, func(tx pgx.Tx) error {
		if e := s.check(ctx, tx, p, l); e != nil {
			return e
		}
		tag, e := tx.Exec(ctx, `UPDATE gateway_telegram_receivers SET next_offset=$3,last_update_at=clock_timestamp() WHERE bot_id=$1 AND next_offset=$2 AND lease_until>clock_timestamp() AND ($3>$2 OR last_update_at<=clock_timestamp()-interval '6 days') AND EXISTS(SELECT 1 FROM gateway_inbox WHERE provider='telegram' AND account_id=$4 AND event_id=($3::bigint-1)::text) AND EXISTS(SELECT 1 FROM gateway_account_directory WHERE scope_id=$5 AND account_id=$4 AND account_json->'config'->>'receive_mode'='long_polling')`, l.BotID, expected, next, l.AccountID, l.ScopeID)
		if e != nil {
			return c.ErrUnavailable
		}
		if tag.RowsAffected() != 1 {
			return d.ErrCursor
		}
		return nil
	})
}

func (s *Store) RegistrationIntent(ctx context.Context, p *c.Permit, l d.Lease, address string) error {
	if address == "" || len(address) > 8192 {
		return c.ErrInvalid
	}
	return s.transaction(ctx, p, func(tx pgx.Tx) error {
		if e := s.check(ctx, tx, p, l); e != nil {
			return e
		}
		_, e := tx.Exec(ctx, `UPDATE gateway_telegram_receivers SET pending_url=$2 WHERE bot_id=$1`, l.BotID, address)
		if e != nil {
			return c.ErrUnavailable
		}
		return nil
	})
}
func (s *Store) ConfirmRegistration(ctx context.Context, p *c.Permit, l d.Lease, address string) error {
	return s.transaction(ctx, p, func(tx pgx.Tx) error {
		if e := s.check(ctx, tx, p, l); e != nil {
			return e
		}
		tag, e := tx.Exec(ctx, `UPDATE gateway_telegram_receivers SET managed_url=$2,pending_url='',managed_revision=$3 WHERE bot_id=$1 AND pending_url=$2`, l.BotID, address, l.Revision)
		if e != nil || tag.RowsAffected() != 1 {
			return d.ErrOwnership
		}
		return nil
	})
}

// AdoptLegacy needs an observed exact expected URL AND a durable legacy READY
// fact. Matching a user-supplied URL alone does not grant takeover authority.
func (s *Store) AdoptLegacy(ctx context.Context, p *c.Permit, l d.Lease, address string) (bool, error) {
	adopted := false
	e := s.transaction(ctx, p, func(tx pgx.Tx) error {
		if e := s.check(ctx, tx, p, l); e != nil {
			return e
		}
		tag, e := tx.Exec(ctx, `UPDATE gateway_telegram_receivers SET managed_url=$2 WHERE bot_id=$1 AND managed_url='' AND EXISTS(SELECT 1 FROM gateway_telegram_registration_attempts WHERE scope_id=$3 AND account_id=$4 AND result='READY')`, l.BotID, address, l.ScopeID, l.AccountID)
		if e != nil {
			return c.ErrUnavailable
		}
		adopted = tag.RowsAffected() == 1
		return nil
	})
	return adopted, e
}

func (s *Store) Defer(ctx context.Context, p *c.Permit, l d.Lease, delay time.Duration, reason string) error {
	if delay < 0 || delay > 24*time.Hour {
		return c.ErrInvalid
	}
	return s.transaction(ctx, p, func(tx pgx.Tx) error {
		if e := s.check(ctx, tx, p, l); e != nil {
			return e
		}
		_, e := tx.Exec(ctx, `UPDATE gateway_telegram_receivers SET next_due=clock_timestamp()+$2*interval '1 millisecond',last_reason=$3 WHERE bot_id=$1`, l.BotID, delay.Milliseconds(), reason)
		if e != nil {
			return c.ErrUnavailable
		}
		return nil
	})
}

// Ready is a diagnostic fact, never an authorization. Only Webhook readiness
// is shared across HTTP replicas; a standby poller does not claim to be owner.
func (s *Store) Ready(ctx context.Context, p *c.Permit) (bool, error) {
	if p.Check() != nil {
		return false, d.ErrOwnership
	}
	b := p.Binding()
	var ready bool
	e := s.pool.QueryRow(ctx, `SELECT managed_revision=$3 AND last_reason='NONE' AND next_due>clock_timestamp() FROM gateway_telegram_receivers WHERE scope_id=$1 AND account_id=$2`, b.ScopeID, b.AccountID, b.ConnectionRevision).Scan(&ready)
	if errors.Is(e, pgx.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, c.ErrUnavailable
	}
	return ready, nil
}

// VerifyPolling is the Admission transaction seam. It is invoked after the
// account guard, and holds the owner row through the durable receipt commit.
func (s *Store) VerifyPolling(ctx context.Context, tx pgx.Tx, scope, account, instance, boot string, epoch, revision int64) error {
	var yes bool
	e := tx.QueryRow(ctx, `SELECT r.instance_id=$2 AND r.instance_epoch=$3 AND r.owner_epoch=$4 AND r.connection_revision=$5 AND r.lease_until>clock_timestamp() AND d.enabled AND d.present AND d.connection_revision=$5 AND d.account_json->'config'->>'receive_mode'='long_polling' FROM gateway_telegram_receivers r JOIN gateway_account_directory d USING(scope_id,account_id) WHERE r.account_id=$1 AND r.scope_id=$6 FOR UPDATE OF r`, account, instance, boot, epoch, revision, scope).Scan(&yes)
	if e != nil || !yes {
		return d.ErrOwnership
	}
	return nil
}

func (s *Store) PollSucceeded(ctx context.Context, p *c.Permit, l d.Lease) error {
	return s.transaction(ctx, p, func(tx pgx.Tx) error {
		if e := s.check(ctx, tx, p, l); e != nil {
			return e
		}
		_, e := tx.Exec(ctx, `UPDATE gateway_telegram_receivers SET last_poll_at=clock_timestamp() WHERE bot_id=$1`, l.BotID)
		if e != nil {
			return c.ErrUnavailable
		}
		return nil
	})
}
func (s *Store) PollingReady(ctx context.Context, p *c.Permit) (bool, error) {
	if p.Check() != nil {
		return false, d.ErrOwnership
	}
	b := p.Binding()
	var yes bool
	e := s.pool.QueryRow(ctx, `SELECT connection_revision=$3 AND last_reason='NONE' AND lease_until>clock_timestamp() AND last_poll_at>clock_timestamp()-interval '30 seconds' FROM gateway_telegram_receivers WHERE scope_id=$1 AND account_id=$2`, b.ScopeID, b.AccountID, b.ConnectionRevision).Scan(&yes)
	if errors.Is(e, pgx.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, c.ErrUnavailable
	}
	return yes, nil
}
