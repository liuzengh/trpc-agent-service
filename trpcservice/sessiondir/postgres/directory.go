package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	// sessionKeyPredicate names every field of the key, tenant_id included.
	// There is no read path in this package that omits it: a query matching on
	// session id alone would be one typo away from returning another tenant's
	// pin.
	sessionKeyPredicate = `tenant_id = $1 AND agent_app_id = $2 AND principal_id = $3
		AND session_id = $4 AND epoch = $5`

	selectPinSQL = `SELECT pinned_revision_id FROM sessions WHERE ` + sessionKeyPredicate

	// insertPinSQL is the linearization point of EnsurePin.
	//
	// The winner is decided by the primary-key index, inside PostgreSQL, in one
	// statement. Whichever transaction inserts first owns the pin; every other
	// one conflicts and inserts nothing. There is no window between a check and
	// a write for a second caller to slip into, and no lock in this process is
	// involved, so the result is the same whether the callers are goroutines,
	// pools or hosts.
	//
	// DO NOTHING rather than DO UPDATE is what makes the pin immutable: an
	// existing row is never rewritten, so a later candidate cannot move a
	// session that has already started, however it arrives.
	//
	// The conflict target is the constraint by name rather than a bare ON
	// CONFLICT. A bare one would silently absorb every future unique or
	// exclusion violation on this table as "someone else got here first",
	// which is only true of this key.
	//
	// RETURNING is what distinguishes the two outcomes: exactly one row when
	// this statement inserted, none when it conflicted.
	insertPinSQL = `INSERT INTO sessions (
			tenant_id, agent_app_id, principal_id, session_id, epoch,
			pinned_revision_id, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT ON CONSTRAINT sessions_pkey DO NOTHING
		RETURNING pinned_revision_id`
)

// ensurePinAttempts bounds the loop in EnsurePin. Only a pin deleted out of
// band between this call's insert and its read costs an extra attempt, and the
// deletion has to happen again to cost another, so anything beyond a couple of
// rounds is a store being actively emptied rather than a race to ride out.
const ensurePinAttempts = 3

// GetPin reports the revision an existing session is pinned to.
//
// A session with no pin is not an error: it is a conversation that has not run
// yet, and the caller answers it by resolving a revision and calling EnsurePin.
// Every failure below that is the store's, and none of them may come back as a
// tenant sentinel — a caller that treated an unreachable database as "not
// pinned" would let the next run adopt whatever revision is current.
func (d *Directory) GetPin(ctx context.Context, key sessiondir.Key) (string, bool, error) {
	if err := d.validateCall(ctx); err != nil {
		return "", false, err
	}
	if err := key.Validate(); err != nil {
		return "", false, err
	}
	return selectPin(ctx, d.pool, key)
}

// EnsurePin returns the revision this session is pinned to, storing
// candidateRevisionID only when the session has no pin yet.
//
// Concurrent first runs of one session each arrive with their own candidate.
// Exactly one may be stored, every caller has to be told which one, and the
// answer has to survive the process: this is the guarantee the whole package
// exists for, and the reason the decision is made by the primary-key index
// rather than anywhere in Go.
//
// The candidate is validated for shape only. Whether it names a revision that
// exists, belongs to this app, or is published is the caller's question, and
// the caller has already answered it before reaching here; see the note on
// foreign keys in migrate.go.
func (d *Directory) EnsurePin(
	ctx context.Context,
	key sessiondir.Key,
	candidateRevisionID string,
) (string, error) {
	if err := d.validateCall(ctx); err != nil {
		return "", err
	}
	if err := key.Validate(); err != nil {
		return "", err
	}
	if err := tenant.ValidateResourceID("revision id", candidateRevisionID); err != nil {
		return "", err
	}

	for attempt := 0; attempt < ensurePinAttempts; attempt++ {
		winner, found, err := d.ensurePinOnce(ctx, key, candidateRevisionID)
		if err != nil {
			return "", err
		}
		if found {
			return winner, nil
		}
		// The insert conflicted with a committed row and the read that followed
		// found nothing, so the pin was deleted in between. Nothing in this
		// package deletes, so this is an out-of-band write; the session simply
		// has no pin again, and another attempt establishes one the same way.
	}
	return "", storageFailure("ensure session pin", fmt.Errorf(
		"the pin was removed between the insert and the read on all %d attempts",
		ensurePinAttempts))
}

// ensurePinOnce runs one attempt. found is false only when the row this call
// conflicted with had disappeared by the time it was read back.
//
// The two statements run in one READ COMMITTED transaction, and both halves of
// that matter.
//
// READ COMMITTED gives each statement its own snapshot. The insert returns no
// rows only once the transaction it conflicted with has committed — PostgreSQL
// waits for an in-flight one and retries the check — so the read that follows
// takes a snapshot that already includes the winner.
//
// Under REPEATABLE READ this does not degrade into a stale read; it stops. The
// losing insert waits for the winner and then aborts the whole transaction with
// "could not serialize access due to concurrent update" (SQLSTATE 40001),
// because the row it would have to skip is invisible to its snapshot. Skipping
// a row for a conflict it cannot see is a Read Committed behaviour and no
// stricter level offers it. That arrives as an error rather than as a
// not-found, so it never reaches the retry loop below: every losing caller in a
// contended first run would be handed ErrStorage where it should be handed the
// winner. The isolation level is therefore set explicitly rather than inherited
// from the server's default_transaction_isolation, which an operator is free to
// change, and TestIntegrationEnsurePinAgreesUnderRepeatableReadDefault runs the
// contended case against pools that have changed it.
//
// For the same reason the read is a second statement rather than a branch of a
// single CTE. Every arm of one statement shares one snapshot, so the familiar
// "INSERT ... ON CONFLICT DO NOTHING RETURNING ... UNION ALL SELECT ..." form
// returns nothing at all in exactly the contended case this has to get right.
//
// The explicit transaction also keeps both statements on one connection, so
// the reasoning above is about two snapshots of one session rather than about
// whatever two pool connections happened to be doing.
func (d *Directory) ensurePinOnce(
	ctx context.Context,
	key sessiondir.Key,
	candidateRevisionID string,
) (string, bool, error) {
	tx, err := d.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return "", false, storageError(ctx, "begin ensure session pin", err)
	}
	// A no-op once the transaction has committed, and the whole cleanup on
	// every path that does not.
	defer func() { _ = tx.Rollback(ctx) }()

	arguments := append(keyArguments(key), candidateRevisionID, storedNow())
	var winner string
	err = tx.QueryRow(ctx, insertPinSQL, arguments...).Scan(&winner)
	switch {
	case err == nil:
		// This call inserted, so its candidate is the pin. It is not the winner
		// until the commit makes it visible to everyone else.
		//
		// RETURNING reports what the row actually holds rather than what was
		// bound, so this is where a trigger that rewrote the value would show
		// up. A pin that would be refused when read back must not be committed.
		if invalid := checkStoredPin("ensure session pin", winner); invalid != nil {
			return "", false, invalid
		}
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return "", false, storageError(ctx, "commit session pin", commitErr)
		}
		return winner, true, nil
	case errors.Is(err, pgx.ErrNoRows):
		// Another caller got there first and has committed. Its revision is the
		// pin; this candidate is discarded, unwritten.
	default:
		return "", false, storageError(ctx, "ensure session pin", err)
	}

	pinned, found, err := selectPin(ctx, tx, key)
	if err != nil {
		return "", false, err
	}
	// Nothing was written on this path, so the deferred rollback ends the
	// transaction and there is no commit to check.
	return pinned, found, nil
}

func selectPin(
	ctx context.Context,
	q queryRower,
	key sessiondir.Key,
) (string, bool, error) {
	var pinned string
	if err := q.QueryRow(ctx, selectPinSQL, keyArguments(key)...).Scan(&pinned); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", false, nil
		}
		return "", false, storageError(ctx, "get session pin", err)
	}
	// A row exists but holds something this package could not have written. It
	// is a storage fault, not a missing pin: reporting it as "not pinned" would
	// let the session adopt a fresh revision, which is the same fail-open as
	// passing the value on. See checkStoredPin.
	if err := checkStoredPin("get session pin", pinned); err != nil {
		return "", false, err
	}
	return pinned, true, nil
}
