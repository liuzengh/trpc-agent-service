package sessionturn

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

var ErrMigrationTargetConflict = errors.New("sessionturn: migration target contains different data")

// MigrationSnapshot is a complete Redis session object plus its shared state
// overlays. ImportFrozenSnapshot accepts it only while the source is frozen
// and the SQL target is not serving traffic; callers enforce that gate.
type MigrationSnapshot struct {
	Key       session.Key
	State     session.StateMap
	Events    []event.Event
	AppState  session.StateMap
	UserState session.StateMap
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ImportFrozenSnapshot imports one complete snapshot without replaying events
// through application code. It is idempotent for identical content and fails
// closed if any session or shared overlay already contains different data.
// This protects a mistakenly active target from being overwritten.
func (p *Postgres) ImportFrozenSnapshot(ctx context.Context, snapshot MigrationSnapshot) error {
	if p == nil || p.pool == nil {
		return fmt.Errorf("%w: postgres store is nil", ErrInvalidRequest)
	}
	if err := validateKey(snapshot.Key); err != nil {
		return err
	}
	if snapshot.CreatedAt.IsZero() {
		snapshot.CreatedAt = time.Now().UTC()
	}
	if snapshot.UpdatedAt.IsZero() {
		snapshot.UpdatedAt = snapshot.CreatedAt
	}
	if snapshot.UpdatedAt.Before(snapshot.CreatedAt) {
		return fmt.Errorf("%w: migration timestamp order is invalid", ErrInvalidRequest)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("sessionturn: begin migration import: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	key := snapshot.Key
	// Shared app/user overlays require a broader lock than the session row.
	// Without it, two executor replicas importing different sessions could
	// both observe an empty overlay and race on the first insert.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('session-migration-app'), hashtext($1))`,
		key.AppName); err != nil {
		return fmt.Errorf("sessionturn: lock migration application: %w", err)
	}
	// PostgreSQL text values cannot contain NUL bytes. Use a length-prefixed
	// pair instead of a NUL separator so distinct user/session combinations
	// still map to distinct advisory-lock keys without invalid UTF-8 input.
	targetLockKey := fmt.Sprintf("%d:%s%d:%s", len(key.UserID), key.UserID, len(key.SessionID), key.SessionID)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))`,
		key.AppName, targetLockKey); err != nil {
		return fmt.Errorf("sessionturn: lock migration target: %w", err)
	}
	if err := ensureFrozenOverlay(ctx, tx, `
		SELECT key, value FROM session_turn_app_states WHERE app_name = $1 ORDER BY key`,
		[]any{key.AppName}, snapshot.AppState, func(name string, value []byte) error {
			_, err := tx.Exec(ctx, `INSERT INTO session_turn_app_states (app_name, key, value) VALUES ($1, $2, $3)`,
				key.AppName, name, value)
			return err
		}); err != nil {
		return fmt.Errorf("sessionturn: import application state: %w", err)
	}
	if err := ensureFrozenOverlay(ctx, tx, `
		SELECT key, value FROM session_turn_user_states WHERE app_name = $1 AND user_id = $2 ORDER BY key`,
		[]any{key.AppName, key.UserID}, snapshot.UserState, func(name string, value []byte) error {
			_, err := tx.Exec(ctx, `INSERT INTO session_turn_user_states (app_name, user_id, key, value) VALUES ($1, $2, $3, $4)`,
				key.AppName, key.UserID, name, value)
			return err
		}); err != nil {
		return fmt.Errorf("sessionturn: import user state: %w", err)
	}

	existing, err := loadSnapshot(ctx, tx, key)
	if err != nil {
		return err
	}
	if existing != nil {
		equal, err := migrationContentEqual(existing.State, existing.Events, snapshot.State, snapshot.Events)
		if err != nil {
			return err
		}
		if !equal {
			return ErrMigrationTargetConflict
		}
		return tx.Commit(ctx)
	}

	stateJSON, err := json.Marshal(cloneState(snapshot.State))
	if err != nil {
		return fmt.Errorf("sessionturn: encode migrated state: %w", err)
	}
	const importedVersion int64 = 1
	if _, err := tx.Exec(ctx, `
		INSERT INTO session_turn_sessions
			(app_name, user_id, session_id, state, version, fencing_token,
			 last_event_sequence, created_at, updated_at)
		VALUES ($1, $2, $3, $4::jsonb, $5, 1, $6, $7, $8)`,
		key.AppName, key.UserID, key.SessionID, stateJSON, importedVersion,
		len(snapshot.Events), snapshot.CreatedAt, snapshot.UpdatedAt); err != nil {
		return fmt.Errorf("sessionturn: insert migrated session: %w", err)
	}
	turnID := migrationTurnID(key)
	if _, err := tx.Exec(ctx, `
		INSERT INTO session_turns
			(app_name, user_id, session_id, turn_id, expected_version, fencing_token,
			 status, replay, committed_version, started_at, completed_at)
		VALUES ($1, $2, $3, $4, 0, 1, 'committed', '\x'::bytea, $5, $6, $7)`,
		key.AppName, key.UserID, key.SessionID, turnID, importedVersion,
		snapshot.CreatedAt, snapshot.UpdatedAt); err != nil {
		return fmt.Errorf("sessionturn: insert migrated turn: %w", err)
	}
	for i := range snapshot.Events {
		encoded, err := json.Marshal(snapshot.Events[i])
		if err != nil {
			return fmt.Errorf("sessionturn: encode migrated event: %w", err)
		}
		createdAt := snapshot.Events[i].Timestamp
		if createdAt.IsZero() {
			createdAt = snapshot.UpdatedAt
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO session_turn_events
				(app_name, user_id, session_id, sequence, turn_id, event_id, event_data, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)`,
			key.AppName, key.UserID, key.SessionID, i+1, turnID,
			snapshot.Events[i].ID, encoded, createdAt); err != nil {
			return fmt.Errorf("sessionturn: insert migrated event: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("sessionturn: commit migration import: %w", err)
	}
	return nil
}

func ensureFrozenOverlay(
	ctx context.Context,
	tx pgx.Tx,
	query string,
	args []any,
	want session.StateMap,
	insert func(string, []byte) error,
) error {
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	got, err := readStateRows(rows, "migration")
	if err != nil {
		return err
	}
	if len(got) > 0 {
		if !stateMapsEqual(got, want) {
			return ErrMigrationTargetConflict
		}
		return nil
	}
	for name, value := range want {
		if name == "" {
			return fmt.Errorf("%w: migration state key is required", ErrInvalidRequest)
		}
		if err := insert(name, append([]byte(nil), value...)); err != nil {
			return err
		}
	}
	return nil
}

func stateMapsEqual(a, b session.StateMap) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		other, ok := b[key]
		if !ok || !bytes.Equal(value, other) {
			return false
		}
	}
	return true
}

func migrationContentEqual(aState session.StateMap, aEvents []event.Event, bState session.StateMap, bEvents []event.Event) (bool, error) {
	if !stateMapsEqual(aState, bState) {
		return false, nil
	}
	aJSON, err := json.Marshal(aEvents)
	if err != nil {
		return false, fmt.Errorf("sessionturn: encode existing migration events: %w", err)
	}
	bJSON, err := json.Marshal(bEvents)
	if err != nil {
		return false, fmt.Errorf("sessionturn: encode source migration events: %w", err)
	}
	return bytes.Equal(aJSON, bJSON), nil
}

func migrationTurnID(key session.Key) string {
	sum := sha256.Sum256([]byte(key.AppName + "\x00" + key.UserID + "\x00" + key.SessionID))
	return "migration_" + hex.EncodeToString(sum[:])
}
