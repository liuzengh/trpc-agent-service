package sessionturn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// Create atomically creates a session with version zero. It never overwrites
// an existing session, so concurrent CreateSession callers cannot both report
// success.
func (p *Postgres) Create(ctx context.Context, key session.Key, state session.StateMap) (*Snapshot, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	stateJSON, err := json.Marshal(cloneState(state))
	if err != nil {
		return nil, fmt.Errorf("sessionturn: encode initial session state: %w", err)
	}
	snapshot := &Snapshot{Key: key, State: cloneState(state), Events: []event.Event{}}
	err = p.pool.QueryRow(ctx, `
		INSERT INTO session_turn_sessions (app_name, user_id, session_id, state)
		VALUES ($1, $2, $3, $4::jsonb)
		ON CONFLICT (app_name, user_id, session_id) DO NOTHING
		RETURNING version, last_event_sequence, created_at, updated_at`,
		key.AppName, key.UserID, key.SessionID, stateJSON).Scan(
		&snapshot.Version,
		&snapshot.LastEventSequence,
		&snapshot.CreatedAt,
		&snapshot.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrSessionExists
	}
	if err != nil {
		return nil, fmt.Errorf("sessionturn: create session: %w", err)
	}
	return snapshot, nil
}

// Load reads one transactionally consistent session snapshot. A missing
// session is reported as (nil, nil).
func (p *Postgres) Load(ctx context.Context, key session.Key) (*Snapshot, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, fmt.Errorf("sessionturn: begin session read: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	snapshot, err := loadSnapshot(ctx, tx, key)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("sessionturn: commit session read: %w", err)
	}
	return snapshot, nil
}

// List reads all session snapshots for one user from one repeatable-read
// transaction. Results are ordered by updated time and session ID descending.
func (p *Postgres) List(ctx context.Context, userKey session.UserKey) ([]*Snapshot, error) {
	if err := userKey.CheckUserKey(); err != nil {
		return nil, err
	}
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, fmt.Errorf("sessionturn: begin session list: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT session_id, state, version, last_event_sequence, created_at, updated_at
		FROM session_turn_sessions
		WHERE app_name = $1 AND user_id = $2
		ORDER BY updated_at DESC, session_id DESC`, userKey.AppName, userKey.UserID)
	if err != nil {
		return nil, fmt.Errorf("sessionturn: query session list: %w", err)
	}

	type rowSnapshot struct {
		snapshot Snapshot
		state    []byte
	}
	metadata := make([]rowSnapshot, 0)
	for rows.Next() {
		var item rowSnapshot
		item.snapshot.Key.AppName = userKey.AppName
		item.snapshot.Key.UserID = userKey.UserID
		if err := rows.Scan(
			&item.snapshot.Key.SessionID,
			&item.state,
			&item.snapshot.Version,
			&item.snapshot.LastEventSequence,
			&item.snapshot.CreatedAt,
			&item.snapshot.UpdatedAt,
		); err != nil {
			rows.Close()
			return nil, fmt.Errorf("sessionturn: scan session list: %w", err)
		}
		metadata = append(metadata, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("sessionturn: iterate session list: %w", err)
	}
	rows.Close()

	result := make([]*Snapshot, 0, len(metadata))
	for i := range metadata {
		state, err := decodeState(metadata[i].state)
		if err != nil {
			return nil, err
		}
		metadata[i].snapshot.State = state
		events, err := readEvents(ctx, tx, metadata[i].snapshot.Key)
		if err != nil {
			return nil, err
		}
		metadata[i].snapshot.Events = events
		copy := metadata[i].snapshot
		result = append(result, &copy)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("sessionturn: commit session list: %w", err)
	}
	return result, nil
}

// Delete removes one session and all of its turns and events. It is
// idempotent for a missing session.
func (p *Postgres) Delete(ctx context.Context, key session.Key) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if _, err := p.pool.Exec(ctx, `
		DELETE FROM session_turn_sessions
		WHERE app_name = $1 AND user_id = $2 AND session_id = $3`,
		key.AppName, key.UserID, key.SessionID); err != nil {
		return fmt.Errorf("sessionturn: delete session: %w", err)
	}
	return nil
}

func loadSnapshot(ctx context.Context, tx pgx.Tx, key session.Key) (*Snapshot, error) {
	var (
		stateJSON []byte
		snapshot  = &Snapshot{Key: key}
	)
	err := tx.QueryRow(ctx, `
		SELECT state, version, last_event_sequence, created_at, updated_at
		FROM session_turn_sessions
		WHERE app_name = $1 AND user_id = $2 AND session_id = $3`,
		key.AppName, key.UserID, key.SessionID).Scan(
		&stateJSON,
		&snapshot.Version,
		&snapshot.LastEventSequence,
		&snapshot.CreatedAt,
		&snapshot.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("sessionturn: read session: %w", err)
	}
	state, err := decodeState(stateJSON)
	if err != nil {
		return nil, err
	}
	snapshot.State = state
	snapshot.Events, err = readEvents(ctx, tx, key)
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

// UpdateAppState atomically upserts all supplied application-scoped values.
// Keys are stored without the framework's app: projection prefix.
func (p *Postgres) UpdateAppState(ctx context.Context, appName string, state session.StateMap) error {
	if appName == "" {
		return session.ErrAppNameRequired
	}
	normalized, err := normalizeAppState(state)
	if err != nil {
		return err
	}
	return p.upsertState(ctx, func(tx pgx.Tx, key string, value []byte) error {
		_, err := tx.Exec(ctx, `
				INSERT INTO session_turn_app_states (app_name, key, value)
				VALUES ($1, $2, $3)
				ON CONFLICT (app_name, key) DO UPDATE
				SET value = EXCLUDED.value, updated_at = clock_timestamp()`,
			appName, key, value)
		return err
	}, normalized, "application")
}

// DeleteAppState deletes one application-scoped value. Both prefixed and raw
// keys are accepted.
func (p *Postgres) DeleteAppState(ctx context.Context, appName, key string) error {
	if appName == "" {
		return session.ErrAppNameRequired
	}
	key, err := normalizeAppStateKey(key)
	if err != nil {
		return err
	}
	if _, err := p.pool.Exec(ctx, `
		DELETE FROM session_turn_app_states WHERE app_name = $1 AND key = $2`, appName, key); err != nil {
		return fmt.Errorf("sessionturn: delete application state: %w", err)
	}
	return nil
}

// ListAppStates returns application-scoped values without app: prefixes.
func (p *Postgres) ListAppStates(ctx context.Context, appName string) (session.StateMap, error) {
	if appName == "" {
		return nil, session.ErrAppNameRequired
	}
	rows, err := p.pool.Query(ctx, `
		SELECT key, value FROM session_turn_app_states
		WHERE app_name = $1 ORDER BY key`, appName)
	if err != nil {
		return nil, fmt.Errorf("sessionturn: query application state: %w", err)
	}
	return readStateRows(rows, "application")
}

// UpdateUserState atomically upserts all supplied user-scoped values. Keys are
// stored without the framework's user: projection prefix.
func (p *Postgres) UpdateUserState(ctx context.Context, userKey session.UserKey, state session.StateMap) error {
	if err := userKey.CheckUserKey(); err != nil {
		return err
	}
	normalized, err := normalizeUserState(state)
	if err != nil {
		return err
	}
	return p.upsertState(ctx, func(tx pgx.Tx, key string, value []byte) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO session_turn_user_states (app_name, user_id, key, value)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (app_name, user_id, key) DO UPDATE
			SET value = EXCLUDED.value, updated_at = clock_timestamp()`,
			userKey.AppName, userKey.UserID, key, value)
		return err
	}, normalized, "user")
}

// DeleteUserState deletes one user-scoped value. Both prefixed and raw keys
// are accepted.
func (p *Postgres) DeleteUserState(ctx context.Context, userKey session.UserKey, key string) error {
	if err := userKey.CheckUserKey(); err != nil {
		return err
	}
	key, err := normalizeUserStateKey(key)
	if err != nil {
		return err
	}
	if _, err := p.pool.Exec(ctx, `
		DELETE FROM session_turn_user_states
		WHERE app_name = $1 AND user_id = $2 AND key = $3`,
		userKey.AppName, userKey.UserID, key); err != nil {
		return fmt.Errorf("sessionturn: delete user state: %w", err)
	}
	return nil
}

// ListUserStates returns user-scoped values without user: prefixes.
func (p *Postgres) ListUserStates(ctx context.Context, userKey session.UserKey) (session.StateMap, error) {
	if err := userKey.CheckUserKey(); err != nil {
		return nil, err
	}
	rows, err := p.pool.Query(ctx, `
		SELECT key, value FROM session_turn_user_states
		WHERE app_name = $1 AND user_id = $2 ORDER BY key`,
		userKey.AppName, userKey.UserID)
	if err != nil {
		return nil, fmt.Errorf("sessionturn: query user state: %w", err)
	}
	return readStateRows(rows, "user")
}

func (p *Postgres) upsertState(
	ctx context.Context,
	upsert func(pgx.Tx, string, []byte) error,
	state session.StateMap,
	scope string,
) error {
	if len(state) == 0 {
		return nil
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("sessionturn: begin %s state update: %w", scope, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for key, value := range state {
		if key == "" {
			return fmt.Errorf("%w: %s state key is required", ErrInvalidRequest, scope)
		}
		if err := upsert(tx, key, append([]byte(nil), value...)); err != nil {
			return fmt.Errorf("sessionturn: update %s state: %w", scope, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("sessionturn: commit %s state update: %w", scope, err)
	}
	return nil
}

type stateRows interface {
	Next() bool
	Scan(...any) error
	Err() error
	Close()
}

func readStateRows(rows stateRows, scope string) (session.StateMap, error) {
	defer rows.Close()
	state := make(session.StateMap)
	for rows.Next() {
		var key string
		var value []byte
		if err := rows.Scan(&key, &value); err != nil {
			return nil, fmt.Errorf("sessionturn: scan %s state: %w", scope, err)
		}
		state[key] = append([]byte(nil), value...)
		if value == nil {
			state[key] = nil
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sessionturn: iterate %s state: %w", scope, err)
	}
	return state, nil
}

func normalizeAppState(state session.StateMap) (session.StateMap, error) {
	return normalizeScopedState(state, normalizeAppStateKey, "application")
}

func normalizeUserState(state session.StateMap) (session.StateMap, error) {
	return normalizeScopedState(state, normalizeUserStateKey, "user")
}

func normalizeScopedState(
	state session.StateMap,
	normalizeKey func(string) (string, error),
	scope string,
) (session.StateMap, error) {
	normalized := make(session.StateMap, len(state))
	for key, value := range state {
		normalizedKey, err := normalizeKey(key)
		if err != nil {
			return nil, err
		}
		if _, duplicate := normalized[normalizedKey]; duplicate {
			return nil, fmt.Errorf(
				"%w: duplicate %s state key %q after prefix normalization",
				ErrInvalidRequest, scope, normalizedKey,
			)
		}
		normalized[normalizedKey] = append([]byte(nil), value...)
		if value == nil {
			normalized[normalizedKey] = nil
		}
	}
	return normalized, nil
}

func normalizeAppStateKey(key string) (string, error) {
	if strings.HasPrefix(key, session.StateUserPrefix) ||
		strings.HasPrefix(key, session.StateTempPrefix) {
		return "", fmt.Errorf("%w: application state key %q is not allowed", ErrInvalidRequest, key)
	}
	key = strings.TrimPrefix(key, session.StateAppPrefix)
	if key == "" {
		return "", fmt.Errorf("%w: application state key is required", ErrInvalidRequest)
	}
	return key, nil
}

func normalizeUserStateKey(key string) (string, error) {
	if strings.HasPrefix(key, session.StateAppPrefix) ||
		strings.HasPrefix(key, session.StateTempPrefix) {
		return "", fmt.Errorf("%w: user state key %q is not allowed", ErrInvalidRequest, key)
	}
	key = strings.TrimPrefix(key, session.StateUserPrefix)
	if key == "" {
		return "", fmt.Errorf("%w: user state key is required", ErrInvalidRequest)
	}
	return key, nil
}
