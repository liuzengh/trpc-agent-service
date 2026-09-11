package sessionturn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"

	"github.com/cyl6/trpc-agent-service/migrations"
	"github.com/cyl6/trpc-agent-service/trpcservice/dbbinding"
	"github.com/cyl6/trpc-agent-service/trpcservice/observability"
)

const (
	turnStatusActive    = "active"
	turnStatusCommitted = "committed"
	turnStatusAborted   = "aborted"
)

// Postgres implements strict turn persistence using one PostgreSQL transaction
// and a row lock on session_turn_sessions as the serialization point.
// The supplied pool remains owned by the caller.
type Postgres struct {
	pool *pgxpool.Pool
}

var _ Service = (*Postgres)(nil)
var _ interface{ DatabaseIdentity() string } = (*Postgres)(nil)

// NewPostgres creates a strict turn store over an existing connection pool.
func NewPostgres(pool *pgxpool.Pool) (*Postgres, error) {
	if pool == nil {
		return nil, fmt.Errorf("%w: postgres pool is nil", ErrInvalidRequest)
	}
	return &Postgres{pool: pool}, nil
}

// EnsureSchema explicitly applies the versioned runtime data-plane migration.
// It is retained for programmatic migration/test compatibility; business
// service startup uses VerifySchema and therefore never performs DDL.
func (p *Postgres) EnsureSchema(ctx context.Context) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("sessionturn: begin schema transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := migrations.ApplyAll(ctx, tx); err != nil {
		return fmt.Errorf("sessionturn: ensure schema: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("sessionturn: commit schema transaction: %w", err)
	}
	return nil
}

// VerifySchema is the read-only business-process startup gate. It fails when
// the independent migrator has not applied the exact migration embedded in
// this binary.
func (p *Postgres) VerifySchema(ctx context.Context) error {
	if p == nil || p.pool == nil {
		return fmt.Errorf("sessionturn: %w: postgres store is nil", ErrInvalidRequest)
	}
	if err := migrations.VerifyAll(ctx, p.pool); err != nil {
		return fmt.Errorf("sessionturn: verify schema: %w", err)
	}
	return nil
}

// DatabaseIdentity returns a credential-free identity for the configured
// PostgreSQL server, database, and search path. It allows Queue and Session to
// prove that a participant can safely join this pool's local transaction.
func (p *Postgres) DatabaseIdentity() string {
	if p == nil {
		return ""
	}
	return dbbinding.FromPool(p.pool)
}

// Begin atomically creates a session when needed, allocates a monotonically
// increasing fence for a new TurnID, and reads the matching session snapshot.
func (p *Postgres) Begin(ctx context.Context, req BeginRequest) (result *BeginResult, err error) {
	ctx, finish := observability.StartStorage(ctx, "session.begin", "postgres", req.Key.AppName, "")
	defer func() { finish(err) }()
	if err := validateKey(req.Key); err != nil {
		return nil, err
	}
	if err := validateTurnID(req.TurnID); err != nil {
		return nil, err
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("sessionturn: begin turn transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	key := req.Key
	if _, err := tx.Exec(ctx, `
		INSERT INTO session_turn_sessions (app_name, user_id, session_id)
		VALUES ($1, $2, $3)
		ON CONFLICT (app_name, user_id, session_id) DO NOTHING`,
		key.AppName, key.UserID, key.SessionID); err != nil {
		return nil, fmt.Errorf("sessionturn: create session: %w", err)
	}

	var (
		stateJSON         []byte
		version           int64
		fencingToken      int64
		lastEventSequence int64
		createdAt         pgtype.Timestamptz
		updatedAt         pgtype.Timestamptz
	)
	if err := tx.QueryRow(ctx, `
		SELECT state, version, fencing_token, last_event_sequence, created_at, updated_at
		FROM session_turn_sessions
		WHERE app_name = $1 AND user_id = $2 AND session_id = $3
		FOR UPDATE`, key.AppName, key.UserID, key.SessionID).Scan(
		&stateJSON, &version, &fencingToken, &lastEventSequence, &createdAt, &updatedAt,
	); err != nil {
		return nil, fmt.Errorf("sessionturn: lock session: %w", err)
	}

	turn, err := readTurn(ctx, tx, key, req.TurnID)
	switch {
	case err == nil && turn.status == turnStatusCommitted:
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("sessionturn: finish replay lookup: %w", err)
		}
		return &BeginResult{
			Handle: Handle{
				Key:             key,
				TurnID:          req.TurnID,
				ExpectedVersion: turn.expectedVersion,
				FencingToken:    turn.fencingToken,
			},
			Replay:   append([]byte(nil), turn.replay...),
			Replayed: true,
		}, nil
	case err == nil && turn.status == turnStatusAborted:
		return nil, ErrTurnAborted
	case err == nil && turn.status == turnStatusActive:
		if turn.expectedVersion != version {
			return nil, ErrVersionConflict
		}
		// A repeated Begin is a takeover, not a second copy of the same
		// capability. Mint a new token so an execution that was still running
		// with the previous handle can no longer commit.
		if fencingToken == int64(^uint64(0)>>1) {
			return nil, fmt.Errorf("%w: fencing token exhausted", ErrFenceLost)
		}
		fencingToken++
		if err := tx.QueryRow(ctx, `
			UPDATE session_turn_sessions
			SET fencing_token = $4, updated_at = clock_timestamp()
			WHERE app_name = $1 AND user_id = $2 AND session_id = $3
			RETURNING updated_at`,
			key.AppName, key.UserID, key.SessionID, fencingToken).Scan(&updatedAt); err != nil {
			return nil, fmt.Errorf("sessionturn: renew fencing token: %w", err)
		}
		tag, err := tx.Exec(ctx, `
			UPDATE session_turns
			SET fencing_token = $5
			WHERE app_name = $1 AND user_id = $2 AND session_id = $3 AND turn_id = $4
				AND status = 'active' AND expected_version = $6`,
			key.AppName, key.UserID, key.SessionID, req.TurnID, fencingToken, version)
		if err != nil {
			return nil, fmt.Errorf("sessionturn: take over active turn: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return nil, ErrFenceLost
		}
	case errors.Is(err, pgx.ErrNoRows):
		if fencingToken == int64(^uint64(0)>>1) {
			return nil, fmt.Errorf("%w: fencing token exhausted", ErrFenceLost)
		}
		fencingToken++
		if err := tx.QueryRow(ctx, `
			UPDATE session_turn_sessions
			SET fencing_token = $4, updated_at = clock_timestamp()
			WHERE app_name = $1 AND user_id = $2 AND session_id = $3
			RETURNING updated_at`,
			key.AppName, key.UserID, key.SessionID, fencingToken).Scan(&updatedAt); err != nil {
			return nil, fmt.Errorf("sessionturn: allocate fencing token: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO session_turns
				(app_name, user_id, session_id, turn_id, expected_version, fencing_token, status)
			VALUES ($1, $2, $3, $4, $5, $6, 'active')`,
			key.AppName, key.UserID, key.SessionID, req.TurnID, version, fencingToken); err != nil {
			return nil, fmt.Errorf("sessionturn: create turn: %w", err)
		}
	case err != nil:
		return nil, fmt.Errorf("sessionturn: read turn: %w", err)
	default:
		return nil, fmt.Errorf("%w: turn has status %q", ErrCorruptData, turn.status)
	}

	state, err := decodeState(stateJSON)
	if err != nil {
		return nil, err
	}
	events, err := readEvents(ctx, tx, key)
	if err != nil {
		return nil, err
	}
	handle := Handle{
		Key:             key,
		TurnID:          req.TurnID,
		ExpectedVersion: version,
		FencingToken:    fencingToken,
	}
	result = &BeginResult{
		Handle: handle,
		Snapshot: &Snapshot{
			Key:               key,
			State:             state,
			Events:            events,
			Version:           version,
			LastEventSequence: lastEventSequence,
			CreatedAt:         createdAt.Time,
			UpdatedAt:         updatedAt.Time,
		},
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("sessionturn: commit begin: %w", err)
	}
	return result, nil
}

// Commit atomically checks the handle, appends all events, replaces state,
// increments the session version, and stores replay data.
func (p *Postgres) Commit(ctx context.Context, req CommitRequest) (*CommitResult, error) {
	return p.CommitWithParticipant(ctx, req, nil)
}

// CommitWithParticipant extends Commit with one callback that runs on the
// same pgx transaction after the canonical replay has been selected. It is
// intentionally concrete rather than part of Service: only PostgreSQL-local
// participants can share this atomic boundary.
func (p *Postgres) CommitWithParticipant(
	ctx context.Context,
	req CommitRequest,
	participant CommitParticipant,
) (result *CommitResult, err error) {
	ctx, finish := observability.StartStorage(ctx, "session.commit", "postgres", req.Handle.Key.AppName, "")
	defer func() { finish(err) }()
	if err := validateHandle(req.Handle); err != nil {
		return nil, err
	}
	stateJSON, err := json.Marshal(cloneState(req.State))
	if err != nil {
		return nil, fmt.Errorf("sessionturn: encode state: %w", err)
	}
	eventJSON := make([][]byte, len(req.Events))
	for i := range req.Events {
		eventJSON[i], err = json.Marshal(req.Events[i])
		if err != nil {
			return nil, fmt.Errorf("sessionturn: encode event %d: %w", i, err)
		}
	}
	// pgx encodes a nil []byte as SQL NULL. replay is NOT NULL because an empty
	// payload is a valid canonical replay value, so always bind a non-nil slice.
	replayValue := append([]byte{}, req.Replay...)

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("sessionturn: begin commit transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	key := req.Handle.Key
	var version, fencingToken, lastEventSequence int64
	if err := tx.QueryRow(ctx, `
		SELECT version, fencing_token, last_event_sequence
		FROM session_turn_sessions
		WHERE app_name = $1 AND user_id = $2 AND session_id = $3
		FOR UPDATE`, key.AppName, key.UserID, key.SessionID).Scan(
		&version, &fencingToken, &lastEventSequence,
	); errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrTurnNotFound
	} else if err != nil {
		return nil, fmt.Errorf("sessionturn: lock session for commit: %w", err)
	}

	turn, err := readTurn(ctx, tx, key, req.Handle.TurnID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrTurnNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("sessionturn: read turn for commit: %w", err)
	}
	switch turn.status {
	case turnStatusCommitted:
		if !turn.committedVersion.Valid {
			return nil, fmt.Errorf("%w: committed turn has no version", ErrCorruptData)
		}
		canonicalReplay := append([]byte(nil), turn.replay...)
		if participant != nil {
			if err := participant(ctx, tx, canonicalReplay); err != nil {
				return nil, fmt.Errorf("sessionturn: commit participant: %w", err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("sessionturn: finish commit replay lookup: %w", err)
		}
		return &CommitResult{
			Version:  turn.committedVersion.Int64,
			Replay:   canonicalReplay,
			Replayed: true,
		}, nil
	case turnStatusAborted:
		return nil, ErrTurnAborted
	case turnStatusActive:
		// Continue with concurrency validation below.
	default:
		return nil, fmt.Errorf("%w: turn has status %q", ErrCorruptData, turn.status)
	}
	if turn.fencingToken != req.Handle.FencingToken || fencingToken != req.Handle.FencingToken {
		return nil, ErrFenceLost
	}
	if turn.expectedVersion != req.Handle.ExpectedVersion || version != req.Handle.ExpectedVersion {
		return nil, ErrVersionConflict
	}

	for i, encoded := range eventJSON {
		sequence := lastEventSequence + int64(i) + 1
		if _, err := tx.Exec(ctx, `
			INSERT INTO session_turn_events
				(app_name, user_id, session_id, sequence, turn_id, event_id, event_data)
			VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)`,
			key.AppName, key.UserID, key.SessionID, sequence,
			req.Handle.TurnID, req.Events[i].ID, encoded); err != nil {
			return nil, fmt.Errorf("sessionturn: append event %d: %w", i, err)
		}
	}

	newVersion := version + 1
	newLastSequence := lastEventSequence + int64(len(req.Events))
	tag, err := tx.Exec(ctx, `
		UPDATE session_turn_sessions
		SET state = $1::jsonb,
			version = $2,
			last_event_sequence = $3,
			updated_at = clock_timestamp()
		WHERE app_name = $4 AND user_id = $5 AND session_id = $6
			AND version = $7 AND fencing_token = $8`,
		stateJSON, newVersion, newLastSequence,
		key.AppName, key.UserID, key.SessionID, version, fencingToken)
	if err != nil {
		return nil, fmt.Errorf("sessionturn: update session: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, ErrFenceLost
	}

	tag, err = tx.Exec(ctx, `
		UPDATE session_turns
		SET status = 'committed', replay = $1, committed_version = $2,
			completed_at = clock_timestamp()
		WHERE app_name = $3 AND user_id = $4 AND session_id = $5 AND turn_id = $6
			AND status = 'active' AND expected_version = $7 AND fencing_token = $8`,
		replayValue, newVersion,
		key.AppName, key.UserID, key.SessionID, req.Handle.TurnID,
		version, fencingToken)
	if err != nil {
		return nil, fmt.Errorf("sessionturn: commit turn row: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, ErrFenceLost
	}
	if participant != nil {
		if err := participant(ctx, tx, replayValue); err != nil {
			return nil, fmt.Errorf("sessionturn: commit participant: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("sessionturn: commit turn transaction: %w", err)
	}
	return &CommitResult{
		Version: newVersion,
		Replay:  append([]byte(nil), req.Replay...),
	}, nil
}

// Abort marks an active turn as aborted without changing session state or
// version. It remains valid even after a newer turn superseded its fence, but
// only the token originally issued for that TurnID may perform the abort.
func (p *Postgres) Abort(ctx context.Context, req AbortRequest) error {
	if err := validateKey(req.Key); err != nil {
		return err
	}
	if err := validateTurnID(req.TurnID); err != nil {
		return err
	}
	if req.FencingToken <= 0 {
		return fmt.Errorf("%w: fencing token must be positive", ErrInvalidRequest)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("sessionturn: begin abort transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	key := req.Key
	var ignored int64
	if err := tx.QueryRow(ctx, `
		SELECT fencing_token
		FROM session_turn_sessions
		WHERE app_name = $1 AND user_id = $2 AND session_id = $3
		FOR UPDATE`, key.AppName, key.UserID, key.SessionID).Scan(&ignored); errors.Is(err, pgx.ErrNoRows) {
		return ErrTurnNotFound
	} else if err != nil {
		return fmt.Errorf("sessionturn: lock session for abort: %w", err)
	}

	turn, err := readTurn(ctx, tx, key, req.TurnID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrTurnNotFound
	}
	if err != nil {
		return fmt.Errorf("sessionturn: read turn for abort: %w", err)
	}
	if turn.fencingToken != req.FencingToken {
		return ErrFenceLost
	}
	switch turn.status {
	case turnStatusCommitted:
		return ErrTurnCommitted
	case turnStatusAborted:
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("sessionturn: finish duplicate abort: %w", err)
		}
		return nil
	case turnStatusActive:
		// Continue with the transition below.
	default:
		return fmt.Errorf("%w: turn has status %q", ErrCorruptData, turn.status)
	}

	tag, err := tx.Exec(ctx, `
		UPDATE session_turns
		SET status = 'aborted', completed_at = clock_timestamp()
		WHERE app_name = $1 AND user_id = $2 AND session_id = $3 AND turn_id = $4
			AND status = 'active' AND fencing_token = $5`,
		key.AppName, key.UserID, key.SessionID, req.TurnID, req.FencingToken)
	if err != nil {
		return fmt.Errorf("sessionturn: abort turn: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrFenceLost
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("sessionturn: commit abort transaction: %w", err)
	}
	return nil
}

type persistedTurn struct {
	expectedVersion  int64
	fencingToken     int64
	status           string
	replay           []byte
	committedVersion pgtype.Int8
}

func readTurn(ctx context.Context, tx pgx.Tx, key session.Key, turnID string) (persistedTurn, error) {
	var turn persistedTurn
	err := tx.QueryRow(ctx, `
		SELECT expected_version, fencing_token, status, replay, committed_version
		FROM session_turns
		WHERE app_name = $1 AND user_id = $2 AND session_id = $3 AND turn_id = $4`,
		key.AppName, key.UserID, key.SessionID, turnID).Scan(
		&turn.expectedVersion, &turn.fencingToken, &turn.status,
		&turn.replay, &turn.committedVersion,
	)
	return turn, err
}

func readEvents(ctx context.Context, tx pgx.Tx, key session.Key) ([]event.Event, error) {
	rows, err := tx.Query(ctx, `
		SELECT event_data
		FROM session_turn_events
		WHERE app_name = $1 AND user_id = $2 AND session_id = $3
		ORDER BY sequence`, key.AppName, key.UserID, key.SessionID)
	if err != nil {
		return nil, fmt.Errorf("sessionturn: query events: %w", err)
	}
	defer rows.Close()

	events := make([]event.Event, 0)
	for rows.Next() {
		var encoded []byte
		if err := rows.Scan(&encoded); err != nil {
			return nil, fmt.Errorf("sessionturn: read event: %w", err)
		}
		var stored event.Event
		if err := json.Unmarshal(encoded, &stored); err != nil {
			return nil, fmt.Errorf("%w: decode event: %v", ErrCorruptData, err)
		}
		events = append(events, stored)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sessionturn: iterate events: %w", err)
	}
	return events, nil
}

func decodeState(encoded []byte) (session.StateMap, error) {
	if len(encoded) == 0 || string(encoded) == "null" {
		return session.StateMap{}, nil
	}
	var state session.StateMap
	if err := json.Unmarshal(encoded, &state); err != nil {
		return nil, fmt.Errorf("%w: decode state: %v", ErrCorruptData, err)
	}
	if state == nil {
		state = session.StateMap{}
	}
	return state, nil
}

func validateHandle(handle Handle) error {
	if err := validateKey(handle.Key); err != nil {
		return err
	}
	if err := validateTurnID(handle.TurnID); err != nil {
		return err
	}
	if handle.ExpectedVersion < 0 {
		return fmt.Errorf("%w: expected version must be non-negative", ErrInvalidRequest)
	}
	if handle.FencingToken <= 0 {
		return fmt.Errorf("%w: fencing token must be positive", ErrInvalidRequest)
	}
	return nil
}
