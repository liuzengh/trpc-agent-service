// Package dlqstore persists dead-lettered inbound messages (DDL 012). The
// bus appends to the Redis DLQ stream for operability; this store is the
// durable record and the replay source for the admin API. Rows are never
// deleted — replay only stamps replayed_at — so the table doubles as the
// compliance trail of every message the platform gave up on.
package dlqstore

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/sqlutil"
)

// ErrNotFound is returned when a dead-letter row does not exist.
var ErrNotFound = errors.New("dlqstore: not found")

// Entry is one dead-lettered message as persisted. Payload is the
// JSON-encoded bus envelope; replay re-publishes exactly these bytes.
type Entry struct {
	ID          int64      `json:"id"`
	MessageID   string     `json:"message_id"`
	StreamEntry string     `json:"stream_entry"`
	TenantID    string     `json:"tenant_id"`
	AgentID     string     `json:"agent_id"`
	SessionID   string     `json:"session_id"`
	Channel     string     `json:"channel"`
	UserID      string     `json:"user_id"`
	TraceID     string     `json:"trace_id"`
	Payload     string     `json:"payload"`
	FailReason  string     `json:"fail_reason"`
	Attempts    int64      `json:"attempts"`
	CreatedAt   time.Time  `json:"created_at"`
	ReplayedAt  *time.Time `json:"replayed_at,omitempty"`
}

// Store is the MySQL-backed dead-letter store.
type Store struct{ db *sql.DB }

// NewStore returns a store over the given pool. The schema is created by
// deployments/mysql/init/012_dead_letters.sql.
func NewStore(db *sql.DB) *Store { return &Store{db: db} }

// Record persists one dead letter. The bus sink calls this best-effort. Rows
// are append-only: a message that is replayed and fails out again gets a NEW
// row (fresh stream entry, fresh attempts), so the table is a faithful event
// log rather than a mutable per-message state.
func (s *Store) Record(ctx context.Context, e Entry) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO dead_letters
			(message_id, stream_entry, tenant_id, agent_id, session_id, channel,
			 user_id, trace_id, payload, fail_reason, attempts)
		VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
		e.MessageID, e.StreamEntry, e.TenantID, e.AgentID, e.SessionID, e.Channel,
		e.UserID, e.TraceID, e.Payload, e.FailReason, e.Attempts)
	return err
}

const entryCols = `id, message_id, stream_entry, tenant_id, agent_id, session_id,
	channel, user_id, trace_id, payload, fail_reason, attempts, created_at, replayed_at`

// List returns dead letters newest first, optionally filtered by tenant.
func (s *Store) List(ctx context.Context, tenantID string, limit int) ([]*Entry, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	q := `SELECT ` + entryCols + ` FROM dead_letters`
	args := []any{}
	if tenantID != "" {
		q += ` WHERE tenant_id = ?`
		args = append(args, tenantID)
	}
	q += ` ORDER BY id DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Entry
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Get returns one dead letter by row id.
func (s *Store) Get(ctx context.Context, id int64) (*Entry, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+entryCols+` FROM dead_letters WHERE id = ?`, id)
	e, err := scanEntry(row)
	if err != nil {
		return nil, sqlutil.NoRows(err, ErrNotFound)
	}
	return e, nil
}

// MarkReplayed stamps the replay time; a second replay of the same row is
// rejected so the same failure is never silently re-injected twice.
func (s *Store) MarkReplayed(ctx context.Context, id int64) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE dead_letters SET replayed_at = NOW() WHERE id = ? AND replayed_at IS NULL`, id)
	if err != nil {
		return err
	}
	return sqlutil.RowsAffected(res, ErrNotFound, "dlq")
}

func scanEntry(r sqlutil.RowScanner) (*Entry, error) {
	var e Entry
	var replayed sql.NullTime
	if err := r.Scan(&e.ID, &e.MessageID, &e.StreamEntry, &e.TenantID, &e.AgentID,
		&e.SessionID, &e.Channel, &e.UserID, &e.TraceID, &e.Payload,
		&e.FailReason, &e.Attempts, &e.CreatedAt, &replayed); err != nil {
		return nil, err
	}
	if replayed.Valid {
		t := replayed.Time
		e.ReplayedAt = &t
	}
	return &e, nil
}
