// Package ledgerstore persists the business conversation ledger in MySQL
// (chat_sessions + chat_messages, init 007). It implements domain/chat.Ledger.
package ledgerstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/chat"
)

// MySQLLedger persists the business conversation ledger in MySQL
// (chat_sessions + chat_messages, init 007).
type MySQLLedger struct {
	db *sql.DB
}

// NewMySQLLedger returns a ledger over an existing connection.
func NewMySQLLedger(db *sql.DB) *MySQLLedger {
	return &MySQLLedger{db: db}
}

// RecordTurn upserts the session row and inserts the USER + ASSISTANT rows of
// one turn in one transaction. INSERT IGNORE keeps redelivery idempotent by
// message_id.
func (l *MySQLLedger) RecordTurn(ctx context.Context, t chat.Turn) error {
	if t.SessionID == "" || t.UserMsgID == "" || t.TurnID == "" {
		return fmt.Errorf("chat: record turn requires session, user message and turn ids")
	}
	if t.TurnTS == 0 {
		t.TurnTS = time.Now().UnixMilli()
	}
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("chat: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO chat_sessions (session_id, tenant_id, agent_id, member_id, channel, last_message_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE last_message_at = VALUES(last_message_at)`,
		t.SessionID, t.TenantID, t.AgentID, t.MemberID, t.Channel, time.Now().UTC()); err != nil {
		return fmt.Errorf("chat: upsert session: %w", err)
	}
	// USER row
	if _, err := tx.ExecContext(ctx, `
		INSERT IGNORE INTO chat_messages
			(message_id, session_id, agent_id, member_id, turn_id, turn_timestamp, role, content, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'SUCCESS')`,
		t.UserMsgID, t.SessionID, t.AgentID, t.MemberID, t.TurnID, t.TurnTS, chat.RoleUser, t.UserText); err != nil {
		return fmt.Errorf("chat: insert user row: %w", err)
	}
	// ASSISTANT row (a reply without a business id gets an empty message_id
	// and is skipped; the worker always supplies one)
	if t.ReplyMsgID != "" {
		if _, err := tx.ExecContext(ctx, `
			INSERT IGNORE INTO chat_messages
				(message_id, session_id, agent_id, member_id, turn_id, turn_timestamp, role, content, status)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'SUCCESS')`,
			t.ReplyMsgID, t.SessionID, t.AgentID, t.MemberID, t.TurnID, t.TurnTS, chat.RoleAssistant, t.ReplyText); err != nil {
			return fmt.Errorf("chat: insert assistant row: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("chat: commit: %w", err)
	}
	return nil
}

// Sessions lists sessions ordered by last activity, newest first.
func (l *MySQLLedger) Sessions(ctx context.Context, q chat.SessionQuery) ([]chat.Session, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 200 {
		limit = 200
	}
	query := `SELECT session_id, tenant_id, agent_id, member_id, channel, last_message_at, updated_at
	          FROM chat_sessions WHERE 1=1`
	args := []any{}
	if q.TenantID != "" {
		query += ` AND tenant_id = ?`
		args = append(args, q.TenantID)
	}
	if q.MemberID != "" {
		query += ` AND member_id = ?`
		args = append(args, q.MemberID)
	}
	query += ` ORDER BY last_message_at DESC LIMIT ?`
	args = append(args, limit)

	rows, err := l.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("chat: list sessions: %w", err)
	}
	defer rows.Close()
	out := make([]chat.Session, 0)
	for rows.Next() {
		var s chat.Session
		if err := rows.Scan(&s.SessionID, &s.TenantID, &s.AgentID, &s.MemberID, &s.Channel,
			&s.LastMessageAt, &s.UpdatedAt); err != nil {
			return nil, fmt.Errorf("chat: scan session: %w", err)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// Session loads one session by id.
func (l *MySQLLedger) Session(ctx context.Context, sessionID string) (*chat.Session, error) {
	var s chat.Session
	err := l.db.QueryRowContext(ctx,
		`SELECT session_id, tenant_id, agent_id, member_id, channel, last_message_at, updated_at
		 FROM chat_sessions WHERE session_id = ?`, sessionID).
		Scan(&s.SessionID, &s.TenantID, &s.AgentID, &s.MemberID, &s.Channel,
			&s.LastMessageAt, &s.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, chat.ErrSessionNotFound
		}
		return nil, fmt.Errorf("chat: get session: %w", err)
	}
	return &s, nil
}

// Messages lists one session's messages, newest turn first (both rows of a
// turn share turn_timestamp, ordered by id within a turn so the user turn
// precedes the assistant reply).
func (l *MySQLLedger) Messages(ctx context.Context, q chat.MessageQuery) ([]chat.Message, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	query := `SELECT id, message_id, session_id, agent_id, member_id, role, content, status, turn_id, turn_timestamp, created_at
	          FROM chat_messages WHERE session_id = ?`
	args := []any{q.SessionID}
	if q.BeforeTurn > 0 {
		query += ` AND turn_timestamp < ?`
		args = append(args, q.BeforeTurn)
	}
	query += ` ORDER BY turn_timestamp DESC, id ASC LIMIT ?`
	args = append(args, limit)

	rows, err := l.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("chat: list messages: %w", err)
	}
	defer rows.Close()
	out := make([]chat.Message, 0)
	for rows.Next() {
		var m chat.Message
		if err := rows.Scan(&m.ID, &m.MessageID, &m.SessionID, &m.AgentID, &m.MemberID,
			&m.Role, &m.Content, &m.Status, &m.TurnID, &m.TurnTimestamp, &m.CreatedAt); err != nil {
			return nil, fmt.Errorf("chat: scan message: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
