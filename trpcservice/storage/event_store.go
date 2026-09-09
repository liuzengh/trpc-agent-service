package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
)

var (
	// ErrDuplicateEvent means the database unique index already contains the
	// inbound platform message.
	ErrDuplicateEvent = errors.New("message event is duplicate")
)

// UserEvent is the durable platform-level representation of an inbound message.
type UserEvent struct {
	ID        int64
	SessionID string
	TenantID  string
	AppID     string
	Channel   string
	MsgID     string
	SenderID  string
	UserRef   string
	Text      string
	TraceID   string
	Consumed  bool
	Timestamp time.Time
}

// EventStore persists inbound events independently from framework Session.
type EventStore interface {
	AppendUserEvent(context.Context, UserEvent) error
	PendingUserEvents(context.Context, string, int64) ([]UserEvent, error)
	MarkConsumed(context.Context, []int64) error
}

// MySQLEventStore implements EventStore with the message_event table.
type MySQLEventStore struct {
	db *sql.DB
}

// NewMySQLEventStore constructs an EventStore from the shared MySQL handle.
func NewMySQLEventStore(db *sql.DB) (*MySQLEventStore, error) {
	if db == nil {
		return nil, errors.New("event store database is required")
	}
	return &MySQLEventStore{db: db}, nil
}

// AppendUserEvent persists the durable handoff before an adapter ACKs.
func (s *MySQLEventStore) AppendUserEvent(ctx context.Context, event UserEvent) error {
	if event.SessionID == "" || event.TenantID == "" || event.AppID == "" || event.Channel == "" ||
		event.MsgID == "" || event.SenderID == "" {
		return errors.New("user event session, tenant, app, channel, message, and sender IDs are required")
	}
	if event.UserRef == "" {
		event.UserRef = event.SenderID
	}
	payload, err := json.Marshal(userEventPayload{
		SenderID: event.SenderID,
		Text:     event.Text,
		TraceID:  event.TraceID,
	})
	if err != nil {
		return fmt.Errorf("encode user event payload: %w", err)
	}
	const ensureSession = `INSERT INTO session
		(session_id, tenant_id, app_id, channel, user_ref)
		VALUES (?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP`
	if _, err := s.db.ExecContext(
		ctx, ensureSession,
		event.SessionID, event.TenantID, event.AppID, event.Channel, event.UserRef,
	); err != nil {
		return fmt.Errorf("ensure platform session: %w", err)
	}
	const query = `INSERT INTO message_event
		(session_id, tenant_id, role, type, payload_json, msg_id, channel, consumed)
		VALUES (?, ?, 'user', 'message', ?, ?, ?, FALSE)`
	if _, err := s.db.ExecContext(
		ctx, query, event.SessionID, event.TenantID, payload, event.MsgID, event.Channel,
	); err != nil {
		var mysqlError *mysqlDriver.MySQLError
		if errors.As(err, &mysqlError) && mysqlError.Number == 1062 {
			return ErrDuplicateEvent
		}
		return fmt.Errorf("append user event: %w", err)
	}
	return nil
}

// PendingUserEvents loads unconsumed user events in deterministic order.
func (s *MySQLEventStore) PendingUserEvents(
	ctx context.Context,
	sessionID string,
	sinceEventID int64,
) ([]UserEvent, error) {
	const query = `SELECT event_id, session_id, tenant_id, channel, msg_id,
		payload_json, consumed, ts
		FROM message_event
		WHERE session_id = ? AND event_id > ? AND role = 'user' AND consumed = FALSE
		ORDER BY event_id`
	rows, err := s.db.QueryContext(ctx, query, sessionID, sinceEventID)
	if err != nil {
		return nil, fmt.Errorf("query pending user events: %w", err)
	}
	defer rows.Close()

	var events []UserEvent
	for rows.Next() {
		var event UserEvent
		var payloadData []byte
		if err := rows.Scan(
			&event.ID, &event.SessionID, &event.TenantID, &event.Channel,
			&event.MsgID, &payloadData, &event.Consumed, &event.Timestamp,
		); err != nil {
			return nil, fmt.Errorf("scan pending user event: %w", err)
		}
		var payload userEventPayload
		if err := json.Unmarshal(payloadData, &payload); err != nil {
			return nil, fmt.Errorf("decode user event %d payload: %w", event.ID, err)
		}
		event.SenderID = payload.SenderID
		event.Text = payload.Text
		event.TraceID = payload.TraceID
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending user events: %w", err)
	}
	return events, nil
}

// MarkConsumed marks the exact events included in a completed Runner turn.
func (s *MySQLEventStore) MarkConsumed(ctx context.Context, eventIDs []int64) error {
	if len(eventIDs) == 0 {
		return nil
	}
	placeholders := make([]string, len(eventIDs))
	args := make([]any, len(eventIDs))
	for index, id := range eventIDs {
		if id <= 0 {
			return errors.New("event IDs must be positive")
		}
		placeholders[index] = "?"
		args[index] = id
	}
	query := `UPDATE message_event SET consumed = TRUE WHERE event_id IN (` +
		strings.Join(placeholders, ",") + `)`
	if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("mark user events consumed: %w", err)
	}
	return nil
}

type userEventPayload struct {
	SenderID string `json:"sender_id"`
	Text     string `json:"text"`
	TraceID  string `json:"trace_id,omitempty"`
}
