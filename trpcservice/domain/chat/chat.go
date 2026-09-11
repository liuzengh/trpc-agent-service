// Package chat hosts the business conversation ledger (007_chat.sql): a
// durable, fact-level record of every dialogue turn, decoupled from the
// framework session sliding window. It powers session listing, turn-paginated
// history and cost attribution, independent of prompt-context truncation.
package chat

import (
	"context"
	"errors"
	"time"
)

// ErrSessionNotFound is returned when a ledger session id is unknown.
var ErrSessionNotFound = errors.New("chat: session not found")

// Message roles recorded in the ledger (a subset of the DDL enum: tool calls
// stay in the framework session_events / audit trail).
const (
	RoleUser      = "USER"
	RoleAssistant = "ASSISTANT"
)

// Turn is one user request plus the agent's final reply.
type Turn struct {
	TenantID  string
	AgentID   string
	SessionID string
	MemberID  string // initiating member (framework UserID)
	Channel   string // wecom | feishu | admin
	// UserMsgID / ReplyMsgID are business UUIDs; unique keys make redelivery
	// idempotent.
	UserMsgID  string
	UserText   string
	ReplyMsgID string
	ReplyText  string
	// TurnID groups the two rows; TurnTS is ms epoch for turn pagination.
	TurnID string
	TurnTS int64
}

// Session is the ledger view of one conversation (list row).
type Session struct {
	SessionID      string    `json:"session_id"`
	TenantID       string    `json:"tenant_id"`
	AgentID        string    `json:"agent_id"`
	MemberID       string    `json:"member_id"`
	Channel        string    `json:"channel"`
	LastMessageAt  time.Time `json:"last_message_at,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// Message is one ledger message row (history item).
type Message struct {
	ID           int64     `json:"id"`
	MessageID    string    `json:"message_id"`
	SessionID    string    `json:"session_id"`
	AgentID      string    `json:"agent_id"`
	MemberID     string    `json:"member_id"`
	Role         string    `json:"role"`
	Content      string    `json:"content"`
	Status       string    `json:"status"`
	TurnID       string    `json:"turn_id"`
	TurnTimestamp int64    `json:"turn_timestamp"`
	CreatedAt    time.Time `json:"created_at"`
}

// SessionQuery filters the session list.
type SessionQuery struct {
	TenantID string
	MemberID string
	Limit    int // page size; <= 0 means default 20
}

// MessageQuery paginates one session's messages by turn, newest first.
type MessageQuery struct {
	SessionID string
	// BeforeTurn fetches messages with turn_timestamp < BeforeTurn (for
	// cursor pagination); 0 means the newest page.
	BeforeTurn int64
	Limit      int
}

// Ledger is the business conversation ledger contract.
type Ledger interface {
	// RecordTurn upserts the session and inserts the USER + ASSISTANT rows
	// of one turn in a single transaction. Duplicate message ids (redelivery)
	// are ignored. Best-effort from the caller's perspective: an error must
	// not fail the underlying reply flow.
	RecordTurn(ctx context.Context, t Turn) error
	// Sessions lists sessions (tenant-scoped, optionally member-filtered),
	// newest activity first.
	Sessions(ctx context.Context, q SessionQuery) ([]Session, error)
	// Session loads one session by id. History access is decided from the
	// session's own tenant/member, so a member cannot read someone else's
	// conversation by guessing a session id.
	Session(ctx context.Context, sessionID string) (*Session, error)
	// Messages lists a session's messages, turn-descending.
	Messages(ctx context.Context, q MessageQuery) ([]Message, error)
}
