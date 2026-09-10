// Package session defines the atomic session authority used by distributed
// Workers.
package session

import (
	"context"
	"encoding/json"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type SessionKey struct{ TenantID, AgentAppID, SessionID string }
type TerminalKey struct {
	SessionKey
	InputSeq uint64
}
type OpenForRunRequest struct {
	SessionKey
	RequestID string
	InputSeq  uint64
	Fence     uint64
}
type SessionHead struct {
	SessionKey
	Version                                 int64
	LastFence, LastSessionSeq, NextInputSeq uint64
	State                                   map[string]any
}
type BufferedEvent struct {
	EventID, EventType, PayloadRef string
	EventSeq                       uint64
	// Payload is the canonical upstream event representation. It is committed
	// with the event row so another Worker can reconstruct conversation history.
	Payload json.RawMessage
}
type SessionSnapshot struct {
	Head   SessionHead
	Events []json.RawMessage
}
type StateDelta map[string]any
type SummaryCandidate struct {
	SummaryID      string
	BaseSessionSeq uint64
	LastEventID    string
	CutoffAt       time.Time
	ContentRef     string
}
type OutboxEvent struct {
	Kind, IdempotencyKey, PayloadRef, TraceParent string
	EventSeq                                      uint64
}
type CommitTurnRequest struct {
	SessionKey
	RequestID, CommitID, Stage string
	InputSeq, Fence            uint64
	ExpectedVersion            int64
	Outcome                    runtime.Outcome
	Events                     []BufferedEvent
	StateDelta                 StateDelta
	SummaryCandidate           *SummaryCandidate
	ResultRef, ReplyCursor     string
	Outbox                     []OutboxEvent
}
type CommitTurnResult struct {
	CommitID               string
	Outcome                runtime.Outcome
	InputSeq               uint64
	SessionVersion         int64
	ResultRef, ReplyCursor string
}

type AtomicSessionStore interface {
	OpenForRun(context.Context, OpenForRunRequest) (SessionHead, error)
	CommitTurn(context.Context, CommitTurnRequest) (CommitTurnResult, error)
	GetTerminalByInputSeq(context.Context, TerminalKey) (CommitTurnResult, error)
	ReadLastFence(context.Context, SessionKey) (uint64, error)
	LoadSession(context.Context, SessionKey) (SessionSnapshot, error)
}
