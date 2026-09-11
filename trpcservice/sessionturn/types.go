// Package sessionturn provides strict, turn-scoped session persistence and a
// staging adapter for trpc-agent-go's session.Service.
//
// Session-level state, appended events, the session version, and the replay
// payload commit in one PostgreSQL transaction. Application- and user-scoped
// state are durable overlays with an intentionally separate lifecycle: they
// are read after the turn snapshot, are excluded from Commit, and cannot be
// written through a strict turn.
package sessionturn

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

const maxTurnIDLength = 256

var (
	// ErrInvalidRequest indicates that a session key, turn ID, version, or
	// fencing token is invalid.
	ErrInvalidRequest = errors.New("sessionturn: invalid request")
	// ErrTurnNotFound indicates that the requested turn was never begun.
	ErrTurnNotFound = errors.New("sessionturn: turn not found")
	// ErrTurnAborted indicates that the stable turn ID was already aborted and
	// therefore cannot be reused.
	ErrTurnAborted = errors.New("sessionturn: turn aborted")
	// ErrTurnCommitted indicates that an already committed turn cannot be
	// aborted. Commit and Begin themselves replay committed turns instead.
	ErrTurnCommitted = errors.New("sessionturn: turn already committed")
	// ErrVersionConflict means the session changed after the caller's snapshot.
	ErrVersionConflict = errors.New("sessionturn: session version conflict")
	// ErrFenceLost means a newer Begin superseded the caller's fencing token.
	ErrFenceLost = errors.New("sessionturn: fencing token lost")
	// ErrCorruptData indicates that persisted JSON cannot be decoded into the
	// matching trpc-agent-go type.
	ErrCorruptData = errors.New("sessionturn: corrupt persisted data")
)

// Service is the strict persistence boundary used around one agent turn.
type Service interface {
	EnsureSchema(ctx context.Context) error
	Begin(ctx context.Context, req BeginRequest) (*BeginResult, error)
	Commit(ctx context.Context, req CommitRequest) (*CommitResult, error)
	Abort(ctx context.Context, req AbortRequest) error
}

// Handle is the immutable concurrency capability returned by Begin.
// ExpectedVersion and FencingToken must be presented unchanged to Commit.
type Handle struct {
	Key             session.Key
	TurnID          string
	ExpectedVersion int64
	FencingToken    int64
}

// Snapshot is a transactionally consistent view of one trpc-agent-go session.
// Events are ordered by their persisted per-session sequence.
type Snapshot struct {
	Key               session.Key
	State             session.StateMap
	Events            []event.Event
	Version           int64
	LastEventSequence int64
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// TRPCSession converts the snapshot to the framework's concrete session type.
// The returned session owns its State and Events slices.
func (s Snapshot) TRPCSession() *session.Session {
	state := cloneState(s.State)
	events := append([]event.Event(nil), s.Events...)
	return session.NewSession(
		s.Key.AppName,
		s.Key.UserID,
		s.Key.SessionID,
		session.WithSessionState(state),
		session.WithSessionEvents(events),
		session.WithSessionCreatedAt(s.CreatedAt),
		session.WithSessionUpdatedAt(s.UpdatedAt),
	)
}

// BeginRequest starts or takes over a stable turn ID for one session. Taking
// over an active turn issues a new fence and invalidates the prior Handle.
type BeginRequest struct {
	Key    session.Key
	TurnID string
}

// BeginResult contains either a fresh/active Handle and Snapshot, or replay
// data for a turn that was committed previously. Snapshot is nil on replay.
type BeginResult struct {
	Handle   Handle
	Snapshot *Snapshot
	Replay   []byte
	Replayed bool
}

// CommitRequest replaces the session-level state and appends Events in one
// transaction. Replay is stored in that same transaction and is the exact blob
// returned to duplicate executions of this stable TurnID.
type CommitRequest struct {
	Handle Handle
	Events []event.Event
	State  session.StateMap
	Replay []byte
}

// CommitResult reports the new session version. Replayed is true when another
// invocation had already committed the same stable TurnID.
type CommitResult struct {
	Version  int64
	Replay   []byte
	Replayed bool
}

// CommitParticipant adds one database-local operation to the strict turn
// transaction. canonicalReplay is the payload already stored by a competing
// commit when Replayed is true, never the losing caller's proposed payload.
// Implementations must use only tx for database work and must not commit or
// roll it back themselves.
type CommitParticipant func(ctx context.Context, tx pgx.Tx, canonicalReplay []byte) error

// AbortRequest permanently marks an uncommitted stable TurnID as aborted.
// Aborting is idempotent when the same fencing token is supplied again.
type AbortRequest struct {
	Key          session.Key
	TurnID       string
	FencingToken int64
}

// DeriveTurnID deterministically derives a stable, opaque turn ID from the
// full session key and an upstream idempotency key (normally the inbox ID).
func DeriveTurnID(key session.Key, idempotencyKey string) (string, error) {
	if err := validateKey(key); err != nil {
		return "", err
	}
	if strings.TrimSpace(idempotencyKey) == "" {
		return "", fmt.Errorf("%w: idempotency key is required", ErrInvalidRequest)
	}

	h := sha256.New()
	_, _ = h.Write([]byte("trpc-session-turn/v1"))
	var length [8]byte
	for _, value := range []string{key.AppName, key.UserID, key.SessionID, idempotencyKey} {
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = h.Write(length[:])
		_, _ = h.Write([]byte(value))
	}
	return "turn_" + hex.EncodeToString(h.Sum(nil)), nil
}

func validateKey(key session.Key) error {
	if err := key.CheckSessionKey(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	return nil
}

func validateTurnID(turnID string) error {
	if strings.TrimSpace(turnID) == "" {
		return fmt.Errorf("%w: turn ID is required", ErrInvalidRequest)
	}
	if len(turnID) > maxTurnIDLength {
		return fmt.Errorf("%w: turn ID exceeds %d bytes", ErrInvalidRequest, maxTurnIDLength)
	}
	return nil
}

func cloneState(state session.StateMap) session.StateMap {
	if state == nil {
		return session.StateMap{}
	}
	cloned := make(session.StateMap, len(state))
	for key, value := range state {
		cloned[key] = append([]byte(nil), value...)
		if value == nil {
			cloned[key] = nil
		}
	}
	return cloned
}
