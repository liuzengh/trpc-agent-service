package datamigration

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

var (
	ErrSourceNotFrozen = errors.New("session migration source is not frozen")
	ErrTargetActive    = errors.New("session migration target is already active")
	ErrRoutingNotReady = errors.New("session migration routing switch is not ready")
)

// SessionObject is one complete migration unit. State contains session-local
// values only; application and user overlays are carried separately.
type SessionObject struct {
	Key       session.Key
	State     session.StateMap
	Events    []event.Event
	AppState  session.StateMap
	UserState session.StateMap
	CreatedAt time.Time
	UpdatedAt time.Time
	SourceVer string
}

// SessionSource returns stable batches. A cursor is opaque to the executor and
// may be replayed after a crash; therefore targets must be idempotent.
type SessionSource interface {
	Scan(ctx context.Context, cursor string, batchSize int) ([]SessionObject, string, bool, error)
}

type SessionTarget interface {
	Import(ctx context.Context, object SessionObject) error
	Hash(ctx context.Context, key session.Key) (string, bool, error)
}

// SessionSafetyGate must be backed by deployment state, not a best-effort
// process-local flag. It is checked on every batch so losing the freeze pauses
// an in-flight migration before it writes more target data.
type SessionSafetyGate interface {
	Check(ctx context.Context, job Job, phase Phase) error
}

type SessionRoutingSwitcher interface {
	Cutover(ctx context.Context, job Job) error
	Rollback(ctx context.Context, job Job) error
}

type ObjectRecorder interface {
	PutObject(ctx context.Context, record ObjectRecord) error
}

type SessionOperator struct {
	source    SessionSource
	target    SessionTarget
	recorder  ObjectRecorder
	gate      SessionSafetyGate
	router    SessionRoutingSwitcher
	batchSize int
}

func NewSessionOperator(
	source SessionSource,
	target SessionTarget,
	recorder ObjectRecorder,
	gate SessionSafetyGate,
	router SessionRoutingSwitcher,
	batchSize int,
) (*SessionOperator, error) {
	if source == nil || target == nil || recorder == nil || gate == nil || router == nil {
		return nil, errors.New("session migration dependencies are required")
	}
	if batchSize <= 0 || batchSize > 10_000 {
		return nil, errors.New("session migration batch size must be between 1 and 10000")
	}
	return &SessionOperator{source: source, target: target, recorder: recorder, gate: gate, router: router, batchSize: batchSize}, nil
}

func (o *SessionOperator) ExecutePhase(ctx context.Context, phase Phase, job Job) (Result, error) {
	if job.ResourceKind != "session" {
		return Result{}, errors.New("session operator received another resource kind")
	}
	if err := o.gate.Check(ctx, job, phase); err != nil {
		return Result{}, err
	}
	switch phase {
	case PhasePrepare:
		return Result{Done: true}, nil
	case PhaseSnapshot, PhaseCatchUp:
		return o.copyBatch(ctx, job)
	case PhaseShadowRead, PhaseCanary, PhaseDrain, PhaseFinalize:
		return o.verifyBatch(ctx, job)
	case PhaseCutover:
		if err := o.router.Cutover(ctx, job); err != nil {
			return Result{}, err
		}
		return Result{Done: true}, nil
	case PhaseRollback:
		if err := o.router.Rollback(ctx, job); err != nil {
			return Result{}, err
		}
		return Result{Done: true}, nil
	default:
		return Result{}, errors.New("session operator received invalid phase")
	}
}

func (o *SessionOperator) copyBatch(ctx context.Context, job Job) (Result, error) {
	objects, next, done, err := o.source.Scan(ctx, checkpointCursor(job.Checkpoint), o.batchSize)
	if err != nil {
		return Result{}, fmt.Errorf("scan session source: %w", err)
	}
	result := Result{Done: done, Checkpoint: map[string]any{"cursor": next}}
	for i := range objects {
		sourceHash, err := HashSessionObject(objects[i])
		if err != nil {
			return result, err
		}
		if err := o.target.Import(ctx, objects[i]); err != nil {
			result.Failed++
			_ = o.record(ctx, job, objects[i], sourceHash, "", ObjectFailed)
			return result, fmt.Errorf("import session target: %w", err)
		}
		targetHash, exists, err := o.target.Hash(ctx, objects[i].Key)
		if err != nil || !exists {
			result.Failed++
			_ = o.record(ctx, job, objects[i], sourceHash, targetHash, ObjectFailed)
			if err != nil {
				return result, fmt.Errorf("hash imported session: %w", err)
			}
			return result, errors.New("imported session is missing")
		}
		status := ObjectCopied
		if targetHash != sourceHash {
			status = ObjectMismatch
			result.Mismatches++
		}
		if err := o.record(ctx, job, objects[i], sourceHash, targetHash, status); err != nil {
			return result, err
		}
		result.Copied++
	}
	return result, nil
}

func (o *SessionOperator) verifyBatch(ctx context.Context, job Job) (Result, error) {
	objects, next, done, err := o.source.Scan(ctx, checkpointCursor(job.Checkpoint), o.batchSize)
	if err != nil {
		return Result{}, fmt.Errorf("scan session source for verification: %w", err)
	}
	result := Result{Done: done, Checkpoint: map[string]any{"cursor": next}}
	for i := range objects {
		sourceHash, err := HashSessionObject(objects[i])
		if err != nil {
			return result, err
		}
		targetHash, exists, err := o.target.Hash(ctx, objects[i].Key)
		if err != nil {
			return result, fmt.Errorf("hash session target: %w", err)
		}
		status := ObjectVerified
		if !exists || targetHash != sourceHash {
			status = ObjectMismatch
			result.Mismatches++
		}
		if err := o.record(ctx, job, objects[i], sourceHash, targetHash, status); err != nil {
			return result, err
		}
		result.Verified++
	}
	return result, nil
}

func (o *SessionOperator) record(ctx context.Context, job Job, object SessionObject, sourceHash, targetHash string, status ObjectStatus) error {
	return o.recorder.PutObject(ctx, ObjectRecord{
		MigrationID: job.MigrationID,
		ObjectKey:   sessionObjectKey(object.Key),
		SourceVer:   object.SourceVer,
		SourceHash:  sourceHash,
		TargetHash:  targetHash,
		Status:      status,
	})
}

func HashSessionObject(object SessionObject) (string, error) {
	payload := struct {
		App       string           `json:"app"`
		User      string           `json:"user"`
		Session   string           `json:"session"`
		State     session.StateMap `json:"state"`
		Events    []event.Event    `json:"events"`
		AppState  session.StateMap `json:"app_state"`
		UserState session.StateMap `json:"user_state"`
	}{object.Key.AppName, object.Key.UserID, object.Key.SessionID, nonNilState(object.State), nonNilEvents(object.Events), nonNilState(object.AppState), nonNilState(object.UserState)}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("hash session migration object: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func sessionObjectKey(key session.Key) string {
	encode := func(value string) string { return base64.RawURLEncoding.EncodeToString([]byte(value)) }
	return "session/" + encode(key.AppName) + "/" + encode(key.UserID) + "/" + encode(key.SessionID)
}

func checkpointCursor(checkpoint map[string]any) string {
	value, _ := checkpoint["cursor"].(string)
	return value
}

func nonNilState(state session.StateMap) session.StateMap {
	if state == nil {
		return session.StateMap{}
	}
	return state
}

func nonNilEvents(events []event.Event) []event.Event {
	if events == nil {
		return []event.Event{}
	}
	return events
}
