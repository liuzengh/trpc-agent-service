// Package migration coordinates durable online tenant backend migration.
package migration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type State string

const (
	StatePlanned   State = "planned"
	StateSnapshot  State = "snapshot"
	StateDualWrite State = "dual_write"
	StateBackfill  State = "backfill"
	StateVerify    State = "verify"
	StateCutover   State = "cutover"
	StateObserve   State = "observe"
	StateCleanup   State = "cleanup"
	StatePaused    State = "paused"
	StateAborted   State = "aborted"
)

type Binding struct {
	ConfigVersion    int64
	BackendProfileID string
	BackendVersion   int64
}

type Verification struct {
	SourceCount, TargetCount                       int64
	SourceDigest, TargetDigest                     string
	SourceWatermark, TargetWatermark, SampleDigest string
}

// SessionSwitchMetadata carries the operator attribution attached to a
// session backend pointer change. It lives in migration rather than a storage
// driver so control-plane consumers do not depend on data-plane packages.
type SessionSwitchMetadata struct {
	SwitchID, ActorID, ReasonCode, CorrelationID, TraceID, Traceparent string
}

// SessionCutoverRequest, SessionObserveRequest, SessionRollbackRequest and
// SessionCleanupRequest are the migration control-plane contract for an
// atomic Session backend switch. Session drivers may implement this contract,
// but Admin and orchestration deliberately depend only on this package.
type SessionCutoverRequest struct {
	TenantID, MigrationID                  string
	ExpectedTenantVersion, ExpectedVersion int64
	Verification                           Verification
	At                                     time.Time
	Metadata                               SessionSwitchMetadata
}

type SessionObserveRequest struct {
	TenantID, MigrationID                  string
	ExpectedTenantVersion, ExpectedVersion int64
	At, ObserveUntil                       time.Time
}

type SessionRollbackRequest struct {
	TenantID, MigrationID                  string
	ExpectedTenantVersion, ExpectedVersion int64
	RollbackSyncWatermark                  string
	At                                     time.Time
	Metadata                               SessionSwitchMetadata
}

type SessionCleanupRequest struct {
	TenantID, MigrationID                  string
	ExpectedTenantVersion, ExpectedVersion int64
	RollbackSyncWatermark                  string
	At                                     time.Time
}

type SessionSwitchResult struct {
	Migration           Migration
	TenantVersion       int64
	ActiveConfigVersion int64
	RolledBack          bool
}

type SessionDrainStatus struct {
	SourceInFlight, TargetInFlight         int64
	ForwardOutstanding, ReverseOutstanding int64
	ActiveConfigVersion                    int64
	RolledBack                             bool
}

// SessionCutoverPublisher owns the atomic tenant configuration pointer
// changes. BeginObserve and Cleanup use the same authority so dispatch cannot
// race their lifecycle gates.
type SessionCutoverPublisher interface {
	Cutover(context.Context, SessionCutoverRequest) (SessionSwitchResult, error)
	BeginObserve(context.Context, SessionObserveRequest) (SessionSwitchResult, error)
	Rollback(context.Context, SessionRollbackRequest) (SessionSwitchResult, error)
	Cleanup(context.Context, SessionCleanupRequest) (SessionSwitchResult, error)
	DrainStatus(context.Context, string, string) (SessionDrainStatus, error)
}

type Migration struct {
	TenantID, MigrationID, Domain string
	Epoch                         int64
	Source, Target                Binding
	State                         State
	SnapshotWatermark             string
	DualWriteRef                  string
	BackfillCheckpoint            string
	NextBatchSeq, BackfillCount   int64
	BackfillComplete              bool
	Verification                  Verification
	CutoverConfigVersion          int64
	CutoverAt, ObserveUntil       time.Time
	RollbackSyncWatermark         string
	PausedFrom                    State
	CreatedAt, UpdatedAt          time.Time
	Version                       int64
}

type CreateRequest struct {
	TenantID, MigrationID, Domain string
	Epoch                         int64
	Source, Target                Binding
	CreatedAt                     time.Time
	Audit                         CreateAudit
}

// CreateAudit is durable operator attribution for creation of a migration
// authority. It is optional for lower-level contract fixtures, but required
// by the Admin control plane and emitted with the same transaction as create.
type CreateAudit struct {
	ActorID, ReasonCode, CorrelationID, TraceID string
}

func (a CreateAudit) Valid() bool {
	return validText(a.ActorID, 256) && validText(a.ReasonCode, 128) &&
		validText(a.CorrelationID, 256) && validText(a.TraceID, 128)
}

type TransitionRequest struct {
	TenantID, MigrationID string
	ExpectedVersion       int64
	To                    State
	At                    time.Time
	SnapshotWatermark     string
	DualWriteRef          string
	Verification          Verification
	CutoverConfigVersion  int64
	ObserveUntil          time.Time
	RollbackSyncWatermark string
}

// ControlMetadata identifies the operator who pauses, resumes, or aborts an
// online migration. These changes do not move a tenant configuration pointer,
// but they are still durable control-plane decisions and must be auditable.
type ControlMetadata struct {
	ActorID, ReasonCode, CorrelationID, TraceID, Traceparent string
}

func (m ControlMetadata) Valid() bool {
	return validText(m.ActorID, 256) && validText(m.ReasonCode, 128) &&
		validText(m.CorrelationID, 256) && validText(m.TraceID, 128) &&
		(m.Traceparent == "" || validText(m.Traceparent, 512))
}

// ControlRequest applies a migration-only CAS. Tenant configuration versions
// are intentionally absent: pause, resume, and pre-dual-write abort never
// mutate the active tenant backend pointer.
type ControlRequest struct {
	TenantID, MigrationID string
	ExpectedVersion       int64
	At                    time.Time
	Metadata              ControlMetadata
}

type BatchRequest struct {
	TenantID, MigrationID, BatchID       string
	Epoch, ExpectedVersion, BatchSeq     int64
	FromCheckpoint, ToCheckpoint, Digest string
	RecordCount                          int64
	Complete                             bool
	CommittedAt                          time.Time
}

// VerificationRequest records evidence produced by a trusted migration
// operator. Evidence is versioned independently while the migration remains
// in verify, so an Admin caller can only cut over using authority-held facts
// rather than browser-supplied digests.
type VerificationRequest struct {
	TenantID, MigrationID string
	ExpectedVersion       int64
	Verification          Verification
	RecordedAt            time.Time
}

type Batch struct {
	BatchRequest
	ResultVersion int64
}

type BatchResult struct {
	Migration Migration
	Batch     Batch
}

type Repository interface {
	Create(context.Context, CreateRequest) (Migration, error)
	Get(context.Context, string, string) (Migration, error)
	List(context.Context, string, string) ([]Migration, error)
	Transition(context.Context, TransitionRequest) (Migration, error)
	CommitBatch(context.Context, BatchRequest) (BatchResult, error)
	RecordVerification(context.Context, VerificationRequest) (Migration, error)
	Pause(context.Context, ControlRequest) (Migration, error)
	Resume(context.Context, ControlRequest) (Migration, error)
	Abort(context.Context, ControlRequest) (Migration, error)
}

func NewMigration(in CreateRequest) (Migration, error) {
	if !validText(in.TenantID, 128) || !validText(in.MigrationID, 128) || !validText(in.Domain, 32) || in.Epoch < 1 ||
		!validBinding(in.Source) || !validBinding(in.Target) || in.Source == in.Target || in.CreatedAt.IsZero() {
		return Migration{}, runtime.ErrInvariantViolation
	}
	return Migration{TenantID: in.TenantID, MigrationID: in.MigrationID, Domain: in.Domain, Epoch: in.Epoch,
		Source: in.Source, Target: in.Target, State: StatePlanned, NextBatchSeq: 1,
		CreatedAt: in.CreatedAt.UTC(), UpdatedAt: in.CreatedAt.UTC(), Version: 1}, nil
}

func ApplyTransition(current Migration, in TransitionRequest) (Migration, error) {
	if in.TenantID != current.TenantID || in.MigrationID != current.MigrationID {
		return Migration{}, runtime.ErrTenantScope
	}
	if in.ExpectedVersion != current.Version {
		return Migration{}, runtime.ErrVersionConflict
	}
	if in.At.IsZero() || in.At.Before(current.UpdatedAt) || !CanTransition(current.State, in.To) {
		return Migration{}, runtime.ErrInvariantViolation
	}
	next := current
	switch in.To {
	case StateSnapshot:
		if !validText(in.SnapshotWatermark, 512) {
			return Migration{}, runtime.ErrInvariantViolation
		}
		next.SnapshotWatermark = in.SnapshotWatermark
	case StateDualWrite:
		if !validText(in.DualWriteRef, 512) {
			return Migration{}, runtime.ErrInvariantViolation
		}
		next.DualWriteRef = in.DualWriteRef
	case StateBackfill:
		// Backfill starts at the empty checkpoint and advances only via CommitBatch.
	case StateVerify:
		if !current.BackfillComplete {
			return Migration{}, runtime.ErrInvariantViolation
		}
	case StateCutover:
		if !validVerification(in.Verification) || in.CutoverConfigVersion != current.Target.ConfigVersion {
			return Migration{}, runtime.ErrInvariantViolation
		}
		next.Verification = in.Verification
		next.CutoverConfigVersion = in.CutoverConfigVersion
		next.CutoverAt = in.At.UTC()
	case StateObserve:
		if in.ObserveUntil.IsZero() || !in.ObserveUntil.After(in.At) {
			return Migration{}, runtime.ErrInvariantViolation
		}
		next.ObserveUntil = in.ObserveUntil.UTC()
	case StateCleanup:
		if current.ObserveUntil.IsZero() || in.At.Before(current.ObserveUntil) || in.RollbackSyncWatermark != current.Verification.TargetWatermark {
			return Migration{}, runtime.ErrInvariantViolation
		}
		next.RollbackSyncWatermark = in.RollbackSyncWatermark
	default:
		return Migration{}, runtime.ErrInvariantViolation
	}
	next.State = in.To
	next.Version++
	next.UpdatedAt = in.At.UTC()
	return next, nil
}

// ApplyPause freezes migration advancement without turning off an already
// enabled dual-write path. It is allowed only before cutover; a cutover or
// observation window has stronger rollback and drain invariants.
func ApplyPause(current Migration, in ControlRequest) (Migration, error) {
	if err := validControl(current, in); err != nil {
		return Migration{}, err
	}
	if !pausable(current.State) {
		return Migration{}, runtime.ErrInvariantViolation
	}
	next := current
	next.State = StatePaused
	next.PausedFrom = current.State
	next.Version++
	next.UpdatedAt = in.At.UTC()
	return next, nil
}

// ApplyResume restores exactly the phase that was paused. It never permits an
// operator to use resume as an arbitrary state transition.
func ApplyResume(current Migration, in ControlRequest) (Migration, error) {
	if err := validControl(current, in); err != nil {
		return Migration{}, err
	}
	if current.State != StatePaused || !pausable(current.PausedFrom) {
		return Migration{}, runtime.ErrInvariantViolation
	}
	next := current
	next.State = current.PausedFrom
	next.PausedFrom = ""
	next.Version++
	next.UpdatedAt = in.At.UTC()
	return next, nil
}

// ApplyAbort is deliberately limited to the pre-dual-write phases. Once a
// dual-write ledger exists, abort needs domain-specific drain and compensation
// work; accepting a generic abort there would make the source/target contract
// unsafe. Operators can pause such migrations while that procedure runs.
func ApplyAbort(current Migration, in ControlRequest) (Migration, error) {
	if err := validControl(current, in); err != nil {
		return Migration{}, err
	}
	state := current.State
	if state == StatePaused {
		state = current.PausedFrom
	}
	if state != StatePlanned && state != StateSnapshot {
		return Migration{}, runtime.ErrInvariantViolation
	}
	next := current
	next.State = StateAborted
	next.PausedFrom = ""
	next.Version++
	next.UpdatedAt = in.At.UTC()
	return next, nil
}

func ApplyBatch(current Migration, in BatchRequest) (Migration, Batch, error) {
	if in.TenantID != current.TenantID || in.MigrationID != current.MigrationID {
		return Migration{}, Batch{}, runtime.ErrTenantScope
	}
	if current.State != StateBackfill || current.BackfillComplete || in.Epoch != current.Epoch || in.ExpectedVersion != current.Version ||
		in.BatchSeq != current.NextBatchSeq || in.FromCheckpoint != current.BackfillCheckpoint ||
		!validText(in.BatchID, 128) || !validText(in.ToCheckpoint, 512) || in.ToCheckpoint == in.FromCheckpoint ||
		!validDigest(in.Digest) || in.RecordCount < 0 || (in.RecordCount == 0 && !in.Complete) || in.CommittedAt.IsZero() || in.CommittedAt.Before(current.UpdatedAt) {
		return Migration{}, Batch{}, runtime.ErrInvariantViolation
	}
	next := current
	next.BackfillCheckpoint = in.ToCheckpoint
	next.NextBatchSeq++
	next.BackfillCount += in.RecordCount
	next.BackfillComplete = in.Complete
	next.Version++
	next.UpdatedAt = in.CommittedAt.UTC()
	return next, Batch{BatchRequest: in, ResultVersion: next.Version}, nil
}

// ApplyVerification accepts evidence only from the verify phase. Replaying
// identical evidence is idempotent; changed evidence advances the migration
// version so a stale cutover CAS is rejected.
func ApplyVerification(current Migration, in VerificationRequest) (Migration, error) {
	if in.TenantID != current.TenantID || in.MigrationID != current.MigrationID {
		return Migration{}, runtime.ErrTenantScope
	}
	if current.State != StateVerify || in.ExpectedVersion != current.Version || in.RecordedAt.IsZero() ||
		in.RecordedAt.Before(current.UpdatedAt) || !validVerification(in.Verification) {
		return Migration{}, runtime.ErrInvariantViolation
	}
	if current.Verification == in.Verification {
		return current, nil
	}
	next := current
	next.Verification = in.Verification
	next.Version++
	next.UpdatedAt = in.RecordedAt.UTC()
	return next, nil
}

func BatchDigest(in BatchRequest) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{in.TenantID, in.MigrationID, in.BatchID, in.FromCheckpoint,
		in.ToCheckpoint, in.Digest, strconv.FormatInt(in.Epoch, 10), strconv.FormatInt(in.BatchSeq, 10),
		strconv.FormatInt(in.RecordCount, 10), strconv.FormatBool(in.Complete)}, "\x00")))
	return hex.EncodeToString(sum[:])
}

func SameBatch(left Batch, right BatchRequest) bool {
	return left.TenantID == right.TenantID && left.MigrationID == right.MigrationID && left.BatchID == right.BatchID &&
		left.Epoch == right.Epoch && left.BatchSeq == right.BatchSeq && left.FromCheckpoint == right.FromCheckpoint &&
		left.ToCheckpoint == right.ToCheckpoint && left.Digest == right.Digest && left.RecordCount == right.RecordCount && left.Complete == right.Complete
}

// CanTransition is the authoritative migration phase graph. Keeping the
// graph as data, rather than deriving it from a switch order, makes review of
// every permitted edge straightforward and prevents future states from
// implicitly gaining transitions.
var allowedTransitions = map[State]map[State]struct{}{
	StatePlanned:   {StateSnapshot: {}},
	StateSnapshot:  {StateDualWrite: {}},
	StateDualWrite: {StateBackfill: {}},
	StateBackfill:  {StateVerify: {}},
	StateVerify:    {StateCutover: {}},
	StateCutover:   {StateObserve: {}},
	StateObserve:   {StateCleanup: {}},
	StateCleanup:   {},
	StatePaused:    {},
	StateAborted:   {},
}

// CanTransition reports whether a direct phase transition is permitted. It
// deliberately has no permissive fallback for an unknown or terminal state.
func CanTransition(from, to State) bool {
	_, ok := allowedTransitions[from][to]
	return ok
}

// Terminal reports whether a migration can never be resumed or advanced.
func Terminal(state State) bool {
	return state == StateCleanup || state == StateAborted
}

func pausable(state State) bool {
	switch state {
	case StatePlanned, StateSnapshot, StateDualWrite, StateBackfill, StateVerify:
		return true
	default:
		return false
	}
}

func validControl(current Migration, in ControlRequest) error {
	if in.TenantID != current.TenantID || in.MigrationID != current.MigrationID {
		return runtime.ErrTenantScope
	}
	if in.ExpectedVersion != current.Version {
		return runtime.ErrVersionConflict
	}
	if in.At.IsZero() || in.At.Before(current.UpdatedAt) || !in.Metadata.Valid() {
		return runtime.ErrInvariantViolation
	}
	return nil
}

func validBinding(value Binding) bool {
	return value.ConfigVersion >= 1 && value.BackendVersion >= 1 && validText(value.BackendProfileID, 128)
}

func validVerification(value Verification) bool {
	return value.SourceCount >= 0 && value.SourceCount == value.TargetCount && validDigest(value.SourceDigest) &&
		value.SourceDigest == value.TargetDigest && validText(value.SourceWatermark, 512) &&
		value.SourceWatermark == value.TargetWatermark && validDigest(value.SampleDigest)
}

func validDigest(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validText(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\x00\r\n")
}
