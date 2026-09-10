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

// PhaseIntentStatus describes whether a durable transition intent has been
// reconciled with the migration authority. An intent is written before the
// authority transition, so a worker crash has an unambiguous recovery point.
type PhaseIntentStatus string

const (
	PhaseIntentPending   PhaseIntentStatus = "pending"
	PhaseIntentCompleted PhaseIntentStatus = "completed"
)

// PhaseIntent is the service-owned journal record for a state transition.
// Request is retained verbatim so recovery replays the exact CAS operation,
// rather than reconstructing one from a potentially newer migration view.
type PhaseIntent struct {
	TenantID, MigrationID, IntentID string
	Request                         TransitionRequest
	RequestDigest                   string
	Status                          PhaseIntentStatus
	ResultVersion                   int64
	CreatedAt, CompletedAt          time.Time
}

// PhaseIntentStore is intentionally separate from Repository. A domain driver
// only needs Repository; a process that wants crash recovery opts into this
// small control-plane journal interface. Implementations must make Begin
// idempotent for the same intent and reject a different request using its key.
type PhaseIntentStore interface {
	BeginPhaseIntent(context.Context, TransitionRequest) (PhaseIntent, error)
	CompletePhaseIntent(context.Context, PhaseIntent, Migration, time.Time) (PhaseIntent, error)
	ListPendingPhaseIntents(context.Context, string, string) ([]PhaseIntent, error)
}

// JournaledRepository decorates a Repository with write-ahead phase intents.
// It does not know about any session or knowledge data plane and therefore
// remains reusable by both domains without coupling them to one another.
type JournaledRepository struct {
	Repository Repository
	Journal    PhaseIntentStore
}

func NewJournaledRepository(repository Repository, journal PhaseIntentStore) *JournaledRepository {
	return &JournaledRepository{Repository: repository, Journal: journal}
}

func (r *JournaledRepository) Create(ctx context.Context, in CreateRequest) (Migration, error) {
	return r.repository().Create(ctx, in)
}

func (r *JournaledRepository) Get(ctx context.Context, tenantID, migrationID string) (Migration, error) {
	return r.repository().Get(ctx, tenantID, migrationID)
}

func (r *JournaledRepository) List(ctx context.Context, tenantID, domain string) ([]Migration, error) {
	return r.repository().List(ctx, tenantID, domain)
}

func (r *JournaledRepository) Transition(ctx context.Context, in TransitionRequest) (Migration, error) {
	if r == nil || r.Journal == nil {
		return Migration{}, runtime.ErrBackendUnavailable
	}
	intent, err := r.Journal.BeginPhaseIntent(ctx, in)
	if err != nil {
		return Migration{}, err
	}
	return r.reconcile(ctx, intent, in.At)
}

func (r *JournaledRepository) CommitBatch(ctx context.Context, in BatchRequest) (BatchResult, error) {
	return r.repository().CommitBatch(ctx, in)
}

func (r *JournaledRepository) RecordVerification(ctx context.Context, in VerificationRequest) (Migration, error) {
	return r.repository().RecordVerification(ctx, in)
}

func (r *JournaledRepository) Pause(ctx context.Context, in ControlRequest) (Migration, error) {
	return r.repository().Pause(ctx, in)
}

func (r *JournaledRepository) Resume(ctx context.Context, in ControlRequest) (Migration, error) {
	return r.repository().Resume(ctx, in)
}

func (r *JournaledRepository) Abort(ctx context.Context, in ControlRequest) (Migration, error) {
	return r.repository().Abort(ctx, in)
}

// RecoverPending reconciles every incomplete transition intent for one
// migration. It is safe to call on each worker start: completed intents are
// excluded and a transition already committed before a crash is recognized by
// its expected result version instead of being applied twice.
func (r *JournaledRepository) RecoverPending(ctx context.Context, tenantID, migrationID string) ([]Migration, error) {
	if r == nil || r.Journal == nil {
		return nil, runtime.ErrBackendUnavailable
	}
	intents, err := r.Journal.ListPendingPhaseIntents(ctx, tenantID, migrationID)
	if err != nil {
		return nil, err
	}
	result := make([]Migration, 0, len(intents))
	for _, intent := range intents {
		next, reconcileErr := r.reconcile(ctx, intent, time.Now().UTC())
		if reconcileErr != nil {
			return nil, reconcileErr
		}
		result = append(result, next)
	}
	return result, nil
}

func (r *JournaledRepository) reconcile(ctx context.Context, intent PhaseIntent, completedAt time.Time) (Migration, error) {
	if !validPhaseIntent(intent) {
		return Migration{}, runtime.ErrInvariantViolation
	}
	current, err := r.repository().Get(ctx, intent.TenantID, intent.MigrationID)
	if err != nil {
		return Migration{}, err
	}
	var next Migration
	switch {
	case current.Version == intent.Request.ExpectedVersion && current.State != intent.Request.To:
		next, err = r.repository().Transition(ctx, intent.Request)
		if err != nil {
			return Migration{}, err
		}
	case current.Version == intent.Request.ExpectedVersion+1 && current.State == intent.Request.To:
		// The authority write committed before the worker could mark the intent
		// complete. Record that fact; do not execute the phase a second time.
		next = current
	default:
		return Migration{}, runtime.ErrVersionConflict
	}
	completed, err := r.Journal.CompletePhaseIntent(ctx, intent, next, completedAt)
	if err != nil {
		return Migration{}, err
	}
	if completed.ResultVersion != next.Version || completed.Status != PhaseIntentCompleted {
		return Migration{}, runtime.ErrInvariantViolation
	}
	return next, nil
}

func (r *JournaledRepository) repository() Repository {
	if r == nil || r.Repository == nil {
		return unavailableRepository{}
	}
	return r.Repository
}

type unavailableRepository struct{}

func (unavailableRepository) Create(context.Context, CreateRequest) (Migration, error) {
	return Migration{}, runtime.ErrBackendUnavailable
}
func (unavailableRepository) Get(context.Context, string, string) (Migration, error) {
	return Migration{}, runtime.ErrBackendUnavailable
}
func (unavailableRepository) List(context.Context, string, string) ([]Migration, error) {
	return nil, runtime.ErrBackendUnavailable
}
func (unavailableRepository) Transition(context.Context, TransitionRequest) (Migration, error) {
	return Migration{}, runtime.ErrBackendUnavailable
}
func (unavailableRepository) CommitBatch(context.Context, BatchRequest) (BatchResult, error) {
	return BatchResult{}, runtime.ErrBackendUnavailable
}
func (unavailableRepository) RecordVerification(context.Context, VerificationRequest) (Migration, error) {
	return Migration{}, runtime.ErrBackendUnavailable
}
func (unavailableRepository) Pause(context.Context, ControlRequest) (Migration, error) {
	return Migration{}, runtime.ErrBackendUnavailable
}
func (unavailableRepository) Resume(context.Context, ControlRequest) (Migration, error) {
	return Migration{}, runtime.ErrBackendUnavailable
}
func (unavailableRepository) Abort(context.Context, ControlRequest) (Migration, error) {
	return Migration{}, runtime.ErrBackendUnavailable
}

func NewPhaseIntent(in TransitionRequest) (PhaseIntent, error) {
	if !validPhaseRequest(in) {
		return PhaseIntent{}, runtime.ErrInvariantViolation
	}
	request := in
	request.At = request.At.UTC()
	return PhaseIntent{TenantID: request.TenantID, MigrationID: request.MigrationID,
		IntentID: phaseIntentID(request), Request: request, RequestDigest: PhaseRequestDigest(request),
		Status: PhaseIntentPending, CreatedAt: request.At}, nil
}

// PhaseRequestDigest gives storage implementations a stable collision check
// without depending on their JSON serialization details.
func PhaseRequestDigest(in TransitionRequest) string {
	fields := []string{
		in.TenantID, in.MigrationID, strconv.FormatInt(in.ExpectedVersion, 10), string(in.To), in.At.UTC().Format(time.RFC3339Nano),
		in.SnapshotWatermark, in.DualWriteRef, strconv.FormatInt(in.Verification.SourceCount, 10), strconv.FormatInt(in.Verification.TargetCount, 10),
		in.Verification.SourceDigest, in.Verification.TargetDigest, in.Verification.SourceWatermark, in.Verification.TargetWatermark,
		in.Verification.SampleDigest, strconv.FormatInt(in.CutoverConfigVersion, 10), in.ObserveUntil.UTC().Format(time.RFC3339Nano), in.RollbackSyncWatermark,
	}
	var value strings.Builder
	for _, field := range fields {
		value.WriteString(strconv.Itoa(len(field)))
		value.WriteByte(':')
		value.WriteString(field)
	}
	digest := sha256.Sum256([]byte(value.String()))
	return hex.EncodeToString(digest[:])
}

func phaseIntentID(in TransitionRequest) string {
	return "phase-v" + strconv.FormatInt(in.ExpectedVersion, 10) + "-" + string(in.To)
}

func validPhaseRequest(in TransitionRequest) bool {
	return validText(in.TenantID, 128) && validText(in.MigrationID, 128) && in.ExpectedVersion >= 1 &&
		knownPhase(in.To) && !in.At.IsZero()
}

func validPhaseIntent(in PhaseIntent) bool {
	return validPhaseRequest(in.Request) && in.TenantID == in.Request.TenantID && in.MigrationID == in.Request.MigrationID &&
		in.IntentID == phaseIntentID(in.Request) && in.RequestDigest == PhaseRequestDigest(in.Request) &&
		(in.Status == PhaseIntentPending || (in.Status == PhaseIntentCompleted && in.ResultVersion == in.Request.ExpectedVersion+1 && !in.CompletedAt.IsZero())) &&
		!in.CreatedAt.IsZero()
}

func knownPhase(value State) bool {
	switch value {
	case StateSnapshot, StateDualWrite, StateBackfill, StateVerify, StateCutover, StateObserve, StateCleanup:
		return true
	default:
		return false
	}
}

var _ Repository = (*JournaledRepository)(nil)
