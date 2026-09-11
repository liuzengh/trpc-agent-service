// Package tooloperation provides a durable idempotency ledger for external
// tool side effects. The ledger deliberately stores only hashes and bounded,
// redacted metadata; raw tool arguments, error strings, credentials, and
// provider response bodies are outside its persistence contract.
package tooloperation

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
)

const (
	// StateReserved means the operation may be leased, but no external side
	// effect has started. Expiry in this state is safe to retry.
	StateReserved State = "reserved"
	// StateExecuting means the durable dispatch fence was crossed. If its
	// outcome cannot be proved, the operation must become unknown.
	StateExecuting State = "executing"
	// StateConfirmed is a successful terminal outcome. Re-reservation returns
	// its redacted Confirmation instead of executing the tool again.
	StateConfirmed State = "confirmed"
	// StateRetryableNotApplied proves that no side effect was applied and is
	// therefore the only automatically retryable outcome.
	StateRetryableNotApplied State = "retryable_not_applied"
	// StatePermanentRejected is a terminal, non-retryable rejection.
	StatePermanentRejected State = "permanent_rejected"
	// StateUnknown means a side effect might have happened. It is never leased
	// automatically and requires an explicit audited resolution.
	StateUnknown State = "unknown"
)

const (
	AttemptReserved  AttemptPhase = "reserved"
	AttemptExecuting AttemptPhase = "executing"
	AttemptFinished  AttemptPhase = "finished"
)

const (
	ResolveConfirm         ResolutionAction = "confirm"
	ResolveRetryNotApplied ResolutionAction = "retry_not_applied"
	ResolveReject          ResolutionAction = "reject"
)

const (
	maxTenantIDBytes       = 256
	maxOperationKeyBytes   = 256
	maxOwnerBytes          = 512
	maxSafeLabelBytes      = 256
	maxLeaseBatch          = 100
	sha256HexBytes         = sha256.Size * 2
	operationKeyDomain     = "trpc-tool-operation/v1"
	leaseExpiredBeforeExec = "lease_expired_before_execution"
	leaseExpiredAfterExec  = "lease_expired_after_execution"
)

var (
	// ErrInvalidRequest indicates malformed hashes, identifiers, times, or an
	// unsupported state transition request. It never includes caller data.
	ErrInvalidRequest = errors.New("tooloperation: invalid request")
	// ErrConflict means the same tenant operation key was reserved with
	// different immutable metadata or a different payload hash.
	ErrConflict = errors.New("tooloperation: operation identity conflict")
	// ErrNotFound is returned without revealing whether another tenant owns an
	// operation with the same key.
	ErrNotFound = errors.New("tooloperation: operation not found")
	// ErrLeaseLost fences an expired, replaced, or otherwise stale attempt.
	ErrLeaseLost = errors.New("tooloperation: lease lost")
	// ErrInvalidTransition means the operation is no longer in the state
	// required by the requested compare-and-set transition.
	ErrInvalidTransition = errors.New("tooloperation: invalid transition")
	// ErrUnknownRequiresResolution documents the fail-closed retry rule for an
	// ambiguous external outcome.
	ErrUnknownRequiresResolution = errors.New("tooloperation: unknown outcome requires explicit resolution")
)

// State is the durable operation state.
type State string

// AttemptPhase identifies whether an attempt crossed the external-dispatch
// boundary. It is intentionally separate from State so an unexecuted lease can
// expire safely without making the operation ambiguous.
type AttemptPhase string

// ResolutionAction is an explicit operator conclusion about an unknown
// result. ResolveRetryNotApplied asserts that external evidence proves the
// operation did not take effect.
type ResolutionAction string

// Metadata is the complete non-secret description persisted beside an
// operation. ToolName and OperationClass are constrained labels; TargetHash
// is an optional SHA-256 of a sensitive target identifier.
type Metadata struct {
	ToolName       string `json:"tool_name"`
	OperationClass string `json:"operation_class,omitempty"`
	TargetHash     string `json:"target_hash,omitempty"`
}

// Confirmation is the replay-safe terminal result. ReplayReference is an
// opaque identifier for separately protected result storage; the ledger never
// stores a raw provider response.
type Confirmation struct {
	ResultHash      string `json:"result_hash"`
	ReplayReference string `json:"replay_reference,omitempty"`
	OutcomeCode     string `json:"outcome_code,omitempty"`
}

// Record contains only hashes, bounded redacted labels, state, and timing
// metadata. In particular, it never contains the lease owner or raw payload.
type Record struct {
	TenantID       string        `json:"tenant_id"`
	OperationKey   string        `json:"operation_key"`
	PayloadHash    string        `json:"payload_hash"`
	Metadata       Metadata      `json:"metadata"`
	State          State         `json:"state"`
	StateVersion   int64         `json:"state_version"`
	AttemptCount   int           `json:"attempt_count"`
	CurrentAttempt int           `json:"current_attempt,omitempty"`
	LeaseExpiresAt time.Time     `json:"lease_expires_at,omitempty"`
	NextAttemptAt  time.Time     `json:"next_attempt_at,omitempty"`
	OutcomeCode    string        `json:"outcome_code,omitempty"`
	Confirmation   *Confirmation `json:"confirmation,omitempty"`
	CreatedAt      time.Time     `json:"created_at"`
	UpdatedAt      time.Time     `json:"updated_at"`
}

// Attempt is immutable attempt history plus its current phase. OwnerHash is a
// SHA-256 of the capability, never the raw lease owner.
type Attempt struct {
	TenantID        string       `json:"tenant_id"`
	OperationKey    string       `json:"operation_key"`
	AttemptNo       int          `json:"attempt_no"`
	OwnerHash       string       `json:"owner_hash"`
	Phase           AttemptPhase `json:"phase"`
	Outcome         State        `json:"outcome,omitempty"`
	OutcomeCode     string       `json:"outcome_code,omitempty"`
	ResultHash      string       `json:"result_hash,omitempty"`
	ReplayReference string       `json:"replay_reference,omitempty"`
	StartedAt       time.Time    `json:"started_at"`
	ExecutingAt     time.Time    `json:"executing_at,omitempty"`
	FinishedAt      time.Time    `json:"finished_at,omitempty"`
	RetryAt         time.Time    `json:"retry_at,omitempty"`
}

// Fence is the attempt capability required by every lease-based mutation.
// Owner must remain process-local and is excluded from JSON serialization.
type Fence struct {
	TenantID     string `json:"tenant_id"`
	OperationKey string `json:"operation_key"`
	AttemptNo    int    `json:"attempt_no"`
	Owner        string `json:"-"`
}

// LeasedOperation pairs a sanitized record with its process-local fence.
type LeasedOperation struct {
	Record Record `json:"record"`
	Fence  Fence  `json:"fence"`
}

// ReserveRequest declares an immutable operation identity. Callers hash raw
// canonical arguments with HashPayload before crossing this API boundary.
type ReserveRequest struct {
	TenantID     string
	OperationKey string
	PayloadHash  string
	Metadata     Metadata
}

// ReserveResult reports whether the operation already existed. Replayed is
// true only for a confirmed operation and includes its sanitized outcome.
type ReserveResult struct {
	Record       Record
	Existing     bool
	Replayed     bool
	Confirmation *Confirmation
}

// LeaseRequest selects due operations for exactly one tenant.
type LeaseRequest struct {
	TenantID string
	Owner    string
	Now      time.Time
	TTL      time.Duration
	Limit    int
}

// FinishRequest records a proved outcome for the current attempt. ResultHash
// and ReplayReference are used only for confirmed replay. OutcomeCode is a
// bounded redacted code, never an error string.
type FinishRequest struct {
	Fence           Fence
	Outcome         State
	OutcomeCode     string
	ResultHash      string
	ReplayReference string
	RetryAt         time.Time
}

// ReclaimResult separates safe pre-execution expiry from ambiguous
// post-execution expiry.
type ReclaimResult struct {
	Retryable int
	Unknown   int
}

// ResolveRequest is an audited compare-and-set conclusion for StateUnknown.
// ActorHash must be a SHA-256; raw operator identities and free-form reasons
// are deliberately rejected by this contract.
type ResolveRequest struct {
	ResolutionID    string
	TenantID        string
	OperationKey    string
	ExpectedVersion int64
	Action          ResolutionAction
	ActorHash       string
	ReasonCode      string
	ResultHash      string
	ReplayReference string
	RetryAt         time.Time
}

// Resolution is the persisted, sanitized audit record for an unknown result.
type Resolution struct {
	ResolutionID    string           `json:"resolution_id"`
	TenantID        string           `json:"tenant_id"`
	OperationKey    string           `json:"operation_key"`
	FromVersion     int64            `json:"from_version"`
	ToVersion       int64            `json:"to_version"`
	Action          ResolutionAction `json:"action"`
	ActorHash       string           `json:"actor_hash"`
	ReasonCode      string           `json:"reason_code"`
	ResultHash      string           `json:"result_hash,omitempty"`
	ReplayReference string           `json:"replay_reference,omitempty"`
	RetryAt         time.Time        `json:"retry_at,omitempty"`
	ResolvedAt      time.Time        `json:"resolved_at"`
}

// Ledger is the storage-neutral operation state-machine contract.
type Ledger interface {
	Reserve(context.Context, ReserveRequest, time.Time) (*ReserveResult, error)
	Lease(context.Context, LeaseRequest) ([]LeasedOperation, error)
	// LeaseOperation atomically leases exactly one caller-selected operation.
	// It never substitutes another due operation from the same tenant. This is
	// the execution-path primitive used after a model tool call has reserved its
	// stable operation key; batch Lease remains available for background work.
	LeaseOperation(context.Context, string, string, string, time.Time, time.Duration) (*LeasedOperation, error)
	Renew(context.Context, Fence, time.Time, time.Duration) error
	MarkExecuting(context.Context, Fence, time.Time) (*Record, error)
	Finish(context.Context, FinishRequest, time.Time) (*Record, error)
	Get(context.Context, string, string) (*Record, error)
	GetAttempt(context.Context, string, string, int) (*Attempt, error)
	ReclaimExpired(context.Context, time.Time, int) (ReclaimResult, error)
	ListUnknown(context.Context, string, int) ([]Record, error)
	ResolveUnknown(context.Context, ResolveRequest, time.Time) (*Record, error)
}

// HashPayload returns the lowercase SHA-256 accepted by ReserveRequest. It is
// safe to use with canonicalized tool arguments; only the digest is persisted.
func HashPayload(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

// DeriveOperationKey creates a stable, opaque identity from a tenant, stable
// turn, stable tool-call ID, and tool name. Length framing avoids ambiguous
// concatenation and including the tenant prevents accidental cross-tenant key
// reuse even before the storage isolation fence.
func DeriveOperationKey(tenantID, turnID, toolCallID, toolName string) (string, error) {
	if !validTenant(tenantID) || !validRequiredOpaque(turnID, maxSafeLabelBytes) ||
		!validRequiredOpaque(toolCallID, maxSafeLabelBytes) || !validSafeLabel(toolName, maxSafeLabelBytes) {
		return "", ErrInvalidRequest
	}
	h := sha256.New()
	writeFramed(h, operationKeyDomain)
	writeFramed(h, tenantID)
	writeFramed(h, turnID)
	writeFramed(h, toolCallID)
	writeFramed(h, toolName)
	return "toolop_" + hex.EncodeToString(h.Sum(nil)), nil
}

type framedWriter interface {
	Write([]byte) (int, error)
}

func writeFramed(w framedWriter, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = w.Write(length[:])
	_, _ = w.Write([]byte(value))
}

func validateReserve(req ReserveRequest) error {
	if !validTenant(req.TenantID) || !validOperationKey(req.OperationKey) || !validSHA256(req.PayloadHash) {
		return ErrInvalidRequest
	}
	if !validSafeLabel(req.Metadata.ToolName, maxSafeLabelBytes) ||
		(req.Metadata.OperationClass != "" && !validSafeLabel(req.Metadata.OperationClass, maxSafeLabelBytes)) ||
		(req.Metadata.TargetHash != "" && !validSHA256(req.Metadata.TargetHash)) {
		return ErrInvalidRequest
	}
	return nil
}

func validateLease(req LeaseRequest) error {
	if !validTenant(req.TenantID) || strings.TrimSpace(req.Owner) == "" || len(req.Owner) > maxOwnerBytes ||
		req.Now.IsZero() || req.TTL <= 0 || req.Limit <= 0 || req.Limit > maxLeaseBatch {
		return ErrInvalidRequest
	}
	return nil
}

func validateLeaseOperation(
	tenantID, operationID, owner string,
	now time.Time,
	ttl time.Duration,
) error {
	if !validOperationKey(operationID) {
		return ErrInvalidRequest
	}
	return validateLease(LeaseRequest{
		TenantID: tenantID,
		Owner:    owner,
		Now:      now,
		TTL:      ttl,
		Limit:    1,
	})
}

func validateFence(f Fence) error {
	if !validTenant(f.TenantID) || !validOperationKey(f.OperationKey) ||
		strings.TrimSpace(f.Owner) == "" || len(f.Owner) > maxOwnerBytes || f.AttemptNo <= 0 {
		return ErrInvalidRequest
	}
	return nil
}

func validateFinish(req FinishRequest, now time.Time) error {
	if err := validateFence(req.Fence); err != nil || now.IsZero() {
		return ErrInvalidRequest
	}
	if req.OutcomeCode != "" && !validSafeLabel(req.OutcomeCode, maxSafeLabelBytes) {
		return ErrInvalidRequest
	}
	if req.ResultHash != "" && !validSHA256(req.ResultHash) {
		return ErrInvalidRequest
	}
	if req.ReplayReference != "" && !validSafeLabel(req.ReplayReference, maxSafeLabelBytes) {
		return ErrInvalidRequest
	}
	switch req.Outcome {
	case StateConfirmed:
		if req.ResultHash == "" || !req.RetryAt.IsZero() {
			return ErrInvalidRequest
		}
	case StateRetryableNotApplied:
		if req.OutcomeCode == "" || req.RetryAt.IsZero() || req.RetryAt.Before(now) ||
			req.ResultHash != "" || req.ReplayReference != "" {
			return ErrInvalidRequest
		}
	case StatePermanentRejected, StateUnknown:
		if req.OutcomeCode == "" || !req.RetryAt.IsZero() || req.ResultHash != "" ||
			req.ReplayReference != "" {
			return ErrInvalidRequest
		}
	default:
		return ErrInvalidRequest
	}
	return nil
}

func validateResolve(req ResolveRequest, now time.Time) error {
	if !validSafeLabel(req.ResolutionID, maxSafeLabelBytes) || !validTenant(req.TenantID) ||
		!validOperationKey(req.OperationKey) || req.ExpectedVersion <= 0 || !validSHA256(req.ActorHash) ||
		!validSafeLabel(req.ReasonCode, maxSafeLabelBytes) || now.IsZero() {
		return ErrInvalidRequest
	}
	if req.ResultHash != "" && !validSHA256(req.ResultHash) {
		return ErrInvalidRequest
	}
	if req.ReplayReference != "" && !validSafeLabel(req.ReplayReference, maxSafeLabelBytes) {
		return ErrInvalidRequest
	}
	switch req.Action {
	case ResolveConfirm:
		if req.ResultHash == "" || !req.RetryAt.IsZero() {
			return ErrInvalidRequest
		}
	case ResolveRetryNotApplied:
		if req.ResultHash != "" || req.ReplayReference != "" || req.RetryAt.IsZero() || req.RetryAt.Before(now) {
			return ErrInvalidRequest
		}
	case ResolveReject:
		if req.ResultHash != "" || req.ReplayReference != "" || !req.RetryAt.IsZero() {
			return ErrInvalidRequest
		}
	default:
		return ErrInvalidRequest
	}
	return nil
}

func validTenant(value string) bool {
	return validSafeLabel(value, maxTenantIDBytes)
}

func validOperationKey(value string) bool {
	return validSafeLabel(value, maxOperationKeyBytes)
}

func validRequiredOpaque(value string, maxBytes int) bool {
	return strings.TrimSpace(value) != "" && len(value) <= maxBytes
}

func validSafeLabel(value string, maxBytes int) bool {
	if value == "" || len(value) > maxBytes {
		return false
	}
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			continue
		}
		switch r {
		case '-', '_', '.', ':', '/':
			continue
		default:
			return false
		}
	}
	return true
}

func validSHA256(value string) bool {
	if len(value) != sha256HexBytes {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && strings.ToLower(value) == value
}

func ownerHash(owner string) string {
	return HashPayload([]byte(owner))
}

func sameMetadata(a, b Metadata) bool {
	return a.ToolName == b.ToolName && a.OperationClass == b.OperationClass && a.TargetHash == b.TargetHash
}

func cloneConfirmation(value *Confirmation) *Confirmation {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneRecord(value Record) Record {
	value.Confirmation = cloneConfirmation(value.Confirmation)
	return value
}

func operationKey(tenantID, operationID string) string {
	// Both fields are length-bounded. The NUL cannot pass validSafeLabel for an
	// operation key and tenant IDs are internal registry identifiers.
	return tenantID + "\x00" + operationID
}

func resolutionKey(tenantID, resolutionID string) string {
	return tenantID + "\x00" + resolutionID
}

func fixedError(prefix string, err error) error {
	// Prefixes are compile-time constants. Never append caller-controlled data.
	return fmt.Errorf("tooloperation: %s: %w", prefix, err)
}
