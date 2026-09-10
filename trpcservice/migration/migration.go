// Package migration defines platform-owned data migration coordination values.
package migration

import (
	"errors"
	"time"
)

var (
	// ErrDrainDeadlineExceeded means accepted source executions did not finish
	// before the migration maintenance window ended.
	ErrDrainDeadlineExceeded = errors.New("data migration drain deadline exceeded")
	// ErrDrainIncomplete means accepted source executions are still draining.
	// The current owner should retry while its lease remains valid.
	ErrDrainIncomplete = errors.New("data migration source executions have not drained")
	// ErrLeaseLost means a different worker owns the migration or its lease
	// expired. The caller must stop without changing the durable record.
	ErrLeaseLost = errors.New("data migration lease lost")
)

// RetryableError tells the migration executor that an operation failed because
// its backend is temporarily unavailable and the durable phase must remain
// resumable. The copier must make each phase idempotent when returning one.
type RetryableError struct{ Err error }

func (e RetryableError) Error() string {
	if e.Err == nil {
		return "retryable data migration error"
	}
	return e.Err.Error()
}

func (e RetryableError) Unwrap() error     { return e.Err }
func (e RetryableError) IsRetryable() bool { return true }

func NewRetryableError(err error) error {
	if err == nil {
		return nil
	}
	return RetryableError{Err: err}
}

// PermanentError marks a migration failure that cannot become valid by
// retrying the same immutable configuration or backend identity.
type PermanentError struct{ Err error }

func (e PermanentError) Error() string {
	if e.Err == nil {
		return "permanent data migration error"
	}
	return e.Err.Error()
}

func (e PermanentError) Unwrap() error { return e.Err }

func (e PermanentError) IsPermanent() bool { return true }

func NewPermanentError(err error) error {
	if err == nil {
		return nil
	}
	return PermanentError{Err: err}
}

func IsPermanentError(err error) bool {
	var marker interface{ IsPermanent() bool }
	return errors.As(err, &marker) && marker.IsPermanent()
}

// Status identifies the durable lifecycle of one backend data migration.
type Status string

// Domain identifies the platform-owned data domain being migrated.
type Domain string

const (
	// DomainSession migrates durable Session data from Redis to PostgreSQL.
	DomainSession Domain = "SESSION"
	// DomainKnowledge migrates derived Knowledge vectors between Qdrant stores.
	DomainKnowledge Domain = "KNOWLEDGE"
)

const (
	// StatusPending means validation completed but request admission remains open.
	StatusPending Status = "PENDING"
	// StatusDraining rejects new admissions while accepted executions complete.
	StatusDraining Status = "DRAINING"
	// StatusCopying moves authoritative data from the source to the target.
	StatusCopying Status = "COPYING"
	// StatusVerifying validates copied authoritative data before activation.
	StatusVerifying Status = "VERIFYING"
	// StatusSucceeded means the target config was activated atomically.
	StatusSucceeded Status = "SUCCEEDED"
	// StatusFailed means the source config remains active after migration failure.
	StatusFailed Status = "FAILED"
)

// Record identifies one Tenant/App migration between immutable config versions.
type Record struct {
	ID                  string    `json:"migration_id"`
	TenantID            string    `json:"tenant_id"`
	AppID               string    `json:"app_id"`
	Domain              Domain    `json:"domain,omitempty"`
	SourceConfigVersion string    `json:"source_config_version"`
	TargetConfigVersion string    `json:"target_config_version"`
	Status              Status    `json:"status"`
	LeaseOwner          string    `json:"lease_owner,omitempty"`
	LeaseUntil          time.Time `json:"lease_until,omitempty"`
	// RunToken is a lease fencing secret. It is never part of an Admin/API
	// response, even though the worker uses it internally.
	RunToken         string     `json:"-"`
	DrainDeadline    time.Time  `json:"drain_deadline,omitempty"`
	FailureReason    string     `json:"failure_reason,omitempty"`
	TotalSessions    int64      `json:"total_sessions"`
	CopyProgress     int64      `json:"copy_progress"`
	VerifyProgress   int64      `json:"verify_progress"`
	SuccessCount     int64      `json:"success_count"`
	LastCheckpointAt *time.Time `json:"last_checkpoint_at,omitempty"`
	LastFailureStage string     `json:"last_failure_stage,omitempty"`
	CreatedAt        time.Time  `json:"created_at,omitempty"`
	UpdatedAt        time.Time  `json:"updated_at,omitempty"`
}

// EffectiveDomain keeps records created before domain-aware migrations
// session-compatible while all persisted new records carry an explicit domain.
func (r Record) EffectiveDomain() Domain {
	if r.Domain == "" {
		return DomainSession
	}
	return r.Domain
}

// Validate checks the persisted identity and lifecycle fields of Record.
func (r Record) Validate() error {
	if r.ID == "" || r.TenantID == "" || r.AppID == "" {
		return errors.New("data migration identity is required")
	}
	if r.EffectiveDomain() != DomainSession && r.EffectiveDomain() != DomainKnowledge {
		return errors.New("data migration domain is invalid")
	}
	if r.SourceConfigVersion == "" || r.TargetConfigVersion == "" {
		return errors.New("data migration config versions are required")
	}
	if r.SourceConfigVersion == r.TargetConfigVersion {
		return errors.New("data migration target config must differ from source")
	}
	if !validStatus(r.Status) {
		return errors.New("data migration status is invalid")
	}
	if r.LeaseOwner == "" && (!r.LeaseUntil.IsZero() || r.RunToken != "") {
		return errors.New("data migration lease owner is required")
	}
	if r.LeaseOwner != "" && (r.LeaseUntil.IsZero() || r.RunToken == "") {
		return errors.New("data migration lease is incomplete")
	}
	if r.TotalSessions < 0 || r.CopyProgress < 0 || r.VerifyProgress < 0 || r.SuccessCount < 0 {
		return errors.New("data migration checkpoint values must be non-negative")
	}
	if r.CopyProgress > r.TotalSessions || r.VerifyProgress > r.TotalSessions {
		return errors.New("data migration checkpoint progress exceeds total sessions")
	}
	if r.SuccessCount > r.TotalSessions {
		return errors.New("data migration success count exceeds total sessions")
	}
	return nil
}

// IsTerminal reports whether Record no longer blocks new admissions.
func (r Record) IsTerminal() bool {
	return r.Status == StatusSucceeded || r.Status == StatusFailed
}

// CanTransition reports whether the durable state machine allows next.
func (r Record) CanTransition(next Status) bool {
	switch r.Status {
	case StatusPending:
		return next == StatusDraining || next == StatusFailed
	case StatusDraining:
		return next == StatusCopying || next == StatusFailed
	case StatusCopying:
		return next == StatusVerifying || next == StatusFailed
	case StatusVerifying:
		return next == StatusSucceeded || next == StatusFailed
	default:
		return false
	}
}

func validStatus(status Status) bool {
	switch status {
	case StatusPending, StatusDraining, StatusCopying, StatusVerifying, StatusSucceeded, StatusFailed:
		return true
	default:
		return false
	}
}
