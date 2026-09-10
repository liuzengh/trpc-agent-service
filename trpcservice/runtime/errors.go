package runtime

import "errors"

var (
	ErrUnsupportedSchema       = errors.New("unsupported schema version")
	ErrInvalidEnvelope         = errors.New("invalid execution envelope")
	ErrTenantScope             = errors.New("tenant scope mismatch")
	ErrCapabilityUnsupported   = errors.New("required capability is unsupported")
	ErrVersionMismatch         = errors.New("fixed version mismatch")
	ErrNotFound                = errors.New("not found")
	ErrVersionConflict         = errors.New("version conflict")
	ErrStaleFence              = errors.New("stale fence")
	ErrInputNotReady           = errors.New("input is not ready")
	ErrPreprocessNotReady      = errors.New("preprocess is not ready")
	ErrInputBlocked            = errors.New("input park deadline or attempts exhausted")
	ErrCancelRequested         = errors.New("execution cancellation requested")
	ErrAlreadyTerminal         = errors.New("input is already terminal")
	ErrLeaseLost               = errors.New("lease lost")
	ErrCommitConflict          = errors.New("commit conflict")
	ErrInvariantViolation      = errors.New("storage invariant violation")
	ErrIdempotencyCollision    = errors.New("idempotency collision")
	ErrBackendUnavailable      = errors.New("backend unavailable")
	ErrExecutionBudgetExceeded = errors.New("execution budget exceeded")
)
