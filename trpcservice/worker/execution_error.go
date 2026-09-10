package worker

import (
	"context"
	"errors"

	"github.com/liuzengh/trpc-agent-service/trpcservice/guardrail"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// ErrSessionLeaseLost means the shared session lock was lost while the
// execution was running. The caller must not treat the resulting cancellation
// as an ordinary permanent failure.
var ErrSessionLeaseLost = errors.New("session lease lost")

// ErrUnsupportedAttachment identifies a model capability mismatch without
// coupling the worker package to the runtime/model adapter package.
var ErrUnsupportedAttachment = errors.New("unsupported model attachment/file capability")

const unsupportedAttachmentErrorType = "unsupported_attachment"

// RetryableExecutionError marks a failure that is safe to try again before a
// runner has started any externally visible work.
type RetryableExecutionError struct{ Err error }

func (e RetryableExecutionError) Error() string {
	if e.Err == nil {
		return "retryable execution error"
	}
	return e.Err.Error()
}

func (e RetryableExecutionError) Unwrap() error { return e.Err }

// PermanentExecutionError marks validation, policy, budget, and other
// failures that must not create another execution attempt.
type PermanentExecutionError struct{ Err error }

func (e PermanentExecutionError) Error() string {
	if e.Err == nil {
		return "permanent execution error"
	}
	return e.Err.Error()
}

func (e PermanentExecutionError) Unwrap() error { return e.Err }

// SideEffectUncertainError means an external side effect may have happened;
// automatic whole-agent retry is forbidden.
type SideEffectUncertainError struct{ Err error }

func (e SideEffectUncertainError) Error() string {
	if e.Err == nil {
		return "execution side effect is uncertain"
	}
	return e.Err.Error()
}

func (e SideEffectUncertainError) Unwrap() error { return e.Err }

func NewRetryableExecutionError(err error) error {
	if err == nil {
		return nil
	}
	return RetryableExecutionError{Err: err}
}

func NewPermanentExecutionError(err error) error {
	if err == nil {
		return nil
	}
	return PermanentExecutionError{Err: err}
}

func NewSideEffectUncertainError(err error) error {
	if err == nil {
		return nil
	}
	return SideEffectUncertainError{Err: err}
}

func IsRetryableExecutionError(err error) bool {
	var classified RetryableExecutionError
	if errors.As(err, &classified) {
		return true
	}
	var pointer *RetryableExecutionError
	return errors.As(err, &pointer)
}

func IsPermanentExecutionError(err error) bool {
	var classified PermanentExecutionError
	if errors.As(err, &classified) {
		return true
	}
	var pointer *PermanentExecutionError
	return errors.As(err, &pointer) ||
		errors.Is(err, tenant.ErrBudgetExceeded) ||
		errors.Is(err, tenant.ErrQuotaExceeded) ||
		errors.Is(err, guardrail.ErrInputBlocked) ||
		errors.Is(err, ErrExecutionCanceled) ||
		errors.Is(err, context.Canceled)
}

func IsSideEffectUncertainError(err error) bool {
	var classified SideEffectUncertainError
	if errors.As(err, &classified) {
		return true
	}
	var pointer *SideEffectUncertainError
	return errors.As(err, &pointer)
}
