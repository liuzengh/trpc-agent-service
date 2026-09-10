package gateway

import (
	"context"
	"errors"

	servicelog "github.com/XnLemon/trpc-agent-service/trpcservice/log"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	runtimerunner "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/runner"
	"go.uber.org/zap"
)

var packageLog = servicelog.NewPrefixedLogger("[gateway]")

func logDispatchFailure(principal Principal, requestID, traceID string, err error) {
	if err == nil || isExpectedDispatchFailure(err) {
		return
	}
	fields := []zap.Field{
		zap.String("request_id", requestID),
		zap.String("trace_id", traceID),
		zap.String("tenant_id", principal.TenantID()),
		zap.String("app_id", principal.AppID()),
		zap.String("error_class", observability.ErrorClass(err)),
		zap.String("error_type", dispatchErrorType(err)),
		zap.String("error_detail", err.Error()),
	}
	if errors.Is(err, context.DeadlineExceeded) {
		packageLog.Warn("dispatch timed out", fields...)
		return
	}
	packageLog.Error("dispatch failed", fields...)
}

func isExpectedDispatchFailure(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, ErrExecutionCanceled) ||
		errors.Is(err, ErrInvalid) ||
		errors.Is(err, ErrUnauthenticated) ||
		errors.Is(err, ErrNotReady) ||
		errors.Is(err, ErrClosed) ||
		errors.Is(err, ErrRateLimited) ||
		isBudgetRejection(err) ||
		errors.Is(err, ErrDuplicateMessage) ||
		errors.Is(err, runtimerunner.ErrNotReady) ||
		errors.Is(err, runtimerunner.ErrClosed)
}

func dispatchErrorType(err error) string {
	switch {
	case errors.Is(err, ErrAuditWriteFailed):
		return ErrAuditWriteFailed.Error()
	case errors.Is(err, ErrPlanUnavailable):
		return ErrPlanUnavailable.Error()
	case errors.Is(err, ErrExecution), errors.Is(err, runtimerunner.ErrRunnerUnavailable):
		return ErrExecution.Error()
	case isBudgetRejection(err):
		return "budget"
	case errors.Is(err, runtimerunner.ErrRunnerCapacity):
		return runtimerunner.ErrRunnerCapacity.Error()
	default:
		return "internal_error"
	}
}
