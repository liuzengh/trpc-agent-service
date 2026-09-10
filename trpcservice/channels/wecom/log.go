package wecom

import (
	"context"
	"errors"

	servicelog "github.com/XnLemon/trpc-agent-service/trpcservice/log"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	"go.uber.org/zap"
)

var packageLog = servicelog.NewPrefixedLogger("[wecom]")

func logIngressFailure(message, requestID, traceID, errorType string, err error) {
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	fields := []zap.Field{
		zap.String("request_id", requestID),
		zap.String("trace_id", traceID),
		zap.String("error_type", errorType),
		zap.String("error_class", observability.ErrorClass(err)),
	}
	if errors.Is(err, context.DeadlineExceeded) {
		packageLog.Warn(message, fields...)
		return
	}
	packageLog.Error(message, fields...)
}

func logIngressAuditFailure(requestID, traceID string, err error) {
	logIngressFailure("ingress audit failed", requestID, traceID, "audit_write_failed", err)
}

func logIngressBuildFailure(requestID, traceID string, err error) {
	errorType := "internal_error"
	if errors.Is(err, ErrAttachment) {
		errorType = ErrAttachment.Error()
	}
	logIngressFailure("ingress message build failed", requestID, traceID, errorType, err)
}
