package wecom_aibot

import (
	"context"
	"errors"

	servicelog "github.com/XnLemon/trpc-agent-service/trpcservice/log"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	"go.uber.org/zap"
)

var packageLog = servicelog.NewPrefixedLogger("[wecom-aibot]")

func logCallbackFailure(message, requestID, errorType string, err error) {
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	fields := []zap.Field{zap.String("request_id", requestID), zap.String("error_type", errorType), zap.String("error_class", observability.ErrorClass(err))}
	if errors.Is(err, context.DeadlineExceeded) {
		packageLog.Warn(message, fields...)
		return
	}
	packageLog.Error(message, fields...)
}

func logCallbackNilStream(requestID string) {
	packageLog.Error("callback dispatch returned nil stream", zap.String("request_id", requestID), zap.String("error_type", "invalid_dispatch_result"))
}
