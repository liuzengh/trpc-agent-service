package admin

import (
	"context"
	"errors"

	servicelog "github.com/XnLemon/trpc-agent-service/trpcservice/log"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	"go.uber.org/zap"
)

var packageLog = servicelog.NewPrefixedLogger("[admin]")

func logRequestFailure(requestID string, status int, errorType string, err error) {
	if err == nil || status < 500 || errors.Is(err, context.Canceled) {
		return
	}
	fields := []zap.Field{
		zap.String("request_id", requestID),
		zap.Int("status", status),
		zap.String("error_type", errorType),
		zap.String("error_class", observability.ErrorClass(err)),
	}
	if errors.Is(err, context.DeadlineExceeded) {
		packageLog.Warn("request failed", fields...)
		return
	}
	packageLog.Error("request failed", fields...)
}
