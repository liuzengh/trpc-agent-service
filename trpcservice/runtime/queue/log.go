package queue

import (
	servicelog "github.com/XnLemon/trpc-agent-service/trpcservice/log"
	"go.uber.org/zap"
)

var packageLog = servicelog.NewPrefixedLogger("[runtime/queue]")

func logWorkerStopped(worker *Worker, err error) {
	if worker == nil || err == nil {
		return
	}
	packageLog.Error("worker stopped", zap.String("tenant_id", worker.tenantID), zap.String("owner", worker.owner), zap.String("error_type", "worker_stopped"), zap.String("error_class", errorClass(err)))
}
