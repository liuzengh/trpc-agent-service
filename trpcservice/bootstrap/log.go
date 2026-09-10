package bootstrap

import (
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	servicelog "github.com/XnLemon/trpc-agent-service/trpcservice/log"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	"go.uber.org/zap"
)

var packageLog = servicelog.NewPrefixedLogger("[bootstrap]")

func logPollingAdapterStopped(adapter channels.PollingAdapter, err error) {
	if adapter == nil || err == nil {
		return
	}
	packageLog.Error("polling adapter stopped", zap.String("channel", string(adapter.Channel())), zap.String("error_type", "polling_adapter_stopped"), zap.String("error_class", observability.ErrorClass(err)))
}
