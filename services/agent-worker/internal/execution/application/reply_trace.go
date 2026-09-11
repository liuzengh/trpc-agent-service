package application

import (
	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

type TracedReply struct {
	domain.OutboxItem
	Carrier tracecontext.Carrier
}
