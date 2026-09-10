package profile

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/agent"
)

type ExecutionProfileResolver interface {
	Resolve(context.Context, ExecutionProfileKey) (ExecutionProfileSnapshot, error)
}

type ChildExecutionProfileResolver interface {
	ResolveChild(context.Context, ExecutionProfileSnapshot, agentapp.AgentNodeSpecV1) (ExecutionProfileSnapshot, error)
}

type RunOptionAssembler interface {
	Assemble(context.Context, tenant.Context, ExecutionProfileSnapshot, governance.PolicySnapshot) ([]agent.RunOption, error)
}
