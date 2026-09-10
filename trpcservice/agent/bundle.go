package agent

import (
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	sdkartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type Bundle struct {
	AppName  string
	Root     agentcore.Agent
	Memory   memory.Service
	Artifact sdkartifact.Service
	Plugins  []plugin.Plugin
}

func (b *Bundle) NewRunner(sessions session.Service) (runner.Runner, error) {
	if b == nil || b.AppName == "" || b.Root == nil || sessions == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	// Await-user-reply routes are SDK session state, but the session service is
	// the service-owned BufferedTurn adapter. It buffers route consumption and
	// any replacement route into the same fenced CommitTurn as events and
	// state, so a recovered Worker resolves the next user turn against the
	// immutable Bundle graph rather than process-local routing state.
	options := []runner.Option{
		runner.WithSessionService(sessions),
		runner.WithAwaitUserReplyRouting(true),
	}
	if b.Memory != nil {
		options = append(options, runner.WithMemoryService(b.Memory))
	}
	if b.Artifact != nil {
		options = append(options, runner.WithArtifactService(b.Artifact))
	}
	if len(b.Plugins) != 0 {
		options = append(options, runner.WithPlugins(b.Plugins...))
	}
	return runner.NewRunner(b.AppName, b.Root, options...), nil
}

var _ profile.RuntimeBundle = (*Bundle)(nil)
