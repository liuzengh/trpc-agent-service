package agent

import (
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	trpcrunner "trpc.group/trpc-go/trpc-agent-go/runner"
)

// Runner is the external Agent runtime lifecycle contract retained by the
// service Agent adapter. Runtime scheduling depends on this service-owned
// name instead of importing Agent-Go directly.
type Runner = trpcrunner.Runner

// Message is the external Agent message payload carried through the Agent
// adapter. Runtime packages use this alias without importing Agent-Go.
type Message = trpcmodel.Message
