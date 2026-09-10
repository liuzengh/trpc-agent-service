package inmemory

import (
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp/contracttest"
)

func TestAgentAppRepositoryContractInMemory(t *testing.T) {
	contracttest.Run(t, func(testing.TB, string) agentapp.Repository { return New() })
}
