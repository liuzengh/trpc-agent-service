package inmemory_test

import (
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/contracttest"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/inmemory"
)

func TestContract(t *testing.T) {
	contracttest.Run(t, func(*testing.T) migration.Repository { return inmemory.New() })
}
