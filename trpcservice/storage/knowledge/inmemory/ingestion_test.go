package inmemory

import (
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/knowledge/contracttest"
)

func TestInMemoryKnowledgeIngestionContract(t *testing.T) {
	contracttest.Suite(t, New(), "kq-tenant")
}
