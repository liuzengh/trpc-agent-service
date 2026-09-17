package inmemory

import (
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit/query/contracttest"
)

func TestInMemoryAuditQueryContract(t *testing.T) {
	store := New()
	contracttest.Suite(t, store, store.Seed)
}
