package inmemory_test

import (
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore/contracttest"
	objectmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore/inmemory"
)

func TestStoreContract(t *testing.T) {
	contracttest.Run(t, func(testing.TB) objectstore.Store {
		return objectmemory.New()
	})
}
