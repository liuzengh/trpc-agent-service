package inmemory

import (
	"testing"

	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/session/contracttest"
)

func TestAtomicSessionStoreContract(t *testing.T) {
	contracttest.Run(t, func(testing.TB, sessionstore.SessionKey, map[string]uint64) sessionstore.AtomicSessionStore {
		return New()
	})
}
