package storage

import (
	"context"
	"os"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestRedisSessionBackendIsSharedAndNamespaceIsolated(t *testing.T) {
	rawURL := os.Getenv("TRPC_AGENT_REDIS_TEST_URL")
	if rawURL == "" {
		t.Skip("TRPC_AGENT_REDIS_TEST_URL is not set")
	}
	first, err := newRedisSession(rawURL, "contract-tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	secondNode, err := newRedisSession(rawURL, "contract-tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	defer secondNode.Close()
	otherTenant, err := newRedisSession(rawURL, "contract-tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	defer otherTenant.Close()

	ctx := context.Background()
	key := session.Key{AppName: "tenant/a/app/assistant", UserID: "user", SessionID: "session"}
	if _, err := first.CreateSession(ctx, key, session.StateMap{"marker": []byte("synthetic")}); err != nil {
		t.Fatal(err)
	}
	if found, err := secondNode.GetSession(ctx, key); err != nil || found == nil {
		t.Fatalf("second node session visible=%t err=%v", found != nil, err)
	}
	if found, err := otherTenant.GetSession(ctx, key); err != nil || found != nil {
		t.Fatalf("other tenant session visible=%t err=%v", found != nil, err)
	}
}
