package checkpointredis

import (
	"context"
	"testing"
	"time"

	redisclient "github.com/redis/go-redis/v9"
)

func TestResolverScopesTenantConfigAndDeclaredNamespace(t *testing.T) {
	client := redisclient.NewClient(&redisclient.Options{Addr: "127.0.0.1:1"})
	defer client.Close()
	resolver := Resolver{Client: client, TTL: time.Hour}

	firstValue, err := resolver.ResolveCheckpointSaver(context.Background(), "tenant-a", "orders", 7)
	if err != nil {
		t.Fatal(err)
	}
	secondValue, err := resolver.ResolveCheckpointSaver(context.Background(), "tenant-b", "orders", 7)
	if err != nil {
		t.Fatal(err)
	}
	thirdValue, err := resolver.ResolveCheckpointSaver(context.Background(), "tenant-a", "orders", 8)
	if err != nil {
		t.Fatal(err)
	}
	fourthValue, err := resolver.ResolveCheckpointSaver(context.Background(), "tenant-a", "billing", 7)
	if err != nil {
		t.Fatal(err)
	}
	values := []*saver{firstValue.(*saver), secondValue.(*saver), thirdValue.(*saver), fourthValue.(*saver)}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value.scope == "" {
			t.Fatal("empty checkpoint scope")
		}
		seen[value.scope] = struct{}{}
		if err := value.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != len(values) {
		t.Fatalf("checkpoint scopes collided: %#v", values)
	}
}

func TestResolverRejectsIncompleteScope(t *testing.T) {
	client := redisclient.NewClient(&redisclient.Options{Addr: "127.0.0.1:1"})
	defer client.Close()
	for _, resolver := range []Resolver{{}, {Client: client}, {Client: client, TTL: time.Hour}} {
		if _, err := resolver.ResolveCheckpointSaver(context.Background(), "", "namespace", 0); err == nil {
			t.Fatal("expected invalid resolver scope to fail closed")
		}
	}
}

func TestCheckpointKeysDoNotExposeCoordinates(t *testing.T) {
	value := &saver{scope: digest("tenant"), ttl: time.Hour}
	key := value.checkpointKey("lineage-{raw-7f31}", "namespace-raw-2a91", "id-raw-91bc")
	for _, secret := range []string{"raw-7f31", "namespace-raw-2a91", "id-raw-91bc"} {
		if contains(key, secret) {
			t.Fatalf("checkpoint key exposed %q: %q", secret, key)
		}
	}
}

func contains(value, fragment string) bool {
	for index := 0; index+len(fragment) <= len(value); index++ {
		if value[index:index+len(fragment)] == fragment {
			return true
		}
	}
	return false
}
