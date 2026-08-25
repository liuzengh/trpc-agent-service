package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/DocJlm/trpc-agent-service/trpcservice/tenant"
)

func TestCachedRuntimeResolverInvalidatesPublishedVersion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	repository := NewMemoryRepository()
	base := tenant.Tenant{ID: "tenant", Enabled: true, Agent: tenant.AgentProfile{ID: "assistant", Version: "1"}}
	if err := repository.SeedTenants(ctx, []tenant.Tenant{base}); err != nil {
		t.Fatal(err)
	}
	resolver := NewCachedRuntimeResolver(repository, time.Minute)
	done := make(chan error, 1)
	go func() { done <- resolver.Run(ctx) }()
	if err := resolver.waitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if profile, err := resolver.Resolve(ctx, base.ID); err != nil || profile.PublishedVersion != "1" {
		t.Fatalf("initial=%+v err=%v", profile, err)
	}
	updated := tenant.RuntimeProfileFromTenant(base)
	updated.Agent.Version = "2"
	payload, _ := json.Marshal(updated)
	if err := repository.CreateAgentVersion(ctx, base.ID, base.Agent.ID, "2", payload); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.PublishAgent(ctx, base.ID, base.Agent.ID, "2"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		profile, err := resolver.Resolve(ctx, base.ID)
		if err == nil && profile.PublishedVersion == "2" && profile.Revision == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("profile never invalidated: %+v err=%v", profile, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
