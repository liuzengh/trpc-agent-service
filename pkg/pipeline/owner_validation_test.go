package pipeline

import (
	"strings"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/channel"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

func TestValidateDerivedOwnerIncludesWorkerSuffix(t *testing.T) {
	if err := validateDerivedOwner(strings.Repeat("x", 254), 1); err != nil {
		t.Fatalf("exact derived owner boundary rejected: %v", err)
	}
	for _, owner := range []string{strings.Repeat("x", 255), strings.Repeat("界", 85)} {
		if err := validateDerivedOwner(owner, 1); err == nil {
			t.Fatalf("owner length overflow accepted: %d bytes", len(owner))
		}
	}
	if err := validateDerivedOwner(strings.Repeat("x", 246), 100); err != nil {
		t.Fatalf("multi-digit suffix boundary rejected: %v", err)
	}
}

func TestPipelineConstructorsRejectInvalidDerivedOwner(t *testing.T) {
	store := reliable.NewMemoryStore()
	tenants := &fakeTenantService{value: &tenant.Tenant{ID: "tenant-a"}}
	workerClient := &fakeWorker{}
	registry := channel.NewAdapterRegistry()
	for _, owner := range []string{strings.Repeat("x", 255), "bad\x00owner", string([]byte{0xff})} {
		if _, err := NewConsumer(store, tenants, workerClient, ConsumerConfig{Owner: owner, Concurrency: 1}); err == nil {
			t.Fatalf("consumer accepted invalid owner %q", owner)
		}
		if _, err := NewDelivery(store, tenants, registry, DeliveryConfig{Owner: owner, Concurrency: 1}); err == nil {
			t.Fatalf("delivery accepted invalid owner %q", owner)
		}
	}
}
