package assembly

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/internal/testutil"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestFactoryCachesRunnersByTenantApplicationAndConfigVersion(t *testing.T) {
	factory := NewFactory(testutil.NewFakeModel("deterministic reply"))
	baseConfig := config.TenantConfig{
		TenantID:      "acme",
		AppCode:       "support",
		Status:        config.AgentActive,
		ConfigVersion: 1,
	}

	first, err := factory.Get(context.Background(), baseConfig)
	if err != nil {
		t.Fatalf("Get() first error = %v", err)
	}
	second, err := factory.Get(context.Background(), baseConfig)
	if err != nil {
		t.Fatalf("Get() second error = %v", err)
	}
	if first != second {
		t.Error("Get() must return the cached Runner for the same tenant, app, and version")
	}

	newVersion := baseConfig
	newVersion.ConfigVersion = 2
	versioned, err := factory.Get(context.Background(), newVersion)
	if err != nil {
		t.Fatalf("Get() new version error = %v", err)
	}
	if first == versioned {
		t.Error("Get() must not reuse a Runner across configuration versions")
	}

	otherTenant := baseConfig
	otherTenant.TenantID = "globex"
	tenantScoped, err := factory.Get(context.Background(), otherTenant)
	if err != nil {
		t.Fatalf("Get() other tenant error = %v", err)
	}
	if first == tenantScoped {
		t.Error("Get() must not reuse a Runner across tenants")
	}
}

func TestFactoryAcquireBoundsIdleConfigurationVersions(t *testing.T) {
	factory := NewFactory(testutil.NewFakeModel("deterministic reply"))
	t.Cleanup(func() { _ = factory.Close() })
	configuration := config.TenantConfig{
		TenantID: "acme", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
	}

	first, releaseFirst, err := factory.Acquire(context.Background(), configuration)
	if err != nil {
		t.Fatal(err)
	}
	releaseFirst()
	for version := uint64(2); version <= 3; version++ {
		configuration.ConfigVersion = version
		_, release, err := factory.Acquire(context.Background(), configuration)
		if err != nil {
			t.Fatalf("Acquire(version=%d) error = %v", version, err)
		}
		release()
	}

	factory.mu.Lock()
	entryCount := len(factory.entries)
	_, firstStillCached := factory.entries["acme/support@1"]
	factory.mu.Unlock()
	if entryCount != maxCachedRunnerVersionsPerApplication {
		t.Fatalf("cached Runner versions = %d, want %d", entryCount, maxCachedRunnerVersionsPerApplication)
	}
	if firstStillCached {
		t.Fatal("oldest idle Runner version was not evicted")
	}

	configuration.ConfigVersion = 1
	rebuilt, releaseRebuilt, err := factory.Acquire(context.Background(), configuration)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseRebuilt()
	if rebuilt == first {
		t.Fatal("evicted Runner version was unexpectedly reused")
	}
}

func TestFactoryAcquireNeverEvictsInFlightRunner(t *testing.T) {
	factory := NewFactory(testutil.NewFakeModel("deterministic reply"))
	t.Cleanup(func() { _ = factory.Close() })
	configuration := config.TenantConfig{
		TenantID: "acme", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
	}
	_, releaseFirst, err := factory.Acquire(context.Background(), configuration)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseFirst()

	for version := uint64(2); version <= 3; version++ {
		configuration.ConfigVersion = version
		_, release, err := factory.Acquire(context.Background(), configuration)
		if err != nil {
			t.Fatalf("Acquire(version=%d) error = %v", version, err)
		}
		release()
	}

	factory.mu.Lock()
	first := factory.entries["acme/support@1"]
	second := factory.entries["acme/support@2"]
	factory.mu.Unlock()
	if first == nil || first.refs != 1 {
		t.Fatalf("in-flight Runner entry = %+v, want one active reference", first)
	}
	if second != nil {
		t.Fatal("idle Runner was retained instead of the older in-flight version")
	}
}

func TestFactoryRunnerProducesFakeModelReply(t *testing.T) {
	factory := NewFactory(testutil.NewFakeModel("deterministic reply"))
	runner, err := factory.Get(context.Background(), config.TenantConfig{
		TenantID:      "acme",
		AppCode:       "support",
		Status:        config.AgentActive,
		ConfigVersion: 1,
	})
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	defer runner.Close()

	events, err := runner.Run(context.Background(), "user-1", "session-1", model.NewUserMessage("hello"))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	var reply string
	for event := range events {
		if event.Response == nil {
			continue
		}
		for _, choice := range event.Response.Choices {
			if choice.Message.Role == model.RoleAssistant {
				reply += choice.Message.Content
			}
		}
	}
	if got, want := reply, "deterministic reply"; got != want {
		t.Errorf("Runner reply = %q, want %q", got, want)
	}
}
