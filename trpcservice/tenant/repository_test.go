package tenant

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

func TestMemoryRepositoryKeepsTenantVersionsAndBindingsIsolated(t *testing.T) {
	t.Parallel()

	repository := NewMemoryRepository()
	tenantAFirst := publishSnapshot(t, repository, config.TenantConfig{
		TenantID:      "tenant-a",
		AppCode:       "support",
		Status:        config.AgentActive,
		ConfigVersion: 1,
		Channels: []config.ChannelBinding{{
			Type:      config.ChannelTelegram,
			BindingID: "telegram-bot-a",
		}},
	})
	publishSnapshot(t, repository, config.TenantConfig{
		TenantID:      "tenant-b",
		AppCode:       "support",
		Status:        config.AgentActive,
		ConfigVersion: 1,
		Channels: []config.ChannelBinding{{
			Type:      config.ChannelTelegram,
			BindingID: "telegram-bot-b",
		}},
	})
	tenantASecond := publishSnapshot(t, repository, config.TenantConfig{
		TenantID:      "tenant-a",
		AppCode:       "support",
		Status:        config.AgentActive,
		ConfigVersion: 2,
		Channels: []config.ChannelBinding{{
			Type:      config.ChannelTelegram,
			BindingID: "telegram-bot-a",
		}},
	})

	resolved, err := repository.ResolveBinding(context.Background(), channels.Telegram, "telegram-bot-a")
	if err != nil {
		t.Fatalf("ResolveBinding() error = %v", err)
	}
	if got, want := resolved.Config.ConfigVersion, uint64(2); got != want {
		t.Fatalf("resolved version = %d, want %d", got, want)
	}
	if got, want := resolved.Config.TenantID, "tenant-a"; got != want {
		t.Fatalf("resolved tenant = %q, want %q", got, want)
	}

	previous, err := repository.GetVersion(context.Background(), "tenant-a", "support", 1)
	if err != nil {
		t.Fatalf("GetVersion() error = %v", err)
	}
	if previous.Checksum != tenantAFirst.Checksum {
		t.Fatalf("old snapshot checksum = %q, want %q", previous.Checksum, tenantAFirst.Checksum)
	}
	if previous.Config.ConfigVersion != 1 {
		t.Fatalf("old snapshot version = %d, want 1", previous.Config.ConfigVersion)
	}
	if tenantASecond.Checksum == tenantAFirst.Checksum {
		t.Fatal("different versions must have different checksums")
	}

	tenantB, err := repository.ResolveBinding(context.Background(), channels.Telegram, "telegram-bot-b")
	if err != nil {
		t.Fatalf("ResolveBinding() tenant B error = %v", err)
	}
	if got, want := tenantB.Config.TenantID, "tenant-b"; got != want {
		t.Fatalf("tenant B binding resolved to %q, want %q", got, want)
	}
}

func TestMemoryRepositoryRejectsVersionRollbackAndBindingReuse(t *testing.T) {
	t.Parallel()

	repository := NewMemoryRepository()
	publishSnapshot(t, repository, config.TenantConfig{
		TenantID:      "tenant-a",
		AppCode:       "support",
		Status:        config.AgentActive,
		ConfigVersion: 2,
		Channels: []config.ChannelBinding{{
			Type:      config.ChannelTelegram,
			BindingID: "telegram-bot-a",
		}},
	})

	_, err := repository.Publish(context.Background(), config.TenantConfig{
		TenantID:      "tenant-a",
		AppCode:       "support",
		Status:        config.AgentActive,
		ConfigVersion: 1,
		Channels: []config.ChannelBinding{{
			Type:      config.ChannelTelegram,
			BindingID: "telegram-bot-a",
		}},
	})
	if !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("rollback error = %v, want ErrVersionConflict", err)
	}

	_, err = repository.Publish(context.Background(), config.TenantConfig{
		TenantID:      "tenant-b",
		AppCode:       "support",
		Status:        config.AgentActive,
		ConfigVersion: 1,
		Channels: []config.ChannelBinding{{
			Type:      config.ChannelTelegram,
			BindingID: "telegram-bot-a",
		}},
	})
	if !errors.Is(err, ErrBindingConflict) {
		t.Fatalf("binding reuse error = %v, want ErrBindingConflict", err)
	}
}

func publishSnapshot(t *testing.T, repository Repository, tenantConfig config.TenantConfig) Snapshot {
	t.Helper()

	snapshot, err := repository.Publish(context.Background(), tenantConfig)
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	return snapshot
}

func TestMemoryRepositoryListApplications(t *testing.T) {
	t.Parallel()

	repository := NewMemoryRepository()
	publish := func(tenantID, appCode string, version uint64) {
		t.Helper()
		_, err := repository.Publish(context.Background(), config.TenantConfig{
			TenantID: tenantID, AppCode: appCode, Status: config.AgentActive, ConfigVersion: version,
		})
		if err != nil {
			t.Fatalf("Publish(%s/%s) error = %v", tenantID, appCode, err)
		}
	}
	publish("tenant-b", "billing", 1)
	publish("tenant-a", "support", 2)
	publish("tenant-a", "support", 3)

	all, err := repository.ListApplications(context.Background(), "")
	if err != nil {
		t.Fatalf("ListApplications() error = %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("ListApplications() = %d snapshots, want 2 active applications", len(all))
	}
	if all[0].Config.TenantID != "tenant-a" || all[0].Config.AppCode != "support" {
		t.Fatalf("first snapshot = %s/%s, want sorted tenant-a/support", all[0].Config.TenantID, all[0].Config.AppCode)
	}
	if all[0].Config.ConfigVersion != 3 {
		t.Fatalf("active version = %d, want 3 (latest publish)", all[0].Config.ConfigVersion)
	}
	filtered, err := repository.ListApplications(context.Background(), "tenant-b")
	if err != nil {
		t.Fatalf("ListApplications(tenant-b) error = %v", err)
	}
	if len(filtered) != 1 || filtered[0].Config.AppCode != "billing" {
		t.Fatalf("ListApplications(tenant-b) = %+v, want only billing", filtered)
	}
}
