package tenant

import (
	"context"
	"errors"
	"reflect"
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

func TestMemoryRepositoryListVersionsNewestFirst(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository := NewMemoryRepository()
	for version := uint64(1); version <= 4; version++ {
		publishSnapshot(t, repository, config.TenantConfig{
			TenantID: "support", AppCode: "assistant", Status: config.AgentActive, ConfigVersion: version,
		})
	}

	versions, err := repository.ListVersions(ctx, "support", "assistant", 2)
	if err != nil {
		t.Fatalf("ListVersions() error = %v", err)
	}
	want := []uint64{4, 3}
	got := make([]uint64, 0, len(versions))
	for _, snapshot := range versions {
		got = append(got, snapshot.Config.ConfigVersion)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ListVersions() = %v, want %v", got, want)
	}

	all, err := repository.ListVersions(ctx, "support", "assistant", 0)
	if err != nil || len(all) != 4 {
		t.Fatalf("ListVersions(default limit) = %d, %v", len(all), err)
	}
	if _, err := repository.ListVersions(ctx, "support", "missing", 10); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ListVersions(missing) error = %v", err)
	}
	if _, err := repository.ListVersions(ctx, "", "assistant", 10); err == nil {
		t.Fatal("ListVersions(invalid application) error = nil")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := repository.ListVersions(cancelled, "support", "assistant", 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("ListVersions(cancelled) error = %v", err)
	}
}

func TestMemoryRepositoryDiscardCandidateLifecycle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository := NewMemoryRepository()
	stable := config.TenantConfig{
		TenantID: "support", AppCode: "assistant", Status: config.AgentActive, ConfigVersion: 1,
		Channels: []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: "support-main"}},
	}
	publishSnapshot(t, repository, stable)
	candidate := stable
	candidate.ConfigVersion = 2
	if _, err := repository.Stage(ctx, candidate); err != nil {
		t.Fatalf("Stage() error = %v", err)
	}
	if err := repository.DiscardCandidate(ctx, "support", "assistant", 0); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("DiscardCandidate(zero version) error = %v", err)
	}
	if err := repository.DiscardCandidate(ctx, "support", "assistant", 3); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("DiscardCandidate(wrong version) error = %v", err)
	}

	rollout, err := repository.SetRollout(ctx, "support", "assistant", RolloutUpdate{BasisPoints: 1000})
	if err != nil {
		t.Fatalf("SetRollout() error = %v", err)
	}
	if err := repository.DiscardCandidate(ctx, "support", "assistant", 2); !errors.Is(err, ErrRolloutInProgress) {
		t.Fatalf("DiscardCandidate(active rollout) error = %v", err)
	}
	if err := repository.StopRollout(ctx, "support", "assistant", rollout.Generation); err != nil {
		t.Fatalf("StopRollout() error = %v", err)
	}
	if err := repository.DiscardCandidate(ctx, "support", "assistant", 2); err != nil {
		t.Fatalf("DiscardCandidate() error = %v", err)
	}
	if _, err := repository.GetCandidate(ctx, "support", "assistant"); !errors.Is(err, ErrCandidateNotFound) {
		t.Fatalf("GetCandidate(after discard) error = %v", err)
	}
	if err := repository.DiscardCandidate(ctx, "support", "assistant", 2); !errors.Is(err, ErrCandidateNotFound) {
		t.Fatalf("DiscardCandidate(second) error = %v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := repository.DiscardCandidate(cancelled, "support", "assistant", 2); !errors.Is(err, context.Canceled) {
		t.Fatalf("DiscardCandidate(cancelled) error = %v", err)
	}
	if err := repository.DiscardCandidate(ctx, "", "assistant", 2); err == nil {
		t.Fatal("DiscardCandidate(invalid application) error = nil")
	}
}

func TestMemoryRepositoryLookupValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository := NewMemoryRepository()

	if _, err := repository.GetActive(ctx, "support", "assistant"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetActive(missing) error = %v", err)
	}
	if _, err := repository.GetCandidate(ctx, "support", "assistant"); !errors.Is(err, ErrCandidateNotFound) {
		t.Fatalf("GetCandidate(missing) error = %v", err)
	}
	if _, err := repository.GetVersion(ctx, "support", "assistant", 0); err == nil {
		t.Fatal("GetVersion(zero) error = nil")
	}
	if _, err := repository.GetVersion(ctx, "support", "assistant", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetVersion(missing) error = %v", err)
	}
	if _, err := repository.ResolveBinding(ctx, channels.Channel("unsupported"), "support-main"); err == nil {
		t.Fatal("ResolveBinding(unsupported channel) error = nil")
	}
	if _, err := repository.ResolveBinding(ctx, channels.Telegram, ""); err == nil {
		t.Fatal("ResolveBinding(empty binding) error = nil")
	}
	if _, err := repository.ResolveBinding(ctx, channels.Telegram, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ResolveBinding(missing) error = %v", err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := repository.GetActive(cancelled, "support", "assistant"); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetActive(cancelled) error = %v", err)
	}
	if _, err := repository.GetCandidate(cancelled, "support", "assistant"); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetCandidate(cancelled) error = %v", err)
	}
	if _, err := repository.GetVersion(cancelled, "support", "assistant", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetVersion(cancelled) error = %v", err)
	}
	if _, err := repository.ResolveBinding(cancelled, channels.Telegram, "support-main"); !errors.Is(err, context.Canceled) {
		t.Fatalf("ResolveBinding(cancelled) error = %v", err)
	}
	if _, err := repository.ListApplications(cancelled, "support"); !errors.Is(err, context.Canceled) {
		t.Fatalf("ListApplications(cancelled) error = %v", err)
	}
}

func TestMemoryRepositoryStageRequiresActiveApplicationAndNextVersion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository := NewMemoryRepository()
	candidate := config.TenantConfig{TenantID: "support", AppCode: "assistant", Status: config.AgentActive, ConfigVersion: 1}
	if _, err := repository.Stage(ctx, candidate); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Stage(without stable) error = %v", err)
	}
	publishSnapshot(t, repository, candidate)
	candidate.ConfigVersion = 3
	if _, err := repository.Stage(ctx, candidate); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("Stage(skipped version) error = %v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := repository.Stage(cancelled, candidate); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stage(cancelled) error = %v", err)
	}
}
