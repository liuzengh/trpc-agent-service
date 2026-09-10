package tenant

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

func TestCachedRepositoryInvalidatesActiveAndBindingSnapshotsOnPublish(t *testing.T) {
	t.Parallel()

	source := &countingRepository{Repository: NewMemoryRepository()}
	repository, err := NewCachedRepository(source, NewMemoryCache(), time.Minute)
	if err != nil {
		t.Fatalf("NewCachedRepository() error = %v", err)
	}
	publishSnapshot(t, repository, config.TenantConfig{
		TenantID:      "tenant-a",
		AppCode:       "support",
		Status:        config.AgentActive,
		ConfigVersion: 1,
		Channels: []config.ChannelBinding{{
			Type:      config.ChannelTelegram,
			BindingID: "telegram-bot-a",
		}},
	})
	source.activeReads = 0
	source.bindingReads = 0

	firstActive, err := repository.GetActive(context.Background(), "tenant-a", "support")
	if err != nil {
		t.Fatalf("first GetActive() error = %v", err)
	}
	secondActive, err := repository.GetActive(context.Background(), "tenant-a", "support")
	if err != nil {
		t.Fatalf("second GetActive() error = %v", err)
	}
	if source.activeReads != 1 {
		t.Fatalf("source active reads = %d, want 1 after cache hit", source.activeReads)
	}
	if secondActive.Checksum != firstActive.Checksum {
		t.Fatal("cached active snapshot changed before publication")
	}

	firstBinding, err := repository.ResolveBinding(context.Background(), channels.Telegram, "telegram-bot-a")
	if err != nil {
		t.Fatalf("first ResolveBinding() error = %v", err)
	}
	_, err = repository.ResolveBinding(context.Background(), channels.Telegram, "telegram-bot-a")
	if err != nil {
		t.Fatalf("second ResolveBinding() error = %v", err)
	}
	if source.bindingReads != 1 {
		t.Fatalf("source binding reads = %d, want 1 after cache hit", source.bindingReads)
	}

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

	currentActive, err := repository.GetActive(context.Background(), "tenant-a", "support")
	if err != nil {
		t.Fatalf("GetActive() after publish error = %v", err)
	}
	if got, want := currentActive.Config.ConfigVersion, uint64(2); got != want {
		t.Fatalf("active version after publish = %d, want %d", got, want)
	}
	currentBinding, err := repository.ResolveBinding(context.Background(), channels.Telegram, "telegram-bot-a")
	if err != nil {
		t.Fatalf("ResolveBinding() after publish error = %v", err)
	}
	if currentBinding.Config.ConfigVersion != 2 {
		t.Fatalf("binding version after publish = %d, want 2", currentBinding.Config.ConfigVersion)
	}
	if firstBinding.Config.ConfigVersion != 1 {
		t.Fatalf("pre-publication request snapshot = %d, want 1", firstBinding.Config.ConfigVersion)
	}
}

type countingRepository struct {
	Repository
	activeReads  int
	bindingReads int
}

func (r *countingRepository) GetActive(ctx context.Context, tenantID, appCode string) (Snapshot, error) {
	r.activeReads++
	return r.Repository.GetActive(ctx, tenantID, appCode)
}

func (r *countingRepository) ResolveBinding(ctx context.Context, channel channels.Channel, bindingID string) (Snapshot, error) {
	r.bindingReads++
	return r.Repository.ResolveBinding(ctx, channel, bindingID)
}

func TestCachedRepositoryPublishDoesNotReportCommittedOutboxPublicationAsCacheFailure(t *testing.T) {
	t.Parallel()

	source := &outboxBackedRepository{Repository: NewMemoryRepository()}
	repository, err := NewCachedRepository(source, failingDeleteCache{}, time.Minute)
	if err != nil {
		t.Fatalf("NewCachedRepository() error = %v", err)
	}

	published, err := repository.Publish(context.Background(), config.TenantConfig{
		TenantID: "tenant-a", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
	})
	if err != nil {
		t.Fatalf("Publish() error = %v; want committed outbox publication to succeed despite Redis failure", err)
	}
	if published.Config.ConfigVersion != 1 {
		t.Fatalf("published configuration version = %d, want 1", published.Config.ConfigVersion)
	}
}

type outboxBackedRepository struct{ Repository }

func (outboxBackedRepository) UsesConfigCacheInvalidationOutbox() bool { return true }

type failingDeleteCache struct{}

func (failingDeleteCache) Get(context.Context, string) (Snapshot, bool, error) {
	return Snapshot{}, false, nil
}
func (failingDeleteCache) Set(context.Context, string, Snapshot, time.Duration) error { return nil }
func (failingDeleteCache) Delete(context.Context, string) error {
	return errors.New("redis unavailable")
}
