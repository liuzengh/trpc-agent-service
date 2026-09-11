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

type configurableCache struct {
	getSnapshot Snapshot
	getFound    bool
	getErr      error
	setErr      error
	deleteErr   error
}

func (c configurableCache) Get(context.Context, string) (Snapshot, bool, error) {
	return c.getSnapshot, c.getFound, c.getErr
}
func (c configurableCache) Set(context.Context, string, Snapshot, time.Duration) error {
	return c.setErr
}
func (c configurableCache) Delete(context.Context, string) error { return c.deleteErr }

func TestNewCachedRepositoryValidatesDependencies(t *testing.T) {
	t.Parallel()
	if _, err := NewCachedRepository(nil, NewMemoryCache(), time.Minute); err == nil {
		t.Fatal("nil source error = nil")
	}
	if _, err := NewCachedRepository(NewMemoryRepository(), nil, time.Minute); err == nil {
		t.Fatal("nil cache error = nil")
	}
	if _, err := NewCachedRepository(NewMemoryRepository(), NewMemoryCache(), 0); err == nil {
		t.Fatal("non-positive TTL error = nil")
	}
}

func TestCachedRepositoryCachesCandidateAndVersionReads(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	source := NewMemoryRepository()
	stable := config.TenantConfig{TenantID: "support", AppCode: "assistant", Status: config.AgentActive, ConfigVersion: 1}
	publishSnapshot(t, source, stable)
	candidate := stable
	candidate.ConfigVersion = 2
	candidate.Instruction = "候选客服配置"
	if _, err := source.Stage(ctx, candidate); err != nil {
		t.Fatal(err)
	}
	cache := NewMemoryCache()
	repository, err := NewCachedRepository(source, cache, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	gotCandidate, err := repository.GetCandidate(ctx, "support", "assistant")
	if err != nil || gotCandidate.Config.ConfigVersion != 2 {
		t.Fatalf("GetCandidate() = %+v, %v", gotCandidate.Config, err)
	}
	if _, found, err := cache.Get(ctx, versionCacheKey("support", "assistant", 2)); err != nil || !found {
		t.Fatalf("candidate cache found=%v err=%v", found, err)
	}

	first, err := repository.GetVersion(ctx, "support", "assistant", 1)
	if err != nil || first.Config.ConfigVersion != 1 {
		t.Fatalf("GetVersion() = %+v, %v", first.Config, err)
	}
	// Remove the durable copy. The second read must still use the immutable
	// version cache populated above.
	delete(source.snapshots, snapshotKey("support/assistant", 1))
	second, err := repository.GetVersion(ctx, "support", "assistant", 1)
	if err != nil || second.Config.ConfigVersion != 1 {
		t.Fatalf("GetVersion(cache hit) = %+v, %v", second.Config, err)
	}
}

func TestCachedRepositoryReportsCacheReadAndWriteFailures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	source := NewMemoryRepository()
	stable := config.TenantConfig{TenantID: "support", AppCode: "assistant", Status: config.AgentActive, ConfigVersion: 1}
	publishSnapshot(t, source, stable)
	candidate := stable
	candidate.ConfigVersion = 2
	if _, err := source.Stage(ctx, candidate); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("cache unavailable")

	readFailure, _ := NewCachedRepository(source, configurableCache{getErr: wantErr}, time.Minute)
	if _, err := readFailure.GetActive(ctx, "support", "assistant"); !errors.Is(err, wantErr) {
		t.Fatalf("GetActive(cache error) = %v", err)
	}
	if _, err := readFailure.GetVersion(ctx, "support", "assistant", 1); !errors.Is(err, wantErr) {
		t.Fatalf("GetVersion(cache error) = %v", err)
	}
	if _, err := readFailure.ResolveBinding(ctx, channels.Telegram, "support-main"); !errors.Is(err, wantErr) {
		t.Fatalf("ResolveBinding(cache error) = %v", err)
	}

	writeFailure, _ := NewCachedRepository(source, configurableCache{setErr: wantErr}, time.Minute)
	if _, err := writeFailure.GetActive(ctx, "support", "assistant"); !errors.Is(err, wantErr) {
		t.Fatalf("GetActive(set error) = %v", err)
	}
	if _, err := writeFailure.GetVersion(ctx, "support", "assistant", 1); !errors.Is(err, wantErr) {
		t.Fatalf("GetVersion(set error) = %v", err)
	}
	if _, err := writeFailure.GetCandidate(ctx, "support", "assistant"); !errors.Is(err, wantErr) {
		t.Fatalf("GetCandidate(set error) = %v", err)
	}
}

func TestCachedRepositoryStageAndRolloutDelegation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	source := NewMemoryRepository()
	stable := config.TenantConfig{
		TenantID: "support", AppCode: "assistant", Status: config.AgentActive, ConfigVersion: 1,
		Channels: []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: "support-main"}},
	}
	publishSnapshot(t, source, stable)
	repository, err := NewCachedRepository(source, NewMemoryCache(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	candidate := stable
	candidate.ConfigVersion = 2
	staged, err := repository.Stage(ctx, candidate)
	if err != nil || staged.Config.ConfigVersion != 2 {
		t.Fatalf("Stage() = %+v, %v", staged.Config, err)
	}
	rollout, err := repository.SetRollout(ctx, "support", "assistant", RolloutUpdate{BasisPoints: 1000})
	if err != nil {
		t.Fatal(err)
	}
	gotRollout, err := repository.GetRollout(ctx, "support", "assistant")
	if err != nil || gotRollout.Generation != rollout.Generation {
		t.Fatalf("GetRollout() = %+v, %v", gotRollout, err)
	}
	if err := repository.StopRollout(ctx, "support", "assistant", rollout.Generation); err != nil {
		t.Fatal(err)
	}
	if err := repository.DiscardCandidate(ctx, "support", "assistant", 2); err != nil {
		t.Fatal(err)
	}
	versions, err := repository.ListVersions(ctx, "support", "assistant", 10)
	if err != nil || len(versions) != 2 {
		t.Fatalf("ListVersions() = %d, %v", len(versions), err)
	}
	applications, err := repository.ListApplications(ctx, "support")
	if err != nil || len(applications) != 1 {
		t.Fatalf("ListApplications() = %d, %v", len(applications), err)
	}
}

func TestCachedRepositoryPromoteCandidateInvalidatesCachedRoutes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	source := NewMemoryRepository()
	stable := config.TenantConfig{
		TenantID: "support", AppCode: "assistant", Status: config.AgentActive, ConfigVersion: 1,
		Channels: []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: "support-main"}},
	}
	publishSnapshot(t, source, stable)
	candidate := stable
	candidate.ConfigVersion = 2
	if _, err := source.Stage(ctx, candidate); err != nil {
		t.Fatal(err)
	}
	cache := NewMemoryCache()
	repository, _ := NewCachedRepository(source, cache, time.Minute)
	if _, err := repository.GetActive(ctx, "support", "assistant"); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.ResolveBinding(ctx, channels.Telegram, "support-main"); err != nil {
		t.Fatal(err)
	}
	promoted, err := repository.PromoteCandidate(ctx, "support", "assistant", 2, 0)
	if err != nil || promoted.Config.ConfigVersion != 2 {
		t.Fatalf("PromoteCandidate() = %+v, %v", promoted.Config, err)
	}
	if _, found, _ := cache.Get(ctx, activeCacheKey("support", "assistant")); found {
		t.Fatal("active cache survived promotion")
	}
	if _, found, _ := cache.Get(ctx, bindingCacheKey(channels.Telegram, "support-main")); found {
		t.Fatal("binding cache survived promotion")
	}
}

func TestMemoryCacheLifecycleExpiryAndIsolation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cache := NewMemoryCache()
	now := time.Date(2026, 9, 11, 0, 0, 0, 0, time.UTC)
	cache.now = func() time.Time { return now }
	snapshot, err := newSnapshot(config.TenantConfig{TenantID: "support", AppCode: "assistant", Status: config.AgentActive, ConfigVersion: 1}, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Set(ctx, "support-key", snapshot, time.Minute); err != nil {
		t.Fatal(err)
	}
	got, found, err := cache.Get(ctx, "support-key")
	if err != nil || !found || got.Config.ConfigVersion != 1 {
		t.Fatalf("Get() = %+v, %v, %v", got.Config, found, err)
	}
	got.Config.Instruction = "mutated"
	again, found, _ := cache.Get(ctx, "support-key")
	if !found || again.Config.Instruction != "" {
		t.Fatal("cache returned aliased snapshot")
	}
	now = now.Add(time.Minute)
	if _, found, err := cache.Get(ctx, "support-key"); err != nil || found {
		t.Fatalf("expired Get() found=%v err=%v", found, err)
	}
	if err := cache.Set(ctx, "support-key", snapshot, 0); err == nil {
		t.Fatal("Set(non-positive TTL) error = nil")
	}
	if err := cache.Delete(ctx, "support-key"); err != nil {
		t.Fatal(err)
	}
	if err := cache.Delete(ctx, "support-key"); err != nil {
		t.Fatalf("repeated Delete() error = %v", err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := cache.Get(cancelled, "support-key"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get(cancelled) error = %v", err)
	}
	if err := cache.Set(cancelled, "support-key", snapshot, time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("Set(cancelled) error = %v", err)
	}
	if err := cache.Delete(cancelled, "support-key"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Delete(cancelled) error = %v", err)
	}
}
