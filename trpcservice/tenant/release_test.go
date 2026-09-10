package tenant

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

func releaseConfig(version uint64) config.TenantConfig {
	return config.TenantConfig{
		TenantID: "tenant-a", AppCode: "support", Status: config.AgentActive, ConfigVersion: version,
		Instruction: "v", Channels: []config.ChannelBinding{{Type: config.ChannelFeishu, BindingID: "bot-a", CredentialRef: "env:FEISHU"}},
	}
}

func TestGrayReleaseLifecycleKeepsCandidateInactiveUntilPromotion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository := NewMemoryRepository()
	stable, err := repository.Publish(ctx, releaseConfig(1))
	if err != nil {
		t.Fatal(err)
	}
	candidateConfig := releaseConfig(2)
	candidateConfig.Instruction = "candidate"
	if _, err := repository.Stage(ctx, candidateConfig); err != nil {
		t.Fatal(err)
	}
	active, err := repository.GetActive(ctx, "tenant-a", "support")
	if err != nil || active.Config.ConfigVersion != 1 {
		t.Fatalf("active after stage = v%d, %v; want v1", active.Config.ConfigVersion, err)
	}

	rollout, err := repository.SetRollout(ctx, "tenant-a", "support", RolloutUpdate{
		BasisPoints: 1000, TestUserIDs: []string{"vip"}, Ingresses: []string{"web"},
	})
	if err != nil {
		t.Fatal(err)
	}
	version, variant := SelectRelease(stable, &rollout, ReleaseTarget{SessionKey: "session-a", PlatformUserID: "vip", Ingress: "web", Scope: "direct"})
	if version != 2 || variant != ReleaseCandidate {
		t.Fatalf("test-user release = v%d/%s, want v2/candidate", version, variant)
	}
	if version, variant = SelectRelease(stable, &rollout, ReleaseTarget{SessionKey: "session-a", PlatformUserID: "vip", Ingress: "feishu/bot-a", Scope: "direct"}); version != 1 || variant != ReleaseStable {
		t.Fatalf("out-of-scope test-user release = v%d/%s, want v1/stable", version, variant)
	}

	firstVersion, firstVariant := SelectRelease(stable, &rollout, ReleaseTarget{SessionKey: "session-a", PlatformUserID: "user-a", Ingress: "web", Scope: "direct"})
	rollout2, err := repository.SetRollout(ctx, "tenant-a", "support", RolloutUpdate{
		ExpectedGeneration: rollout.Generation, BasisPoints: 2000, Ingresses: []string{"web"},
	})
	if err != nil {
		t.Fatal(err)
	}
	secondVersion, secondVariant := SelectRelease(stable, &rollout2, ReleaseTarget{SessionKey: "session-a", PlatformUserID: "user-a", Ingress: "web", Scope: "direct"})
	if firstVariant == ReleaseCandidate && (secondVariant != ReleaseCandidate || secondVersion != firstVersion) {
		t.Fatalf("increasing rollout reshuffled existing candidate session: first=%v/%s second=%v/%s", firstVersion, firstVariant, secondVersion, secondVariant)
	}

	if err := repository.StopRollout(ctx, "tenant-a", "support", rollout2.Generation); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.GetRollout(ctx, "tenant-a", "support"); !errors.Is(err, ErrRolloutNotFound) {
		t.Fatalf("GetRollout after stop error = %v, want ErrRolloutNotFound", err)
	}
	active, _ = repository.GetActive(ctx, "tenant-a", "support")
	if active.Config.ConfigVersion != 1 {
		t.Fatalf("stop changed active version to %d", active.Config.ConfigVersion)
	}
	if candidate, err := repository.GetCandidate(ctx, "tenant-a", "support"); err != nil || candidate.Config.ConfigVersion != 2 {
		t.Fatalf("candidate after stop = %+v, %v; want v2", candidate.Config, err)
	}

	restarted, err := repository.SetRollout(ctx, "tenant-a", "support", RolloutUpdate{BasisPoints: 10000})
	if err != nil {
		t.Fatal(err)
	}
	if restarted.Generation <= rollout2.Generation {
		t.Fatalf("restarted rollout generation = %d, want > previous generation %d", restarted.Generation, rollout2.Generation)
	}
	promoted, err := repository.PromoteCandidate(ctx, "tenant-a", "support", 2, restarted.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if promoted.Config.ConfigVersion != 2 || promoted.Config.Instruction != "candidate" {
		t.Fatalf("promoted snapshot = %+v, want staged v2", promoted.Config)
	}
	active, _ = repository.GetActive(ctx, "tenant-a", "support")
	if active.Config.ConfigVersion != 2 {
		t.Fatalf("active after promote = v%d, want v2", active.Config.ConfigVersion)
	}
	if _, err := repository.GetCandidate(ctx, "tenant-a", "support"); !errors.Is(err, ErrCandidateNotFound) {
		t.Fatalf("GetCandidate after promote error = %v, want ErrCandidateNotFound", err)
	}
}

func TestGrayReleaseRejectsCandidateThatChangesChannelTopology(t *testing.T) {
	t.Parallel()
	repository := NewMemoryRepository()
	if _, err := repository.Publish(context.Background(), releaseConfig(1)); err != nil {
		t.Fatal(err)
	}
	candidate := releaseConfig(2)
	candidate.Channels[0].BindingID = "other-bot"
	if _, err := repository.Stage(context.Background(), candidate); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.SetRollout(context.Background(), "tenant-a", "support", RolloutUpdate{BasisPoints: 1000}); err == nil {
		t.Fatal("SetRollout() error = nil, want channel topology rejection")
	}
}

func TestGrayReleaseOnlyLatestStagedCandidateCanStart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository := NewMemoryRepository()
	if _, err := repository.Publish(ctx, releaseConfig(1)); err != nil {
		t.Fatal(err)
	}
	second := releaseConfig(2)
	second.Instruction = "candidate-2"
	if _, err := repository.Stage(ctx, second); err != nil {
		t.Fatal(err)
	}
	third := releaseConfig(3)
	third.Instruction = "candidate-3"
	if _, err := repository.Stage(ctx, third); err != nil {
		t.Fatal(err)
	}
	candidate, err := repository.GetCandidate(ctx, "tenant-a", "support")
	if err != nil || candidate.Config.ConfigVersion != 3 {
		t.Fatalf("current candidate = %+v, %v; want v3", candidate.Config, err)
	}
	rollout, err := repository.SetRollout(ctx, "tenant-a", "support", RolloutUpdate{BasisPoints: 10000})
	if err != nil || rollout.CandidateVersion != 3 {
		t.Fatalf("rollout candidate = v%d, %v; want v3", rollout.CandidateVersion, err)
	}
}

func TestGrayReleaseScopePrecedesExplicitUserTarget(t *testing.T) {
	t.Parallel()
	stable := Snapshot{Config: releaseConfig(1)}
	rollout := RolloutPolicy{
		TenantID: "tenant-a", AppCode: "support", StableVersion: 1, CandidateVersion: 2,
		BasisPoints: 0, TestUserIDs: []string{"vip"}, Ingresses: []string{"web"},
	}
	version, variant := SelectRelease(stable, &rollout, ReleaseTarget{SessionKey: "session-a", PlatformUserID: "vip", Ingress: "feishu/bot-a", Scope: "direct"})
	if version != 1 || variant != ReleaseStable {
		t.Fatalf("out-of-scope explicit user release = v%d/%s, want v1/stable", version, variant)
	}
}

func TestGrayReleaseUsesSessionStickinessAndIgnoresTestUsersForGroups(t *testing.T) {
	t.Parallel()
	stable := Snapshot{Config: releaseConfig(1)}
	rollout := RolloutPolicy{
		TenantID: "tenant-a", AppCode: "support", StableVersion: 1, CandidateVersion: 2,
		BasisPoints: 5000, TestUserIDs: []string{"vip"}, Ingresses: []string{"feishu/bot-a"},
	}
	direct := ReleaseTarget{SessionKey: "tenant-a/support/session/shared", PlatformUserID: "vip", Ingress: "feishu/bot-a", Scope: "direct"}
	if version, variant := SelectRelease(stable, &rollout, direct); version != 2 || variant != ReleaseCandidate {
		t.Fatalf("direct test user = v%d/%s, want v2/candidate", version, variant)
	}
	group := direct
	group.Scope = "group"
	group.PlatformUserID = "vip"
	groupVersion, groupVariant := SelectRelease(stable, &rollout, group)
	withoutActor := group
	withoutActor.PlatformUserID = ""
	version, variant := SelectRelease(stable, &rollout, withoutActor)
	if groupVersion != version || groupVariant != variant {
		t.Fatalf("group actor changed release: with actor v%d/%s without actor v%d/%s", groupVersion, groupVariant, version, variant)
	}
}
