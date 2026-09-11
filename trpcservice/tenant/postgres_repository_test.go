package tenant

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/liuzengh/trpc-agent-service/migrations"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

func postgresTestRepository(t *testing.T) *PostgresRepository {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := migrations.Apply(context.Background(), database); err != nil {
		t.Fatalf("migrations.Apply() error = %v", err)
	}
	repository, err := NewPostgresRepository(database)
	if err != nil {
		t.Fatalf("NewPostgresRepository() error = %v", err)
	}
	return repository
}

func seedUnlinkedChannelIdentity(t *testing.T, repository *PostgresRepository, tenantID, channelType, bindingID, externalUserID string) {
	t.Helper()
	tx, err := repository.beginTenantTransaction(context.Background(), tenantID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(context.Background(), `
INSERT INTO channel_identities (tenant_id, channel_type, binding_id, external_user_id)
VALUES ($1,$2,$3,$4)`, tenantID, channelType, bindingID, externalUserID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func countChannelIdentities(t *testing.T, repository *PostgresRepository, tenantID, channelType, bindingID string) int {
	t.Helper()
	tx, err := repository.beginTenantTransaction(context.Background(), tenantID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	var count int
	if err := tx.QueryRowContext(context.Background(), `
SELECT COUNT(*) FROM channel_identities
WHERE tenant_id=$1 AND channel_type=$2 AND binding_id=$3`, tenantID, channelType, bindingID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestPostgresRepositoryPublishAndQueries(t *testing.T) {
	repository := postgresTestRepository(t)
	ctx := context.Background()
	tenantID := "support-publish-" + uuid.NewString()
	bindingID := "support-publish-bot-" + uuid.NewString()

	published, err := repository.Publish(ctx, config.TenantConfig{
		TenantID: tenantID, AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Channels: []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: bindingID}},
	})
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	active, err := repository.GetActive(ctx, tenantID, "support")
	if err != nil {
		t.Fatalf("GetActive() error = %v", err)
	}
	if active.Checksum != published.Checksum || active.Config.ConfigVersion != 1 {
		t.Fatalf("GetActive() = %+v, want checksum %q version 1", active, published.Checksum)
	}
	versioned, err := repository.GetVersion(ctx, tenantID, "support", 1)
	if err != nil {
		t.Fatalf("GetVersion() error = %v", err)
	}
	if versioned.Checksum != published.Checksum {
		t.Fatalf("GetVersion() checksum = %q, want %q", versioned.Checksum, published.Checksum)
	}
	resolved, err := repository.ResolveBinding(ctx, channels.Telegram, bindingID)
	if err != nil {
		t.Fatalf("ResolveBinding() error = %v", err)
	}
	if resolved.Config.AppCode != "support" {
		t.Fatalf("ResolveBinding() = %+v, want support", resolved)
	}
	applications, err := repository.ListApplications(ctx, tenantID)
	if err != nil {
		t.Fatalf("ListApplications() error = %v", err)
	}
	if len(applications) != 1 || applications[0].Config.AppCode != "support" {
		t.Fatalf("ListApplications() = %+v", applications)
	}
	if _, err := repository.GetActive(ctx, tenantID, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetActive(missing) error = %v, want ErrNotFound", err)
	}
	if _, err := repository.ResolveBinding(ctx, channels.Telegram, "missing-bot"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ResolveBinding(missing) error = %v, want ErrNotFound", err)
	}
}

func TestPostgresRepositoryVersionAndBindingConflicts(t *testing.T) {
	repository := postgresTestRepository(t)
	ctx := context.Background()
	tenantID := "support-conflict-" + uuid.NewString()
	bindingID := "support-conflict-bot-" + uuid.NewString()
	base := config.TenantConfig{
		TenantID: tenantID, AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Channels: []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: bindingID}},
	}
	if _, err := repository.Publish(ctx, base); err != nil {
		t.Fatalf("first Publish() error = %v", err)
	}
	stale := base
	if _, err := repository.Publish(ctx, stale); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale Publish() error = %v, want ErrVersionConflict", err)
	}

	other := config.TenantConfig{
		TenantID: tenantID + "-other", AppCode: "support-alt", Status: config.AgentActive, ConfigVersion: 1,
		Channels: []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: bindingID}},
	}
	if _, err := repository.Publish(ctx, other); !errors.Is(err, ErrBindingConflict) {
		t.Fatalf("binding-conflict Publish() error = %v, want ErrBindingConflict", err)
	}

	bumped := base
	bumped.ConfigVersion = 2
	bumped.Channels = []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: bindingID + "-v2"}}
	updated, err := repository.Publish(ctx, bumped)
	if err != nil {
		t.Fatalf("bumped Publish() error = %v", err)
	}
	if updated.Config.ConfigVersion != 2 {
		t.Fatalf("bumped snapshot version = %d, want 2", updated.Config.ConfigVersion)
	}
	if _, err := repository.ResolveBinding(ctx, channels.Telegram, bindingID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ResolveBinding(old binding) error = %v, want ErrNotFound after rebind", err)
	}
	active, err := repository.GetActive(ctx, tenantID, "support")
	if err != nil || active.Config.ConfigVersion != 2 {
		t.Fatalf("GetActive() after bump = %+v, %v; want version 2", active, err)
	}
}

func TestPostgresRepositoryPublishPreservesRetainedChannelIdentities(t *testing.T) {
	repository := postgresTestRepository(t)
	ctx := context.Background()
	tenantID := "support-channel-identity-" + uuid.NewString()
	bindingID := "support-channel-identity-bot-" + uuid.NewString()
	base := config.TenantConfig{
		TenantID: tenantID, AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Channels: []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: bindingID}},
	}
	if _, err := repository.Publish(ctx, base); err != nil {
		t.Fatal(err)
	}
	seedUnlinkedChannelIdentity(t, repository, tenantID, config.ChannelTelegram, bindingID, "external-user")

	updated := base
	updated.ConfigVersion = 2
	updated.Channels = []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: bindingID, AccessPolicy: "public"}}
	if _, err := repository.Publish(ctx, updated); err != nil {
		t.Fatalf("Publish(retained binding) error = %v", err)
	}
	if got := countChannelIdentities(t, repository, tenantID, config.ChannelTelegram, bindingID); got != 1 {
		t.Fatalf("retained binding identities = %d, want 1", got)
	}

	replaced := updated
	replaced.ConfigVersion = 3
	replaced.Channels = []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: bindingID + "-replacement"}}
	if _, err := repository.Publish(ctx, replaced); err != nil {
		t.Fatalf("Publish(replaced binding) error = %v", err)
	}
	if got := countChannelIdentities(t, repository, tenantID, config.ChannelTelegram, bindingID); got != 0 {
		t.Fatalf("removed binding identities = %d, want 0", got)
	}
}

func TestPostgresRepositorySuspendedApplicationDoesNotResolve(t *testing.T) {
	repository := postgresTestRepository(t)
	ctx := context.Background()
	tenantID := "support-suspend-" + uuid.NewString()
	bindingID := "support-suspend-bot-" + uuid.NewString()
	if _, err := repository.Publish(ctx, config.TenantConfig{
		TenantID: tenantID, AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Channels: []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: bindingID}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if _, err := repository.Publish(ctx, config.TenantConfig{
		TenantID: tenantID, AppCode: "support", Status: config.AgentDisabled, ConfigVersion: 2,
		Channels: []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: bindingID}},
	}); err != nil {
		t.Fatalf("suspend Publish() error = %v", err)
	}
	if _, err := repository.ResolveBinding(ctx, channels.Telegram, bindingID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ResolveBinding(suspended) error = %v, want ErrNotFound", err)
	}
	active, err := repository.GetActive(ctx, tenantID, "support")
	if err != nil || active.Config.Status != config.AgentDisabled {
		t.Fatalf("GetActive(suspended) = %+v, %v", active, err)
	}
}

func TestPostgresRepositoryPersistsAcrossInstances(t *testing.T) {
	first := postgresTestRepository(t)
	ctx := context.Background()
	tenantID := "support-persist-" + uuid.NewString()
	if _, err := first.Publish(ctx, config.TenantConfig{
		TenantID: tenantID, AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Channels: []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: "support-persist-bot-" + uuid.NewString()}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	// A second repository over a fresh pool simulates a restarted node.
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	second, err := NewPostgresRepository(database)
	if err != nil {
		t.Fatalf("NewPostgresRepository() error = %v", err)
	}
	active, err := second.GetActive(ctx, tenantID, "support")
	if err != nil {
		t.Fatalf("second instance GetActive() error = %v", err)
	}
	if active.Config.ConfigVersion != 1 {
		t.Fatalf("second instance version = %d, want 1", active.Config.ConfigVersion)
	}
}

func TestPostgresRepositorySerializesConcurrentPublish(t *testing.T) {
	repository := postgresTestRepository(t)
	tenantConfig := config.TenantConfig{
		TenantID: "support-concurrent-" + uuid.NewString(), AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
	}

	start := make(chan struct{})
	results := make(chan error, 2)
	for index := 0; index < 2; index++ {
		go func() {
			<-start
			_, err := repository.Publish(context.Background(), tenantConfig)
			results <- err
		}()
	}
	close(start)

	successes, conflicts := 0, 0
	for index := 0; index < 2; index++ {
		err := <-results
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrVersionConflict):
			conflicts++
		default:
			t.Fatalf("concurrent Publish() error = %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("concurrent Publish() successes=%d conflicts=%d, want 1 and 1", successes, conflicts)
	}
}

func TestPostgresRepositoryCandidateRolloutLifecycle(t *testing.T) {
	repository := postgresTestRepository(t)
	ctx := context.Background()
	tenantID := "support-rollout-" + uuid.NewString()
	bindingID := "support-rollout-bot-" + uuid.NewString()
	stable := config.TenantConfig{
		TenantID: tenantID, AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Channels: []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: bindingID}},
	}
	if _, err := repository.Publish(ctx, stable); err != nil {
		t.Fatalf("Publish(stable) error = %v", err)
	}
	seedUnlinkedChannelIdentity(t, repository, tenantID, config.ChannelTelegram, bindingID, "rollout-user")
	if _, err := repository.GetCandidate(ctx, tenantID, "support"); !errors.Is(err, ErrCandidateNotFound) {
		t.Fatalf("GetCandidate(before stage) error = %v, want ErrCandidateNotFound", err)
	}

	candidateConfig := stable
	candidateConfig.ConfigVersion = 2
	candidate, err := repository.Stage(ctx, candidateConfig)
	if err != nil || candidate.Config.ConfigVersion != 2 {
		t.Fatalf("Stage() = %+v, %v", candidate, err)
	}
	gotCandidate, err := repository.GetCandidate(ctx, tenantID, "support")
	if err != nil || gotCandidate.Config.ConfigVersion != 2 {
		t.Fatalf("GetCandidate() = %+v, %v", gotCandidate, err)
	}
	versions, err := repository.ListVersions(ctx, tenantID, "support", 10)
	if err != nil || len(versions) != 2 || versions[0].Config.ConfigVersion != 2 || versions[1].Config.ConfigVersion != 1 {
		t.Fatalf("ListVersions() = %+v, %v", versions, err)
	}

	rollout, err := repository.SetRollout(ctx, tenantID, "support", RolloutUpdate{
		BasisPoints: 0,
		TestUserIDs: []string{" support-user ", "support-user"},
		Ingresses:   []string{"web", "telegram/" + bindingID},
	})
	if err != nil || rollout.Generation == 0 || len(rollout.TestUserIDs) != 1 {
		t.Fatalf("SetRollout() = %+v, %v", rollout, err)
	}
	readRollout, err := repository.GetRollout(ctx, tenantID, "support")
	if err != nil || readRollout.Generation != rollout.Generation || readRollout.CandidateVersion != 2 {
		t.Fatalf("GetRollout() = %+v, %v", readRollout, err)
	}
	selection, err := ResolveRelease(ctx, repository, mustGetActive(t, repository, tenantID, "support"), ReleaseTarget{
		SessionKey: "support-session", PlatformUserID: "support-user", Ingress: "web", Scope: "direct",
	})
	if err != nil || selection.Variant != ReleaseCandidate || selection.Snapshot.Config.ConfigVersion != 2 {
		t.Fatalf("ResolveRelease(test user) = %+v, %v", selection, err)
	}

	updated, err := repository.SetRollout(ctx, tenantID, "support", RolloutUpdate{
		ExpectedGeneration: rollout.Generation,
		BasisPoints:        10000,
		Ingresses:          []string{"web"},
	})
	if err != nil || updated.Generation <= rollout.Generation {
		t.Fatalf("SetRollout(update) = %+v, %v", updated, err)
	}
	if _, err := repository.SetRollout(ctx, tenantID, "support", RolloutUpdate{ExpectedGeneration: rollout.Generation}); !errors.Is(err, ErrRolloutConflict) {
		t.Fatalf("SetRollout(stale generation) error = %v, want ErrRolloutConflict", err)
	}
	if _, err := repository.PromoteCandidate(ctx, tenantID, "support", 2, rollout.Generation); !errors.Is(err, ErrRolloutConflict) {
		t.Fatalf("PromoteCandidate(stale generation) error = %v, want ErrRolloutConflict", err)
	}
	promoted, err := repository.PromoteCandidate(ctx, tenantID, "support", 2, updated.Generation)
	if err != nil || promoted.Config.ConfigVersion != 2 {
		t.Fatalf("PromoteCandidate() = %+v, %v", promoted, err)
	}
	if active := mustGetActive(t, repository, tenantID, "support"); active.Config.ConfigVersion != 2 {
		t.Fatalf("active version after promotion = %d, want 2", active.Config.ConfigVersion)
	}
	if got := countChannelIdentities(t, repository, tenantID, config.ChannelTelegram, bindingID); got != 1 {
		t.Fatalf("retained binding identities after promotion = %d, want 1", got)
	}
	if _, err := repository.GetRollout(ctx, tenantID, "support"); !errors.Is(err, ErrRolloutNotFound) {
		t.Fatalf("GetRollout(after promotion) error = %v, want ErrRolloutNotFound", err)
	}
}

func TestPostgresRepositoryCanStopRolloutAndDiscardCandidate(t *testing.T) {
	repository := postgresTestRepository(t)
	ctx := context.Background()
	tenantID := "support-discard-" + uuid.NewString()
	bindingID := "support-discard-bot-" + uuid.NewString()
	stable := config.TenantConfig{
		TenantID: tenantID, AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Channels: []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: bindingID}},
	}
	if _, err := repository.Publish(ctx, stable); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	candidate := stable
	candidate.ConfigVersion = 2
	if _, err := repository.Stage(ctx, candidate); err != nil {
		t.Fatalf("Stage() error = %v", err)
	}
	rollout, err := repository.SetRollout(ctx, tenantID, "support", RolloutUpdate{BasisPoints: 5000, Ingresses: []string{"web"}})
	if err != nil {
		t.Fatalf("SetRollout() error = %v", err)
	}
	if err := repository.StopRollout(ctx, tenantID, "support", rollout.Generation+1); !errors.Is(err, ErrRolloutConflict) {
		t.Fatalf("StopRollout(stale) error = %v, want ErrRolloutConflict", err)
	}
	if err := repository.StopRollout(ctx, tenantID, "support", rollout.Generation); err != nil {
		t.Fatalf("StopRollout() error = %v", err)
	}
	if err := repository.StopRollout(ctx, tenantID, "support", rollout.Generation); !errors.Is(err, ErrRolloutNotFound) {
		t.Fatalf("StopRollout(second) error = %v, want ErrRolloutNotFound", err)
	}
	if err := repository.DiscardCandidate(ctx, tenantID, "support", 1); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("DiscardCandidate(wrong version) error = %v, want ErrVersionConflict", err)
	}
	if err := repository.DiscardCandidate(ctx, tenantID, "support", 2); err != nil {
		t.Fatalf("DiscardCandidate() error = %v", err)
	}
	if _, err := repository.GetCandidate(ctx, tenantID, "support"); !errors.Is(err, ErrCandidateNotFound) {
		t.Fatalf("GetCandidate(after discard) error = %v, want ErrCandidateNotFound", err)
	}
}

func TestPostgresRepositoryCandidateAndRolloutFailureBoundaries(t *testing.T) {
	repository := postgresTestRepository(t)
	ctx := context.Background()
	tenantID := "support-boundary-" + uuid.NewString()
	bindingID := "support-boundary-bot-" + uuid.NewString()
	stable := config.TenantConfig{
		TenantID: tenantID, AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Channels: []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: bindingID}},
	}

	missingCandidate := stable
	missingCandidate.ConfigVersion = 2
	if _, err := repository.Stage(ctx, missingCandidate); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Stage(without active) error = %v, want ErrNotFound", err)
	}
	if _, err := repository.SetRollout(ctx, tenantID, "support", RolloutUpdate{BasisPoints: 100}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("SetRollout(without application) error = %v, want ErrNotFound", err)
	}
	if _, err := repository.PromoteCandidate(ctx, tenantID, "support", 2, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("PromoteCandidate(without application) error = %v, want ErrNotFound", err)
	}
	if err := repository.DiscardCandidate(ctx, tenantID, "support", 2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DiscardCandidate(without application) error = %v, want ErrNotFound", err)
	}

	if _, err := repository.Publish(ctx, stable); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.SetRollout(ctx, tenantID, "support", RolloutUpdate{BasisPoints: 100}); !errors.Is(err, ErrCandidateNotFound) {
		t.Fatalf("SetRollout(without candidate) error = %v, want ErrCandidateNotFound", err)
	}
	if _, err := repository.PromoteCandidate(ctx, tenantID, "support", 2, 0); !errors.Is(err, ErrCandidateNotFound) {
		t.Fatalf("PromoteCandidate(without candidate) error = %v, want ErrCandidateNotFound", err)
	}
	if err := repository.DiscardCandidate(ctx, tenantID, "support", 2); !errors.Is(err, ErrCandidateNotFound) {
		t.Fatalf("DiscardCandidate(without candidate) error = %v, want ErrCandidateNotFound", err)
	}

	wrongVersion := stable
	wrongVersion.ConfigVersion = 3
	if _, err := repository.Stage(ctx, wrongVersion); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("Stage(non-next version) error = %v, want ErrVersionConflict", err)
	}

	candidate := stable
	candidate.ConfigVersion = 2
	if _, err := repository.Stage(ctx, candidate); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.PromoteCandidate(ctx, tenantID, "support", 1, 0); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("PromoteCandidate(wrong candidate) error = %v, want ErrVersionConflict", err)
	}
	if _, err := repository.SetRollout(ctx, tenantID, "support", RolloutUpdate{BasisPoints: 100, Ingresses: []string{"telegram/not-owned"}}); err == nil {
		t.Fatal("SetRollout() accepted unowned ingress")
	}
	rollout, err := repository.SetRollout(ctx, tenantID, "support", RolloutUpdate{BasisPoints: 100, Ingresses: []string{"web"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Publish(ctx, config.TenantConfig{
		TenantID: tenantID, AppCode: "support", Status: config.AgentActive, ConfigVersion: 3,
		Channels: stable.Channels,
	}); !errors.Is(err, ErrRolloutInProgress) {
		t.Fatalf("Publish(during rollout) error = %v, want ErrRolloutInProgress", err)
	}
	if _, err := repository.Stage(ctx, config.TenantConfig{
		TenantID: tenantID, AppCode: "support", Status: config.AgentActive, ConfigVersion: 3,
		Channels: stable.Channels,
	}); !errors.Is(err, ErrRolloutInProgress) {
		t.Fatalf("Stage(during rollout) error = %v, want ErrRolloutInProgress", err)
	}
	if err := repository.DiscardCandidate(ctx, tenantID, "support", 2); !errors.Is(err, ErrRolloutInProgress) {
		t.Fatalf("DiscardCandidate(during rollout) error = %v, want ErrRolloutInProgress", err)
	}
	if err := repository.StopRollout(ctx, tenantID, "support", rollout.Generation); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresRepositoryRejectsInvalidRolloutCandidates(t *testing.T) {
	repository := postgresTestRepository(t)
	ctx := context.Background()

	t.Run("disabled candidate", func(t *testing.T) {
		tenantID := "support-disabled-candidate-" + uuid.NewString()
		bindingID := "support-disabled-bot-" + uuid.NewString()
		stable := config.TenantConfig{
			TenantID: tenantID, AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
			Channels: []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: bindingID}},
		}
		if _, err := repository.Publish(ctx, stable); err != nil {
			t.Fatal(err)
		}
		candidate := stable
		candidate.ConfigVersion = 2
		candidate.Status = config.AgentDisabled
		if _, err := repository.Stage(ctx, candidate); err != nil {
			t.Fatal(err)
		}
		if _, err := repository.SetRollout(ctx, tenantID, "support", RolloutUpdate{BasisPoints: 100}); err == nil || !strings.Contains(err.Error(), "must be active") {
			t.Fatalf("SetRollout(disabled candidate) error = %v", err)
		}
		if _, err := repository.PromoteCandidate(ctx, tenantID, "support", 2, 0); err == nil || !strings.Contains(err.Error(), "not active") {
			t.Fatalf("PromoteCandidate(disabled candidate) error = %v", err)
		}
	})

	t.Run("channel topology", func(t *testing.T) {
		tenantID := "support-topology-" + uuid.NewString()
		stable := config.TenantConfig{
			TenantID: tenantID, AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
			Channels: []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: "support-stable-" + uuid.NewString()}},
		}
		if _, err := repository.Publish(ctx, stable); err != nil {
			t.Fatal(err)
		}
		candidate := stable
		candidate.ConfigVersion = 2
		candidate.Channels = []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: "support-candidate-" + uuid.NewString()}}
		if _, err := repository.Stage(ctx, candidate); err != nil {
			t.Fatal(err)
		}
		if _, err := repository.SetRollout(ctx, tenantID, "support", RolloutUpdate{BasisPoints: 100}); err == nil || !strings.Contains(err.Error(), "cannot change channel bindings") {
			t.Fatalf("SetRollout(topology change) error = %v", err)
		}
	})
}

func TestPostgresRepositoryValidatesContextAndArguments(t *testing.T) {
	repository := postgresTestRepository(t)
	ctx := context.Background()
	if !repository.UsesConfigCacheInvalidationOutbox() {
		t.Fatal("UsesConfigCacheInvalidationOutbox() = false")
	}
	if _, err := repository.beginTenantTransaction(ctx, " "); err == nil {
		t.Fatal("beginTenantTransaction() accepted blank tenant")
	}
	if _, err := repository.GetVersion(ctx, "tenant", "support", 0); err == nil {
		t.Fatal("GetVersion() accepted zero version")
	}
	if _, err := repository.ResolveBinding(ctx, channels.Channel("unknown"), "binding"); err == nil {
		t.Fatal("ResolveBinding() accepted unsupported channel")
	}
	if _, err := repository.ResolveBinding(ctx, channels.Telegram, " "); err == nil {
		t.Fatal("ResolveBinding() accepted blank binding")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	for name, operation := range map[string]func() error{
		"publish": func() error {
			_, err := repository.Publish(cancelled, config.TenantConfig{})
			return err
		},
		"get active":    func() error { _, err := repository.GetActive(cancelled, "tenant", "support"); return err },
		"get candidate": func() error { _, err := repository.GetCandidate(cancelled, "tenant", "support"); return err },
		"get version":   func() error { _, err := repository.GetVersion(cancelled, "tenant", "support", 1); return err },
		"list versions": func() error { _, err := repository.ListVersions(cancelled, "tenant", "support", 10); return err },
		"stage":         func() error { _, err := repository.Stage(cancelled, config.TenantConfig{}); return err },
		"get rollout":   func() error { _, err := repository.GetRollout(cancelled, "tenant", "support"); return err },
		"set rollout": func() error {
			_, err := repository.SetRollout(cancelled, "tenant", "support", RolloutUpdate{})
			return err
		},
		"stop rollout":      func() error { return repository.StopRollout(cancelled, "tenant", "support", 1) },
		"discard candidate": func() error { return repository.DiscardCandidate(cancelled, "tenant", "support", 1) },
		"promote candidate": func() error { _, err := repository.PromoteCandidate(cancelled, "tenant", "support", 1, 0); return err },
		"resolve binding":   func() error { _, err := repository.ResolveBinding(cancelled, channels.Telegram, "binding"); return err },
		"list applications": func() error { _, err := repository.ListApplications(cancelled, "tenant"); return err },
	} {
		t.Run(name, func(t *testing.T) {
			if err := operation(); !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v, want context.Canceled", err)
			}
		})
	}
}

func mustGetActive(t *testing.T, repository Repository, tenantID, appCode string) Snapshot {
	t.Helper()
	snapshot, err := repository.GetActive(context.Background(), tenantID, appCode)
	if err != nil {
		t.Fatalf("GetActive() error = %v", err)
	}
	return snapshot
}
