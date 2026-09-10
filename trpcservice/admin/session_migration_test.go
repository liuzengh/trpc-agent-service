package admin

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	agentmemory "github.com/liuzengh/trpc-agent-service/trpcservice/agentapp/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	configmemory "github.com/liuzengh/trpc-agent-service/trpcservice/config/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	migrationmemory "github.com/liuzengh/trpc-agent-service/trpcservice/migration/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	tenantmemory "github.com/liuzengh/trpc-agent-service/trpcservice/tenant/inmemory"
)

// TestSessionMigrationControlPlaneUsesAuthorityHeldEvidence exercises the
// operator-facing lifecycle without allowing a browser request to supply a
// digest or a rollback watermark. The PostgreSQL publisher applies these same
// requests atomically in production; this recorder makes the API contract
// inspectable without embedding SQL behaviour in the Admin package.
func TestSessionMigrationControlPlaneUsesAuthorityHeldEvidence(t *testing.T) {
	ctx := context.Background()
	service, configs, migrations := sessionMigrationService(t, ctx)
	principal := Principal{Authenticated: true, TenantID: "tenant-a", SubjectID: "operator", CanManage: true}
	metadata := tenant.ChangeMetadata{ActorType: "admin", ActorID: "operator", ReasonCode: "session_move", CorrelationID: "request-1", TraceID: "trace-1"}

	source := sessionConfig("source", 1)
	published, err := configs.Publish(ctx, config.PublishInput{TenantID: "tenant-a", ExpectedTenantVersion: 1, Payload: source, Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := configs.Stage(ctx, config.StageInput{TenantID: "tenant-a", ExpectedTenantVersion: published.Tenant.Version, Payload: sessionConfig("target", 2), Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	if candidate.State != config.StatePublished || candidate.ConfigVersion <= published.Snapshot.ConfigVersion {
		t.Fatalf("candidate must be an immutable non-active snapshot: %#v", candidate)
	}
	created, err := service.CreateSessionMigration(ctx, principal, "tenant-a", SessionMigrationCreateInput{MigrationID: "move-1", TargetConfigVersion: candidate.ConfigVersion}, metadata)
	if err != nil {
		t.Fatal(err)
	}
	if created.Source.ConfigVersion != published.Snapshot.ConfigVersion || created.Target.ConfigVersion != candidate.ConfigVersion || created.Source.BackendProfileID != "source" || created.Target.BackendProfileID != "target" {
		t.Fatalf("migration bindings=%#v", created)
	}
	paused, err := service.PauseSessionMigration(ctx, principal, "tenant-a", created.MigrationID,
		SessionMigrationControlInput{ExpectedMigrationVersion: created.Version}, metadata)
	if err != nil || paused.State != migration.StatePaused || paused.PausedFrom != migration.StatePlanned {
		t.Fatalf("pause=%#v err=%v", paused, err)
	}
	created, err = service.ResumeSessionMigration(ctx, principal, "tenant-a", created.MigrationID,
		SessionMigrationControlInput{ExpectedMigrationVersion: paused.Version}, metadata)
	if err != nil || created.State != migration.StatePlanned || created.PausedFrom != "" {
		t.Fatalf("resume=%#v err=%v", created, err)
	}

	verified := advanceSessionMigrationToVerify(t, ctx, migrations, created)
	result, err := service.CutoverSessionMigration(ctx, principal, "tenant-a", created.MigrationID, SessionMigrationSwitchInput{
		ExpectedTenantVersion: published.Tenant.Version, ExpectedMigrationVersion: verified.Version, SwitchID: "switch-1",
	}, metadata)
	if err != nil {
		t.Fatal(err)
	}
	publisher := service.SessionMigrationPublisher.(*recordingSessionPublisher)
	if result.ActiveConfigVersion != candidate.ConfigVersion || publisher.cutover.Verification != verified.Verification || publisher.cutover.Metadata.ActorID != metadata.ActorID || publisher.cutover.Metadata.SwitchID != "switch-1" {
		t.Fatalf("cutover=%#v recorded=%#v", result, publisher.cutover)
	}

	// The observation and rollback requests contain only CAS coordinates. The
	// authority-held verification watermark is copied by Admin rather than
	// accepted from a client payload.
	if _, err = service.BeginSessionMigrationObserve(ctx, principal, "tenant-a", created.MigrationID, SessionMigrationObserveInput{
		ExpectedTenantVersion: published.Tenant.Version, ExpectedMigrationVersion: verified.Version, ObserveUntil: time.Now().UTC().Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = service.RollbackSessionMigration(ctx, principal, "tenant-a", created.MigrationID, SessionMigrationSwitchInput{
		ExpectedTenantVersion: published.Tenant.Version, ExpectedMigrationVersion: verified.Version, SwitchID: "rollback-1",
	}, metadata); err != nil {
		t.Fatal(err)
	}
	if publisher.rollback.RollbackSyncWatermark != verified.Verification.TargetWatermark || publisher.rollback.Metadata.SwitchID != "rollback-1" {
		t.Fatalf("rollback must use authority watermark: %#v", publisher.rollback)
	}
}

func sessionMigrationService(t *testing.T, ctx context.Context) (Service, *configmemory.Repository, *migrationmemory.Store) {
	t.Helper()
	meta := tenant.ChangeMetadata{ActorType: "admin", ActorID: "operator", ReasonCode: "setup", CorrelationID: "setup", TraceID: "setup"}
	tenants := tenantmemory.New()
	if _, err := tenants.Create(ctx, tenant.CreateInput{Tenant: tenant.Tenant{TenantID: "tenant-a", TenantKey: "tenant-a", DisplayName: "Tenant A"}, ChangeMetadata: meta}); err != nil {
		t.Fatal(err)
	}
	apps := agentmemory.New()
	appMeta := agentapp.ChangeMetadata{ActorType: "admin", ActorID: "operator", Reason: "setup", CorrelationID: "setup", TraceID: "setup"}
	app, err := apps.Create(ctx, agentapp.CreateInput{App: agentapp.AgentApp{TenantID: "tenant-a", AgentAppID: "app", AgentAppKey: "assistant", DisplayName: "Assistant"}, ChangeMetadata: appMeta})
	if err != nil {
		t.Fatal(err)
	}
	draft, err := apps.CreateDraft(ctx, agentapp.CreateDraftInput{TenantID: "tenant-a", AgentAppID: "app", ExpectedAppVersion: app.Version, Revision: agentapp.Revision{AgentKind: agentapp.AgentKindLLM, Instruction: "help", ModelProfileID: "fake", ModelProfileVersion: 1}, ChangeMetadata: appMeta})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = apps.Publish(ctx, agentapp.PublishInput{TenantID: "tenant-a", AgentAppID: "app", Revision: draft.Revision, ExpectedAppVersion: app.Version + 1, ExpectedDraftVersion: draft.DraftVersion, ChangeMetadata: appMeta}); err != nil {
		t.Fatal(err)
	}
	configs := configmemory.New(tenants, apps)
	migrations := migrationmemory.New()
	publisher := &recordingSessionPublisher{active: 2}
	return Service{Configs: configs, Migrations: migrations, SessionMigrationPublisher: publisher}, configs, migrations
}

func sessionConfig(profileID string, version int64) config.ConfigV1 {
	return config.ConfigV1{SchemaVersion: config.CurrentSchemaVersion, DefaultAgentAppID: "app", PolicyVersion: 1,
		BackendBindings: []config.BackendBinding{{Domain: sessionMigrationDomain, BackendProfileID: profileID, BackendVersion: version, Required: []string{"atomic_turn_commit"}}}}
}

func advanceSessionMigrationToVerify(t *testing.T, ctx context.Context, store migration.Repository, current migration.Migration) migration.Migration {
	t.Helper()
	at := current.CreatedAt.Add(time.Second)
	transition := func(to migration.State, mutate func(*migration.TransitionRequest)) {
		request := migration.TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version, To: to, At: at}
		mutate(&request)
		var err error
		current, err = store.Transition(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		at = at.Add(time.Second)
	}
	transition(migration.StateSnapshot, func(in *migration.TransitionRequest) { in.SnapshotWatermark = "snapshot:1" })
	transition(migration.StateDualWrite, func(in *migration.TransitionRequest) { in.DualWriteRef = "ledger://move-1" })
	transition(migration.StateBackfill, func(*migration.TransitionRequest) {})
	var err error
	batchDigest := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	batch, err := store.CommitBatch(ctx, migration.BatchRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, BatchID: "batch-1", Epoch: current.Epoch, ExpectedVersion: current.Version, BatchSeq: current.NextBatchSeq, ToCheckpoint: "done", Digest: batchDigest, RecordCount: 1, Complete: true, CommittedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	current = batch.Migration
	at = at.Add(time.Second)
	transition(migration.StateVerify, func(*migration.TransitionRequest) {})
	verification := migration.Verification{SourceCount: 1, TargetCount: 1, SourceDigest: batchDigest, TargetDigest: batchDigest, SourceWatermark: "done", TargetWatermark: "done", SampleDigest: batchDigest}
	current, err = store.RecordVerification(ctx, migration.VerificationRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version, Verification: verification, RecordedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	return current
}

type recordingSessionPublisher struct {
	cutover  migration.SessionCutoverRequest
	observe  migration.SessionObserveRequest
	rollback migration.SessionRollbackRequest
	cleanup  migration.SessionCleanupRequest
	active   int64
}

func (p *recordingSessionPublisher) Cutover(_ context.Context, in migration.SessionCutoverRequest) (migration.SessionSwitchResult, error) {
	p.cutover, p.active = in, 2
	return migration.SessionSwitchResult{ActiveConfigVersion: p.active}, nil
}
func (p *recordingSessionPublisher) BeginObserve(_ context.Context, in migration.SessionObserveRequest) (migration.SessionSwitchResult, error) {
	p.observe = in
	return migration.SessionSwitchResult{ActiveConfigVersion: p.active}, nil
}
func (p *recordingSessionPublisher) Rollback(_ context.Context, in migration.SessionRollbackRequest) (migration.SessionSwitchResult, error) {
	p.rollback = in
	return migration.SessionSwitchResult{ActiveConfigVersion: 1, RolledBack: true}, nil
}
func (p *recordingSessionPublisher) Cleanup(_ context.Context, in migration.SessionCleanupRequest) (migration.SessionSwitchResult, error) {
	p.cleanup = in
	return migration.SessionSwitchResult{ActiveConfigVersion: p.active}, nil
}
func (p *recordingSessionPublisher) DrainStatus(context.Context, string, string) (migration.SessionDrainStatus, error) {
	return migration.SessionDrainStatus{ActiveConfigVersion: p.active}, nil
}
