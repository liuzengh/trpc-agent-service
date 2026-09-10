package admin

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestKnowledgeMigrationControlPlaneOwnsObserveAndCleanup(t *testing.T) {
	ctx := context.Background()
	service, configs, migrations := sessionMigrationService(t, ctx)
	publisher := &recordingKnowledgePublisher{active: 2}
	service.KnowledgeMigrationPublisher = publisher
	principal := Principal{Authenticated: true, TenantID: "tenant-a", SubjectID: "operator", CanManage: true}
	metadata := tenant.ChangeMetadata{ActorType: "admin", ActorID: "operator", ReasonCode: "knowledge_move", CorrelationID: "request-1", TraceID: "trace-1"}

	source := knowledgeConfig("source", 1)
	published, err := configs.Publish(ctx, config.PublishInput{TenantID: "tenant-a", ExpectedTenantVersion: 1, Payload: source, Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := configs.Stage(ctx, config.StageInput{TenantID: "tenant-a", ExpectedTenantVersion: published.Tenant.Version, Payload: knowledgeConfig("target", 2), Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.CreateKnowledgeMigration(ctx, principal, "tenant-a", KnowledgeMigrationCreateInput{MigrationID: "move-knowledge", TargetConfigVersion: candidate.ConfigVersion}, metadata)
	if err != nil {
		t.Fatal(err)
	}
	verified := advanceSessionMigrationToVerify(t, ctx, migrations, created)
	if _, err = service.CutoverKnowledgeMigration(ctx, principal, "tenant-a", created.MigrationID, KnowledgeMigrationSwitchInput{ExpectedTenantVersion: published.Tenant.Version, ExpectedMigrationVersion: verified.Version, SwitchID: "cutover-1"}, metadata); err != nil {
		t.Fatal(err)
	}
	if _, err = service.BeginKnowledgeMigrationObserve(ctx, principal, "tenant-a", created.MigrationID, KnowledgeMigrationObserveInput{ExpectedTenantVersion: published.Tenant.Version, ExpectedMigrationVersion: verified.Version, ObserveUntil: time.Now().UTC().Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if publisher.observe.ObserveUntil.IsZero() {
		t.Fatal("observe was not delegated to the knowledge publisher")
	}
	if _, err = service.CleanupKnowledgeMigration(ctx, principal, "tenant-a", created.MigrationID, KnowledgeMigrationSwitchInput{ExpectedTenantVersion: published.Tenant.Version, ExpectedMigrationVersion: verified.Version}); err != nil {
		t.Fatal(err)
	}
	if publisher.cleanup.RollbackSyncWatermark != verified.Verification.TargetWatermark {
		t.Fatalf("cleanup watermark=%q want authority watermark=%q", publisher.cleanup.RollbackSyncWatermark, verified.Verification.TargetWatermark)
	}
}

func knowledgeConfig(profileID string, version int64) config.ConfigV1 {
	return config.ConfigV1{SchemaVersion: config.CurrentSchemaVersion, DefaultAgentAppID: "app", PolicyVersion: 1,
		BackendBindings: []config.BackendBinding{{Domain: knowledgeMigrationDomain, BackendProfileID: profileID, BackendVersion: version, Required: []string{"tenant_filter"}}}}
}

type recordingKnowledgePublisher struct {
	cutover  knowledgedriver.CutoverRequest
	observe  knowledgedriver.ObserveRequest
	rollback knowledgedriver.RollbackRequest
	cleanup  knowledgedriver.CleanupRequest
	active   int64
}

func (p *recordingKnowledgePublisher) Cutover(_ context.Context, in knowledgedriver.CutoverRequest) (knowledgedriver.SwitchResult, error) {
	p.cutover = in
	return knowledgedriver.SwitchResult{ActiveConfigVersion: p.active}, nil
}
func (p *recordingKnowledgePublisher) BeginObserve(_ context.Context, in knowledgedriver.ObserveRequest) (knowledgedriver.SwitchResult, error) {
	p.observe = in
	return knowledgedriver.SwitchResult{ActiveConfigVersion: p.active}, nil
}
func (p *recordingKnowledgePublisher) Rollback(_ context.Context, in knowledgedriver.RollbackRequest) (knowledgedriver.SwitchResult, error) {
	p.rollback = in
	return knowledgedriver.SwitchResult{ActiveConfigVersion: p.active, RolledBack: true}, nil
}
func (p *recordingKnowledgePublisher) Cleanup(_ context.Context, in knowledgedriver.CleanupRequest) (knowledgedriver.SwitchResult, error) {
	p.cleanup = in
	return knowledgedriver.SwitchResult{ActiveConfigVersion: p.active}, nil
}
func (p *recordingKnowledgePublisher) DrainStatus(context.Context, string, string) (knowledgedriver.DrainStatus, error) {
	return knowledgedriver.DrainStatus{ActiveConfigVersion: p.active}, nil
}

var _ knowledgedriver.CutoverPublisher = (*recordingKnowledgePublisher)(nil)
var _ = migration.StateObserve
