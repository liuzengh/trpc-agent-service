package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	migrationmemory "github.com/liuzengh/trpc-agent-service/trpcservice/migration/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver"
	ledgermemory "github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver/inmemory"
)

func TestLoadKnowledgeMigrationConfigRejectsMissingEnvironment(t *testing.T) {
	_, err := loadKnowledgeMigrationConfig(func(string) string { return "" })
	if err == nil || !strings.Contains(err.Error(), "required knowledge migration configuration") {
		t.Fatalf("error=%v, want missing configuration rejection", err)
	}
}

func TestLoadKnowledgeMigrationConfigAcceptsBoundedOperatorConfiguration(t *testing.T) {
	values := map[string]string{
		"TRPC_POSTGRES_DSN":                    "postgres://control",
		"TRPC_SECRET_ROOT":                     "/var/run/trpc-secrets",
		"TRPC_KNOWLEDGE_MIGRATION_TENANT_ID":   "tenant-a",
		"TRPC_KNOWLEDGE_MIGRATION_ID":          "knowledge-migration-a",
		"TRPC_KNOWLEDGE_MIGRATION_WORKER_ID":   "knowledge-worker-a",
		"TRPC_KNOWLEDGE_MIGRATION_CONFIRM":     "true",
		"TRPC_KNOWLEDGE_MIGRATION_BATCH_LIMIT": "50",
		"TRPC_KNOWLEDGE_MIGRATION_TIMEOUT":     "15m",
	}
	config, err := loadKnowledgeMigrationConfig(func(key string) string { return values[key] })
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if config.BatchLimit != 50 || config.Timeout != 15*time.Minute || config.RepairLimit != 100 {
		t.Fatalf("config=%+v", config)
	}
}

func TestRunRoleRecognizesKnowledgeMigrateBeforeOpeningDependencies(t *testing.T) {
	err := runRole(context.Background(), func(string) string { return "" }, testLogger(), "knowledge-migrate")
	if err == nil || !strings.Contains(err.Error(), "knowledge migration configuration rejected") {
		t.Fatalf("error=%v, want knowledge migration configuration rejection", err)
	}
}

func TestRunKnowledgeMigrationStepAdvancesBackfillAndObservesDrain(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	authority := migrationmemory.New()
	current, err := authority.Create(ctx, migration.CreateRequest{TenantID: "tenant-a", MigrationID: "knowledge-a", Domain: knowledgedriver.Domain,
		Epoch: 2, Source: migration.Binding{ConfigVersion: 1, BackendProfileID: "source", BackendVersion: 1}, Target: migration.Binding{ConfigVersion: 2, BackendProfileID: "target", BackendVersion: 1}, CreatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range []struct {
		state             migration.State
		watermark, ledger string
	}{{migration.StateSnapshot, "snapshot-a", ""}, {migration.StateDualWrite, "", "ledger://knowledge-a"}, {migration.StateBackfill, "", ""}} {
		in := migration.TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version, To: step.state, At: now.Add(time.Minute)}
		in.SnapshotWatermark, in.DualWriteRef = step.watermark, step.ledger
		current, err = authority.Transition(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Minute)
	}
	batch, err := authority.CommitBatch(ctx, migration.BatchRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, BatchID: "empty", Epoch: current.Epoch, ExpectedVersion: current.Version, BatchSeq: current.NextBatchSeq, ToCheckpoint: "done", Digest: roleDigest('a'), Complete: true, CommittedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	current = batch.Migration
	driver := knowledgedriver.Driver{Authority: authority, Ledger: ledgermemory.New(), Source: roleKnowledgeReader{}, Target: roleKnowledgeWriter{}}
	config := knowledgeMigrationConfig{WorkerID: "worker-a", RepairLimit: 10, Lease: time.Second, RetryDelay: 0}
	publisher := &roleKnowledgePublisher{}
	if err := runKnowledgeMigrationStep(ctx, testLogger(), authority, driver, publisher, "snapshot-a", config, current); err != nil {
		t.Fatal(err)
	}
	current, err = authority.Get(ctx, current.TenantID, current.MigrationID)
	if err != nil || current.State != migration.StateVerify {
		t.Fatalf("current=%+v err=%v", current, err)
	}
	verification := migration.Verification{SourceCount: 1, TargetCount: 1, SourceDigest: roleDigest('b'), TargetDigest: roleDigest('b'), SourceWatermark: "done", TargetWatermark: "done", SampleDigest: roleDigest('b')}
	at := current.UpdatedAt.Add(time.Second)
	current, err = authority.RecordVerification(ctx, migration.VerificationRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version, Verification: verification, RecordedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	current, err = authority.Transition(ctx, migration.TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version, To: migration.StateCutover, At: at.Add(time.Second), Verification: verification, CutoverConfigVersion: current.Target.ConfigVersion})
	if err != nil {
		t.Fatal(err)
	}
	observeAt := current.UpdatedAt.Add(time.Second)
	current, err = authority.Transition(ctx, migration.TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version, To: migration.StateObserve, At: observeAt, ObserveUntil: observeAt.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if err := runKnowledgeMigrationStep(ctx, testLogger(), authority, driver, publisher, "snapshot-a", config, current); err != nil {
		t.Fatal(err)
	}
	if publisher.drainCalls != 1 {
		t.Fatalf("drain calls=%d, want 1", publisher.drainCalls)
	}
}

type roleKnowledgeReader struct{}

func (roleKnowledgeReader) LoadChunk(context.Context, knowledgedriver.ChunkKey) (knowledgedriver.ChunkImage, error) {
	return knowledgedriver.ChunkImage{}, nil
}

type roleKnowledgeWriter struct{}

func (roleKnowledgeWriter) ApplyChunk(context.Context, knowledgedriver.ApplyRequest) (knowledgedriver.ApplyResult, error) {
	return knowledgedriver.ApplyResult{}, nil
}

type roleKnowledgePublisher struct{ drainCalls int }

func (*roleKnowledgePublisher) Cutover(context.Context, knowledgedriver.CutoverRequest) (knowledgedriver.SwitchResult, error) {
	return knowledgedriver.SwitchResult{}, nil
}
func (*roleKnowledgePublisher) BeginObserve(context.Context, knowledgedriver.ObserveRequest) (knowledgedriver.SwitchResult, error) {
	return knowledgedriver.SwitchResult{}, nil
}
func (*roleKnowledgePublisher) Rollback(context.Context, knowledgedriver.RollbackRequest) (knowledgedriver.SwitchResult, error) {
	return knowledgedriver.SwitchResult{}, nil
}
func (*roleKnowledgePublisher) Cleanup(context.Context, knowledgedriver.CleanupRequest) (knowledgedriver.SwitchResult, error) {
	return knowledgedriver.SwitchResult{}, nil
}
func (p *roleKnowledgePublisher) DrainStatus(context.Context, string, string) (knowledgedriver.DrainStatus, error) {
	p.drainCalls++
	return knowledgedriver.DrainStatus{}, nil
}
func roleDigest(value byte) string { return strings.Repeat(string(value), 64) }
