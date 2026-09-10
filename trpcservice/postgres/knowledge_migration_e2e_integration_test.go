//go:build integration && e2e

package postgres_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	platformknowledge "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge"
	knowledgeqdrant "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge/qdrant"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	platformpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	qdrantpb "github.com/qdrant/go-client/qdrant"
	"google.golang.org/protobuf/proto"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	frameworkqdrant "trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/qdrant"
	qdrantstorage "trpc.group/trpc-go/trpc-agent-go/storage/qdrant"
)

const (
	knowledgeMigrationQdrantHostEnv   = "TRPC_AGENT_SERVICE_QDRANT_TEST_HOST"
	knowledgeMigrationQdrantPortEnv   = "TRPC_AGENT_SERVICE_QDRANT_TEST_PORT"
	knowledgeMigrationReportEnv       = "TRPC_KNOWLEDGE_MIGRATION_E2E_REPORT"
	knowledgeMigrationChunkCount      = 5
	knowledgeMigrationItemPause       = 20 * time.Second
	knowledgeMigrationLeaseWaitBuffer = 45 * time.Second
)

// TestKnowledgeMigrationAcrossRealWorkerProcesses proves that the production
// SQL catalog, Qdrant copier, migration lease, checkpoint and cutover paths
// work across two real trpc-service worker processes. The source collection
// also contains an unauthorized point; it must not reach the target.
func TestKnowledgeMigrationAcrossRealWorkerProcesses(t *testing.T) {
	host := strings.TrimSpace(os.Getenv(knowledgeMigrationQdrantHostEnv))
	if host == "" {
		t.Fatalf("%s is required for Knowledge Migration E2E", knowledgeMigrationQdrantHostEnv)
	}
	port := 6334
	if value := strings.TrimSpace(os.Getenv(knowledgeMigrationQdrantPortEnv)); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			t.Fatalf("parse qdrant port: %q", value)
		}
		port = parsed
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool, postgresDSN := openRuntimePool(t)
	store, err := platformpostgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate postgres: %v", err)
	}

	runID := strings.ReplaceAll(uuid.NewString(), "-", "")
	tenantID := "knowledge-migration-e2e-" + runID
	appID := "search"
	baseID := "handbook"
	documentID := "manual"
	generation := "generation-" + runID
	sourceProfile := "source-" + runID
	targetProfile := "target-" + runID
	sourceBackendName := "qdrant-source-" + runID
	targetBackendName := "qdrant-target-" + runID
	sourceCollection := "knowledge-" + sourceProfile + "-" + generation
	targetCollection := "knowledge-" + targetProfile + "-" + generation
	scope := tenant.Scope{TenantID: tenantID, AppID: appID}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM platform.data_migration WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM platform.knowledge_chunk WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM platform.knowledge_document WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM platform.knowledge_base WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM platform.app_config_version WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM platform.agent_app WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM platform.tenant WHERE tenant_id = $1`, tenantID)
	})

	qdrantClient, err := qdrantstorage.NewClient(ctx,
		qdrantstorage.WithHost(host),
		qdrantstorage.WithPort(port),
	)
	if err != nil {
		t.Fatalf("create qdrant client: %v", err)
	}
	t.Cleanup(func() {
		cleanupQdrantCtx, cleanupQdrantCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupQdrantCancel()
		_ = qdrantClient.DeleteCollection(cleanupQdrantCtx, sourceCollection)
		_ = qdrantClient.DeleteCollection(cleanupQdrantCtx, targetCollection)
		_ = qdrantClient.Close()
	})
	createKnowledgeCollection(t, ctx, host, port, sourceCollection)

	v1 := knowledgeMigrationConfig(tenantID, appID, "v1")
	v2 := v1.Clone()
	v2.Version = "v2"
	v2.KnowledgeBaseIDs = []string{baseID}
	v2.BackendConfig.Knowledge = qdrantMigrationBackend(sourceBackendName, sourceProfile, generation)
	v3 := v2.Clone()
	v3.Version = "v3"
	v3.BackendConfig.Knowledge = qdrantMigrationBackend(targetBackendName, targetProfile, generation)
	controlPlane := admin.API{Repository: store}
	if err := controlPlane.CreateTenant(ctx, tenant.Tenant{
		ID: tenantID, Name: "Knowledge Migration E2E", Status: tenant.StatusActive,
	}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if err := controlPlane.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID: tenantID, AppID: appID, Name: "Knowledge Migration E2E",
		ActiveConfigVersion: v1.Version, Status: tenant.StatusActive,
	}, v1); err != nil {
		t.Fatalf("create app: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO platform.knowledge_base (tenant_id, app_id, knowledge_base_id, status)
VALUES ($1, $2, $3, 'ACTIVE')`, tenantID, appID, baseID); err != nil {
		t.Fatalf("create knowledge base catalog: %v", err)
	}
	if err := controlPlane.PublishAppConfig(ctx, v2); err != nil {
		t.Fatalf("publish source knowledge config: %v", err)
	}
	if err := controlPlane.ActivateAppConfig(ctx, scope, v2.Version); err != nil {
		t.Fatalf("activate source knowledge config: %v", err)
	}
	if err := controlPlane.PublishAppConfig(ctx, v3); err != nil {
		t.Fatalf("publish target knowledge config: %v", err)
	}

	refs := make([]platformknowledge.ChunkRef, 0, knowledgeMigrationChunkCount)
	setupStore, err := frameworkqdrant.New(ctx,
		frameworkqdrant.WithHost(host),
		frameworkqdrant.WithPort(port),
		frameworkqdrant.WithCollectionName(sourceCollection),
		frameworkqdrant.WithDimension(3),
	)
	if err != nil {
		t.Fatalf("open source qdrant store: %v", err)
	}
	for index := 0; index < knowledgeMigrationChunkCount; index++ {
		chunkID := fmt.Sprintf("chunk-%02d", index)
		ref := platformknowledge.ChunkRef{
			Scope: scope, ConfigVersion: v2.Version,
			KnowledgeBaseID: baseID, DocumentID: documentID,
			DocumentVersion: "1", ChunkID: chunkID, IndexGeneration: generation,
		}
		refs = append(refs, ref)
		if err := setupStore.Add(ctx, knowledgeMigrationDocument(ref, "point-"+chunkID), []float64{
			float64(index) + 0.1, 0.2, 0.3,
		}); err != nil {
			t.Fatalf("insert source qdrant chunk %s: %v", chunkID, err)
		}
	}
	foreignRef := platformknowledge.ChunkRef{
		Scope: tenant.Scope{TenantID: "other-tenant-" + runID, AppID: appID}, ConfigVersion: v2.Version,
		KnowledgeBaseID: baseID, DocumentID: documentID, DocumentVersion: "1",
		ChunkID: "foreign", IndexGeneration: generation,
	}
	if err := setupStore.Add(ctx, knowledgeMigrationDocument(foreignRef, "foreign-"+runID), []float64{9.1, 9.2, 9.3}); err != nil {
		t.Fatalf("insert foreign qdrant point: %v", err)
	}
	if err := setupStore.Close(); err != nil {
		t.Fatalf("close source qdrant store: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO platform.knowledge_document (
    tenant_id, app_id, knowledge_base_id, document_id, version,
    status, index_generation
) VALUES ($1, $2, $3, $4, 1, 'AVAILABLE', $5)`, tenantID, appID, baseID, documentID, generation); err != nil {
		t.Fatalf("create knowledge document catalog: %v", err)
	}
	for _, ref := range refs {
		if _, err := pool.Exec(ctx, `
INSERT INTO platform.knowledge_chunk (
    tenant_id, app_id, knowledge_base_id, document_id, document_version,
    index_generation, chunk_id, status
) VALUES ($1, $2, $3, $4, $5, $6, $7, 'AVAILABLE')`,
			ref.Scope.TenantID, ref.Scope.AppID, ref.KnowledgeBaseID, ref.DocumentID,
			1, ref.IndexGeneration, ref.ChunkID); err != nil {
			t.Fatalf("create knowledge chunk catalog %s: %v", ref.ChunkID, err)
		}
	}
	catalogRefs, err := store.ListKnowledgeMigrationChunks(ctx, migration.Record{
		ID: "catalog-check", TenantID: tenantID, AppID: appID,
		Domain: migration.DomainKnowledge, SourceConfigVersion: v2.Version,
		TargetConfigVersion: v3.Version, Status: migration.StatusPending,
	})
	if err != nil {
		t.Fatalf("list SQL-authorized knowledge inventory: %v", err)
	}
	if len(catalogRefs) != len(refs) {
		t.Fatalf("knowledge catalog returned %d refs, want %d", len(catalogRefs), len(refs))
	}

	record, err := controlPlane.CreateKnowledgeMigration(ctx, scope, v2.Version, v3.Version)
	if err != nil {
		t.Fatalf("create knowledge migration: %v", err)
	}
	if started, err := controlPlane.BeginDataMigration(ctx, scope, record.ID, "", time.Now().Add(2*time.Minute), 0); err != nil {
		t.Fatalf("begin knowledge migration: %v", err)
	} else if started.Status != migration.StatusDraining || started.LeaseOwner != "" {
		t.Fatalf("knowledge migration begin = %#v", started)
	}

	endpointJSON, err := json.Marshal(map[string]knowledgeqdrant.Endpoint{
		sourceBackendName: {Host: host, Port: port},
		targetBackendName: {Host: host, Port: port},
	})
	if err != nil {
		t.Fatalf("encode qdrant endpoints: %v", err)
	}
	workerBinary := buildMigrationWorker(t)
	streamName := "trpc-agent-service:knowledge-migration-e2e:" + uuid.NewString()
	group := "knowledge-migration-e2e-workers"
	workerEnv := migrationWorkerEnv(postgresDSN, runtimeRedisURL(), streamName, group)
	workerEnv["TRPC_AGENT_SERVICE_QDRANT_ENDPOINTS"] = string(endpointJSON)
	workerEnv["TRPC_AGENT_SERVICE_FAULT_PAUSE_AFTER_MIGRATION_COPY_ITEM"] = knowledgeMigrationItemPause.String()

	evidence := knowledgeMigrationE2EEvidence{
		MigrationID: record.ID, TenantID: tenantID, AppID: appID,
		SourceConfig: v2.Version, TargetConfig: v3.Version,
		SourceCollection: sourceCollection, TargetCollection: targetCollection,
		Stream: streamName, CatalogCount: len(catalogRefs), ForeignPointID: "foreign-" + runID,
	}
	defer func() { writeKnowledgeMigrationE2EEvidence(t, evidence) }()

	first := startMigrationWorker(t, ctx, workerBinary, workerEnv, "knowledge-migration-worker-a")
	checkpoint, err := waitMigrationE2EState(ctx, pool, record.ID, func(value migrationE2EState) bool {
		return value.Status == string(migration.StatusCopying) && value.CopyProgress > 0 && value.CopyProgress < int64(len(refs))
	})
	if err != nil {
		t.Fatalf("wait knowledge migration checkpoint: %v\n%s", err,
			knowledgeMigrationDiagnostics(ctx, pool, qdrantClient, record.ID, tenantID, appID, sourceCollection, targetCollection, first.logs.String()))
	}
	evidence.CrashCheckpoint = checkpoint
	app, err := store.ResolveAgentApp(ctx, tenantID, appID)
	if err != nil {
		t.Fatalf("resolve app before knowledge migration crash: %v", err)
	}
	if app.ActiveConfigVersion != v2.Version {
		t.Fatalf("active config changed before knowledge verify: %q", app.ActiveConfigVersion)
	}
	stopRuntimeWorker(first, true)

	resumeEnv := cloneStringMap(workerEnv)
	delete(resumeEnv, "TRPC_AGENT_SERVICE_FAULT_PAUSE_AFTER_MIGRATION_COPY_ITEM")
	second := startMigrationWorker(t, ctx, workerBinary, resumeEnv, "knowledge-migration-worker-b")
	resumed, err := waitMigrationE2EState(ctx, pool, record.ID, func(value migrationE2EState) bool {
		// Copy progress is durable and monotonic. Lease ownership is cleared
		// when the migration reaches a terminal state, so it is too transient
		// to use as the resume wait condition.
		if value.CopyProgress <= checkpoint.CopyProgress {
			return false
		}
		switch value.Status {
		case string(migration.StatusCopying), string(migration.StatusVerifying), string(migration.StatusSucceeded):
			return true
		default:
			return false
		}
	})
	if err != nil {
		t.Fatalf("wait knowledge migration reclaim/resume after %s: %v\n%s", knowledgeMigrationLeaseWaitBuffer, err,
			knowledgeMigrationDiagnostics(ctx, pool, qdrantClient, record.ID, tenantID, appID, sourceCollection, targetCollection, second.logs.String()))
	}
	evidence.Resume = resumed
	terminal, err := waitMigrationE2EState(ctx, pool, record.ID, func(value migrationE2EState) bool {
		return value.Status == string(migration.StatusSucceeded)
	})
	if err != nil {
		t.Fatalf("wait knowledge migration terminal state: %v\n%s", err,
			knowledgeMigrationDiagnostics(ctx, pool, qdrantClient, record.ID, tenantID, appID, sourceCollection, targetCollection, second.logs.String()))
	}
	evidence.Terminal = terminal
	if terminal.TotalSessions != int64(len(refs)) ||
		terminal.CopyProgress != terminal.TotalSessions ||
		terminal.VerifyProgress != terminal.TotalSessions ||
		terminal.SuccessCount != terminal.TotalSessions ||
		terminal.LeaseOwner != "" || terminal.FailureStage != "" {
		t.Fatalf("knowledge migration terminal checkpoint = %#v", terminal)
	}
	app, err = store.ResolveAgentApp(ctx, tenantID, appID)
	if err != nil {
		t.Fatalf("resolve app after knowledge migration: %v", err)
	}
	if app.ActiveConfigVersion != v3.Version {
		t.Fatalf("active config after knowledge migration = %q, want %q", app.ActiveConfigVersion, v3.Version)
	}
	evidence.ActiveConfig = app.ActiveConfigVersion

	sourceDigest, sourceCount, err := knowledgeMigrationDigest(ctx, qdrantClient, sourceCollection, refs)
	if err != nil {
		t.Fatalf("digest source authorized knowledge points: %v", err)
	}
	targetDigest, targetCount, err := knowledgeMigrationDigest(ctx, qdrantClient, targetCollection, refs)
	if err != nil {
		t.Fatalf("digest target authorized knowledge points: %v", err)
	}
	evidence.SourceAuthorizedCount = sourceCount
	evidence.TargetAuthorizedCount = targetCount
	evidence.SourceDigest = sourceDigest
	evidence.TargetDigest = targetDigest
	if sourceDigest != targetDigest || sourceCount != len(refs) || targetCount != len(refs) {
		t.Fatalf("knowledge migration digest/count source=%s/%d target=%s/%d", sourceDigest, sourceCount, targetDigest, targetCount)
	}
	allTarget, err := scrollKnowledgePoints(ctx, qdrantClient, targetCollection, nil, uint32(len(refs)+1))
	if err != nil {
		t.Fatalf("scan target points for isolation assertion: %v", err)
	}
	if len(allTarget) != len(refs) {
		t.Fatalf("target point count = %d, want %d; unauthorized point may have crossed boundary", len(allTarget), len(refs))
	}
	for _, point := range allTarget {
		if pointID(point) == evidence.ForeignPointID {
			t.Fatalf("foreign point %q was copied to target", evidence.ForeignPointID)
		}
	}
	evidence.NoForeignPoint = true
	stopRuntimeWorker(second, true)
}

func knowledgeMigrationDiagnostics(
	ctx context.Context,
	pool *pgxpool.Pool,
	qdrantClient qdrantstorage.Client,
	migrationID, tenantID, appID, sourceCollection, targetCollection, workerLogs string,
) string {
	diagnosticCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	lines := []string{fmt.Sprintf("knowledge migration diagnostics: migration_id=%s", migrationID)}
	if value, err := readMigrationE2EState(diagnosticCtx, pool, migrationID); err != nil {
		lines = append(lines, "state_error="+platformlog.SafeError(err))
	} else {
		lines = append(lines, fmt.Sprintf(
			"status=%s lease_owner=%s total=%d copy_progress=%d verify_progress=%d failure_stage=%s failure_reason=%s",
			value.Status, value.LeaseOwner, value.TotalSessions, value.CopyProgress,
			value.VerifyProgress, value.FailureStage, safeKnowledgeDiagnostic(value.FailureReason)))
	}

	var catalogCount int64
	if err := pool.QueryRow(diagnosticCtx, `
SELECT count(*)
FROM platform.knowledge_chunk
WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID).Scan(&catalogCount); err != nil {
		lines = append(lines, "knowledge_catalog_count_error="+platformlog.SafeError(err))
	} else {
		lines = append(lines, fmt.Sprintf("knowledge_catalog_count=%d", catalogCount))
	}

	sourceExists, sourceErr := qdrantClient.CollectionExists(diagnosticCtx, sourceCollection)
	targetExists, targetErr := qdrantClient.CollectionExists(diagnosticCtx, targetCollection)
	lines = append(lines, fmt.Sprintf("source_collection=%s exists=%t", sourceCollection, sourceExists))
	if sourceErr != nil {
		lines = append(lines, "source_collection_error="+platformlog.SafeError(sourceErr))
	}
	lines = append(lines, fmt.Sprintf("target_collection=%s exists=%t", targetCollection, targetExists))
	if targetErr != nil {
		lines = append(lines, "target_collection_error="+platformlog.SafeError(targetErr))
	}
	lines = append(lines, "worker_last_log:\n"+safeKnowledgeWorkerLogTail(workerLogs))
	return strings.Join(lines, "\n")
}

func safeKnowledgeDiagnostic(value string) string {
	if value == "" {
		return ""
	}
	return platformlog.SafeError(fmt.Errorf("%s", value))
}

func safeKnowledgeWorkerLogTail(logs string) string {
	lines := strings.Split(strings.TrimSpace(logs), "\n")
	if len(lines) == 1 && lines[0] == "" {
		return "(none)"
	}
	if len(lines) > 20 {
		lines = lines[len(lines)-20:]
	}
	safeLines := make([]string, 0, len(lines))
	for _, line := range lines {
		lower := strings.ToLower(line)
		if strings.Contains(lower, "payload") || strings.Contains(lower, "request body") ||
			strings.Contains(lower, "raw request") || strings.Contains(lower, "raw payload") {
			safeLines = append(safeLines, "[redacted worker log line]")
			continue
		}
		safeLines = append(safeLines, platformlog.SafeError(fmt.Errorf("%s", line)))
	}
	return strings.Join(safeLines, "\n")
}

type knowledgeMigrationE2EEvidence struct {
	MigrationID           string            `json:"migration_id"`
	TenantID              string            `json:"tenant_id"`
	AppID                 string            `json:"app_id"`
	SourceConfig          string            `json:"source_config"`
	TargetConfig          string            `json:"target_config"`
	SourceCollection      string            `json:"source_collection"`
	TargetCollection      string            `json:"target_collection"`
	Stream                string            `json:"stream"`
	CatalogCount          int               `json:"catalog_count"`
	CrashCheckpoint       migrationE2EState `json:"crash_checkpoint"`
	Resume                migrationE2EState `json:"resume"`
	Terminal              migrationE2EState `json:"terminal"`
	ActiveConfig          string            `json:"active_config"`
	SourceAuthorizedCount int               `json:"source_authorized_count"`
	TargetAuthorizedCount int               `json:"target_authorized_count"`
	SourceDigest          string            `json:"source_digest"`
	TargetDigest          string            `json:"target_digest"`
	ForeignPointID        string            `json:"foreign_point_id"`
	NoForeignPoint        bool              `json:"no_foreign_point"`
}

func knowledgeMigrationConfig(tenantID, appID, version string) tenant.AppConfig {
	return tenant.AppConfig{
		TenantID: tenantID,
		AppID:    appID,
		Version:  version,
		Model: tenant.ModelConfig{
			Provider:  tenant.ModelProviderOpenAI,
			Model:     "knowledge-migration-e2e-model",
			APIKeyRef: tenant.SecretRef{Name: "knowledge-migration-e2e-model-key", Version: "1"},
		},
		BackendConfig: tenant.BackendConfig{
			Name:    "knowledge-migration-e2e",
			Session: tenant.BackendRef{Kind: tenant.BackendRedis, Provider: "redis", Name: "knowledge-migration-e2e-session"},
		},
		SecretRefs: []tenant.SecretRef{{Name: "knowledge-migration-e2e-model-key", Version: "1"}},
	}
}

func qdrantMigrationBackend(name, profile, generation string) tenant.BackendRef {
	return tenant.BackendRef{
		Kind: tenant.BackendVector, Provider: "qdrant", Name: name,
		Options: map[string]string{
			"embedding_model":      "knowledge-migration-e2e-embedding",
			"embedding_dimensions": "3",
			"embedding_profile":    profile,
			"index_generation":     generation,
		},
	}
}

func knowledgeMigrationDocument(ref platformknowledge.ChunkRef, id string) *document.Document {
	return &document.Document{
		ID:      id,
		Content: "knowledge migration content " + ref.ChunkID,
		Metadata: map[string]any{
			platformknowledge.MetadataTenantID:        ref.Scope.TenantID,
			platformknowledge.MetadataAppID:           ref.Scope.AppID,
			platformknowledge.MetadataKnowledgeBaseID: ref.KnowledgeBaseID,
			platformknowledge.MetadataDocumentID:      ref.DocumentID,
			platformknowledge.MetadataDocumentVersion: ref.DocumentVersion,
			platformknowledge.MetadataChunkID:         ref.ChunkID,
			platformknowledge.MetadataIndexGeneration: ref.IndexGeneration,
		},
	}
}

func createKnowledgeCollection(t *testing.T, ctx context.Context, host string, port int, collection string) {
	t.Helper()
	store, err := frameworkqdrant.New(ctx,
		frameworkqdrant.WithHost(host),
		frameworkqdrant.WithPort(port),
		frameworkqdrant.WithCollectionName(collection),
		frameworkqdrant.WithDimension(3),
	)
	if err != nil {
		t.Fatalf("create qdrant collection %s: %v", collection, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close qdrant collection %s: %v", collection, err)
	}
}

func knowledgeMigrationDigest(
	ctx context.Context,
	client qdrantstorage.Client,
	collection string,
	refs []platformknowledge.ChunkRef,
) (string, int, error) {
	digest := sha256.New()
	for _, ref := range refs {
		points, err := scrollKnowledgePoints(ctx, client, collection, knowledgeMigrationFilter(ref), 2)
		if err != nil {
			return "", 0, err
		}
		if len(points) != 1 {
			return "", 0, fmt.Errorf("collection %s has %d points for chunk %s", collection, len(points), ref.ChunkID)
		}
		encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(points[0])
		if err != nil {
			return "", 0, err
		}
		_, _ = digest.Write([]byte(ref.ChunkID))
		_, _ = digest.Write(encoded)
	}
	return hex.EncodeToString(digest.Sum(nil)), len(refs), nil
}

func scrollKnowledgePoints(
	ctx context.Context,
	client qdrantstorage.Client,
	collection string,
	filter *qdrantpb.Filter,
	limit uint32,
) ([]*qdrantpb.RetrievedPoint, error) {
	return client.Scroll(ctx, &qdrantpb.ScrollPoints{
		CollectionName: collection,
		Filter:         filter,
		Limit:          &limit,
		WithPayload:    qdrantpb.NewWithPayload(true),
		WithVectors:    qdrantpb.NewWithVectors(true),
	})
}

func knowledgeMigrationFilter(ref platformknowledge.ChunkRef) *qdrantpb.Filter {
	return &qdrantpb.Filter{Must: []*qdrantpb.Condition{
		qdrantpb.NewMatch("metadata."+platformknowledge.MetadataTenantID, ref.Scope.TenantID),
		qdrantpb.NewMatch("metadata."+platformknowledge.MetadataAppID, ref.Scope.AppID),
		qdrantpb.NewMatch("metadata."+platformknowledge.MetadataKnowledgeBaseID, ref.KnowledgeBaseID),
		qdrantpb.NewMatch("metadata."+platformknowledge.MetadataDocumentID, ref.DocumentID),
		qdrantpb.NewMatch("metadata."+platformknowledge.MetadataDocumentVersion, ref.DocumentVersion),
		qdrantpb.NewMatch("metadata."+platformknowledge.MetadataChunkID, ref.ChunkID),
		qdrantpb.NewMatch("metadata."+platformknowledge.MetadataIndexGeneration, ref.IndexGeneration),
	}}
}

func pointID(point *qdrantpb.RetrievedPoint) string {
	if point == nil || point.Id == nil {
		return ""
	}
	return point.Id.GetUuid()
}

func cloneStringMap(values map[string]string) map[string]string {
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func writeKnowledgeMigrationE2EEvidence(t *testing.T, evidence knowledgeMigrationE2EEvidence) {
	t.Helper()
	path := strings.TrimSpace(os.Getenv(knowledgeMigrationReportEnv))
	if path == "" {
		t.Logf("knowledge migration e2e evidence: %+v", evidence)
		return
	}
	encoded, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Logf("encode knowledge migration e2e evidence: %v", err)
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Logf("create knowledge migration evidence directory: %v", err)
		return
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Logf("write knowledge migration e2e evidence: %v", err)
	}
}
