package knowledgeingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/source"
)

type MigrationPipeline interface {
	LoadKnowledgeSource(context.Context, storage.KnowledgeLoadRequest, source.Source) (int, error)
	CountKnowledgeDocument(context.Context, string, string, string, config.BackendConfig) (int, error)
	ListKnowledgeDocuments(context.Context, string, string) ([]storage.KnowledgeDocument, error)
}

type MigrationManager interface {
	StartKnowledgeMigration(context.Context, string, string, string) (storage.KnowledgeMigrationStatus, error)
	AdvanceKnowledgeMigration(context.Context, string, string, string, uint64) (storage.KnowledgeMigrationStatus, error)
	RollbackKnowledgeMigration(context.Context, string, string, string, uint64) (storage.KnowledgeMigrationStatus, error)
}

// Migrator keeps source-backend reads available while rebuilding a remote
// derived index from canonical source documents. Knowledge writes are frozen
// by the Console for the lifetime of the active migration, so no dual-write
// vector protocol is required and the cutover is one immutable config publish.
type Migrator struct {
	store    storage.KnowledgeMigrationStore
	configs  tenant.Repository
	sources  storage.KnowledgeSourceStore
	pipeline MigrationPipeline
	profiles storage.BackendProfileResolver
}

func NewMigrator(store storage.KnowledgeMigrationStore, configs tenant.Repository, sources storage.KnowledgeSourceStore, pipeline MigrationPipeline, profiles storage.BackendProfileResolver) (*Migrator, error) {
	if store == nil || configs == nil || sources == nil || pipeline == nil || profiles == nil {
		return nil, errors.New("knowledge migration store, configuration repository, source store, pipeline, and backend profiles are required")
	}
	return &Migrator{store: store, configs: configs, sources: sources, pipeline: pipeline, profiles: profiles}, nil
}

func (m *Migrator) StartKnowledgeMigration(ctx context.Context, tenantID, appCode, targetProfileID string) (storage.KnowledgeMigrationStatus, error) {
	active, err := m.configs.GetActive(ctx, tenantID, appCode)
	if err != nil {
		return storage.KnowledgeMigrationStatus{}, err
	}
	sourceProfileID := strings.TrimSpace(active.Config.Storage.Knowledge.ProfileID)
	targetProfileID = strings.TrimSpace(targetProfileID)
	if sourceProfileID == "" || targetProfileID == "" {
		return storage.KnowledgeMigrationStatus{}, errors.New("knowledge migration source and target profiles are required")
	}
	sourcePhysical, err := m.profiles.ResolveTenantBackend(ctx, tenantID, storage.BackendDomainKnowledge, sourceProfileID)
	if err != nil {
		return storage.KnowledgeMigrationStatus{}, fmt.Errorf("resolve knowledge migration source profile: %w", err)
	}
	targetPhysical, err := m.profiles.ResolveTenantBackend(ctx, tenantID, storage.BackendDomainKnowledge, targetProfileID)
	if err != nil {
		return storage.KnowledgeMigrationStatus{}, fmt.Errorf("resolve knowledge migration target profile: %w", err)
	}
	sourceBackend := normalizedMigrationBackend(sourcePhysical)
	target := normalizedMigrationBackend(targetPhysical)
	if sourceBackend.Driver != "pgvector" || target.Driver != "qdrant" {
		return storage.KnowledgeMigrationStatus{}, errors.New("knowledge migration currently supports pgvector to qdrant")
	}
	if strings.TrimSpace(target.ConnectionRef) == "" {
		return storage.KnowledgeMigrationStatus{}, errors.New("Qdrant migration target requires connection_ref")
	}
	if sourceBackend.Driver == target.Driver && sourceBackend.ConnectionRef == target.ConnectionRef {
		return storage.KnowledgeMigrationStatus{}, errors.New("knowledge migration target already matches active backend")
	}
	documents, err := m.pipeline.ListKnowledgeDocuments(ctx, tenantID, appCode)
	if err != nil {
		return storage.KnowledgeMigrationStatus{}, fmt.Errorf("inspect knowledge documents before migration: %w", err)
	}
	for _, document := range documents {
		if document.Status != "ready" {
			return storage.KnowledgeMigrationStatus{}, fmt.Errorf("knowledge document %q is %q; migration requires every document to be ready", document.DocumentID, document.Status)
		}
	}
	// Probe the target using the same framework VectorStore constructor used by
	// retrieval before freezing knowledge mutations.
	if _, err := m.pipeline.CountKnowledgeDocument(ctx, tenantID, appCode, "__migration_probe__", target); err != nil {
		return storage.KnowledgeMigrationStatus{}, fmt.Errorf("prepare knowledge migration target: %w", err)
	}
	return m.store.CreateKnowledgeMigration(ctx, storage.KnowledgeMigrationStatus{
		TenantID: tenantID, AppCode: appCode,
		SourceProfileID: sourceProfileID, TargetProfileID: targetProfileID,
		Source: sourceBackend, Target: target,
	})
}

func (m *Migrator) AdvanceKnowledgeMigration(ctx context.Context, tenantID, appCode, id string, expected uint64) (storage.KnowledgeMigrationStatus, error) {
	var result storage.KnowledgeMigrationStatus
	err := m.store.WithKnowledgeMigrationLock(ctx, tenantID, appCode, func() error {
		status, err := m.store.GetKnowledgeMigration(ctx, tenantID, appCode, id)
		if err != nil {
			return err
		}
		if status.Generation != expected || expected == 0 {
			return storage.ErrKnowledgeMigrationConflict
		}
		switch status.Phase {
		case storage.KnowledgeMigrationPrepared:
			status.ReindexedDocuments, err = m.reindex(ctx, status)
			if err == nil {
				status.Phase = storage.KnowledgeMigrationReindexed
			}
		case storage.KnowledgeMigrationReindexed:
			status.VerifiedDocuments, err = m.verify(ctx, status)
			if err == nil {
				status.Phase = storage.KnowledgeMigrationVerified
			}
		case storage.KnowledgeMigrationVerified:
			err = m.publishTarget(ctx, status)
			if err == nil {
				status.Phase = storage.KnowledgeMigrationDone
			}
		case storage.KnowledgeMigrationDone, storage.KnowledgeMigrationRolledBack:
			result = status
			return nil
		default:
			return fmt.Errorf("unsupported knowledge migration phase %q", status.Phase)
		}
		if err != nil {
			status.LastError = truncateMigrationError(err.Error())
			updated, updateErr := m.store.UpdateKnowledgeMigration(ctx, status, expected)
			result = updated
			return errors.Join(err, updateErr)
		}
		status.LastError = ""
		result, err = m.store.UpdateKnowledgeMigration(ctx, status, expected)
		return err
	})
	return result, err
}

func (m *Migrator) RollbackKnowledgeMigration(ctx context.Context, tenantID, appCode, id string, expected uint64) (storage.KnowledgeMigrationStatus, error) {
	status, err := m.store.GetKnowledgeMigration(ctx, tenantID, appCode, id)
	if err != nil {
		return storage.KnowledgeMigrationStatus{}, err
	}
	if status.Generation != expected || expected == 0 {
		return storage.KnowledgeMigrationStatus{}, storage.ErrKnowledgeMigrationConflict
	}
	if status.Phase == storage.KnowledgeMigrationDone {
		return storage.KnowledgeMigrationStatus{}, errors.New("completed knowledge migration cannot be rolled back automatically")
	}
	if status.Phase == storage.KnowledgeMigrationRolledBack {
		return status, nil
	}
	status.Phase = storage.KnowledgeMigrationRolledBack
	status.LastError = ""
	return m.store.UpdateKnowledgeMigration(ctx, status, expected)
}

func (m *Migrator) reindex(ctx context.Context, status storage.KnowledgeMigrationStatus) (int, error) {
	sources, err := m.sources.ListKnowledgeDocumentSources(ctx, status.TenantID, status.AppCode)
	if err != nil {
		return 0, err
	}
	for index, canonical := range sources {
		if len(canonical.CanonicalDocuments) == 0 {
			return index, fmt.Errorf("knowledge document %q has no canonical framework snapshot", canonical.DocumentID)
		}
		var documents []*document.Document
		if err := json.Unmarshal(canonical.CanonicalDocuments, &documents); err != nil {
			return index, fmt.Errorf("decode canonical knowledge document %q: %w", canonical.DocumentID, err)
		}
		if len(documents) == 0 {
			return index, fmt.Errorf("knowledge document %q canonical snapshot is empty", canonical.DocumentID)
		}
		metadata := make(map[string]any, len(canonical.Metadata))
		for key, value := range canonical.Metadata {
			metadata[key] = value
		}
		src := &snapshotSource{name: canonical.Name, sourceType: "snapshot", metadata: metadata, documents: documents}
		count, loadErr := m.pipeline.LoadKnowledgeSource(ctx, storage.KnowledgeLoadRequest{
			TenantID: status.TenantID, AppCode: status.AppCode, DocumentID: canonical.DocumentID,
			JobID: status.ID, Owner: "knowledge-migration", ProfileID: status.TargetProfileID, Backend: status.Target,
		}, src)
		if loadErr != nil {
			return index, fmt.Errorf("reindex knowledge document %q: %w", canonical.DocumentID, loadErr)
		}
		if count == 0 {
			return index, fmt.Errorf("reindex knowledge document %q produced no chunks", canonical.DocumentID)
		}
	}
	return len(sources), nil
}

func (m *Migrator) verify(ctx context.Context, status storage.KnowledgeMigrationStatus) (int, error) {
	documents, err := m.pipeline.ListKnowledgeDocuments(ctx, status.TenantID, status.AppCode)
	if err != nil {
		return 0, err
	}
	verified := 0
	for _, document := range documents {
		if document.Status != "ready" {
			return verified, fmt.Errorf("knowledge document %q is not ready", document.DocumentID)
		}
		count, err := m.pipeline.CountKnowledgeDocument(ctx, status.TenantID, status.AppCode, document.DocumentID, status.Target)
		if err != nil {
			return verified, err
		}
		if count != document.TotalChunks {
			return verified, fmt.Errorf("knowledge document %q target chunks=%d source projection=%d", document.DocumentID, count, document.TotalChunks)
		}
		verified++
	}
	return verified, nil
}

func (m *Migrator) publishTarget(ctx context.Context, status storage.KnowledgeMigrationStatus) error {
	active, err := m.configs.GetActive(ctx, status.TenantID, status.AppCode)
	if err != nil {
		return err
	}
	currentProfileID := strings.TrimSpace(active.Config.Storage.Knowledge.ProfileID)
	if currentProfileID == status.TargetProfileID {
		return nil
	}
	if currentProfileID != status.SourceProfileID {
		return errors.New("active knowledge backend changed during migration")
	}
	versions, err := m.configs.ListVersions(ctx, status.TenantID, status.AppCode, 1)
	if err != nil || len(versions) == 0 {
		return fmt.Errorf("resolve next knowledge configuration version: %w", err)
	}
	next := active.Config
	next.ConfigVersion = versions[0].Config.ConfigVersion + 1
	next.Storage.Knowledge = config.BackendProfileRef{ProfileID: status.TargetProfileID}
	_, err = m.configs.Publish(ctx, next)
	return err
}

func normalizedMigrationBackend(backend config.BackendConfig) config.BackendConfig {
	backend.Driver = strings.ToLower(strings.TrimSpace(backend.Driver))
	if backend.Driver == "" {
		backend.Driver = "pgvector"
	}
	backend.ConnectionRef = strings.TrimSpace(backend.ConnectionRef)
	return backend
}

func truncateMigrationError(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 512 {
		return value[:512]
	}
	return value
}

var _ MigrationManager = (*Migrator)(nil)
