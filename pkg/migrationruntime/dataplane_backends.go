package migrationruntime

import (
	"context"
	"database/sql"
	"errors"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/datamigration"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/dataprojection"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	qdrantstore "trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/qdrant"
)

type knowledgeLiveBackend struct {
	runtime           *DataPlaneRuntime
	tenantID, profile string
	info              datamigration.LiveBackendInfo
}

func (b *knowledgeLiveBackend) MigrationInfo() datamigration.LiveBackendInfo { return b.info }
func (b *knowledgeLiveBackend) Keys(ctx context.Context, cursor string, limit int) ([]string, string, bool, error) {
	identities, next, done, err := b.runtime.scanKnowledge(ctx, b.tenantID, b.profile, cursor, limit)
	if err != nil {
		return nil, "", false, err
	}
	keys := make([]string, 0, len(identities))
	for _, identity := range identities {
		key, err := knowledgeKey(identity.AppName, identity.DocumentID)
		if err != nil {
			return nil, "", false, err
		}
		keys = append(keys, key)
	}
	return keys, next, done, nil
}

func (b *knowledgeLiveBackend) Read(ctx context.Context, key string, version int64) (datamigration.Record, error) {
	identity, err := parseKnowledgeKey(key)
	if err != nil {
		return datamigration.Record{}, err
	}
	store, err := b.runtime.openKnowledge(ctx, b.tenantID, identity.AgentAppID, b.profile)
	if err != nil {
		return datamigration.Record{}, err
	}
	defer store.Close()
	doc, embedding, err := store.Get(ctx, identity.DocumentID)
	if errors.Is(err, qdrantstore.ErrNotFound) || (err == nil && doc == nil) {
		return dataprojection.NewKnowledgeTombstone(identity.AgentAppID, identity.DocumentID, version)
	}
	if err != nil {
		return datamigration.Record{}, err
	}
	return dataprojection.NewKnowledgeRecord(identity.AgentAppID, doc, embedding, version)
}

func (b *knowledgeLiveBackend) Apply(ctx context.Context, record datamigration.Record) error {
	if err := record.Validate(); err != nil {
		return err
	}
	if _, err := parseKnowledgeKey(record.Key); err != nil {
		return err
	}
	// Canonical record construction is shared with the projector. Each live
	// apply closes its own raw client, including failed writes.
	identity, _ := parseKnowledgeKey(record.Key)
	store, err := b.runtime.openKnowledge(ctx, b.tenantID, identity.AgentAppID, b.profile)
	if err != nil {
		return err
	}
	defer store.Close()
	if record.Deleted {
		err := store.Delete(ctx, identity.DocumentID)
		if errors.Is(err, qdrantstore.ErrNotFound) {
			return nil
		}
		return err
	}
	var payload struct {
		Document  *document.Document `json:"document"`
		Embedding []float64          `json:"embedding"`
	}
	if err := decodeDataPlanePayload(record.Payload, &payload); err != nil || payload.Document == nil || payload.Document.ID != identity.DocumentID {
		return datamigration.ErrInvalidRecord
	}
	canonical, err := dataprojection.NewKnowledgeRecord(identity.AgentAppID, payload.Document, payload.Embedding, record.Version)
	if err != nil || canonical.Hash != record.Hash {
		return datamigration.ErrInvalidRecord
	}
	return store.Add(ctx, payload.Document, payload.Embedding)
}

type artifactLiveBackend struct {
	service  dataPlaneArtifactService
	tenantID string
	info     datamigration.LiveBackendInfo
}

func (b *artifactLiveBackend) MigrationInfo() datamigration.LiveBackendInfo { return b.info }
func (b *artifactLiveBackend) TargetEmpty(ctx context.Context) (bool, error) {
	return b.service.MigrationTargetEmpty(ctx)
}
func (b *artifactLiveBackend) ReconcileSource(ctx context.Context, key string) error {
	identity, err := parseArtifactKey(key)
	if err != nil {
		return err
	}
	return b.service.ReconcileMigrationVersion(ctx, identity.sessionInfo(), identity.Filename, identity.ArtifactVersion, identity.MIMEType)
}
func (b *artifactLiveBackend) Keys(ctx context.Context, cursor string, limit int) ([]string, string, bool, error) {
	entries, next, done, err := b.service.MigrationEntries(ctx, cursor, limit)
	if err != nil {
		return nil, "", false, err
	}
	keys := make([]string, 0, len(entries))
	for _, entry := range entries {
		key, err := artifactEntryKey(entry)
		if err != nil {
			return nil, "", false, err
		}
		keys = append(keys, key)
	}
	return keys, next, done, nil
}

func (b *artifactLiveBackend) Read(ctx context.Context, key string, version int64) (datamigration.Record, error) {
	if version < 0 {
		return datamigration.Record{}, datamigration.ErrInvalidRecord
	}
	identity, err := parseArtifactKey(key)
	if err != nil {
		return datamigration.Record{}, err
	}
	entry, value, err := b.service.ReadMigrationVersion(ctx, identity.sessionInfo(), identity.Filename, identity.ArtifactVersion)
	missing := errors.Is(err, sql.ErrNoRows)
	// A failed allocation can leave an intent for a MIME identity that was
	// never committed. A later allocation of this version may use another MIME.
	if entry.MIMEType != "" && entry.MIMEType != identity.MIMEType {
		missing, err = true, nil
	}
	if err != nil && !missing {
		return datamigration.Record{}, err
	}
	// Capture reads use zero before the journal allocates a sequence. The
	// projector constructors require a positive sequence for stored records.
	constructionVersion := max(version, 1)
	var record datamigration.Record
	if missing || entry.Deleted {
		record, err = dataprojection.NewArtifactTombstone(identity.sessionInfo(), identity.Filename, identity.ArtifactVersion, identity.MIMEType, constructionVersion)
	} else {
		record, err = dataprojection.NewArtifactRecord(identity.sessionInfo(), identity.Filename, identity.ArtifactVersion, value, constructionVersion)
	}
	record.Version = version
	return record, err
}

func (b *artifactLiveBackend) Apply(ctx context.Context, record datamigration.Record) error {
	if err := record.Validate(); err != nil {
		return err
	}
	identity, err := parseArtifactKey(record.Key)
	if err != nil {
		return err
	}
	if record.Deleted {
		return b.service.DeleteMigrationVersion(ctx, identity.sessionInfo(), identity.Filename, identity.ArtifactVersion, identity.MIMEType)
	}
	projector, err := dataprojection.NewArtifactProjector(func(context.Context, string) (dataprojection.ArtifactVersionStore, error) { return b.service, nil })
	if err != nil {
		return err
	}
	return projector.Apply(ctx, b.tenantID, record)
}

var _ datamigration.LiveBackend = (*knowledgeLiveBackend)(nil)
var _ datamigration.LiveBackend = (*artifactLiveBackend)(nil)
