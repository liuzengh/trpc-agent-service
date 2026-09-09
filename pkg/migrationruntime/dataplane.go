package migrationruntime

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"

	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/artifactplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/datamigration"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/dataprojection"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/knowledgeplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/runtimeplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
)

type DataPlaneCoordinator interface {
	WithRead(context.Context, string, datamigration.Domain, string, func(context.Context, string) error) error
	WithWrite(context.Context, string, datamigration.Domain, string, []string, func(context.Context, string) error) error
	WithWriteKeys(context.Context, string, datamigration.Domain, string, func(context.Context, string) ([]string, error), func(context.Context, string) error) error
}

type dataPlaneArtifactService interface {
	artifact.Service
	ProjectVersion(context.Context, artifact.SessionInfo, string, int, *artifact.Artifact) error
	DeleteProjectedVersion(context.Context, artifact.SessionInfo, string, int) error
	MigrationEntries(context.Context, string, int) ([]artifactplane.MigrationEntry, string, bool, error)
	MigrationFileEntries(context.Context, artifact.SessionInfo, string) ([]artifactplane.MigrationEntry, error)
	NextMigrationVersion(context.Context, artifact.SessionInfo, string) (int, error)
	ReadMigrationVersion(context.Context, artifact.SessionInfo, string, int) (artifactplane.MigrationEntry, *artifact.Artifact, error)
	ReconcileMigrationVersion(context.Context, artifact.SessionInfo, string, int, string) error
	DeleteMigrationVersion(context.Context, artifact.SessionInfo, string, int, string) error
	MigrationTargetEmpty(context.Context) (bool, error)
}

type DataPlaneRuntime struct {
	lifecycle     sync.Mutex
	closed        bool
	closers       map[io.Closer]struct{}
	coordinator   DataPlaneCoordinator
	openKnowledge func(context.Context, string, string, string) (vectorstore.VectorStore, error)
	scanKnowledge func(context.Context, string, string, string, int) ([]knowledgeplane.DocumentIdentity, string, bool, error)
	openArtifact  func(context.Context, string, string) (dataPlaneArtifactService, error)
	profileInfo   func(string, string, string) (runtimeplane.MigrationProfileInfo, error)
}

func NewDataPlaneRuntime(db *sql.DB, catalog *runtimeplane.Catalog, coordinator DataPlaneCoordinator) (*DataPlaneRuntime, error) {
	if db == nil || catalog == nil || coordinator == nil {
		return nil, datamigration.ErrMigrationCapability
	}
	return &DataPlaneRuntime{coordinator: coordinator,
		openKnowledge: func(ctx context.Context, tenantID, appName, profile string) (vectorstore.VectorStore, error) {
			return catalog.OpenKnowledgeStore(ctx, tenantID, appName, profile)
		},
		scanKnowledge: catalog.ScanKnowledge,
		openArtifact: func(ctx context.Context, tenantID, profile string) (dataPlaneArtifactService, error) {
			return catalog.OpenArtifactService(ctx, tenantID, profile, db)
		},
		profileInfo: catalog.MigrationProfile,
	}, nil
}

// Backend handles are scoped to a single coordinator operation. Qdrant handles
// close immediately after use; MinIO uses its shared HTTP transport.
func (r *DataPlaneRuntime) Close() error {
	r.lifecycle.Lock()
	r.closed = true
	closers := make([]io.Closer, 0, len(r.closers))
	for closer := range r.closers {
		closers = append(closers, closer)
	}
	r.lifecycle.Unlock()
	var result error
	for _, closer := range closers {
		result = errors.Join(result, closer.Close())
	}
	return result
}

func (r *DataPlaneRuntime) register(closer io.Closer) error {
	r.lifecycle.Lock()
	defer r.lifecycle.Unlock()
	if r.closed {
		return datamigration.ErrMigrationCapability
	}
	if r.closers == nil {
		r.closers = make(map[io.Closer]struct{})
	}
	r.closers[closer] = struct{}{}
	return nil
}
func (r *DataPlaneRuntime) unregister(closer io.Closer) {
	r.lifecycle.Lock()
	delete(r.closers, closer)
	r.lifecycle.Unlock()
}

func (r *DataPlaneRuntime) ResolveBackend(ctx context.Context, tenantID string, domain datamigration.Domain, profile string) (datamigration.LiveBackend, func(), error) {
	if r == nil || tenant.ValidateTenantID(tenantID) != nil || profile == "" {
		return nil, nil, datamigration.ErrMigrationCapability
	}
	backend := ""
	switch domain {
	case datamigration.DomainKnowledge:
		backend = "qdrant"
	case datamigration.DomainArtifact:
		backend = "s3"
	default:
		return nil, nil, datamigration.ErrMigrationCapability
	}
	info, err := r.profileInfo(tenantID, profile, backend)
	if err != nil {
		return nil, nil, err
	}
	metadata := datamigration.LiveBackendInfo{Backend: info.Backend, Identity: info.Identity, Compatibility: info.Compatibility}
	if domain == datamigration.DomainKnowledge {
		probe, err := r.openKnowledge(ctx, tenantID, "migration-probe", profile)
		if err != nil {
			return nil, nil, err
		}
		if err := probe.Close(); err != nil {
			return nil, nil, err
		}
		return &knowledgeLiveBackend{runtime: r, tenantID: tenantID, profile: profile, info: metadata}, func() {}, nil
	}
	service, err := r.openArtifact(ctx, tenantID, profile)
	if err != nil {
		return nil, nil, err
	}
	return &artifactLiveBackend{service: service, tenantID: tenantID, info: metadata}, func() {}, nil
}

func (r *DataPlaneRuntime) DecorateKnowledge(_ context.Context, tenantID, appName, profile string, inner vectorstore.VectorStore) (vectorstore.VectorStore, error) {
	if r == nil || r.coordinator == nil || inner == nil || tenant.ValidateTenantID(tenantID) != nil || tenant.ValidateAgentAppName(appName) != nil || profile == "" {
		return nil, datamigration.ErrMigrationCapability
	}
	store := &liveKnowledgeStore{runtime: r, tenantID: tenantID, appName: appName, profile: profile}
	store.cache = newDataPlaneCache(profile, inner, func(ctx context.Context, profile string) (vectorstore.VectorStore, error) {
		return r.openKnowledge(ctx, tenantID, appName, profile)
	}, func(value vectorstore.VectorStore) error { return value.Close() })
	if err := r.register(store); err != nil {
		return nil, err
	}
	return store, nil
}

func (r *DataPlaneRuntime) DecorateArtifact(_ context.Context, tenantID, profile string, inner artifact.Service) (artifact.Service, error) {
	if r == nil || r.coordinator == nil || inner == nil || tenant.ValidateTenantID(tenantID) != nil || profile == "" {
		return nil, datamigration.ErrMigrationCapability
	}
	raw, ok := inner.(dataPlaneArtifactService)
	if !ok {
		return nil, datamigration.ErrMigrationCapability
	}
	service := &liveArtifactService{runtime: r, tenantID: tenantID, profile: profile}
	service.cache = newDataPlaneCache(profile, raw, func(ctx context.Context, profile string) (dataPlaneArtifactService, error) {
		return r.openArtifact(ctx, tenantID, profile)
	}, func(dataPlaneArtifactService) error { return nil })
	if err := r.register(service); err != nil {
		return nil, err
	}
	return service, nil
}

type knowledgeLiveIdentity struct {
	AgentAppID string `json:"agent_app_id"`
	DocumentID string `json:"document_id"`
}
type artifactLiveIdentity struct {
	AppName         string `json:"app_name"`
	UserID          string `json:"user_id"`
	SessionID       string `json:"session_id"`
	Filename        string `json:"filename"`
	ArtifactVersion int    `json:"artifact_version"`
	MIMEType        string `json:"mime_type"`
}

func decodeLiveKey(key, prefix string, target any) error {
	if len(key) > 4096 || !strings.HasPrefix(key, prefix) {
		return datamigration.ErrInvalidRecord
	}
	body, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(key, prefix))
	if err != nil {
		return datamigration.ErrInvalidRecord
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil || !errors.Is(decoder.Decode(&struct{}{}), io.EOF) {
		return datamigration.ErrInvalidRecord
	}
	canonical, err := json.Marshal(target)
	if err != nil || prefix+base64.RawURLEncoding.EncodeToString(canonical) != key {
		return datamigration.ErrInvalidRecord
	}
	return nil
}

func knowledgeKey(appName, documentID string) (string, error) {
	record, err := dataprojection.NewKnowledgeTombstone(appName, documentID, 1)
	return record.Key, err
}

func artifactEntryKey(entry artifactplane.MigrationEntry) (string, error) {
	record, err := dataprojection.NewArtifactTombstone(entry.SessionInfo(), entry.Filename, entry.Version, entry.MIMEType, 1)
	return record.Key, err
}

func parseKnowledgeKey(key string) (knowledgeLiveIdentity, error) {
	var identity knowledgeLiveIdentity
	if err := decodeLiveKey(key, "knowledge/v1/", &identity); err != nil {
		return identity, err
	}
	canonical, err := knowledgeKey(identity.AgentAppID, identity.DocumentID)
	if err != nil || canonical != key {
		return identity, datamigration.ErrInvalidRecord
	}
	return identity, nil
}

func parseArtifactKey(key string) (artifactLiveIdentity, error) {
	var identity artifactLiveIdentity
	if err := decodeLiveKey(key, "artifact/v1/", &identity); err != nil {
		return identity, err
	}
	canonical, err := dataprojection.NewArtifactTombstone(identity.sessionInfo(), identity.Filename, identity.ArtifactVersion, identity.MIMEType, 1)
	if err != nil || canonical.Key != key {
		return identity, datamigration.ErrInvalidRecord
	}
	return identity, nil
}

func (i artifactLiveIdentity) sessionInfo() artifact.SessionInfo {
	return artifact.SessionInfo{AppName: i.AppName, UserID: i.UserID, SessionID: i.SessionID}
}

func decodeDataPlanePayload(body []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil || !errors.Is(decoder.Decode(&struct{}{}), io.EOF) {
		return datamigration.ErrInvalidRecord
	}
	return nil
}
