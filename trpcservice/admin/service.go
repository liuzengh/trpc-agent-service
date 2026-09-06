// Package admin validates and mutates control-plane configuration.
package admin

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/background"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecommcp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	platformtenant "github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/toolexec"
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
var ErrInvalid = errors.New("invalid Admin request")

type Service struct {
	repository   controlplane.MutableRepository
	tools        *platformtool.Catalog
	audit        audit.Writer
	knowledge    *platformstorage.KnowledgeRouter
	jobs         background.Repository
	secrets      secret.Authorizer
	operations   *toolexec.Operations
	toolJournal  toolexec.Journal
	channelState wecommcp.Store
}

func (s *Service) WithToolOperations(operations *toolexec.Operations, journal toolexec.Journal) *Service {
	s.operations, s.toolJournal = operations, journal
	return s
}

func (s *Service) WithBackgroundJobs(repository background.Repository) *Service {
	if s != nil {
		s.jobs = repository
	}
	return s
}

func (s *Service) WithKnowledgeRouter(router *platformstorage.KnowledgeRouter) *Service {
	if s != nil {
		s.knowledge = router
	}
	return s
}

type KnowledgeDocumentInput struct {
	TenantID    string         `json:"tenant_id"`
	AppID       string         `json:"app_id"`
	RevisionID  string         `json:"revision_id"`
	DocumentID  string         `json:"document_id"`
	OperationID string         `json:"operation_id,omitempty"`
	Name        string         `json:"name"`
	Content     string         `json:"content"`
	Metadata    map[string]any `json:"metadata"`
}

type KnowledgeOperationResult struct {
	JobID      string `json:"job_id,omitempty"`
	DocumentID string `json:"document_id"`
	Chunks     int    `json:"chunks,omitempty"`
	Queued     bool   `json:"queued"`
	Duplicate  bool   `json:"duplicate,omitempty"`
}

func (s *Service) UpsertKnowledgeDocument(
	ctx context.Context,
	input KnowledgeDocumentInput,
) (int, error) {
	if s.knowledge == nil {
		return 0, invalidf("knowledge router is unavailable")
	}
	if !identifierPattern.MatchString(input.TenantID) ||
		!identifierPattern.MatchString(input.AppID) ||
		!identifierPattern.MatchString(input.RevisionID) ||
		!identifierPattern.MatchString(input.DocumentID) || strings.TrimSpace(input.Content) == "" {
		return 0, invalidf("knowledge document identity and content are invalid")
	}
	revision, scope, err := s.knowledgeScope(ctx, input.TenantID, input.AppID, input.RevisionID)
	if err != nil {
		return 0, err
	}
	chunks, err := s.knowledge.UpsertDocument(ctx, scope, revision, platformstorage.KnowledgeDocument{
		ID: input.DocumentID, Name: input.Name, Content: input.Content, Metadata: input.Metadata,
	})
	if err != nil {
		return 0, err
	}
	if err := s.record(ctx, input.TenantID, "admin_knowledge_document_upserted", map[string]any{
		"app_id": input.AppID, "revision_id": input.RevisionID,
		"document_id": input.DocumentID, "chunks": chunks,
	}); err != nil {
		return 0, err
	}
	return chunks, nil
}

func (s *Service) SubmitKnowledgeDocument(
	ctx context.Context,
	input KnowledgeDocumentInput,
) (KnowledgeOperationResult, error) {
	if s.jobs == nil {
		chunks, err := s.UpsertKnowledgeDocument(ctx, input)
		return KnowledgeOperationResult{
			DocumentID: input.DocumentID, Chunks: chunks,
		}, err
	}
	if _, _, err := s.knowledgeScope(ctx, input.TenantID, input.AppID, input.RevisionID); err != nil {
		return KnowledgeOperationResult{}, err
	}
	if !identifierPattern.MatchString(input.DocumentID) || strings.TrimSpace(input.Content) == "" {
		return KnowledgeOperationResult{}, invalidf("knowledge document identity and content are invalid")
	}
	payload, err := json.Marshal(background.KnowledgeUpsertPayload{Document: platformstorage.KnowledgeDocument{
		ID: input.DocumentID, Name: input.Name, Content: input.Content, Metadata: input.Metadata,
	}})
	if err != nil {
		return KnowledgeOperationResult{}, err
	}
	operationID := strings.TrimSpace(input.OperationID)
	if operationID == "" {
		digest := sha256.Sum256(payload)
		operationID = hex.EncodeToString(digest[:])
	}
	if !identifierPattern.MatchString(operationID) {
		return KnowledgeOperationResult{}, invalidf("knowledge operation_id is invalid")
	}
	queued, err := s.jobs.Enqueue(ctx, background.EnqueueRequest{
		TenantID: input.TenantID, AppID: input.AppID, RevisionID: input.RevisionID,
		Type:      background.JobKnowledgeUpsert,
		DedupeKey: input.DocumentID + ":" + operationID,
		Payload:   payload, TraceParent: background.TraceParent(ctx),
	})
	if err != nil {
		return KnowledgeOperationResult{}, err
	}
	if err := s.record(ctx, input.TenantID, "admin_knowledge_job_enqueued", map[string]any{
		"app_id": input.AppID, "revision_id": input.RevisionID,
		"document_id": input.DocumentID, "job_id": queued.Job.ID,
	}); err != nil {
		return KnowledgeOperationResult{}, err
	}
	return KnowledgeOperationResult{
		JobID: queued.Job.ID, DocumentID: input.DocumentID,
		Queued: true, Duplicate: queued.Duplicate,
	}, nil
}

func (s *Service) DeleteKnowledgeDocument(
	ctx context.Context,
	tenantID string,
	appID string,
	revisionID string,
	documentID string,
) error {
	if s.knowledge == nil {
		return invalidf("knowledge router is unavailable")
	}
	if !identifierPattern.MatchString(documentID) {
		return invalidf("knowledge document ID is invalid")
	}
	revision, scope, err := s.knowledgeScope(ctx, tenantID, appID, revisionID)
	if err != nil {
		return err
	}
	if err := s.knowledge.DeleteDocument(ctx, scope, revision, documentID); err != nil {
		return err
	}
	return s.record(ctx, tenantID, "admin_knowledge_document_deleted", map[string]any{
		"app_id": appID, "revision_id": revisionID, "document_id": documentID,
	})
}

func (s *Service) SubmitKnowledgeDelete(
	ctx context.Context,
	tenantID string,
	appID string,
	revisionID string,
	documentID string,
	operationID string,
) (KnowledgeOperationResult, error) {
	if s.jobs == nil {
		err := s.DeleteKnowledgeDocument(ctx, tenantID, appID, revisionID, documentID)
		return KnowledgeOperationResult{DocumentID: documentID}, err
	}
	if _, _, err := s.knowledgeScope(ctx, tenantID, appID, revisionID); err != nil {
		return KnowledgeOperationResult{}, err
	}
	if !identifierPattern.MatchString(documentID) || !identifierPattern.MatchString(operationID) {
		return KnowledgeOperationResult{}, invalidf("knowledge document_id and operation_id are invalid")
	}
	payload, err := json.Marshal(background.KnowledgeDeletePayload{DocumentID: documentID})
	if err != nil {
		return KnowledgeOperationResult{}, err
	}
	queued, err := s.jobs.Enqueue(ctx, background.EnqueueRequest{
		TenantID: tenantID, AppID: appID, RevisionID: revisionID,
		Type:      background.JobKnowledgeDelete,
		DedupeKey: documentID + ":" + operationID,
		Payload:   payload, TraceParent: background.TraceParent(ctx),
	})
	if err != nil {
		return KnowledgeOperationResult{}, err
	}
	return KnowledgeOperationResult{
		JobID: queued.Job.ID, DocumentID: documentID,
		Queued: true, Duplicate: queued.Duplicate,
	}, nil
}

func (s *Service) GetBackgroundJob(
	ctx context.Context,
	tenantID string,
	jobID string,
) (background.Job, error) {
	if s.jobs == nil {
		return background.Job{}, invalidf("background jobs are unavailable")
	}
	if !identifierPattern.MatchString(tenantID) || !identifierPattern.MatchString(jobID) {
		return background.Job{}, invalidf("background job identity is invalid")
	}
	return s.jobs.Get(ctx, tenantID, jobID)
}

func (s *Service) RetryBackgroundJob(
	ctx context.Context,
	tenantID string,
	jobID string,
) error {
	if s.jobs == nil {
		return invalidf("background jobs are unavailable")
	}
	if !identifierPattern.MatchString(tenantID) || !identifierPattern.MatchString(jobID) {
		return invalidf("background job identity is invalid")
	}
	if err := s.jobs.Retry(ctx, tenantID, jobID); err != nil {
		return err
	}
	return s.record(ctx, tenantID, "admin_background_job_retried", map[string]any{
		"job_id": jobID,
	})
}

func (s *Service) QueryAudit(
	ctx context.Context,
	query audit.Query,
) ([]audit.Event, error) {
	reader, ok := s.audit.(audit.Reader)
	if !ok {
		return nil, invalidf("audit query is unavailable")
	}
	if !identifierPattern.MatchString(query.TenantID) {
		return nil, invalidf("audit tenant_id is invalid")
	}
	return reader.Query(ctx, query)
}

func (s *Service) knowledgeScope(
	ctx context.Context,
	tenantID string,
	appID string,
	revisionID string,
) (controlplane.AgentRevision, runtimecontext.Scope, error) {
	revision, err := s.repository.GetRevision(ctx, tenantID, revisionID)
	if err != nil {
		return controlplane.AgentRevision{}, runtimecontext.Scope{}, err
	}
	if revision.AppID != appID {
		return controlplane.AgentRevision{}, runtimecontext.Scope{}, invalidf("revision does not belong to app")
	}
	scope, err := runtimecontext.NewScope(tenantID, appID, revisionID, "admin", "admin-knowledge")
	return revision, scope, err
}

func New(repository controlplane.Repository, catalogs ...*platformtool.Catalog) (*Service, error) {
	mutable, ok := repository.(controlplane.MutableRepository)
	if !ok {
		return nil, fmt.Errorf("control-plane repository is not mutable")
	}
	catalog := platformtool.DefaultCatalog()
	if len(catalogs) > 0 && catalogs[0] != nil {
		catalog = catalogs[0]
	}
	return &Service{repository: mutable, tools: catalog}, nil
}

// WithAuditWriter enables fail-closed audit recording for successful control
// plane mutations. The writer is owned by the process, not by Service.
func (s *Service) WithAuditWriter(writer audit.Writer) *Service {
	if s != nil {
		s.audit = writer
	}
	return s
}

func (s *Service) CreateTenant(ctx context.Context, tenant controlplane.Tenant) (controlplane.Tenant, error) {
	if !identifierPattern.MatchString(tenant.ID) || strings.TrimSpace(tenant.DisplayName) == "" {
		return controlplane.Tenant{}, invalidf("tenant ID and display name are invalid")
	}
	if tenant.Status == "" {
		tenant.Status = controlplane.StatusActive
	}
	if tenant.Region == "" || tenant.SecretNamespace == "" {
		return controlplane.Tenant{}, invalidf("tenant region and secret namespace are required")
	}
	if err := normalizeJSON(&tenant.QuotaConfig); err != nil {
		return controlplane.Tenant{}, invalidf("quota config: %v", err)
	}
	if _, err := platformtenant.ParseQuotaPolicy(tenant.QuotaConfig); err != nil {
		return controlplane.Tenant{}, invalidf("quota config: %v", err)
	}
	if err := normalizeJSON(&tenant.AuditPolicy); err != nil {
		return controlplane.Tenant{}, invalidf("audit policy: %v", err)
	}
	if _, err := audit.ParsePolicy(tenant.AuditPolicy); err != nil {
		return controlplane.Tenant{}, invalidf("audit policy: %v", err)
	}
	now := time.Now().UTC()
	tenant.Version = 1
	tenant.CreatedAt = now
	tenant.UpdatedAt = now
	if err := s.repository.CreateTenant(ctx, tenant); err != nil {
		return controlplane.Tenant{}, err
	}
	if err := s.record(ctx, tenant.ID, "admin_tenant_created", map[string]any{
		"tenant_id": tenant.ID, "version": tenant.Version,
	}); err != nil {
		return controlplane.Tenant{}, err
	}
	return tenant, nil
}

func (s *Service) CreateAgentApp(ctx context.Context, app controlplane.AgentApp) (controlplane.AgentApp, error) {
	if !identifierPattern.MatchString(app.ID) || !identifierPattern.MatchString(app.TenantID) ||
		strings.TrimSpace(app.Name) == "" {
		return controlplane.AgentApp{}, invalidf("Agent app identity is invalid")
	}
	if app.Status == "" {
		app.Status = controlplane.StatusActive
	}
	if err := normalizeJSON(&app.RolloutPolicy); err != nil {
		return controlplane.AgentApp{}, invalidf("rollout policy: %v", err)
	}
	now := time.Now().UTC()
	app.StableRevisionID = ""
	app.Version = 1
	app.CreatedAt = now
	app.UpdatedAt = now
	if err := s.repository.CreateAgentApp(ctx, app); err != nil {
		return controlplane.AgentApp{}, err
	}
	if err := s.record(ctx, app.TenantID, "admin_app_created", map[string]any{
		"app_id": app.ID, "version": app.Version,
	}); err != nil {
		return controlplane.AgentApp{}, err
	}
	return app, nil
}

func (s *Service) CreateRevision(
	ctx context.Context,
	revision controlplane.AgentRevision,
) (controlplane.AgentRevision, error) {
	if revision.ID == "" {
		revision.ID = "rev-" + uuid.NewString()
	}
	if !identifierPattern.MatchString(revision.ID) ||
		!identifierPattern.MatchString(revision.TenantID) ||
		!identifierPattern.MatchString(revision.AppID) || revision.RevisionNo <= 0 {
		return controlplane.AgentRevision{}, invalidf("Agent revision identity is invalid")
	}
	if revision.AgentType == "" || revision.CreatedBy == "" {
		return controlplane.AgentRevision{}, invalidf("Agent type and creator are required")
	}
	for name, value := range map[string]*json.RawMessage{
		"agent_config":     &revision.AgentConfig,
		"model_config":     &revision.ModelConfig,
		"tool_policy":      &revision.ToolPolicy,
		"knowledge_config": &revision.KnowledgeConfig,
		"memory_config":    &revision.MemoryConfig,
		"guardrail_config": &revision.GuardrailConfig,
	} {
		if err := normalizeJSON(value); err != nil {
			return controlplane.AgentRevision{}, invalidf("%s: %v", name, err)
		}
	}
	if err := s.authorizeRevisionSecrets(ctx, revision); err != nil {
		return controlplane.AgentRevision{}, err
	}
	toolPolicy, err := governance.ParseToolPolicy(revision.ToolPolicy)
	if err != nil {
		return controlplane.AgentRevision{}, invalidf("tool_policy: %v", err)
	}
	if _, err := s.tools.Resolve(toolPolicy.AllowedTools); err != nil {
		return controlplane.AgentRevision{}, invalidf("tool_policy: %v", err)
	}
	if err := platformstorage.ValidateRevisionKnowledgeConfig(revision.KnowledgeConfig); err != nil {
		return controlplane.AgentRevision{}, invalidf("knowledge_config: %v", err)
	}
	if _, err := governance.BuildModelCallbacks(revision.GuardrailConfig); err != nil {
		return controlplane.AgentRevision{}, invalidf("guardrail_config: %v", err)
	}
	revision.Checksum = controlplane.RevisionChecksum(revision)
	revision.CreatedAt = time.Now().UTC()
	if err := s.repository.CreateRevision(ctx, revision); err != nil {
		return controlplane.AgentRevision{}, err
	}
	if err := s.record(ctx, revision.TenantID, "admin_revision_created", map[string]any{
		"app_id": revision.AppID, "revision_id": revision.ID,
		"revision_no": revision.RevisionNo, "checksum": revision.Checksum,
	}); err != nil {
		return controlplane.AgentRevision{}, err
	}
	return revision, nil
}

func (s *Service) PublishRevision(
	ctx context.Context,
	tenantID string,
	appID string,
	revisionID string,
	expectedVersion int64,
) (controlplane.AgentApp, error) {
	if expectedVersion <= 0 {
		return controlplane.AgentApp{}, invalidf("expected app version must be positive")
	}
	app, err := s.repository.PublishRevision(ctx, tenantID, appID, revisionID, expectedVersion)
	if err != nil {
		return controlplane.AgentApp{}, err
	}
	if err := s.record(ctx, tenantID, "admin_revision_published", map[string]any{
		"app_id": appID, "revision_id": revisionID,
		"previous_version": expectedVersion, "version": app.Version,
	}); err != nil {
		return controlplane.AgentApp{}, err
	}
	return app, nil
}

func (s *Service) UpdateRolloutPolicy(
	ctx context.Context,
	tenantID string,
	appID string,
	policy json.RawMessage,
	expectedVersion int64,
) (controlplane.AgentApp, error) {
	if expectedVersion <= 0 {
		return controlplane.AgentApp{}, invalidf("expected app version must be positive")
	}
	if err := normalizeJSON(&policy); err != nil {
		return controlplane.AgentApp{}, invalidf("rollout policy: %v", err)
	}
	var config struct {
		Mode             string `json:"mode"`
		CanaryRevisionID string `json:"canary_revision_id"`
		CanaryPercent    int    `json:"canary_percent"`
		Salt             string `json:"salt"`
	}
	decoder := json.NewDecoder(bytes.NewReader(policy))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil ||
		decoder.Decode(&struct{}{}) != io.EOF ||
		config.CanaryPercent < 0 || config.CanaryPercent > 100 {
		return controlplane.AgentApp{}, invalidf("rollout policy is invalid")
	}
	if config.CanaryPercent > 0 {
		if config.CanaryRevisionID == "" {
			return controlplane.AgentApp{}, invalidf("canary_revision_id is required")
		}
		revision, err := s.repository.GetRevision(ctx, tenantID, config.CanaryRevisionID)
		if err != nil {
			return controlplane.AgentApp{}, err
		}
		if revision.AppID != appID {
			return controlplane.AgentApp{}, invalidf("canary revision does not belong to app")
		}
	}
	app, err := s.repository.UpdateRolloutPolicy(
		ctx, tenantID, appID, policy, expectedVersion,
	)
	if err != nil {
		return controlplane.AgentApp{}, err
	}
	if err := s.record(ctx, tenantID, "admin_rollout_policy_updated", map[string]any{
		"app_id": appID, "canary_revision_id": config.CanaryRevisionID,
		"canary_percent": config.CanaryPercent, "version": app.Version,
	}); err != nil {
		return controlplane.AgentApp{}, err
	}
	return app, nil
}

func (s *Service) CreateChannelBinding(
	ctx context.Context,
	binding controlplane.ChannelBinding,
) (controlplane.ChannelBinding, error) {
	if binding.ID == "" {
		binding.ID = "binding-" + uuid.NewString()
	}
	if binding.CallbackKey == "" {
		binding.CallbackKey = strings.ReplaceAll(uuid.NewString(), "-", "")
	}
	if !identifierPattern.MatchString(binding.ID) ||
		!identifierPattern.MatchString(binding.TenantID) ||
		!identifierPattern.MatchString(binding.AppID) ||
		!identifierPattern.MatchString(binding.CallbackKey) ||
		binding.ChannelType == "" || binding.AccountID == "" || binding.SecretRef == "" {
		return controlplane.ChannelBinding{}, invalidf("channel binding is incomplete")
	}
	if err := normalizeJSON(&binding.Config); err != nil {
		return controlplane.ChannelBinding{}, invalidf("channel binding config: %v", err)
	}
	if err := s.authorizeChannelSecrets(ctx, binding); err != nil {
		return controlplane.ChannelBinding{}, err
	}
	if binding.Status == "" {
		binding.Status = controlplane.StatusActive
	}
	now := time.Now().UTC()
	binding.Version = 1
	binding.CreatedAt = now
	binding.UpdatedAt = now
	if err := s.repository.CreateChannelBinding(ctx, binding); err != nil {
		return controlplane.ChannelBinding{}, err
	}
	if err := s.record(ctx, binding.TenantID, "admin_channel_binding_created", map[string]any{
		"app_id": binding.AppID, "binding_id": binding.ID,
		"channel": binding.ChannelType, "version": binding.Version,
	}); err != nil {
		return controlplane.ChannelBinding{}, err
	}
	return binding, nil
}

func (s *Service) UpdateChannelBinding(
	ctx context.Context,
	tenantID string,
	bindingID string,
	config json.RawMessage,
	status string,
	expectedVersion int64,
) (controlplane.ChannelBinding, error) {
	if !identifierPattern.MatchString(tenantID) ||
		!identifierPattern.MatchString(bindingID) || expectedVersion <= 0 {
		return controlplane.ChannelBinding{}, invalidf("channel binding update identity is invalid")
	}
	if status != controlplane.StatusActive && status != controlplane.StatusDisabled {
		return controlplane.ChannelBinding{}, invalidf("channel binding status is invalid")
	}
	if err := normalizeJSON(&config); err != nil {
		return controlplane.ChannelBinding{}, invalidf("channel binding config: %v", err)
	}
	current, err := s.repository.GetChannelBinding(ctx, tenantID, bindingID)
	if err != nil {
		return controlplane.ChannelBinding{}, err
	}
	current.Config = config
	if err := validateChannelShape(current); err != nil {
		return controlplane.ChannelBinding{}, err
	}
	// Disabling remains possible after a grant has been revoked.
	if status == controlplane.StatusActive {
		if err := s.authorizeChannelSecrets(ctx, current); err != nil {
			return controlplane.ChannelBinding{}, err
		}
	}
	binding, err := s.repository.UpdateChannelBinding(
		ctx, tenantID, bindingID, config, status, expectedVersion,
	)
	if err != nil {
		return controlplane.ChannelBinding{}, err
	}
	if err := s.record(ctx, tenantID, "admin_channel_binding_updated", map[string]any{
		"app_id": binding.AppID, "binding_id": binding.ID,
		"channel": binding.ChannelType, "previous_version": expectedVersion,
		"version": binding.Version, "status": binding.Status,
	}); err != nil {
		return controlplane.ChannelBinding{}, err
	}
	return binding, nil
}

func (s *Service) CreateBackendBinding(
	ctx context.Context,
	binding controlplane.BackendBinding,
) (controlplane.BackendBinding, error) {
	if binding.ID == "" {
		binding.ID = "backend-" + uuid.NewString()
	}
	if !identifierPattern.MatchString(binding.ID) ||
		!identifierPattern.MatchString(binding.TenantID) ||
		(binding.AppID != "" && !identifierPattern.MatchString(binding.AppID)) ||
		binding.ResourceType == "" || binding.BackendType == "" {
		return controlplane.BackendBinding{}, invalidf("backend binding is incomplete")
	}
	if err := normalizeJSON(&binding.Config); err != nil {
		return controlplane.BackendBinding{}, invalidf("backend binding config: %v", err)
	}
	if binding.SecretRef != "" {
		if err := s.authorizeSecret(ctx, binding.TenantID, binding.ResourceType, binding.SecretRef); err != nil {
			return controlplane.BackendBinding{}, err
		}
	}
	if binding.IsolationLevel == "" {
		binding.IsolationLevel = "shared"
	}
	if binding.MigrationState == "" {
		binding.MigrationState = "active"
	}
	now := time.Now().UTC()
	binding.Version = 1
	binding.CreatedAt = now
	binding.UpdatedAt = now
	if err := s.repository.CreateBackendBinding(ctx, binding); err != nil {
		return controlplane.BackendBinding{}, err
	}
	if err := s.record(ctx, binding.TenantID, "admin_backend_binding_created", map[string]any{
		"app_id": binding.AppID, "binding_id": binding.ID,
		"resource_type": binding.ResourceType, "backend_type": binding.BackendType,
		"version": binding.Version,
	}); err != nil {
		return controlplane.BackendBinding{}, err
	}
	return binding, nil
}

func (s *Service) CreateBackendMigration(
	ctx context.Context,
	migration controlplane.BackendMigration,
) (controlplane.BackendMigration, error) {
	if migration.ID == "" {
		migration.ID = "migration-" + uuid.NewString()
	}
	if !identifierPattern.MatchString(migration.ID) ||
		!identifierPattern.MatchString(migration.TenantID) ||
		(migration.AppID != "" && !identifierPattern.MatchString(migration.AppID)) ||
		migration.ResourceType == "" || migration.SourceBindingID == migration.TargetBindingID {
		return controlplane.BackendMigration{}, invalidf("backend migration identity is invalid")
	}
	source, err := s.repository.GetBackendBinding(ctx, migration.TenantID, migration.SourceBindingID)
	if err != nil {
		return controlplane.BackendMigration{}, err
	}
	target, err := s.repository.GetBackendBinding(ctx, migration.TenantID, migration.TargetBindingID)
	if err != nil {
		return controlplane.BackendMigration{}, err
	}
	if source.AppID != migration.AppID || target.AppID != migration.AppID ||
		source.ResourceType != migration.ResourceType || target.ResourceType != migration.ResourceType ||
		source.MigrationState != "active" || target.MigrationState == "active" {
		return controlplane.BackendMigration{}, invalidf("source and target backend bindings are incompatible")
	}
	now := time.Now().UTC()
	migration.State = controlplane.MigrationPlanned
	migration.Checkpoint = json.RawMessage(`{}`)
	migration.Verification = json.RawMessage(`{}`)
	migration.Version = 1
	migration.CreatedAt = now
	migration.UpdatedAt = now
	if err := s.repository.CreateBackendMigration(ctx, migration); err != nil {
		return controlplane.BackendMigration{}, err
	}
	if err := s.record(ctx, migration.TenantID, "admin_backend_migration_created", map[string]any{
		"migration_id": migration.ID, "resource_type": migration.ResourceType,
		"source_binding_id": migration.SourceBindingID,
		"target_binding_id": migration.TargetBindingID,
	}); err != nil {
		return controlplane.BackendMigration{}, err
	}
	return migration, nil
}

func (s *Service) TransitionBackendMigration(
	ctx context.Context,
	tenantID string,
	migrationID string,
	nextState string,
	expectedVersion int64,
	checkpoint json.RawMessage,
	verification json.RawMessage,
) (controlplane.BackendMigration, error) {
	current, err := s.repository.GetBackendMigration(ctx, tenantID, migrationID)
	if err != nil {
		return controlplane.BackendMigration{}, err
	}
	if !allowedMigrationTransition(current.State, nextState) {
		return controlplane.BackendMigration{}, invalidf(
			"backend migration cannot transition from %s to %s", current.State, nextState,
		)
	}
	if current.ResourceType == "knowledge" && len(verification) > 0 {
		return controlplane.BackendMigration{}, invalidf("knowledge verification is generated by server jobs, not supplied by callers")
	}
	if len(checkpoint) > 0 {
		if err := normalizeJSON(&checkpoint); err != nil {
			return controlplane.BackendMigration{}, invalidf("migration checkpoint: %v", err)
		}
	}
	if len(verification) > 0 {
		if err := normalizeJSON(&verification); err != nil {
			return controlplane.BackendMigration{}, invalidf("migration verification: %v", err)
		}
	}
	if nextState == controlplane.MigrationCutover {
		candidate := verification
		if len(candidate) == 0 {
			candidate = current.Verification
		}
		var result struct {
			Passed bool `json:"passed"`
		}
		if json.Unmarshal(candidate, &result) != nil || !result.Passed {
			return controlplane.BackendMigration{}, invalidf("cutover requires verification.passed=true")
		}
	}
	updated, err := s.repository.TransitionBackendMigration(
		ctx, tenantID, migrationID, nextState, expectedVersion, checkpoint, verification,
	)
	if err != nil {
		return controlplane.BackendMigration{}, err
	}
	if err := s.record(ctx, tenantID, "admin_backend_migration_transitioned", map[string]any{
		"migration_id": migrationID, "from": current.State,
		"to": nextState, "version": updated.Version,
	}); err != nil {
		return controlplane.BackendMigration{}, err
	}
	return updated, nil
}

func allowedMigrationTransition(current string, next string) bool {
	allowed := map[string]map[string]bool{
		controlplane.MigrationPlanned: {
			controlplane.MigrationDualWrite: true, controlplane.MigrationFailed: true,
		},
		controlplane.MigrationDualWrite: {
			controlplane.MigrationBackfill: true, controlplane.MigrationRollback: true,
			controlplane.MigrationFailed: true,
		},
		controlplane.MigrationBackfill: {
			controlplane.MigrationVerify: true, controlplane.MigrationRollback: true,
			controlplane.MigrationFailed: true,
		},
		controlplane.MigrationVerify: {
			controlplane.MigrationCutover: true, controlplane.MigrationRollback: true,
			controlplane.MigrationFailed: true,
		},
		controlplane.MigrationCutover: {
			controlplane.MigrationCompleted: true, controlplane.MigrationRollback: true,
			controlplane.MigrationFailed: true,
		},
		controlplane.MigrationRollback: {
			controlplane.MigrationRolledBack: true, controlplane.MigrationFailed: true,
		},
	}
	return allowed[current][next]
}

type MigrationJobResult struct {
	JobID       string `json:"job_id"`
	MigrationID string `json:"migration_id"`
	JobType     string `json:"job_type"`
	Duplicate   bool   `json:"duplicate"`
}

func (s *Service) SubmitMemoryMigrationJob(
	ctx context.Context,
	tenantID string,
	migrationID string,
	jobType string,
	operationID string,
	userIDs []string,
) (MigrationJobResult, error) {
	if s.jobs == nil {
		return MigrationJobResult{}, invalidf("background jobs are unavailable")
	}
	if jobType != background.JobMemoryBackfill && jobType != background.JobMemoryVerify {
		return MigrationJobResult{}, invalidf("unsupported memory migration job")
	}
	if !identifierPattern.MatchString(operationID) || len(userIDs) == 0 {
		return MigrationJobResult{}, invalidf("operation_id and user_ids are required")
	}
	for _, userID := range userIDs {
		if strings.TrimSpace(userID) == "" || len(userID) > 512 {
			return MigrationJobResult{}, invalidf("memory migration user_id is invalid")
		}
	}
	migration, err := s.repository.GetBackendMigration(ctx, tenantID, migrationID)
	if err != nil {
		return MigrationJobResult{}, err
	}
	if migration.ResourceType != "memory" || migration.State != controlplane.MigrationBackfill {
		return MigrationJobResult{}, invalidf("memory migration must be in backfill state")
	}
	app, err := s.repository.GetAgentApp(ctx, tenantID, migration.AppID)
	if err != nil {
		return MigrationJobResult{}, err
	}
	payload, err := json.Marshal(background.MemoryMigrationPayload{
		MigrationID: migration.ID, UserIDs: userIDs, ExpectedVersion: migration.Version,
	})
	if err != nil {
		return MigrationJobResult{}, err
	}
	queued, err := s.jobs.Enqueue(ctx, background.EnqueueRequest{
		TenantID: tenantID, AppID: migration.AppID, RevisionID: app.StableRevisionID,
		Type: jobType, DedupeKey: migration.ID + ":" + operationID,
		Payload: payload, TraceParent: background.TraceParent(ctx),
	})
	if err != nil {
		return MigrationJobResult{}, err
	}
	return MigrationJobResult{
		JobID: queued.Job.ID, MigrationID: migration.ID,
		JobType: jobType, Duplicate: queued.Duplicate,
	}, nil
}

func (s *Service) SubmitSessionMigrationJob(
	ctx context.Context,
	tenantID string,
	migrationID string,
	jobType string,
	operationID string,
	sessions []platformstorage.SessionMigrationItem,
) (MigrationJobResult, error) {
	if s.jobs == nil {
		return MigrationJobResult{}, invalidf("background jobs are unavailable")
	}
	if jobType != background.JobSessionBackfill && jobType != background.JobSessionVerify {
		return MigrationJobResult{}, invalidf("unsupported session migration job")
	}
	if !identifierPattern.MatchString(operationID) || len(sessions) == 0 {
		return MigrationJobResult{}, invalidf("operation_id and sessions are required")
	}
	for _, item := range sessions {
		if strings.TrimSpace(item.UserID) == "" || strings.TrimSpace(item.SessionID) == "" ||
			len(item.UserID) > 512 || len(item.SessionID) > 512 {
			return MigrationJobResult{}, invalidf("session migration identity is invalid")
		}
	}
	migration, err := s.repository.GetBackendMigration(ctx, tenantID, migrationID)
	if err != nil {
		return MigrationJobResult{}, err
	}
	if migration.ResourceType != "session" || migration.State != controlplane.MigrationBackfill {
		return MigrationJobResult{}, invalidf("session migration must be in backfill state")
	}
	app, err := s.repository.GetAgentApp(ctx, tenantID, migration.AppID)
	if err != nil {
		return MigrationJobResult{}, err
	}
	payload, err := json.Marshal(background.SessionMigrationPayload{
		MigrationID: migration.ID, Sessions: sessions, ExpectedVersion: migration.Version,
	})
	if err != nil {
		return MigrationJobResult{}, err
	}
	queued, err := s.jobs.Enqueue(ctx, background.EnqueueRequest{
		TenantID: tenantID, AppID: migration.AppID, RevisionID: app.StableRevisionID,
		Type: jobType, DedupeKey: migration.ID + ":" + operationID,
		Payload: payload, TraceParent: background.TraceParent(ctx),
	})
	if err != nil {
		return MigrationJobResult{}, err
	}
	return MigrationJobResult{
		JobID: queued.Job.ID, MigrationID: migration.ID,
		JobType: jobType, Duplicate: queued.Duplicate,
	}, nil
}

func (s *Service) record(
	ctx context.Context,
	tenantID string,
	decision string,
	details map[string]any,
) error {
	if s.audit == nil {
		return nil
	}
	if err := s.audit.Record(ctx, audit.Event{
		TenantID: tenantID,
		UserID:   PrincipalName(ctx),
		TraceID:  audit.TraceID(ctx),
		Decision: decision,
		Details:  details,
	}); err != nil {
		return fmt.Errorf("record Admin audit: %w", err)
	}
	return nil
}

func normalizeJSON(value *json.RawMessage) error {
	if len(*value) == 0 {
		*value = json.RawMessage(`{}`)
		return nil
	}
	var decoded any
	if err := json.Unmarshal(*value, &decoded); err != nil {
		return err
	}
	canonical, err := json.Marshal(decoded)
	if err != nil {
		return err
	}
	*value = canonical
	return nil
}

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}
