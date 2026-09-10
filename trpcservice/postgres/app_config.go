package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	platformaudit "github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// InsertAppConfigVersion inserts a new immutable application config version.
// It does not change the application's active version.
func (s *Store) InsertAppConfigVersion(ctx context.Context, cfg tenant.AppConfig) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	tnt, err := s.ResolveTenant(ctx, cfg.TenantID)
	if err != nil {
		return fmt.Errorf("resolve tenant for app config: %w", err)
	}
	if err := tnt.Audit.ValidateAppConfig(cfg.Audit); err != nil {
		return err
	}
	if err := s.validateKnowledgeBaseIDs(ctx, cfg); err != nil {
		return err
	}
	return insertAppConfig(ctx, s.pool, cfg)
}

// ActivateAppConfig changes which immutable config version new admissions use.
// Session, Memory, and Artifact backend changes require a data migration.
// Knowledge indexes are derived data and catch up asynchronously after the
// active configuration switches.
func (s *Store) ActivateAppConfig(ctx context.Context, tenantID, appID, version string) error {
	if err := s.validate(); err != nil {
		return err
	}
	if tenantID == "" {
		return errors.New("tenant_id is required")
	}
	if appID == "" {
		return errors.New("app_id is required")
	}
	if version == "" {
		return errors.New("config version is required")
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin activate app config: %w", err)
	}
	defer func() {
		rollback(tx)
	}()
	var activeVersion string
	if err := tx.QueryRow(
		ctx,
		`SELECT active_config_version
FROM platform.agent_app
WHERE tenant_id = $1 AND app_id = $2
FOR UPDATE`,
		tenantID,
		appID,
	).Scan(&activeVersion); err != nil {
		return fmt.Errorf("lock agent app: %w", resolveError("agent app", err))
	}
	if activeVersion == version {
		return tx.Commit(ctx)
	}
	blocked, err := migrationBlocksAdmission(ctx, tx, tenantID, appID)
	if err != nil {
		return err
	}
	if blocked {
		return errors.New("app config activation is blocked by data migration")
	}
	activeBackend, err := storedBackendConfig(ctx, tx, tenantID, appID, activeVersion)
	if err != nil {
		return err
	}
	targetBackend, err := storedBackendConfig(ctx, tx, tenantID, appID, version)
	if err != nil {
		return err
	}
	if !sameAuthoritativeBackends(activeBackend, targetBackend) {
		return errors.New("config activation changes authoritative backends; migrate the backends before switching the active version")
	}
	commandTag, err := tx.Exec(
		ctx,
		`UPDATE platform.agent_app
SET active_config_version = $3, updated_at = now()
WHERE tenant_id = $1 AND app_id = $2`,
		tenantID,
		appID,
		version,
	)
	if err != nil {
		return fmt.Errorf("activate app config: %w", err)
	}
	if commandTag.RowsAffected() == 0 {
		return fmt.Errorf("agent app config: %w", ErrNotFound)
	}
	event := controlPlaneAuditEvent(
		ctx, tenantID, appID, version, platformaudit.ConfigActivated, "activated",
	)
	if err := recordControlPlaneAuditTx(ctx, tx, event); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit activate app config: %w", err)
	}
	return nil
}

func storedBackendConfig(
	ctx context.Context,
	db databaseQueryer,
	tenantID, appID, version string,
) (tenant.BackendConfig, error) {
	var encoded []byte
	err := db.QueryRow(
		ctx,
		`SELECT backend_config
FROM platform.app_config_version
WHERE tenant_id = $1 AND app_id = $2 AND version = $3 AND status = 'PUBLISHED'`,
		tenantID,
		appID,
		version,
	).Scan(&encoded)
	if err != nil {
		return tenant.BackendConfig{}, fmt.Errorf("resolve app config %s: %w", version, resolveError("app config", err))
	}
	var document backendConfigDocument
	if err := json.Unmarshal(encoded, &document); err != nil {
		return tenant.BackendConfig{}, fmt.Errorf("unmarshal app config %s: %w", version, err)
	}
	return document.value(), nil
}

func sameAuthoritativeBackends(a, b tenant.BackendConfig) bool {
	return sameBackendRef(a.Session, b.Session) &&
		sameBackendRef(a.Memory, b.Memory) &&
		sameBackendRef(a.Artifact, b.Artifact)
}

func sameBackendRef(a, b tenant.BackendRef) bool {
	return a.Kind == b.Kind &&
		a.Provider == b.Provider &&
		a.Name == b.Name &&
		a.SecretRef == b.SecretRef &&
		reflect.DeepEqual(a.Options, b.Options)
}

// ResolveAppConfig returns one exact immutable application config version.
func (s *Store) ResolveAppConfig(
	ctx context.Context,
	tenantID string,
	appID string,
	version string,
) (tenant.AppConfig, error) {
	if err := s.validate(); err != nil {
		return tenant.AppConfig{}, err
	}
	if tenantID == "" {
		return tenant.AppConfig{}, errors.New("tenant_id is required")
	}
	if appID == "" {
		return tenant.AppConfig{}, errors.New("app_id is required")
	}
	if version == "" {
		return tenant.AppConfig{}, errors.New("config version is required")
	}

	var encoded appConfigColumns
	err := s.pool.QueryRow(
		ctx,
		`SELECT
    model_config,
    tool_policy,
    backend_config,
    audit_policy,
    secret_refs,
    channel_binding_ids,
    knowledge_base_ids
FROM platform.app_config_version
WHERE tenant_id = $1 AND app_id = $2 AND version = $3 AND status = 'PUBLISHED'`,
		tenantID,
		appID,
		version,
	).Scan(
		&encoded.modelConfig,
		&encoded.toolPolicy,
		&encoded.backendConfig,
		&encoded.auditPolicy,
		&encoded.secretRefs,
		&encoded.channelBindingIDs,
		&encoded.knowledgeBaseIDs,
	)
	if err != nil {
		return tenant.AppConfig{}, resolveError("app config", err)
	}
	cfg, err := unmarshalAppConfig(tenantID, appID, version, encoded)
	if err != nil {
		return tenant.AppConfig{}, err
	}
	if err := config.ValidateAppConfigBindings(ctx, cfg, s); err != nil {
		return tenant.AppConfig{}, fmt.Errorf("stored app config channel bindings: %w", err)
	}
	return cfg, nil
}

type databaseExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

type appConfigColumns struct {
	modelConfig       []byte
	toolPolicy        []byte
	backendConfig     []byte
	auditPolicy       []byte
	secretRefs        []byte
	channelBindingIDs []byte
	knowledgeBaseIDs  []byte
}

func insertAppConfig(ctx context.Context, db databaseExecutor, cfg tenant.AppConfig) error {
	modelConfig, toolPolicy, backendConfig, auditPolicy, secretRefs, bindings, knowledgeBaseIDs, err :=
		marshalAppConfig(cfg)
	if err != nil {
		return err
	}
	if _, err := db.Exec(
		ctx,
		`INSERT INTO platform.app_config_version (
    tenant_id,
    app_id,
    version,
    model_config,
    tool_policy,
    backend_config,
    audit_policy,
    secret_refs,
    channel_binding_ids,
    knowledge_base_ids
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		cfg.TenantID,
		cfg.AppID,
		cfg.Version,
		modelConfig,
		toolPolicy,
		backendConfig,
		auditPolicy,
		secretRefs,
		bindings,
		knowledgeBaseIDs,
	); err != nil {
		return fmt.Errorf("insert app config: %w", err)
	}
	return nil
}

func (s *Store) validateKnowledgeBaseIDs(ctx context.Context, cfg tenant.AppConfig) error {
	if len(cfg.KnowledgeBaseIDs) == 0 {
		return nil
	}
	var count int
	if err := s.pool.QueryRow(ctx, `
SELECT count(*)
FROM platform.knowledge_base
WHERE tenant_id = $1
  AND app_id = $2
  AND status = 'ACTIVE'
  AND knowledge_base_id = ANY($3)`,
		cfg.TenantID,
		cfg.AppID,
		cfg.KnowledgeBaseIDs,
	).Scan(&count); err != nil {
		return fmt.Errorf("validate knowledge base bindings: %w", err)
	}
	if count != len(cfg.KnowledgeBaseIDs) {
		return errors.New("knowledge base binding is missing or inactive")
	}
	return nil
}

type modelConfigDocument struct {
	Provider               string                             `json:"provider"`
	Model                  string                             `json:"model"`
	APIKeyRef              secretRefDocument                  `json:"api_key_ref,omitempty"`
	Parameters             map[string]string                  `json:"parameters,omitempty"`
	AttachmentCapabilities tenant.ModelAttachmentCapabilities `json:"attachment_capabilities,omitempty"`
}

type toolPolicyDocument struct {
	VisibleTools        []string `json:"visible_tools,omitempty"`
	ExecutableTools     []string `json:"executable_tools,omitempty"`
	ReviewRequiredTools []string `json:"review_required_tools,omitempty"`
}

type backendConfigDocument struct {
	Name      string             `json:"name"`
	Session   backendRefDocument `json:"session"`
	Memory    backendRefDocument `json:"memory,omitempty"`
	Knowledge backendRefDocument `json:"knowledge,omitempty"`
	Artifact  backendRefDocument `json:"artifact,omitempty"`
}

type backendRefDocument struct {
	Kind      tenant.BackendKind `json:"kind,omitempty"`
	Provider  string             `json:"provider"`
	Name      string             `json:"name,omitempty"`
	SecretRef secretRefDocument  `json:"secret_ref,omitempty"`
	Options   map[string]string  `json:"options,omitempty"`
}

type auditPolicyDocument struct {
	Enabled             bool                   `json:"enabled"`
	RecordToolDecisions bool                   `json:"record_tool_decisions,omitempty"`
	RecordExecutions    bool                   `json:"record_executions,omitempty"`
	RetentionDays       int                    `json:"retention_days"`
	RedactPII           bool                   `json:"redact_pii"`
	IMAccess            *tenant.IMAccessPolicy `json:"im_access,omitempty"`
	Budget              *tenant.BudgetPolicy   `json:"budget,omitempty"`
}

type secretRefDocument struct {
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
}

func marshalAppConfig(cfg tenant.AppConfig) (
	[]byte,
	[]byte,
	[]byte,
	[]byte,
	[]byte,
	[]byte,
	[]byte,
	error,
) {
	modelConfig, err := json.Marshal(modelConfigDocument{
		Provider:               cfg.Model.Provider,
		Model:                  cfg.Model.Model,
		APIKeyRef:              newSecretRefDocument(cfg.Model.APIKeyRef),
		Parameters:             cfg.Model.Parameters,
		AttachmentCapabilities: cfg.Model.AttachmentCapabilities,
	})
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, fmt.Errorf("marshal model config: %w", err)
	}
	toolPolicy, err := json.Marshal(toolPolicyDocument{
		VisibleTools:        cfg.Tools.VisibleTools,
		ExecutableTools:     cfg.Tools.ExecutableTools,
		ReviewRequiredTools: cfg.Tools.ReviewRequiredTools,
	})
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, fmt.Errorf("marshal tool policy: %w", err)
	}
	backendConfig, err := json.Marshal(newBackendConfigDocument(cfg.BackendConfig))
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, fmt.Errorf("marshal backend config: %w", err)
	}
	auditPolicy, err := marshalAppConfigPolicy(cfg)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}
	var secretRefDocuments []secretRefDocument
	if cfg.SecretRefs != nil {
		secretRefDocuments = make([]secretRefDocument, len(cfg.SecretRefs))
	}
	for i, ref := range cfg.SecretRefs {
		secretRefDocuments[i] = secretRefDocument{Name: ref.Name, Version: ref.Version}
	}
	encodedSecretRefs, err := json.Marshal(secretRefDocuments)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, fmt.Errorf("marshal secret refs: %w", err)
	}
	bindings, err := json.Marshal(cfg.ChannelBinding)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, fmt.Errorf("marshal channel bindings: %w", err)
	}
	knowledgeBaseIDsValue := cfg.KnowledgeBaseIDs
	if knowledgeBaseIDsValue == nil {
		knowledgeBaseIDsValue = []string{}
	}
	knowledgeBaseIDs, err := json.Marshal(knowledgeBaseIDsValue)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, fmt.Errorf("marshal knowledge base ids: %w", err)
	}
	return modelConfig, toolPolicy, backendConfig, auditPolicy, encodedSecretRefs, bindings, knowledgeBaseIDs, nil
}

func unmarshalAppConfig(
	tenantID string,
	appID string,
	version string,
	encoded appConfigColumns,
) (tenant.AppConfig, error) {
	var modelDocument modelConfigDocument
	if err := json.Unmarshal(encoded.modelConfig, &modelDocument); err != nil {
		return tenant.AppConfig{}, fmt.Errorf("unmarshal model config: %w", err)
	}
	var toolDocument toolPolicyDocument
	if err := json.Unmarshal(encoded.toolPolicy, &toolDocument); err != nil {
		return tenant.AppConfig{}, fmt.Errorf("unmarshal tool policy: %w", err)
	}
	var backendDocument backendConfigDocument
	if err := json.Unmarshal(encoded.backendConfig, &backendDocument); err != nil {
		return tenant.AppConfig{}, fmt.Errorf("unmarshal backend config: %w", err)
	}
	audit, err := unmarshalAuditPolicy(encoded.auditPolicy)
	if err != nil {
		return tenant.AppConfig{}, err
	}
	imAccess, budget, err := unmarshalAppConfigPolicy(encoded.auditPolicy)
	if err != nil {
		return tenant.AppConfig{}, err
	}
	var secretDocuments []secretRefDocument
	if err := json.Unmarshal(encoded.secretRefs, &secretDocuments); err != nil {
		return tenant.AppConfig{}, fmt.Errorf("unmarshal secret refs: %w", err)
	}
	var channelBinding []string
	if err := json.Unmarshal(encoded.channelBindingIDs, &channelBinding); err != nil {
		return tenant.AppConfig{}, fmt.Errorf("unmarshal channel bindings: %w", err)
	}
	var knowledgeBaseIDList []string
	if err := json.Unmarshal(encoded.knowledgeBaseIDs, &knowledgeBaseIDList); err != nil {
		return tenant.AppConfig{}, fmt.Errorf("unmarshal knowledge base ids: %w", err)
	}
	if len(knowledgeBaseIDList) == 0 {
		knowledgeBaseIDList = nil
	}
	var refs []tenant.SecretRef
	if secretDocuments != nil {
		refs = make([]tenant.SecretRef, len(secretDocuments))
	}
	for i, ref := range secretDocuments {
		refs[i] = tenant.SecretRef{Name: ref.Name, Version: ref.Version}
	}
	cfg := tenant.AppConfig{
		TenantID: tenantID,
		AppID:    appID,
		Version:  version,
		Model: tenant.ModelConfig{
			Provider:               modelDocument.Provider,
			Model:                  modelDocument.Model,
			APIKeyRef:              modelDocument.APIKeyRef.value(),
			Parameters:             modelDocument.Parameters,
			AttachmentCapabilities: modelDocument.AttachmentCapabilities,
		},
		Tools: tenant.ToolPolicy{
			VisibleTools:        toolDocument.VisibleTools,
			ExecutableTools:     toolDocument.ExecutableTools,
			ReviewRequiredTools: toolDocument.ReviewRequiredTools,
		},
		IMAccess:         imAccess,
		Budget:           budget,
		BackendConfig:    backendDocument.value(),
		Audit:            audit,
		SecretRefs:       refs,
		ChannelBinding:   channelBinding,
		KnowledgeBaseIDs: knowledgeBaseIDList,
	}
	if err := cfg.Validate(); err != nil {
		return tenant.AppConfig{}, fmt.Errorf("stored app config: %w", err)
	}
	return cfg, nil
}

func newSecretRefDocument(ref tenant.SecretRef) secretRefDocument {
	return secretRefDocument{Name: ref.Name, Version: ref.Version}
}

func (d secretRefDocument) value() tenant.SecretRef {
	return tenant.SecretRef{Name: d.Name, Version: d.Version}
}

func marshalAuditPolicy(policy tenant.AuditPolicy) ([]byte, error) {
	encoded, err := json.Marshal(auditPolicyDocument{
		Enabled:             policy.Enabled,
		RecordToolDecisions: policy.RecordToolDecisions,
		RecordExecutions:    policy.RecordExecutions,
		RetentionDays:       policy.RetentionDays,
		RedactPII:           policy.RedactPII,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal audit policy: %w", err)
	}
	return encoded, nil
}

func unmarshalAuditPolicy(encoded []byte) (tenant.AuditPolicy, error) {
	var document auditPolicyDocument
	if err := json.Unmarshal(encoded, &document); err != nil {
		return tenant.AuditPolicy{}, fmt.Errorf("unmarshal audit policy: %w", err)
	}
	policy := tenant.AuditPolicy{
		Enabled:             document.Enabled,
		RecordToolDecisions: document.RecordToolDecisions,
		RecordExecutions:    document.RecordExecutions,
		RetentionDays:       document.RetentionDays,
		RedactPII:           document.RedactPII,
	}
	if err := policy.Validate(); err != nil {
		return tenant.AuditPolicy{}, fmt.Errorf("stored audit policy: %w", err)
	}
	return policy, nil
}

func marshalAppConfigPolicy(cfg tenant.AppConfig) ([]byte, error) {
	access := cfg.IMAccess.Clone()
	budget := cfg.Budget
	encoded, err := json.Marshal(auditPolicyDocument{
		Enabled:             cfg.Audit.Enabled,
		RecordToolDecisions: cfg.Audit.RecordToolDecisions,
		RecordExecutions:    cfg.Audit.RecordExecutions,
		RetentionDays:       cfg.Audit.RetentionDays,
		RedactPII:           cfg.Audit.RedactPII,
		IMAccess:            &access,
		Budget:              &budget,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal app governance policy: %w", err)
	}
	return encoded, nil
}

func unmarshalAppConfigPolicy(encoded []byte) (tenant.IMAccessPolicy, tenant.BudgetPolicy, error) {
	var document auditPolicyDocument
	if err := json.Unmarshal(encoded, &document); err != nil {
		return tenant.IMAccessPolicy{}, tenant.BudgetPolicy{}, fmt.Errorf("unmarshal app governance policy: %w", err)
	}
	var access tenant.IMAccessPolicy
	if document.IMAccess != nil {
		access = document.IMAccess.Clone()
	}
	var budget tenant.BudgetPolicy
	if document.Budget != nil {
		budget = *document.Budget
	}
	return access, budget, nil
}

func newBackendConfigDocument(config tenant.BackendConfig) backendConfigDocument {
	return backendConfigDocument{
		Name:      config.Name,
		Session:   newBackendRefDocument(config.Session),
		Memory:    newBackendRefDocument(config.Memory),
		Knowledge: newBackendRefDocument(config.Knowledge),
		Artifact:  newBackendRefDocument(config.Artifact),
	}
}

func newBackendRefDocument(ref tenant.BackendRef) backendRefDocument {
	return backendRefDocument{
		Kind:      ref.Kind,
		Provider:  ref.Provider,
		Name:      ref.Name,
		SecretRef: newSecretRefDocument(ref.SecretRef),
		Options:   ref.Options,
	}
}

func (d backendConfigDocument) value() tenant.BackendConfig {
	return tenant.BackendConfig{
		Name:      d.Name,
		Session:   d.Session.value(),
		Memory:    d.Memory.value(),
		Knowledge: d.Knowledge.value(),
		Artifact:  d.Artifact.value(),
	}
}

func (d backendRefDocument) value() tenant.BackendRef {
	return tenant.BackendRef{
		Kind:      d.Kind,
		Provider:  d.Provider,
		Name:      d.Name,
		SecretRef: d.SecretRef.value(),
		Options:   d.Options,
	}
}
