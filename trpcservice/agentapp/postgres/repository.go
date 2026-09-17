// Package postgres implements the Agent App Repository using tenant-scoped SQL
// and controlled publish/rollback/status functions.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
)

type Repository struct{ db *sql.DB }

func New(db *sql.DB) *Repository { return &Repository{db: db} }

func (r *Repository) Create(ctx context.Context, in agentapp.CreateInput) (agentapp.AgentApp, error) {
	if err := in.ChangeMetadata.Validate(); err != nil {
		return agentapp.AgentApp{}, err
	}
	a := in.App
	if a.TenantID == "" || a.AgentAppID == "" || a.AgentAppKey == "" || a.DisplayName == "" {
		return agentapp.AgentApp{}, agentapp.ErrInvalid
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return agentapp.AgentApp{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var current sql.NullInt64
	err = tx.QueryRowContext(ctx, `INSERT INTO agent_app(tenant_id,agent_app_id,agent_app_key,display_name,description) VALUES($1,$2,$3,$4,$5) RETURNING status,current_revision,next_revision,version,created_at,updated_at`, a.TenantID, a.AgentAppID, a.AgentAppKey, a.DisplayName, a.Description).Scan(&a.Status, &current, &a.NextRevision, &a.Version, &a.CreatedAt, &a.UpdatedAt)
	if err != nil {
		return agentapp.AgentApp{}, classify(err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref) VALUES($1,$2,'audit',$3,1,$4,$5)`, a.TenantID, "agent-app-create-audit:"+a.TenantID+":"+a.AgentAppID, a.AgentAppID, "agent-app-create:"+a.TenantID+":"+a.AgentAppID+":audit", "agent-app://"+a.TenantID+"/"+a.AgentAppID)
	if err != nil {
		return agentapp.AgentApp{}, classify(err)
	}
	if err = tx.Commit(); err != nil {
		return agentapp.AgentApp{}, classify(err)
	}
	return a, nil
}
func (r *Repository) Get(ctx context.Context, tenantID, appID string) (agentapp.AgentApp, error) {
	var a agentapp.AgentApp
	var current sql.NullInt64
	err := r.db.QueryRowContext(ctx, `SELECT tenant_id,agent_app_id,agent_app_key,display_name,description,status,current_revision,next_revision,version,created_at,updated_at FROM agent_app WHERE tenant_id=$1 AND agent_app_id=$2`, tenantID, appID).Scan(&a.TenantID, &a.AgentAppID, &a.AgentAppKey, &a.DisplayName, &a.Description, &a.Status, &current, &a.NextRevision, &a.Version, &a.CreatedAt, &a.UpdatedAt)
	if err != nil {
		return agentapp.AgentApp{}, classify(err)
	}
	if current.Valid {
		a.CurrentRevision = current.Int64
	}
	return a, nil
}
func (r *Repository) CreateDraft(ctx context.Context, in agentapp.CreateDraftInput) (agentapp.Revision, error) {
	if err := in.ChangeMetadata.Validate(); err != nil {
		return agentapp.Revision{}, err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return agentapp.Revision{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var revision, appVersion int64
	err = tx.QueryRowContext(ctx, `UPDATE agent_app SET next_revision=next_revision+1,version=version+1 WHERE tenant_id=$1 AND agent_app_id=$2 AND version=$3 AND status<>'disabled' RETURNING next_revision-1,version`, in.TenantID, in.AgentAppID, in.ExpectedAppVersion).Scan(&revision, &appVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return agentapp.Revision{}, agentapp.ErrVersionConflict
	}
	if err != nil {
		return agentapp.Revision{}, classify(err)
	}
	value := agentapp.NormalizeRevision(in.Revision)
	value.TenantID = in.TenantID
	value.AgentAppID = in.AgentAppID
	value.Revision = revision
	value.State = agentapp.RevisionDraft
	value.DraftVersion = 1
	value.SchemaVersion = 1
	if err = value.ValidateDraft(); err != nil {
		return agentapp.Revision{}, err
	}
	generation, err := json.Marshal(value.GenerationConfig)
	if err != nil {
		return agentapp.Revision{}, agentapp.ErrInvalid
	}
	policy, err := json.Marshal(value.RuntimePolicy)
	if err != nil {
		return agentapp.Revision{}, agentapp.ErrInvalid
	}
	agentSpec, err := json.Marshal(value.AgentSpec)
	if err != nil {
		return agentapp.Revision{}, agentapp.ErrInvalid
	}
	fallbacks, err := json.Marshal(value.FallbackModelRefs)
	if err != nil {
		return agentapp.Revision{}, agentapp.ErrInvalid
	}
	err = tx.QueryRowContext(ctx, `INSERT INTO agent_app_revision(tenant_id,agent_app_id,revision,state,draft_version,agent_kind,schema_version,agent_spec,description,instruction,global_instruction,model_profile_id,model_profile_version,fallback_model_refs,generation_config,runtime_policy,max_llm_calls,max_tool_calls,max_parallel_tools,execution_timeout_seconds) VALUES($1,$2,$3,'draft',1,$4,$5,$6::jsonb,$7,$8,$9,$10,$11,$12::jsonb,$13::jsonb,$14::jsonb,$15,$16,$17,$18) RETURNING created_at,updated_at`, value.TenantID, value.AgentAppID, value.Revision, value.AgentKind, value.SchemaVersion, string(agentSpec), value.Description, value.Instruction, value.GlobalInstruction, nullableString(value.ModelProfileID), nullablePositive(value.ModelProfileVersion), string(fallbacks), string(generation), string(policy), value.ExecutionBudget.MaxLLMCalls, value.ExecutionBudget.MaxToolCalls, value.ExecutionBudget.MaxParallelTools, value.ExecutionBudget.ExecutionTimeoutSeconds).Scan(&value.CreatedAt, &value.UpdatedAt)
	if err != nil {
		return agentapp.Revision{}, classify(err)
	}
	if err = writeRefs(ctx, tx, value); err != nil {
		return agentapp.Revision{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref) VALUES($1,$2,'audit',$3,$4,$5,$6)`, value.TenantID, fmt.Sprintf("agent-app-draft-audit:%s:%s:%d", value.TenantID, value.AgentAppID, value.Revision), value.AgentAppID, appVersion, fmt.Sprintf("agent-app-draft:%s:%s:%d:audit", value.TenantID, value.AgentAppID, value.Revision), fmt.Sprintf("agent-app-revision://%s/%s/%d", value.TenantID, value.AgentAppID, value.Revision))
	if err != nil {
		return agentapp.Revision{}, classify(err)
	}
	if err = tx.Commit(); err != nil {
		return agentapp.Revision{}, classify(err)
	}
	return value, nil
}
func (r *Repository) UpdateDraft(ctx context.Context, in agentapp.UpdateDraftInput) (agentapp.Revision, error) {
	if err := in.ChangeMetadata.Validate(); err != nil {
		return agentapp.Revision{}, err
	}
	value := agentapp.NormalizeRevision(in.Revision)
	if err := value.ValidateDraft(); err != nil {
		return agentapp.Revision{}, err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return agentapp.Revision{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var locked int
	if err = tx.QueryRowContext(ctx, `SELECT 1 FROM agent_app WHERE tenant_id=$1 AND agent_app_id=$2 FOR UPDATE`, value.TenantID, value.AgentAppID).Scan(&locked); err != nil {
		return agentapp.Revision{}, classify(err)
	}
	var state string
	var draftVersion int64
	if err = tx.QueryRowContext(ctx, `SELECT state,draft_version FROM agent_app_revision WHERE tenant_id=$1 AND agent_app_id=$2 AND revision=$3 FOR UPDATE`, value.TenantID, value.AgentAppID, value.Revision).Scan(&state, &draftVersion); err != nil {
		return agentapp.Revision{}, classify(err)
	}
	if state != "draft" {
		return agentapp.Revision{}, agentapp.ErrImmutable
	}
	if draftVersion != in.ExpectedDraftVersion {
		return agentapp.Revision{}, agentapp.ErrVersionConflict
	}
	generation, err := json.Marshal(value.GenerationConfig)
	if err != nil {
		return agentapp.Revision{}, agentapp.ErrInvalid
	}
	policy, err := json.Marshal(value.RuntimePolicy)
	if err != nil {
		return agentapp.Revision{}, agentapp.ErrInvalid
	}
	agentSpec, err := json.Marshal(value.AgentSpec)
	if err != nil {
		return agentapp.Revision{}, agentapp.ErrInvalid
	}
	fallbacks, err := json.Marshal(value.FallbackModelRefs)
	if err != nil {
		return agentapp.Revision{}, agentapp.ErrInvalid
	}
	err = tx.QueryRowContext(ctx, `UPDATE agent_app_revision SET draft_version=draft_version+1,agent_kind=$4,schema_version=$5,agent_spec=$6::jsonb,description=$7,instruction=$8,global_instruction=$9,model_profile_id=$10,model_profile_version=$11,fallback_model_refs=$12::jsonb,generation_config=$13::jsonb,runtime_policy=$14::jsonb,max_llm_calls=$15,max_tool_calls=$16,max_parallel_tools=$17,execution_timeout_seconds=$18,updated_at=now() WHERE tenant_id=$1 AND agent_app_id=$2 AND revision=$3 RETURNING draft_version,created_at,updated_at`, value.TenantID, value.AgentAppID, value.Revision, value.AgentKind, value.SchemaVersion, string(agentSpec), value.Description, value.Instruction, value.GlobalInstruction, nullableString(value.ModelProfileID), nullablePositive(value.ModelProfileVersion), string(fallbacks), string(generation), string(policy), value.ExecutionBudget.MaxLLMCalls, value.ExecutionBudget.MaxToolCalls, value.ExecutionBudget.MaxParallelTools, value.ExecutionBudget.ExecutionTimeoutSeconds).Scan(&value.DraftVersion, &value.CreatedAt, &value.UpdatedAt)
	if err != nil {
		return agentapp.Revision{}, classify(err)
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM agent_app_revision_tool WHERE tenant_id=$1 AND agent_app_id=$2 AND revision=$3`, value.TenantID, value.AgentAppID, value.Revision); err != nil {
		return agentapp.Revision{}, classify(err)
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM agent_app_revision_knowledge WHERE tenant_id=$1 AND agent_app_id=$2 AND revision=$3`, value.TenantID, value.AgentAppID, value.Revision); err != nil {
		return agentapp.Revision{}, classify(err)
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM agent_app_revision_skill WHERE tenant_id=$1 AND agent_app_id=$2 AND revision=$3`, value.TenantID, value.AgentAppID, value.Revision); err != nil {
		return agentapp.Revision{}, classify(err)
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM agent_app_revision_plugin WHERE tenant_id=$1 AND agent_app_id=$2 AND revision=$3`, value.TenantID, value.AgentAppID, value.Revision); err != nil {
		return agentapp.Revision{}, classify(err)
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM agent_app_revision_child WHERE tenant_id=$1 AND agent_app_id=$2 AND revision=$3`, value.TenantID, value.AgentAppID, value.Revision); err != nil {
		return agentapp.Revision{}, classify(err)
	}
	if err = writeRefs(ctx, tx, value); err != nil {
		return agentapp.Revision{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref) VALUES($1,$2,'audit',$3,$4,$5,$6)`, value.TenantID, fmt.Sprintf("agent-app-draft-update-audit:%s:%s:%d:%d", value.TenantID, value.AgentAppID, value.Revision, value.DraftVersion), value.AgentAppID, value.DraftVersion, fmt.Sprintf("agent-app-draft-update:%s:%s:%d:%d:audit", value.TenantID, value.AgentAppID, value.Revision, value.DraftVersion), fmt.Sprintf("agent-app-revision://%s/%s/%d", value.TenantID, value.AgentAppID, value.Revision))
	if err != nil {
		return agentapp.Revision{}, classify(err)
	}
	if err = tx.Commit(); err != nil {
		return agentapp.Revision{}, classify(err)
	}
	return value, nil
}
func (r *Repository) GetRevision(ctx context.Context, tenantID, appID string, revision int64) (agentapp.Revision, error) {
	var value agentapp.Revision
	var agentSpec, fallbacks, generation, policy []byte
	var digest sql.NullString
	var published sql.NullTime
	var modelProfileID sql.NullString
	var modelProfileVersion sql.NullInt64
	err := r.db.QueryRowContext(ctx, `SELECT tenant_id,agent_app_id,revision,state,draft_version,agent_kind,schema_version,agent_spec,description,instruction,global_instruction,model_profile_id,model_profile_version,fallback_model_refs,generation_config,runtime_policy,max_llm_calls,max_tool_calls,max_parallel_tools,execution_timeout_seconds,content_digest,published_at,created_at,updated_at FROM agent_app_revision WHERE tenant_id=$1 AND agent_app_id=$2 AND revision=$3`, tenantID, appID, revision).Scan(&value.TenantID, &value.AgentAppID, &value.Revision, &value.State, &value.DraftVersion, &value.AgentKind, &value.SchemaVersion, &agentSpec, &value.Description, &value.Instruction, &value.GlobalInstruction, &modelProfileID, &modelProfileVersion, &fallbacks, &generation, &policy, &value.ExecutionBudget.MaxLLMCalls, &value.ExecutionBudget.MaxToolCalls, &value.ExecutionBudget.MaxParallelTools, &value.ExecutionBudget.ExecutionTimeoutSeconds, &digest, &published, &value.CreatedAt, &value.UpdatedAt)
	if err != nil {
		return agentapp.Revision{}, classify(err)
	}
	value.ModelProfileID = modelProfileID.String
	value.ModelProfileVersion = modelProfileVersion.Int64
	if err = json.Unmarshal(agentSpec, &value.AgentSpec); err != nil {
		return agentapp.Revision{}, agentapp.ErrInvalid
	}
	if err = json.Unmarshal(fallbacks, &value.FallbackModelRefs); err != nil {
		return agentapp.Revision{}, agentapp.ErrInvalid
	}
	if err = json.Unmarshal(generation, &value.GenerationConfig); err != nil {
		return agentapp.Revision{}, agentapp.ErrInvalid
	}
	if err = json.Unmarshal(policy, &value.RuntimePolicy); err != nil {
		return agentapp.Revision{}, agentapp.ErrInvalid
	}
	if digest.Valid {
		value.ContentDigest = digest.String
	}
	if published.Valid {
		v := published.Time
		value.PublishedAt = &v
	}
	toolRows, err := r.db.QueryContext(ctx, `SELECT tool_id,tool_version,content_digest,required FROM agent_app_revision_tool WHERE tenant_id=$1 AND agent_app_id=$2 AND revision=$3 ORDER BY tool_id`, tenantID, appID, revision)
	if err != nil {
		return agentapp.Revision{}, classify(err)
	}
	for toolRows.Next() {
		var ref agentapp.VersionedRef
		var digest sql.NullString
		if err = toolRows.Scan(&ref.ID, &ref.Version, &digest, &ref.Required); err != nil {
			toolRows.Close()
			return agentapp.Revision{}, err
		}
		ref.ContentDigest = digest.String
		value.ToolRefs = append(value.ToolRefs, ref)
	}
	if err = toolRows.Err(); err != nil {
		toolRows.Close()
		return agentapp.Revision{}, err
	}
	if err = toolRows.Close(); err != nil {
		return agentapp.Revision{}, err
	}
	knowledgeRows, err := r.db.QueryContext(ctx, `SELECT knowledge_id,knowledge_version FROM agent_app_revision_knowledge WHERE tenant_id=$1 AND agent_app_id=$2 AND revision=$3 ORDER BY knowledge_id`, tenantID, appID, revision)
	if err != nil {
		return agentapp.Revision{}, classify(err)
	}
	for knowledgeRows.Next() {
		var ref agentapp.VersionedRef
		if err = knowledgeRows.Scan(&ref.ID, &ref.Version); err != nil {
			knowledgeRows.Close()
			return agentapp.Revision{}, err
		}
		value.KnowledgeRefs = append(value.KnowledgeRefs, ref)
	}
	if err = knowledgeRows.Err(); err != nil {
		knowledgeRows.Close()
		return agentapp.Revision{}, err
	}
	if err = knowledgeRows.Close(); err != nil {
		return agentapp.Revision{}, err
	}
	skillRows, err := r.db.QueryContext(ctx, `SELECT skill_id,skill_version,content_digest FROM agent_app_revision_skill WHERE tenant_id=$1 AND agent_app_id=$2 AND revision=$3 ORDER BY skill_id`, tenantID, appID, revision)
	if err != nil {
		return agentapp.Revision{}, classify(err)
	}
	for skillRows.Next() {
		var ref agentapp.SkillRef
		if err = skillRows.Scan(&ref.ID, &ref.Version, &ref.ContentDigest); err != nil {
			skillRows.Close()
			return agentapp.Revision{}, err
		}
		value.SkillRefs = append(value.SkillRefs, ref)
	}
	if err = skillRows.Err(); err != nil {
		skillRows.Close()
		return agentapp.Revision{}, err
	}
	if err = skillRows.Close(); err != nil {
		return agentapp.Revision{}, err
	}
	pluginRows, err := r.db.QueryContext(ctx, `SELECT plugin_id,plugin_version FROM agent_app_revision_plugin WHERE tenant_id=$1 AND agent_app_id=$2 AND revision=$3 ORDER BY plugin_id`, tenantID, appID, revision)
	if err != nil {
		return agentapp.Revision{}, classify(err)
	}
	for pluginRows.Next() {
		var ref agentapp.PluginRef
		if err = pluginRows.Scan(&ref.ID, &ref.Version); err != nil {
			pluginRows.Close()
			return agentapp.Revision{}, err
		}
		value.PluginRefs = append(value.PluginRefs, ref)
	}
	if err = pluginRows.Err(); err != nil {
		pluginRows.Close()
		return agentapp.Revision{}, err
	}
	if err = pluginRows.Close(); err != nil {
		return agentapp.Revision{}, err
	}
	return value, nil
}
func (r *Repository) Publish(ctx context.Context, in agentapp.PublishInput) (agentapp.PublishResult, error) {
	if err := in.ChangeMetadata.Validate(); err != nil {
		return agentapp.PublishResult{}, err
	}
	revision, err := r.GetRevision(ctx, in.TenantID, in.AgentAppID, in.Revision)
	if err != nil {
		return agentapp.PublishResult{}, err
	}
	digest, err := revision.ComputeContentDigest()
	if err != nil {
		return agentapp.PublishResult{}, err
	}
	var next int64
	err = r.db.QueryRowContext(ctx, `SELECT publish_agent_app_revision($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, in.TenantID, in.AgentAppID, in.Revision, in.ExpectedAppVersion, in.ExpectedDraftVersion, digest, in.ActorID, in.Reason, in.CorrelationID, in.TraceID, nil).Scan(&next)
	if err != nil {
		return agentapp.PublishResult{}, classify(err)
	}
	app, err := r.Get(ctx, in.TenantID, in.AgentAppID)
	if err != nil {
		return agentapp.PublishResult{}, err
	}
	published, err := r.GetRevision(ctx, in.TenantID, in.AgentAppID, in.Revision)
	if err != nil {
		return agentapp.PublishResult{}, err
	}
	if app.Version != next {
		return agentapp.PublishResult{}, agentapp.ErrVersionConflict
	}
	return agentapp.PublishResult{App: app, Revision: published}, nil
}
func (r *Repository) Rollback(ctx context.Context, in agentapp.RollbackInput) (agentapp.RollbackResult, error) {
	if err := in.ChangeMetadata.Validate(); err != nil {
		return agentapp.RollbackResult{}, err
	}
	var next int64
	err := r.db.QueryRowContext(ctx, `SELECT rollback_agent_app_revision($1,$2,$3,$4,$5,$6,$7,$8,$9)`, in.TenantID, in.AgentAppID, in.TargetRevision, in.ExpectedAppVersion, in.ActorID, in.Reason, in.CorrelationID, in.TraceID, nil).Scan(&next)
	if err != nil {
		return agentapp.RollbackResult{}, classify(err)
	}
	app, err := r.Get(ctx, in.TenantID, in.AgentAppID)
	if err != nil {
		return agentapp.RollbackResult{}, err
	}
	if app.Version != next {
		return agentapp.RollbackResult{}, agentapp.ErrVersionConflict
	}
	return agentapp.RollbackResult{App: app}, nil
}
func (r *Repository) TransitionStatus(ctx context.Context, in agentapp.TransitionStatusInput) (agentapp.ChangeResult, error) {
	if err := in.ChangeMetadata.Validate(); err != nil {
		return agentapp.ChangeResult{}, err
	}
	var next int64
	err := r.db.QueryRowContext(ctx, `SELECT transition_agent_app_status($1,$2,$3,$4,$5,$6,$7,$8,$9)`, in.TenantID, in.AgentAppID, in.ExpectedAppVersion, in.NextStatus, in.ActorID, in.Reason, in.CorrelationID, in.TraceID, nil).Scan(&next)
	if err != nil {
		return agentapp.ChangeResult{}, classify(err)
	}
	app, err := r.Get(ctx, in.TenantID, in.AgentAppID)
	if err != nil {
		return agentapp.ChangeResult{}, err
	}
	if app.Version != next {
		return agentapp.ChangeResult{}, agentapp.ErrVersionConflict
	}
	return agentapp.ChangeResult{App: app}, nil
}
func writeRefs(ctx context.Context, tx *sql.Tx, value agentapp.Revision) error {
	for ordinal, node := range value.AgentSpec.Nodes {
		if _, err := tx.ExecContext(ctx, `INSERT INTO agent_app_revision_child(tenant_id,agent_app_id,revision,node_key,ordinal,child_agent_app_id,child_revision,child_digest) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, value.TenantID, value.AgentAppID, value.Revision, node.Key, ordinal, node.AgentRef.AgentAppID, node.AgentRef.Revision, node.AgentRef.ContentDigest); err != nil {
			return classify(err)
		}
	}
	for _, ref := range value.ToolRefs {
		if _, err := tx.ExecContext(ctx, `INSERT INTO agent_app_revision_tool(tenant_id,agent_app_id,revision,tool_id,tool_version,content_digest,required) VALUES($1,$2,$3,$4,$5,$6,$7)`, value.TenantID, value.AgentAppID, value.Revision, ref.ID, ref.Version, nullableString(ref.ContentDigest), ref.Required); err != nil {
			return classify(err)
		}
	}
	for _, ref := range value.KnowledgeRefs {
		if _, err := tx.ExecContext(ctx, `INSERT INTO agent_app_revision_knowledge(tenant_id,agent_app_id,revision,knowledge_id,knowledge_version) VALUES($1,$2,$3,$4,$5)`, value.TenantID, value.AgentAppID, value.Revision, ref.ID, ref.Version); err != nil {
			return classify(err)
		}
	}
	for _, ref := range value.SkillRefs {
		if _, err := tx.ExecContext(ctx, `INSERT INTO agent_app_revision_skill(tenant_id,agent_app_id,revision,skill_id,skill_version,content_digest) VALUES($1,$2,$3,$4,$5,$6)`, value.TenantID, value.AgentAppID, value.Revision, ref.ID, ref.Version, ref.ContentDigest); err != nil {
			return classify(err)
		}
	}
	for _, ref := range value.PluginRefs {
		if _, err := tx.ExecContext(ctx, `INSERT INTO agent_app_revision_plugin(tenant_id,agent_app_id,revision,plugin_id,plugin_version) VALUES($1,$2,$3,$4,$5)`, value.TenantID, value.AgentAppID, value.Revision, ref.ID, ref.Version); err != nil {
			return classify(err)
		}
	}
	return nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullablePositive(value int64) any {
	if value < 1 {
		return nil
	}
	return value
}

type sqlStater interface{ SQLState() string }

func classify(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return agentapp.ErrNotFound
	}
	var state sqlStater
	if errors.As(err, &state) {
		switch state.SQLState() {
		case "40001":
			return fmt.Errorf("%w: %v", agentapp.ErrVersionConflict, err)
		case "P0002":
			return fmt.Errorf("%w: %v", agentapp.ErrNotFound, err)
		case "23505":
			return fmt.Errorf("%w: %v", agentapp.ErrVersionConflict, err)
		case "55000":
			return fmt.Errorf("%w: %v", agentapp.ErrImmutable, err)
		case "22023", "23503":
			return fmt.Errorf("%w: %v", agentapp.ErrInvalid, err)
		case "23514":
			return fmt.Errorf("%w: %v", agentapp.ErrStatusConflict, err)
		}
	}
	return err
}

var _ agentapp.Repository = (*Repository)(nil)
