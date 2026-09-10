// Package postgres implements immutable ConfigSnapshot publication using the
// transaction functions installed by the control-plane migration.
package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type Repository struct {
	db      *sql.DB
	tenants tenant.Repository
}

func New(db *sql.DB, tenants tenant.Repository) *Repository {
	return &Repository{db: db, tenants: tenants}
}

func (r *Repository) Validate(ctx context.Context, in config.ValidateInput) error {
	if in.TenantID == "" || in.Payload.SchemaVersion != config.CurrentSchemaVersion || in.Payload.PolicyVersion < 1 || in.Payload.DefaultAgentAppID == "" {
		return config.ErrInvalid
	}
	apps := map[string]struct{}{in.Payload.DefaultAgentAppID: {}}
	channels := map[string]struct{}{}
	for _, binding := range in.Payload.ChannelBindings {
		if binding.BindingID == "" || binding.Channel == "" || binding.ExternalAccountID == "" || binding.AgentAppID == "" || binding.SecretRef.Ref == "" || binding.SecretRef.Version < 1 || !config.ValidSendSecret(binding) {
			return config.ErrInvalid
		}
		key := binding.Channel + "\x00" + binding.ExternalAccountID
		if _, ok := channels[key]; ok {
			return config.ErrInvalid
		}
		channels[key] = struct{}{}
		apps[binding.AgentAppID] = struct{}{}
	}
	domains := map[string]struct{}{}
	for _, binding := range in.Payload.BackendBindings {
		if binding.Domain == "" || binding.BackendProfileID == "" || binding.BackendVersion < 1 {
			return config.ErrInvalid
		}
		if _, ok := domains[binding.Domain]; ok {
			return config.ErrInvalid
		}
		domains[binding.Domain] = struct{}{}
		var status string
		var digest string
		var capabilitiesSatisfied bool
		if err := r.db.QueryRowContext(ctx, `SELECT p.status,v.content_digest,
NOT EXISTS (SELECT capability FROM unnest($4::text[]) capability EXCEPT SELECT capability FROM unnest(v.capabilities) capability)
FROM backend_profile p JOIN backend_profile_revision v USING (tenant_id,backend_profile_id)
WHERE p.tenant_id=$1 AND p.backend_profile_id=$2 AND v.profile_version=$3`, in.TenantID, binding.BackendProfileID, binding.BackendVersion, binding.Required).Scan(&status, &digest, &capabilitiesSatisfied); err != nil {
			return classify(err)
		}
		if status != "active" || len(digest) != 64 || !capabilitiesSatisfied {
			return config.ErrInvalid
		}
	}
	if err := config.ValidateBackendTopology(in.Payload.BackendBindings); err != nil {
		return err
	}
	for appID := range apps {
		var status string
		var revision sql.NullInt64
		err := r.db.QueryRowContext(ctx, `SELECT status,current_revision FROM agent_app WHERE tenant_id=$1 AND agent_app_id=$2`, in.TenantID, appID).Scan(&status, &revision)
		if err != nil {
			return classify(err)
		}
		if status != string(agentapp.StatusActive) || !revision.Valid {
			return config.ErrInvalid
		}
		var state, digest string
		if err = r.db.QueryRowContext(ctx, `SELECT state,content_digest FROM agent_app_revision WHERE tenant_id=$1 AND agent_app_id=$2 AND revision=$3`, in.TenantID, appID, revision.Int64).Scan(&state, &digest); err != nil {
			return classify(err)
		}
		if state != string(agentapp.RevisionPublished) || digest == "" {
			return config.ErrInvalid
		}
	}
	return nil
}

func (r *Repository) Publish(ctx context.Context, in config.PublishInput) (config.PublishResult, error) {
	if err := in.Metadata.Validate(); err != nil {
		return config.PublishResult{}, err
	}
	if err := r.Validate(ctx, config.ValidateInput{TenantID: in.TenantID, Payload: in.Payload}); err != nil {
		return config.PublishResult{}, err
	}
	digest, data, err := config.ContentDigest(in.Payload)
	if err != nil {
		return config.PublishResult{}, err
	}
	var configVersion, tenantVersion int64
	err = r.db.QueryRowContext(ctx, `SELECT config_version,tenant_version FROM publish_config_snapshot($1,$2,$3,$4::jsonb,$5,$6,$7,$8,$9,$10,$11)`, in.TenantID, in.ExpectedTenantVersion, config.CurrentSchemaVersion, string(data), digest, in.Payload.DefaultAgentAppID, in.Metadata.ActorID, in.Metadata.ReasonCode, in.Metadata.CorrelationID, in.Metadata.TraceID, nil).Scan(&configVersion, &tenantVersion)
	if err != nil {
		return config.PublishResult{}, classify(err)
	}
	snapshot, err := r.Get(ctx, in.TenantID, configVersion)
	if err != nil {
		return config.PublishResult{}, err
	}
	current, err := r.tenants.Get(ctx, in.TenantID)
	if err != nil {
		return config.PublishResult{}, err
	}
	if current.Version != tenantVersion {
		return config.PublishResult{}, config.ErrVersionConflict
	}
	return config.PublishResult{Snapshot: snapshot, Tenant: current}, nil
}

func (r *Repository) Stage(ctx context.Context, in config.StageInput) (config.Snapshot, error) {
	if err := in.Metadata.Validate(); err != nil {
		return config.Snapshot{}, err
	}
	if err := r.Validate(ctx, config.ValidateInput{TenantID: in.TenantID, Payload: in.Payload}); err != nil {
		return config.Snapshot{}, err
	}
	digest, data, err := config.ContentDigest(in.Payload)
	if err != nil {
		return config.Snapshot{}, err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return config.Snapshot{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var currentVersion int64
	var status string
	if err = tx.QueryRowContext(ctx, `SELECT version,status FROM tenant WHERE tenant_id=$1 FOR UPDATE`, in.TenantID).Scan(&currentVersion, &status); err != nil {
		return config.Snapshot{}, classify(err)
	}
	if status != string(tenant.StatusActive) {
		return config.Snapshot{}, config.ErrInvalid
	}
	if currentVersion != in.ExpectedTenantVersion {
		return config.Snapshot{}, config.ErrVersionConflict
	}
	var version int64
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(max(config_version),0)+1 FROM config_snapshot WHERE tenant_id=$1`, in.TenantID).Scan(&version); err != nil {
		return config.Snapshot{}, classify(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO config_snapshot(tenant_id,config_version,schema_version,payload,content_digest,state,actor_id,reason_code,correlation_id,trace_id)
VALUES($1,$2,$3,$4::jsonb,$5,'published',$6,$7,$8,$9)`, in.TenantID, version, config.CurrentSchemaVersion, string(data), digest,
		in.Metadata.ActorID, in.Metadata.ReasonCode, in.Metadata.CorrelationID, in.Metadata.TraceID); err != nil {
		return config.Snapshot{}, classify(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO channel_binding(tenant_id,config_version,binding_id,channel,external_account_id,agent_app_id,secret_ref,secret_version,send_secret_ref,send_secret_version)
SELECT $1,$2,item.binding_id,item.channel,item.external_account_id,item.agent_app_id,item.secret_ref->>'ref',(item.secret_ref->>'version')::bigint,
NULLIF(item.send_secret_ref->>'ref',''),NULLIF(item.send_secret_ref->>'version','')::bigint
FROM jsonb_to_recordset(COALESCE($3::jsonb->'channel_bindings','[]'::jsonb)) AS item(binding_id text,channel text,external_account_id text,agent_app_id text,secret_ref jsonb,send_secret_ref jsonb)`, in.TenantID, version, string(data)); err != nil {
		return config.Snapshot{}, classify(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO backend_binding(tenant_id,config_version,domain,backend_profile_id,backend_version,required)
SELECT $1,$2,item.domain,item.backend_profile_id,item.backend_version,ARRAY(SELECT jsonb_array_elements_text(COALESCE(item.required,'[]'::jsonb)))
FROM jsonb_to_recordset(COALESCE($3::jsonb->'backend_bindings','[]'::jsonb)) AS item(domain text,backend_profile_id text,backend_version bigint,required jsonb)`, in.TenantID, version, string(data)); err != nil {
		return config.Snapshot{}, classify(err)
	}
	if err = tx.Commit(); err != nil {
		return config.Snapshot{}, err
	}
	return r.Get(ctx, in.TenantID, version)
}
func (r *Repository) Get(ctx context.Context, tenantID string, version int64) (config.Snapshot, error) {
	var result config.Snapshot
	var payload []byte
	err := r.db.QueryRowContext(ctx, `SELECT tenant_id,config_version,schema_version,payload,content_digest,state,published_at,created_at FROM config_snapshot WHERE tenant_id=$1 AND config_version=$2`, tenantID, version).Scan(&result.TenantID, &result.ConfigVersion, &result.SchemaVersion, &payload, &result.ContentDigest, &result.State, &result.PublishedAt, &result.CreatedAt)
	if err != nil {
		return config.Snapshot{}, classify(err)
	}
	decoded, err := config.DecodeV1(payload)
	if err != nil {
		return config.Snapshot{}, config.ErrInvalid
	}
	result.Payload = decoded
	return result, nil
}
func (r *Repository) GetCurrent(ctx context.Context, tenantID string) (config.Snapshot, error) {
	current, err := r.tenants.Get(ctx, tenantID)
	if err != nil {
		return config.Snapshot{}, err
	}
	if current.ActiveConfigVersion < 1 {
		return config.Snapshot{}, config.ErrNotFound
	}
	return r.Get(ctx, tenantID, current.ActiveConfigVersion)
}
func (r *Repository) Rollback(ctx context.Context, in config.RollbackInput) (config.PublishResult, error) {
	target, err := r.Get(ctx, in.TenantID, in.TargetVersion)
	if err != nil {
		return config.PublishResult{}, err
	}
	return r.Publish(ctx, config.PublishInput{TenantID: in.TenantID, ExpectedTenantVersion: in.ExpectedTenantVersion, Payload: target.Payload, Metadata: in.Metadata})
}

func (r *Repository) CreateRelease(ctx context.Context, in config.ReleaseCreateInput) (config.Release, error) {
	if err := validReleaseCreate(in); err != nil {
		return config.Release{}, err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return config.Release{}, err
	}
	defer func() { _ = tx.Rollback() }()
	for _, target := range in.Targets {
		var active int64
		var status string
		if err = tx.QueryRowContext(ctx, `SELECT active_config_version,status FROM tenant WHERE tenant_id=$1 FOR UPDATE`, target.TenantID).Scan(&active, &status); err != nil {
			return config.Release{}, classify(err)
		}
		if status != string(tenant.StatusActive) || active != target.BaselineConfigVersion {
			return config.Release{}, config.ErrReleaseConflict
		}
		var exists bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM config_snapshot WHERE tenant_id=$1 AND config_version=$2)`, target.TenantID, target.CandidateConfigVersion).Scan(&exists); err != nil || !exists {
			if err != nil {
				return config.Release{}, classify(err)
			}
			return config.Release{}, config.ErrNotFound
		}
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM config_release_target target JOIN config_release release USING(release_id) WHERE target.tenant_id=$1 AND release.state='active')`, target.TenantID).Scan(&exists); err != nil {
			return config.Release{}, classify(err)
		}
		if exists {
			return config.Release{}, config.ErrReleaseConflict
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO config_release(release_id,state,percentage,salt,version,actor_id,reason_code,correlation_id,trace_id)
VALUES($1,'active',$2,$3,1,$4,$5,$6,$7)`, in.ReleaseID, in.Percentage, in.Salt, in.Metadata.ActorID, in.Metadata.ReasonCode, in.Metadata.CorrelationID, in.Metadata.TraceID); err != nil {
		return config.Release{}, classify(err)
	}
	for _, target := range in.Targets {
		if _, err = tx.ExecContext(ctx, `INSERT INTO config_release_target(release_id,tenant_id,baseline_config_version,candidate_config_version,allowlisted)
VALUES($1,$2,$3,$4,$5)`, in.ReleaseID, target.TenantID, target.BaselineConfigVersion, target.CandidateConfigVersion, target.Allowlisted); err != nil {
			return config.Release{}, classify(err)
		}
		if err = insertInvalidation(ctx, tx, target.TenantID, in.ReleaseID, 1); err != nil {
			return config.Release{}, err
		}
	}
	if err = tx.Commit(); err != nil {
		return config.Release{}, err
	}
	return r.GetRelease(ctx, in.ReleaseID)
}

func (r *Repository) GetRelease(ctx context.Context, releaseID string) (config.Release, error) {
	var value config.Release
	if err := r.db.QueryRowContext(ctx, `SELECT release_id,state,percentage,salt,version,created_at,updated_at FROM config_release WHERE release_id=$1`, releaseID).
		Scan(&value.ReleaseID, &value.State, &value.Percentage, &value.Salt, &value.Version, &value.CreatedAt, &value.UpdatedAt); err != nil {
		return config.Release{}, classifyRelease(err)
	}
	rows, err := r.db.QueryContext(ctx, `SELECT tenant_id,baseline_config_version,candidate_config_version,allowlisted FROM config_release_target WHERE release_id=$1 ORDER BY tenant_id`, releaseID)
	if err != nil {
		return config.Release{}, classify(err)
	}
	defer rows.Close()
	for rows.Next() {
		var target config.ReleaseTarget
		if err = rows.Scan(&target.TenantID, &target.BaselineConfigVersion, &target.CandidateConfigVersion, &target.Allowlisted); err != nil {
			return config.Release{}, err
		}
		value.Targets = append(value.Targets, target)
	}
	if err = rows.Err(); err != nil {
		return config.Release{}, err
	}
	return value, nil
}

func (r *Repository) UpdateRelease(ctx context.Context, in config.ReleaseUpdateInput) (config.Release, error) {
	if err := in.Metadata.Validate(); err != nil || in.ReleaseID == "" || in.ExpectedVersion < 1 || in.Percentage < 0 || in.Percentage > 100 {
		return config.Release{}, config.ErrInvalid
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return config.Release{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var version int64
	if err = tx.QueryRowContext(ctx, `UPDATE config_release SET percentage=$3,version=version+1,updated_at=now() WHERE release_id=$1 AND version=$2 AND state='active' RETURNING version`, in.ReleaseID, in.ExpectedVersion, in.Percentage).Scan(&version); err != nil {
		return config.Release{}, classifyRelease(err)
	}
	if err = r.invalidateReleaseTargets(ctx, tx, in.ReleaseID, version); err != nil {
		return config.Release{}, err
	}
	if err = tx.Commit(); err != nil {
		return config.Release{}, err
	}
	return r.GetRelease(ctx, in.ReleaseID)
}

func (r *Repository) RollbackRelease(ctx context.Context, in config.ReleaseRollbackInput) (config.Release, error) {
	if err := in.Metadata.Validate(); err != nil || in.ReleaseID == "" || in.ExpectedVersion < 1 {
		return config.Release{}, config.ErrInvalid
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return config.Release{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var version int64
	if err = tx.QueryRowContext(ctx, `UPDATE config_release SET state='rolled_back',version=version+1,updated_at=now() WHERE release_id=$1 AND version=$2 AND state='active' RETURNING version`, in.ReleaseID, in.ExpectedVersion).Scan(&version); err != nil {
		return config.Release{}, classifyRelease(err)
	}
	if err = r.invalidateReleaseTargets(ctx, tx, in.ReleaseID, version); err != nil {
		return config.Release{}, err
	}
	if err = tx.Commit(); err != nil {
		return config.Release{}, err
	}
	return r.GetRelease(ctx, in.ReleaseID)
}

func (r *Repository) invalidateReleaseTargets(ctx context.Context, tx *sql.Tx, releaseID string, version int64) error {
	rows, err := tx.QueryContext(ctx, `SELECT tenant_id FROM config_release_target WHERE release_id=$1`, releaseID)
	if err != nil {
		return err
	}
	var tenantIDs []string
	for rows.Next() {
		var tenantID string
		if err = rows.Scan(&tenantID); err != nil {
			_ = rows.Close()
			return err
		}
		tenantIDs = append(tenantIDs, tenantID)
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err = rows.Close(); err != nil {
		return err
	}
	for _, tenantID := range tenantIDs {
		if err = insertInvalidation(ctx, tx, tenantID, releaseID, version); err != nil {
			return err
		}
	}
	return nil
}

func insertInvalidation(ctx context.Context, tx *sql.Tx, tenantID, releaseID string, version int64) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref)
VALUES($1,$2,'config-invalidation',$3,$4,$5,$6)`, tenantID, "release-invalidation:"+releaseID+":"+tenantID+":"+fmt.Sprint(version), releaseID, version,
		"release:"+releaseID+":"+tenantID+":"+fmt.Sprint(version)+":invalidate", "config://release/"+releaseID+"/"+fmt.Sprint(version))
	return classify(err)
}

func (r *Repository) SelectEffective(ctx context.Context, tenantID string, baseline int64) (config.Snapshot, error) {
	current, err := r.tenants.Get(ctx, tenantID)
	if err != nil {
		return config.Snapshot{}, err
	}
	if baseline == 0 {
		baseline = current.ActiveConfigVersion
	}
	if baseline < 1 || baseline != current.ActiveConfigVersion {
		return config.Snapshot{}, config.ErrReleaseConflict
	}
	var releaseID, salt string
	var percentage int
	var candidate int64
	var allowlisted bool
	err = r.db.QueryRowContext(ctx, `SELECT release.release_id,release.salt,release.percentage,target.candidate_config_version,target.allowlisted
FROM config_release release JOIN config_release_target target USING(release_id)
WHERE target.tenant_id=$1 AND target.baseline_config_version=$2 AND release.state='active'
ORDER BY release.created_at DESC LIMIT 1`, tenantID, baseline).Scan(&releaseID, &salt, &percentage, &candidate, &allowlisted)
	if errors.Is(err, sql.ErrNoRows) {
		return r.Get(ctx, tenantID, baseline)
	}
	if err != nil {
		return config.Snapshot{}, classify(err)
	}
	if allowlisted || stableBucket(releaseID, tenantID, salt) < percentage {
		return r.Get(ctx, tenantID, candidate)
	}
	return r.Get(ctx, tenantID, baseline)
}
func (r *Repository) ResolveExecutionBinding(ctx context.Context, tc tenant.Context) (tenant.ExecutionBinding, error) {
	if err := tc.Validate(); err != nil {
		return tenant.ExecutionBinding{}, err
	}
	current, err := r.tenants.Get(ctx, tc.TenantID)
	if err != nil {
		return tenant.ExecutionBinding{}, err
	}
	if current.Status != tenant.StatusActive || current.Version != tc.TenantVersion {
		return tenant.ExecutionBinding{}, runtime.ErrVersionMismatch
	}
	snapshot, err := r.SelectEffective(ctx, tc.TenantID, current.ActiveConfigVersion)
	return r.resolveExecutionBinding(ctx, tc, current, snapshot, err)
}

func (r *Repository) ResolveExecutionBindingAt(ctx context.Context, tc tenant.Context, version int64) (tenant.ExecutionBinding, error) {
	if err := tc.Validate(); err != nil || version < 1 {
		return tenant.ExecutionBinding{}, config.ErrInvalid
	}
	current, err := r.tenants.Get(ctx, tc.TenantID)
	if err != nil {
		return tenant.ExecutionBinding{}, err
	}
	snapshot, getErr := r.Get(ctx, tc.TenantID, version)
	return r.resolveExecutionBinding(ctx, tc, current, snapshot, getErr)
}

func (r *Repository) resolveExecutionBinding(ctx context.Context, tc tenant.Context, current tenant.Tenant, snapshot config.Snapshot, getErr error) (tenant.ExecutionBinding, error) {
	if err := tc.Validate(); err != nil {
		return tenant.ExecutionBinding{}, err
	}
	if current.Status != tenant.StatusActive || current.Version != tc.TenantVersion {
		return tenant.ExecutionBinding{}, runtime.ErrVersionMismatch
	}
	if getErr != nil {
		return tenant.ExecutionBinding{}, getErr
	}
	allowed := tc.AgentAppID == snapshot.Payload.DefaultAgentAppID && tc.TrustedSource == "authenticated_api"
	for _, binding := range snapshot.Payload.ChannelBindings {
		if binding.AgentAppID == tc.AgentAppID && binding.Channel == tc.Channel && tc.TrustedSource == "channel_binding:"+binding.BindingID {
			allowed = true
			break
		}
	}
	if !allowed {
		return tenant.ExecutionBinding{}, config.ErrTenantScope
	}
	var appVersion, revision int64
	var status string
	if err := r.db.QueryRowContext(ctx, `SELECT version,current_revision,status FROM agent_app WHERE tenant_id=$1 AND agent_app_id=$2`, tc.TenantID, tc.AgentAppID).Scan(&appVersion, &revision, &status); err != nil {
		return tenant.ExecutionBinding{}, classify(err)
	}
	if status != string(agentapp.StatusActive) {
		return tenant.ExecutionBinding{}, config.ErrInvalid
	}
	var digest, state string
	var budget runtime.ExecutionBudget
	if err := r.db.QueryRowContext(ctx, `SELECT content_digest,state,max_llm_calls,max_tool_calls,max_parallel_tools,execution_timeout_seconds FROM agent_app_revision WHERE tenant_id=$1 AND agent_app_id=$2 AND revision=$3`, tc.TenantID, tc.AgentAppID, revision).Scan(&digest, &state, &budget.MaxLLMCalls, &budget.MaxToolCalls, &budget.MaxParallelTools, &budget.ExecutionTimeoutSeconds); err != nil {
		return tenant.ExecutionBinding{}, classify(err)
	}
	if state != string(agentapp.RevisionPublished) {
		return tenant.ExecutionBinding{}, config.ErrInvalid
	}
	result := tenant.ExecutionBinding{AgentAppVersion: appVersion, AgentAppRevision: revision, AgentContentDigest: digest, ConfigVersion: snapshot.ConfigVersion, PolicyVersion: snapshot.Payload.PolicyVersion, ExecutionBudget: budget}
	return result, result.Validate()
}

func validReleaseCreate(in config.ReleaseCreateInput) error {
	if err := in.Metadata.Validate(); err != nil || in.ReleaseID == "" || in.Salt == "" || in.Percentage < 0 || in.Percentage > 100 || len(in.Targets) == 0 {
		return config.ErrInvalid
	}
	seen := map[string]struct{}{}
	for _, target := range in.Targets {
		if target.TenantID == "" || target.BaselineConfigVersion < 1 || target.CandidateConfigVersion < 1 || target.BaselineConfigVersion == target.CandidateConfigVersion {
			return config.ErrInvalid
		}
		if _, exists := seen[target.TenantID]; exists {
			return config.ErrInvalid
		}
		seen[target.TenantID] = struct{}{}
	}
	return nil
}

func stableBucket(releaseID, tenantID, salt string) int {
	sum := sha256.Sum256([]byte(releaseID + "\x00" + tenantID + "\x00" + salt))
	return int(binary.BigEndian.Uint64(sum[:8]) % 100)
}

func classifyRelease(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return config.ErrReleaseNotFound
	}
	return classify(err)
}

type sqlStater interface{ SQLState() string }

func classify(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return config.ErrNotFound
	}
	var state sqlStater
	if errors.As(err, &state) {
		switch state.SQLState() {
		case "40001":
			return fmt.Errorf("%w: %v", config.ErrVersionConflict, err)
		case "P0002":
			return fmt.Errorf("%w: %v", config.ErrNotFound, err)
		case "23503":
			return fmt.Errorf("%w: %v", config.ErrTenantScope, err)
		case "22023", "23514", "23505", "55000":
			return fmt.Errorf("%w: %v", config.ErrInvalid, err)
		}
	}
	return err
}

var _ config.Repository = (*Repository)(nil)
