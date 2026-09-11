package controlplane

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// ModelProfile is one immutable version of a tenant's upstream model choice.
// api_key_ref is a reference (env:NAME / file:/abs/path), never a key.
type ModelProfile struct {
	ProfileID int64
	TenantID  string
	PublicID  string
	Version   uint32
	ModelName string
	BaseURL   string
	APIKeyRef string
}

// BackendProfile is one immutable version of a tenant's session/embedding
// backend choice.
type BackendProfile struct {
	ProfileID         int64
	TenantID          string
	PublicID          string
	Version           uint32
	SessionBackend    string
	RedisKeyPrefix    string
	SessionTTLSeconds int64
	EmbeddingModel    string
	EmbeddingDim      uint32
}

// CreateModelProfile inserts version 1 of a new profile, or the next version
// of an existing public_id. Versioning here is what makes "a session fixed at
// a revision" mean something after a key or endpoint rotation (approved plan,
// "不可变修订与 RuntimePlan").
func (s Scope) CreateModelProfile(ctx context.Context, publicID, modelName, baseURL, apiKeyRef string) (int64, error) {
	if publicID == "" || modelName == "" {
		return 0, errors.New("controlplane: a model profile needs a public id and a model name")
	}
	next, err := s.nextProfileVersion(ctx, "model_profiles", publicID)
	if err != nil {
		return 0, err
	}
	res, err := s.Exec(ctx, `
		INSERT INTO model_profiles (tenant_id, public_id, version, model_name, base_url, api_key_ref)
		VALUES (?, ?, ?, ?, ?, ?)`,
		s.tenantID, publicID, next, modelName, baseURL, apiKeyRef)
	if err != nil {
		return 0, fmt.Errorf("controlplane: create model profile %q: %w", publicID, err)
	}
	return res.LastInsertId()
}

// GetModelProfile reads the latest version of a profile by public id.
func (s Scope) GetModelProfile(ctx context.Context, publicID string) (ModelProfile, error) {
	var p ModelProfile
	row, err := s.QueryRow(ctx, `
		SELECT profile_id, tenant_id, public_id, version, model_name, base_url, api_key_ref
		FROM model_profiles
		WHERE tenant_id = ? AND public_id = ?
		ORDER BY version DESC LIMIT 1`, s.tenantID, publicID)
	if err != nil {
		return ModelProfile{}, err
	}
	switch err := row.Scan(&p.ProfileID, &p.TenantID, &p.PublicID, &p.Version,
		&p.ModelName, &p.BaseURL, &p.APIKeyRef); {
	case errors.Is(err, sql.ErrNoRows):
		return ModelProfile{}, ErrNotFound
	case err != nil:
		return ModelProfile{}, fmt.Errorf("controlplane: get model profile: %w", err)
	}
	return p, nil
}

// GetModelProfileByID reads one exact profile row. A revision pins a profile
// by id (not by public_id), because that is what makes the revision fixed:
// resolving through public_id+"latest version" would let a later profile
// bump silently change what an already-published revision means.
func (s Scope) GetModelProfileByID(ctx context.Context, profileID int64) (ModelProfile, error) {
	var p ModelProfile
	row, err := s.QueryRow(ctx, `
		SELECT profile_id, tenant_id, public_id, version, model_name, base_url, api_key_ref
		FROM model_profiles
		WHERE tenant_id = ? AND profile_id = ?`, s.tenantID, profileID)
	if err != nil {
		return ModelProfile{}, err
	}
	switch err := row.Scan(&p.ProfileID, &p.TenantID, &p.PublicID, &p.Version,
		&p.ModelName, &p.BaseURL, &p.APIKeyRef); {
	case errors.Is(err, sql.ErrNoRows):
		return ModelProfile{}, ErrNotFound
	case err != nil:
		return ModelProfile{}, fmt.Errorf("controlplane: get model profile by id: %w", err)
	}
	return p, nil
}

// CreateBackendProfile inserts the next version of a backend profile.
func (s Scope) CreateBackendProfile(ctx context.Context, p BackendProfile) (int64, error) {
	if p.PublicID == "" {
		return 0, errors.New("controlplane: a backend profile needs a public id")
	}
	backend := p.SessionBackend
	if backend == "" {
		backend = "redis"
	}
	next, err := s.nextProfileVersion(ctx, "backend_profiles", p.PublicID)
	if err != nil {
		return 0, err
	}
	res, err := s.Exec(ctx, `
		INSERT INTO backend_profiles
			(tenant_id, public_id, version, session_backend, redis_key_prefix, session_ttl,
			 embedding_model, embedding_dim)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		s.tenantID, p.PublicID, next, backend, p.RedisKeyPrefix, p.SessionTTLSeconds,
		p.EmbeddingModel, p.EmbeddingDim)
	if err != nil {
		return 0, fmt.Errorf("controlplane: create backend profile %q: %w", p.PublicID, err)
	}
	return res.LastInsertId()
}

// GetBackendProfile reads the latest version of a backend profile by public id.
func (s Scope) GetBackendProfile(ctx context.Context, publicID string) (BackendProfile, error) {
	var p BackendProfile
	row, err := s.QueryRow(ctx, `
		SELECT profile_id, tenant_id, public_id, version, session_backend, redis_key_prefix,
		       session_ttl, embedding_model, embedding_dim
		FROM backend_profiles
		WHERE tenant_id = ? AND public_id = ?
		ORDER BY version DESC LIMIT 1`, s.tenantID, publicID)
	if err != nil {
		return BackendProfile{}, err
	}
	switch err := row.Scan(&p.ProfileID, &p.TenantID, &p.PublicID, &p.Version,
		&p.SessionBackend, &p.RedisKeyPrefix, &p.SessionTTLSeconds,
		&p.EmbeddingModel, &p.EmbeddingDim); {
	case errors.Is(err, sql.ErrNoRows):
		return BackendProfile{}, ErrNotFound
	case err != nil:
		return BackendProfile{}, fmt.Errorf("controlplane: get backend profile: %w", err)
	}
	return p, nil
}

// nextProfileVersion reads the highest version already recorded for one
// public id in this tenant. It is a plain read, not a lock: two concurrent
// "bump this profile" calls for the same public id would both compute the
// same next version, and the unique index on (tenant_id, public_id, version)
// rejects exactly one of them — the database, not this function, is what
// makes the version sequence safe.
func (s Scope) nextProfileVersion(ctx context.Context, table, publicID string) (uint32, error) {
	var next uint32
	// table is a constant chosen by this package's own callers, never a
	// request field; it cannot be a placeholder because it names a table.
	q := "SELECT COALESCE(MAX(version), 0) + 1 FROM " + table + " WHERE tenant_id = ? AND public_id = ?"
	row, err := s.QueryRow(ctx, q, s.tenantID, publicID)
	if err != nil {
		return 0, err
	}
	if err := row.Scan(&next); err != nil {
		return 0, fmt.Errorf("controlplane: next version of %s: %w", publicID, err)
	}
	return next, nil
}

// ChannelBinding is one tenant+app's connection to an IM channel.
type ChannelBinding struct {
	BindingID     int64
	TenantID      string
	AppID         int64
	ChannelType   string
	PublicID      string
	CredentialRef string
	Config        json.RawMessage
	Status        string
}

// BindChannel attaches an IM channel to an app.
func (s Scope) BindChannel(ctx context.Context, b ChannelBinding) (int64, error) {
	if b.AppID == 0 || b.ChannelType == "" || b.PublicID == "" {
		return 0, errors.New("controlplane: a channel binding needs an app, a channel type, and a public id")
	}
	status := b.Status
	if status == "" {
		status = "active"
	}
	res, err := s.Exec(ctx, `
		INSERT INTO channel_bindings
			(tenant_id, app_id, channel_type, public_id, credential_ref, config, status)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		s.tenantID, b.AppID, b.ChannelType, b.PublicID, b.CredentialRef, nullableJSON(b.Config), status)
	if err != nil {
		return 0, fmt.Errorf("controlplane: bind channel: %w", err)
	}
	return res.LastInsertId()
}

// GetChannelBinding reads a binding by channel type and public id.
func (s Scope) GetChannelBinding(ctx context.Context, channelType, publicID string) (ChannelBinding, error) {
	var b ChannelBinding
	var config jsonCol
	row, err := s.QueryRow(ctx, `
		SELECT binding_id, tenant_id, app_id, channel_type, public_id, credential_ref, config, status
		FROM channel_bindings WHERE tenant_id = ? AND channel_type = ? AND public_id = ?`,
		s.tenantID, channelType, publicID)
	if err != nil {
		return ChannelBinding{}, err
	}
	switch err := row.Scan(&b.BindingID, &b.TenantID, &b.AppID, &b.ChannelType,
		&b.PublicID, &b.CredentialRef, &config, &b.Status); {
	case errors.Is(err, sql.ErrNoRows):
		return ChannelBinding{}, ErrNotFound
	case err != nil:
		return ChannelBinding{}, fmt.Errorf("controlplane: get channel binding: %w", err)
	}
	b.Config = config.RawMessage
	return b, nil
}

// ToolBinding is one tenant+app's attachment of a Go builtin or an HTTP
// template. spec/input_schema/output_schema/secret_refs are validated by
// trpcservice/tool at load time, not here — this is storage, not policy.
type ToolBinding struct {
	ToolID   int64
	TenantID string
	AppID    int64
	Name     string
	Kind     string
	// Version is filled in by BindTool, not by the caller: it is the next
	// number in this tool's sequence, which only the database can pick safely.
	Version      uint32
	RiskLevel    string
	SideEffect   string
	Idempotent   bool
	Spec         json.RawMessage
	InputSchema  json.RawMessage
	OutputSchema json.RawMessage
	TimeoutMS    int
	SecretRefs   json.RawMessage
	Status       string
}

// BindTool attaches a tool to an app at a new version. Version is chosen
// here, not by the caller: a publisher says "this tool named X on app Y
// should now behave like this", and the platform is what decides that means
// version N+1.
func (s Scope) BindTool(ctx context.Context, t ToolBinding) (int64, error) {
	if t.AppID == 0 || t.Name == "" || t.Kind == "" {
		return 0, errors.New("controlplane: a tool binding needs an app, a name, and a kind")
	}
	if len(t.InputSchema) == 0 {
		return 0, errors.New("controlplane: a tool binding needs an input schema")
	}
	version, err := s.nextToolVersion(ctx, t.AppID, t.Name)
	if err != nil {
		return 0, err
	}
	risk := t.RiskLevel
	if risk == "" {
		risk = "low"
	}
	side := t.SideEffect
	if side == "" {
		side = "none"
	}
	timeout := t.TimeoutMS
	if timeout <= 0 {
		timeout = 10000
	}
	res, err := s.Exec(ctx, `
		INSERT INTO tool_bindings
			(tenant_id, app_id, name, kind, version, risk_level, side_effect, idempotent,
			 spec, input_schema, output_schema, timeout_ms, secret_refs, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		s.tenantID, t.AppID, t.Name, t.Kind, version, risk, side, t.Idempotent,
		t.Spec, t.InputSchema, nullableJSON(t.OutputSchema), timeout, nullableJSON(t.SecretRefs), "active")
	if err != nil {
		return 0, fmt.Errorf("controlplane: bind tool: %w", err)
	}
	return res.LastInsertId()
}

// GetToolBinding reads a tool binding by id, returning the found version so a
// caller can pin against it without re-querying.
func (s Scope) GetToolBinding(ctx context.Context, toolID int64) (ToolBinding, error) {
	var t ToolBinding
	var outputSchema, secretRefs jsonCol
	row, err := s.QueryRow(ctx, `
		SELECT tool_id, tenant_id, app_id, name, kind, version, risk_level, side_effect,
		       idempotent, spec, input_schema, output_schema, timeout_ms, secret_refs, status
		FROM tool_bindings WHERE tenant_id = ? AND tool_id = ?`, s.tenantID, toolID)
	if err != nil {
		return ToolBinding{}, err
	}
	switch err := row.Scan(&t.ToolID, &t.TenantID, &t.AppID, &t.Name, &t.Kind, &t.Version,
		&t.RiskLevel, &t.SideEffect, &t.Idempotent, &t.Spec, &t.InputSchema, &outputSchema,
		&t.TimeoutMS, &secretRefs, &t.Status); {
	case errors.Is(err, sql.ErrNoRows):
		return ToolBinding{}, ErrNotFound
	case err != nil:
		return ToolBinding{}, fmt.Errorf("controlplane: get tool binding: %w", err)
	}
	t.OutputSchema, t.SecretRefs = outputSchema.RawMessage, secretRefs.RawMessage
	return t, nil
}

// GetToolBindingByName loads one exact version of a tool on an app. The
// reliable path pins (name, version) in the revision, and this is how a
// worker turns that pin into a definition without ever reading "whatever is
// current now" — a pin that resolves to a different row than it pinned would
// make the revision's manifest hash a lie.
func (s Scope) GetToolBindingByName(ctx context.Context, appID int64, name string, version uint32) (ToolBinding, error) {
	var t ToolBinding
	var outputSchema, secretRefs jsonCol
	row, err := s.QueryRow(ctx, `
		SELECT tool_id, tenant_id, app_id, name, kind, version, risk_level, side_effect,
		       idempotent, spec, input_schema, output_schema, timeout_ms, secret_refs, status
		FROM tool_bindings WHERE tenant_id = ? AND app_id = ? AND name = ? AND version = ?`,
		s.tenantID, appID, name, version)
	if err != nil {
		return ToolBinding{}, err
	}
	switch err := row.Scan(&t.ToolID, &t.TenantID, &t.AppID, &t.Name, &t.Kind, &t.Version,
		&t.RiskLevel, &t.SideEffect, &t.Idempotent, &t.Spec, &t.InputSchema, &outputSchema,
		&t.TimeoutMS, &secretRefs, &t.Status); {
	case errors.Is(err, sql.ErrNoRows):
		return ToolBinding{}, ErrNotFound
	case err != nil:
		return ToolBinding{}, fmt.Errorf("controlplane: get tool binding by name: %w", err)
	}
	t.OutputSchema, t.SecretRefs = outputSchema.RawMessage, secretRefs.RawMessage
	return t, nil
}

// nextToolVersion is nextProfileVersion's sibling for a different table shape:
// tool_bindings are keyed by (tenant, app, name, version), not by a public_id
// column, so the version lookup cannot share the profile helper.
func (s Scope) nextToolVersion(ctx context.Context, appID int64, name string) (uint32, error) {
	var next uint32
	row, err := s.QueryRow(ctx,
		"SELECT COALESCE(MAX(version), 0) + 1 FROM tool_bindings WHERE tenant_id = ? AND app_id = ? AND name = ?",
		s.tenantID, appID, name)
	if err != nil {
		return 0, err
	}
	if err := row.Scan(&next); err != nil {
		return 0, fmt.Errorf("controlplane: next version of tool %q: %w", name, err)
	}
	return next, nil
}

// KnowledgeBase is one tenant+app document collection.
type KnowledgeBase struct {
	KBID     int64
	TenantID string
	AppID    int64
	PublicID string
	Name     string
	Status   string
}

// CreateKnowledgeBase registers a new, empty knowledge base.
func (s Scope) CreateKnowledgeBase(ctx context.Context, appID int64, publicID, name string) (int64, error) {
	if appID == 0 || publicID == "" {
		return 0, errors.New("controlplane: a knowledge base needs an app and a public id")
	}
	res, err := s.Exec(ctx,
		"INSERT INTO knowledge_bases (tenant_id, app_id, public_id, name) VALUES (?, ?, ?, ?)",
		s.tenantID, appID, publicID, name)
	if err != nil {
		return 0, fmt.Errorf("controlplane: create knowledge base: %w", err)
	}
	return res.LastInsertId()
}

// GetKnowledgeBase reads a knowledge base by public id.
func (s Scope) GetKnowledgeBase(ctx context.Context, publicID string) (KnowledgeBase, error) {
	var kb KnowledgeBase
	row, err := s.QueryRow(ctx,
		"SELECT kb_id, tenant_id, app_id, public_id, name, status FROM knowledge_bases WHERE tenant_id = ? AND public_id = ?",
		s.tenantID, publicID)
	if err != nil {
		return KnowledgeBase{}, err
	}
	switch err := row.Scan(&kb.KBID, &kb.TenantID, &kb.AppID, &kb.PublicID, &kb.Name, &kb.Status); {
	case errors.Is(err, sql.ErrNoRows):
		return KnowledgeBase{}, ErrNotFound
	case err != nil:
		return KnowledgeBase{}, fmt.Errorf("controlplane: get knowledge base: %w", err)
	}
	return kb, nil
}

// BindRevisionKnowledge attaches a knowledge base to an already-published
// revision. The revision and the KB must both be this tenant's and the KB
// must belong to the same app as the revision; the database's own
// tenant-composite foreign keys catch the first case, this call catches the
// second.
func (s Scope) BindRevisionKnowledge(ctx context.Context, revisionID, kbID int64) error {
	return s.WithTx(ctx, func(tx *TxScope) error {
		var revApp, kbApp int64
		if err := tx.QueryRow(ctx,
			"SELECT app_id FROM agent_revisions WHERE tenant_id = ? AND revision_id = ?",
			s.tenantID, revisionID).Scan(&revApp); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("controlplane: load revision for kb binding: %w", err)
		}
		if err := tx.QueryRow(ctx,
			"SELECT app_id FROM knowledge_bases WHERE tenant_id = ? AND kb_id = ?",
			s.tenantID, kbID).Scan(&kbApp); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("controlplane: load kb for binding: %w", err)
		}
		if revApp != kbApp {
			return fmt.Errorf("%w: revision %d is on app %d, kb %d is on app %d",
				ErrCrossTenantReference, revisionID, revApp, kbID, kbApp)
		}
		if _, err := tx.Exec(ctx,
			"INSERT INTO knowledge_bindings (tenant_id, revision_id, kb_id) VALUES (?, ?, ?)",
			s.tenantID, revisionID, kbID); err != nil {
			return fmt.Errorf("controlplane: bind knowledge: %w", err)
		}
		return nil
	})
}
