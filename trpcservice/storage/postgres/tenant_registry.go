package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	tenantctx "github.com/liuzengh/trpc-agent-service/trpcservice/storage/tenantctx"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const metadataQueryTimeout = 5 * time.Second

type TenantRegistry struct {
	pool    *pgxpool.Pool
	timeout time.Duration
}

func NewTenantRegistry(pool *pgxpool.Pool) (*TenantRegistry, error) {
	if pool == nil {
		return nil, errors.New("postgres: tenant registry pool is required")
	}
	return &TenantRegistry{pool: pool, timeout: metadataQueryTimeout}, nil
}

func (r *TenantRegistry) queryContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if r == nil || r.pool == nil || ctx == nil {
		return nil, nil, storage.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	queryCtx, cancel := context.WithTimeout(ctx, r.timeout)
	return queryCtx, cancel, nil
}

func (r *TenantRegistry) Tenant(ctx context.Context, id string) (tenant.Tenant, error) {
	if id == "" {
		return tenant.Tenant{}, storage.ErrInvalidArgument
	}
	queryCtx, cancel, err := r.queryContext(ctx)
	if err != nil {
		return tenant.Tenant{}, err
	}
	defer cancel()
	var value tenant.Tenant
	var defaultAgent *string
	var backend []byte
	err = WithTenantContext(queryCtx, r.pool, id, "tenant read", func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT tenant_id, name, status, config_version, default_agent_app_id, backend_config FROM tenant WHERE tenant_id=$1`, id).Scan(&value.ID, &value.Name, &value.Status, &value.ConfigVersion, &defaultAgent, &backend)
	})
	if err != nil {
		return tenant.Tenant{}, registryError(err)
	}
	if defaultAgent != nil {
		value.DefaultAgentID = *defaultAgent
	}
	value.Backend = decodeBackendPolicy(backend)
	if err := value.Validate(); err != nil {
		return tenant.Tenant{}, storage.ErrBackendUnavailable
	}
	return value, nil
}

func (r *TenantRegistry) Agent(ctx context.Context, tenantID, id string) (tenant.AgentApp, error) {
	if tenantID == "" || id == "" {
		return tenant.AgentApp{}, storage.ErrInvalidArgument
	}
	queryCtx, cancel, err := r.queryContext(ctx)
	if err != nil {
		return tenant.AgentApp{}, err
	}
	defer cancel()
	var value tenant.AgentApp
	var version int64
	var modelRef, systemPrompt, guardrail *string
	err = WithTenantContext(queryCtx, r.pool, tenantID, "agent read", func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT a.tenant_id, a.agent_app_id, a.name, a.status, a.model_config_ref, a.system_prompt, a.guardrail_ref, t.config_version FROM agent_app a JOIN tenant t ON t.tenant_id=a.tenant_id WHERE a.tenant_id=$1 AND a.agent_app_id=$2`, tenantID, id).Scan(&value.TenantID, &value.ID, &value.Name, &value.Status, &modelRef, &systemPrompt, &guardrail, &version)
	})
	if err != nil {
		return tenant.AgentApp{}, registryError(err)
	}
	if modelRef != nil {
		value.ModelConfigRef = *modelRef
	}
	if systemPrompt != nil {
		value.SystemPrompt = *systemPrompt
	}
	if guardrail != nil {
		value.GuardrailRef = *guardrail
	}
	value.ToolPolicyID = "default"
	value.Version = version
	if err := value.Validate(); err != nil {
		return tenant.AgentApp{}, storage.ErrBackendUnavailable
	}
	return value, nil
}

func (r *TenantRegistry) Binding(ctx context.Context, channel, externalAppID string) (tenant.ChannelBinding, error) {
	return r.ResolveBinding(ctx, channel, externalAppID)
}

func (r *TenantRegistry) ResolveBinding(ctx context.Context, channel, externalAppID string) (tenant.ChannelBinding, error) {
	if (channel != tenant.ChannelLark && channel != tenant.ChannelTelegram) || externalAppID == "" {
		return tenant.ChannelBinding{}, tenant.ErrBindingNotFound
	}
	queryCtx, cancel, err := r.queryContext(ctx)
	if err != nil {
		return tenant.ChannelBinding{}, err
	}
	defer cancel()
	// Pre-tenant webhook resolution: the only cross-tenant read on this
	// table, executed through the fixed SECURITY DEFINER resolve function.
	rows, err := r.pool.Query(queryCtx, `SELECT tenant_id, channel, binding_id, external_app_id, secret_ref, verify_token_ref, status, enabled, version, created_at, updated_at, expires_at, disabled_at, external_target_type, external_target_id FROM trpc_binding_resolve($1, $2)`, channel, externalAppID)
	if err != nil {
		return tenant.ChannelBinding{}, registryError(err)
	}
	defer rows.Close()
	var value tenant.ChannelBinding
	count := 0
	for rows.Next() {
		count++
		if count > 1 {
			return tenant.ChannelBinding{}, tenant.ErrBindingConflict
		}
		var secretRef, verifyRef, targetID *string
		var expiresAt, disabledAt *time.Time
		if err := rows.Scan(&value.TenantID, &value.Channel, &value.ID, &value.ExternalAppID, &secretRef, &verifyRef, &value.Status, &value.Enabled, &value.Version, &value.CreatedAt, &value.UpdatedAt, &expiresAt, &disabledAt, &value.ExternalTargetType, &targetID); err != nil {
			return tenant.ChannelBinding{}, registryError(err)
		}
		if secretRef != nil {
			value.SecretRef = *secretRef
		}
		if verifyRef != nil {
			value.VerifyTokenRef = *verifyRef
		}
		if expiresAt != nil {
			value.ExpiresAt = *expiresAt
		}
		if disabledAt != nil {
			value.DisabledAt = *disabledAt
		}
		if targetID != nil {
			value.ExternalTargetID = *targetID
		}
	}
	if err := rows.Err(); err != nil {
		return tenant.ChannelBinding{}, registryError(err)
	}
	if count == 0 {
		return tenant.ChannelBinding{}, tenant.ErrBindingNotFound
	}
	if err := value.Validate(); err != nil {
		return tenant.ChannelBinding{}, tenant.ErrBindingNotFound
	}
	return value, nil
}

func (r *TenantRegistry) GetBinding(ctx context.Context, tc tenant.TenantContext, id string) (tenant.ChannelBinding, error) {
	if err := tenantContextForMetadata(ctx, tc); err != nil {
		return tenant.ChannelBinding{}, err
	}
	if id == "" {
		return tenant.ChannelBinding{}, storage.ErrInvalidArgument
	}
	queryCtx, cancel, err := r.queryContext(ctx)
	if err != nil {
		return tenant.ChannelBinding{}, err
	}
	defer cancel()
	return r.bindingByTenant(queryCtx, tc.TenantID, tc.Channel, id)
}

func (r *TenantRegistry) CreateBinding(ctx context.Context, tc tenant.TenantContext, value tenant.ChannelBinding) error {
	if err := tenantContextForMetadata(ctx, tc); err != nil {
		return err
	}
	value = value.Canonical()
	if err := value.Validate(); err != nil {
		return storage.ErrInvalidArgument
	}
	if value.TenantID != tc.TenantID || value.Channel != tc.Channel {
		return storage.ErrTenantMismatch
	}
	queryCtx, cancel, err := r.queryContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	err = WithTenantContext(queryCtx, r.pool, tc.TenantID, "binding create", func(ctx context.Context, tx pgx.Tx) error {
		_, execErr := tx.Exec(ctx, `INSERT INTO channel_binding (tenant_id, channel, binding_id, external_app_id, secret_ref, verify_token_ref, status, enabled, version, expires_at, disabled_at, external_target_type, external_target_id) VALUES ($1,$2,$3,$4,NULLIF($5,''),NULLIF($6,''),$7,$8,$9,$10,$11,$12,NULLIF($13,''))`, value.TenantID, value.Channel, value.ID, value.ExternalAppID, value.SecretRef, value.VerifyTokenRef, value.Status, value.Enabled, value.Version, nullableTime(value.ExpiresAt), nullableTime(value.DisabledAt), value.ExternalTargetType, value.ExternalTargetID)
		return execErr
	})
	return metadataError(err)
}

func (r *TenantRegistry) UpdateBinding(ctx context.Context, tc tenant.TenantContext, value tenant.ChannelBinding, expectedVersion int64) error {
	if err := tenantContextForMetadata(ctx, tc); err != nil {
		return err
	}
	value = value.Canonical()
	if err := value.Validate(); err != nil {
		return storage.ErrInvalidArgument
	}
	if value.TenantID != tc.TenantID || value.Channel != tc.Channel {
		return storage.ErrTenantMismatch
	}
	if expectedVersion < 1 || value.Version != expectedVersion+1 {
		return storage.ErrInvalidArgument
	}
	queryCtx, cancel, err := r.queryContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	err = WithTenantContext(queryCtx, r.pool, tc.TenantID, "binding update", func(ctx context.Context, tx pgx.Tx) error {
		command, execErr := tx.Exec(ctx, `UPDATE channel_binding SET external_app_id=$4, secret_ref=NULLIF($5,''), verify_token_ref=NULLIF($6,''), status=$7, enabled=$8, version=$9, expires_at=$10, disabled_at=$11, external_target_type=$12, external_target_id=NULLIF($13,''), updated_at=now() WHERE tenant_id=$1 AND channel=$2 AND binding_id=$3 AND version=$14`, value.TenantID, value.Channel, value.ID, value.ExternalAppID, value.SecretRef, value.VerifyTokenRef, value.Status, value.Enabled, value.Version, nullableTime(value.ExpiresAt), nullableTime(value.DisabledAt), value.ExternalTargetType, value.ExternalTargetID, expectedVersion)
		if execErr != nil {
			return execErr
		}
		if command.RowsAffected() != 1 {
			return storage.ErrConflict
		}
		return nil
	})
	return metadataError(err)
}

func (r *TenantRegistry) ResolveIdentity(ctx context.Context, tc tenant.TenantContext) (tenant.Identity, error) {
	if err := tenantContextForMetadata(ctx, tc); err != nil {
		return tenant.Identity{}, err
	}
	candidate, scope, err := tenant.DeriveIdentity(tc)
	if err != nil {
		return tenant.Identity{}, err
	}
	queryCtx, cancel, err := r.queryContext(ctx)
	if err != nil {
		return tenant.Identity{}, err
	}
	defer cancel()
	var value tenant.Identity
	var storedScope string
	var storedChat, storedThread *string
	upsertErr := tenantctx.WithTenantContext(queryCtx, r.pool, tc.TenantID, "identity upsert", func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `INSERT INTO user_identity (tenant_id, identity_id, channel, binding_id, external_user_id, internal_user_id, scope, external_chat, external_thread_id, status, version, last_seen_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,NULLIF($8,''),NULLIF($9,''),'active',1,now(),now()) ON CONFLICT (tenant_id, channel, binding_id, external_user_id) DO UPDATE SET last_seen_at=now(), updated_at=now() RETURNING tenant_id, identity_id, channel, binding_id, external_user_id, internal_user_id, status, version, scope, external_chat, external_thread_id`, tc.TenantID, candidate.ID, candidate.Channel, candidate.BindingID, candidate.ExternalUserID, candidate.InternalUserID, scopeName(scope), tc.ExternalChat, tc.ExternalThreadID).Scan(&value.TenantID, &value.ID, &value.Channel, &value.BindingID, &value.ExternalUserID, &value.InternalUserID, &value.Status, &value.Version, &storedScope, &storedChat, &storedThread)
	})
	if upsertErr != nil && !errors.Is(upsertErr, pgx.ErrNoRows) && !identityPrimaryKeyConflict(upsertErr) {
		return tenant.Identity{}, registryError(upsertErr)
	}
	if upsertErr != nil {
		// The failed insert aborted its transaction; the canonical-key read runs
		// in a fresh transaction so an idempotent insert race still converges.
		fallbackErr := WithTenantContext(queryCtx, r.pool, tc.TenantID, "identity read", func(ctx context.Context, tx pgx.Tx) error {
			return tx.QueryRow(ctx, `SELECT tenant_id, identity_id, channel, binding_id, external_user_id, internal_user_id, status, version, scope, external_chat, external_thread_id FROM user_identity WHERE tenant_id=$1 AND channel=$2 AND binding_id=$3 AND external_user_id=$4`, tc.TenantID, candidate.Channel, candidate.BindingID, candidate.ExternalUserID).Scan(&value.TenantID, &value.ID, &value.Channel, &value.BindingID, &value.ExternalUserID, &value.InternalUserID, &value.Status, &value.Version, &storedScope, &storedChat, &storedThread)
		})
		if fallbackErr != nil {
			if errors.Is(fallbackErr, pgx.ErrNoRows) {
				return tenant.Identity{}, storage.ErrConflict
			}
			return tenant.Identity{}, registryError(fallbackErr)
		}
	}
	if value.ID != candidate.ID || value.InternalUserID != candidate.InternalUserID {
		return tenant.Identity{}, tenant.ErrIdentityConflict
	}
	value.Scope = identityScopeKey(storedScope, nullableString(storedChat), nullableString(storedThread), value.ExternalUserID)
	value.ExternalChat = nullableString(storedChat)
	value.ExternalThreadID = nullableString(storedThread)
	if value.Scope != candidate.Scope || value.ExternalChat != candidate.ExternalChat || value.ExternalThreadID != candidate.ExternalThreadID {
		return tenant.Identity{}, tenant.ErrIdentityConflict
	}
	if err := value.Validate(); err != nil {
		return tenant.Identity{}, tenant.ErrIdentityInvalid
	}
	return value, nil
}

func (r *TenantRegistry) AppendBindingEvent(ctx context.Context, tc tenant.TenantContext, event storage.BindingAuditEvent) error {
	if err := tenantContextForMetadata(ctx, tc); err != nil {
		return err
	}
	if event.TenantID != tc.TenantID || event.BindingID != tc.BindingID || event.Channel != tc.Channel || event.AuditID == "" || event.Operation == "" || event.Version < 1 || len(event.Operation) > 64 || len(event.ErrorType) > 80 || len(event.IdentityFingerprint) > 80 || len(event.SecretFingerprint) > 80 {
		return storage.ErrInvalidArgument
	}
	queryCtx, cancel, err := r.queryContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	err = WithTenantContext(queryCtx, r.pool, tc.TenantID, "binding audit", func(ctx context.Context, tx pgx.Tx) error {
		_, execErr := tx.Exec(ctx, `INSERT INTO channel_binding_audit (tenant_id, audit_id, channel, binding_id, identity_fingerprint, secret_fingerprint, operation, success, error_type, version, created_at) VALUES ($1,$2,$3,$4,NULLIF($5,''),NULLIF($6,''),$7,$8,NULLIF($9,''),$10,COALESCE($11,now()))`, event.TenantID, event.AuditID, event.Channel, event.BindingID, event.IdentityFingerprint, event.SecretFingerprint, event.Operation, event.Success, event.ErrorType, event.Version, nullableTime(event.CreatedAt))
		return execErr
	})
	return metadataError(err)
}

func (r *TenantRegistry) bindingByTenant(ctx context.Context, tenantID, channel, id string) (tenant.ChannelBinding, error) {
	var value tenant.ChannelBinding
	var secretRef, verifyRef, targetID *string
	var expiresAt, disabledAt *time.Time
	err := WithTenantContext(ctx, r.pool, tenantID, "binding read", func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT tenant_id, channel, binding_id, external_app_id, secret_ref, verify_token_ref, status, enabled, version, created_at, updated_at, expires_at, disabled_at, external_target_type, external_target_id FROM channel_binding WHERE tenant_id=$1 AND channel=$2 AND binding_id=$3`, tenantID, channel, id).Scan(&value.TenantID, &value.Channel, &value.ID, &value.ExternalAppID, &secretRef, &verifyRef, &value.Status, &value.Enabled, &value.Version, &value.CreatedAt, &value.UpdatedAt, &expiresAt, &disabledAt, &value.ExternalTargetType, &targetID)
	})
	if err != nil {
		return tenant.ChannelBinding{}, registryError(err)
	}
	if secretRef != nil {
		value.SecretRef = *secretRef
	}
	if verifyRef != nil {
		value.VerifyTokenRef = *verifyRef
	}
	if expiresAt != nil {
		value.ExpiresAt = *expiresAt
	}
	if disabledAt != nil {
		value.DisabledAt = *disabledAt
	}
	if targetID != nil {
		value.ExternalTargetID = *targetID
	}
	if err := value.Validate(); err != nil {
		return tenant.ChannelBinding{}, tenant.ErrBindingNotFound
	}
	return value, nil
}

func tenantContextForMetadata(ctx context.Context, tc tenant.TenantContext) error {
	if ctx == nil {
		return storage.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tc.Validate(); err != nil {
		return storage.ErrTenantMismatch
	}
	if tc.Channel != tenant.ChannelLark && tc.Channel != tenant.ChannelTelegram {
		return tenant.ErrUnsupportedBindingChannel
	}
	return nil
}

func decodeBackendPolicy(raw []byte) tenant.BackendPolicy {
	value := tenant.BackendPolicy{Session: "none", Memory: "none", Vector: "none", Object: "none"}
	var fields map[string]string
	if json.Unmarshal(raw, &fields) == nil {
		for key, field := range map[string]*string{"session": &value.Session, "memory": &value.Memory, "vector": &value.Vector, "object": &value.Object} {
			if fields[key] != "" {
				*field = fields[key]
			}
		}
	}
	return value
}

func scopeName(scope string) string {
	if strings.HasPrefix(scope, "topic:") {
		return tenant.IdentityScopeTopic
	}
	if strings.HasPrefix(scope, "group:") {
		return tenant.IdentityScopeGroup
	}
	return tenant.IdentityScopePrivate
}

func identityScopeKey(kind, chat, thread, externalUser string) string {
	switch kind {
	case tenant.IdentityScopeTopic:
		return tenant.IdentityScopeTopic + ":" + chat + ":" + thread
	case tenant.IdentityScopeGroup:
		return tenant.IdentityScopeGroup + ":" + chat
	default:
		if chat != "" {
			return tenant.IdentityScopePrivate + ":" + chat
		}
		return tenant.IdentityScopePrivate + ":" + externalUser
	}
}

func nullableString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
func identityPrimaryKeyConflict(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "user_identity_pkey"
}

func registryError(err error) error {
	if errors.Is(err, ErrTenantContextRequired) || errors.Is(err, storage.ErrConflict) ||
		errors.Is(err, storage.ErrInvalidArgument) || errors.Is(err, storage.ErrTenantMismatch) ||
		errors.Is(err, storage.ErrNotFound) || errors.Is(err, tenantctx.ErrRuntimeRolePrivileged) {
		return err
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.ErrNotFound
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && (pgErr.Code == "23505" || pgErr.Code == "23514" || pgErr.Code == "23503") {
		if pgErr.Code == "23505" {
			return storage.ErrConflict
		}
		return storage.ErrInvalidArgument
	}
	return storage.ErrBackendUnavailable
}

func metadataError(err error) error {
	if err == nil {
		return nil
	}
	return registryError(err)
}

var _ tenant.Registry = (*TenantRegistry)(nil)
var _ storage.BindingMetadataRepository = (*TenantRegistry)(nil)
var _ storage.IdentityMetadataRepository = (*TenantRegistry)(nil)
var _ storage.BindingAuditRepository = (*TenantRegistry)(nil)
