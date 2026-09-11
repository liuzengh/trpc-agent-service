package tenant

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/dbscope"
)

// PostgresRepository is the durable control-plane Repository backed by the
// control-plane tables (tenants, applications, application_configs,
// channel_bindings). Published configuration versions are immutable; queries
// always return independent copies reconstructed from the stored JSON.
type PostgresRepository struct {
	database *sql.DB
}

// NewPostgresRepository constructs a durable repository. The caller owns the
// database pool lifecycle.
func NewPostgresRepository(database *sql.DB) (*PostgresRepository, error) {
	if database == nil {
		return nil, fmt.Errorf("PostgreSQL database is required")
	}
	return &PostgresRepository{database: database}, nil
}

// UsesConfigCacheInvalidationOutbox reports that Publish writes configuration
// cache invalidation events in the same transaction as the active version.
func (r *PostgresRepository) UsesConfigCacheInvalidationOutbox() bool { return true }

func (r *PostgresRepository) beginTenantTransaction(ctx context.Context, tenantID string) (*sql.Tx, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("tenant ID is required for control-plane transaction")
	}
	return dbscope.BeginTenantTransaction(ctx, r.database, tenantID)
}

func syncChannelBindings(ctx context.Context, tx *sql.Tx, tenantConfig config.TenantConfig) error {
	rows, err := tx.QueryContext(ctx, `
SELECT channel_type, external_binding_id
FROM channel_bindings
WHERE tenant_id=$1 AND app_code=$2
FOR UPDATE`, tenantConfig.TenantID, tenantConfig.AppCode)
	if err != nil {
		return fmt.Errorf("list existing channel bindings for %q: %w", tenantConfig.AppName(), err)
	}
	existing := make(map[string][2]string)
	for rows.Next() {
		var channelType, bindingID string
		if err := rows.Scan(&channelType, &bindingID); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan existing channel binding for %q: %w", tenantConfig.AppName(), err)
		}
		existing[channelType+"\x00"+bindingID] = [2]string{channelType, bindingID}
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close existing channel bindings for %q: %w", tenantConfig.AppName(), err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("list existing channel bindings for %q: %w", tenantConfig.AppName(), err)
	}

	desired := make(map[string]struct{}, len(tenantConfig.Channels))
	for _, binding := range tenantConfig.Channels {
		key := binding.Type + "\x00" + binding.BindingID
		desired[key] = struct{}{}
		allowlistJSON, err := marshalChannelAllowlist(binding.Allowlist)
		if err != nil {
			return fmt.Errorf("encode channel allowlist %q: %w", binding.BindingID, err)
		}
		result, err := tx.ExecContext(ctx, `
UPDATE channel_bindings
SET trusted_enterprise_id=$5, access_policy=$6, allowlist=$7::jsonb, updated_at=NOW()
WHERE channel_type=$1 AND external_binding_id=$2 AND tenant_id=$3 AND app_code=$4`,
			binding.Type, binding.BindingID, tenantConfig.TenantID, tenantConfig.AppCode,
			binding.TrustedEnterpriseID, binding.EffectiveAccessPolicy(), string(allowlistJSON))
		if err != nil {
			return fmt.Errorf("update channel binding %q for %q: %w", binding.BindingID, tenantConfig.AppName(), err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("inspect channel binding update %q for %q: %w", binding.BindingID, tenantConfig.AppName(), err)
		}
		if affected > 0 {
			continue
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO channel_bindings (channel_type, external_binding_id, tenant_id, app_code, trusted_enterprise_id, access_policy, allowlist)
VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)`,
			binding.Type, binding.BindingID, tenantConfig.TenantID, tenantConfig.AppCode,
			binding.TrustedEnterpriseID, binding.EffectiveAccessPolicy(), string(allowlistJSON)); err != nil {
			var postgresError *pgconn.PgError
			if errors.As(err, &postgresError) && postgresError.Code == "23505" {
				return fmt.Errorf("%w: binding=%q", ErrBindingConflict, binding.Type+"/"+binding.BindingID)
			}
			return fmt.Errorf("insert channel binding %q for %q: %w", binding.BindingID, tenantConfig.AppName(), err)
		}
	}

	for key, binding := range existing {
		if _, keep := desired[key]; keep {
			continue
		}
		if _, err := tx.ExecContext(ctx, `
DELETE FROM channel_identities
WHERE tenant_id=$1 AND channel_type=$2 AND binding_id=$3`, tenantConfig.TenantID, binding[0], binding[1]); err != nil {
			return fmt.Errorf("remove channel identities for stale binding %q in %q: %w", binding[1], tenantConfig.AppName(), err)
		}
		if _, err := tx.ExecContext(ctx, `
DELETE FROM channel_bindings
WHERE channel_type=$1 AND external_binding_id=$2 AND tenant_id=$3 AND app_code=$4`,
			binding[0], binding[1], tenantConfig.TenantID, tenantConfig.AppCode); err != nil {
			return fmt.Errorf("remove stale channel binding %q for %q: %w", binding[1], tenantConfig.AppName(), err)
		}
	}
	return nil
}

// Publish atomically activates a new immutable configuration version for one
// tenant application. The same validation, checksum, and binding-conflict
// rules as MemoryRepository apply, but every effect is committed to PostgreSQL
// so configuration survives restarts and is shared across nodes.
func (r *PostgresRepository) Publish(ctx context.Context, tenantConfig config.TenantConfig) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if err := validateTenantConfig(tenantConfig); err != nil {
		return Snapshot{}, err
	}
	snapshot, err := newSnapshot(tenantConfig, time.Now().UTC())
	if err != nil {
		return Snapshot{}, err
	}

	tx, err := r.beginTenantTransaction(ctx, tenantConfig.TenantID)
	if err != nil {
		return Snapshot{}, fmt.Errorf("begin control-plane publish: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	// Serialize publishers for the same application across service replicas.
	// Without this lock, two cold-starting replicas can both observe no active
	// version and race on the deterministic cache-invalidation outbox ID.
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", tenantConfig.AppName()); err != nil {
		return Snapshot{}, fmt.Errorf("lock application publish %q: %w", tenantConfig.AppName(), err)
	}
	var rolloutExists bool
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(SELECT 1 FROM application_rollouts WHERE tenant_id=$1 AND app_code=$2)`,
		tenantConfig.TenantID, tenantConfig.AppCode).Scan(&rolloutExists); err != nil {
		return Snapshot{}, fmt.Errorf("read rollout before direct publish: %w", err)
	}
	if rolloutExists {
		return Snapshot{}, ErrRolloutInProgress
	}
	var latestVersion uint64
	if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(MAX(version), 0)
FROM application_configs
WHERE tenant_id = $1 AND app_code = $2`, tenantConfig.TenantID, tenantConfig.AppCode).Scan(&latestVersion); err != nil {
		return Snapshot{}, fmt.Errorf("read latest configuration version: %w", err)
	}
	if latestVersion > 0 && tenantConfig.ConfigVersion <= latestVersion {
		return Snapshot{}, fmt.Errorf("%w: latest=%d requested=%d", ErrVersionConflict, latestVersion, tenantConfig.ConfigVersion)
	}

	var activeVersion uint64
	var activeConfigJSON []byte
	err = tx.QueryRowContext(ctx, `
SELECT a.active_config_version, ac.config_json
FROM applications a
JOIN application_configs ac
  ON ac.tenant_id = a.tenant_id AND ac.app_code = a.app_code AND ac.version = a.active_config_version
WHERE a.tenant_id = $1 AND a.app_code = $2`,
		tenantConfig.TenantID, tenantConfig.AppCode).Scan(&activeVersion, &activeConfigJSON)
	var previousConfig config.TenantConfig
	switch {
	case err == nil:
		if tenantConfig.ConfigVersion <= activeVersion {
			return Snapshot{}, fmt.Errorf("%w: current=%d requested=%d", ErrVersionConflict, activeVersion, tenantConfig.ConfigVersion)
		}
		if err := json.Unmarshal(activeConfigJSON, &previousConfig); err != nil {
			return Snapshot{}, fmt.Errorf("decode active configuration before publish: %w", err)
		}
	case errors.Is(err, sql.ErrNoRows):
		activeVersion = 0
	default:
		return Snapshot{}, fmt.Errorf("read active configuration version: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO tenants (id, display_name) VALUES ($1, $1)
ON CONFLICT (id) DO NOTHING`, tenantConfig.TenantID); err != nil {
		return Snapshot{}, fmt.Errorf("upsert tenant %q: %w", tenantConfig.TenantID, err)
	}
	if _, err := tx.ExecContext(ctx, `
	INSERT INTO applications (tenant_id, app_code, status, active_config_version, candidate_config_version)
	VALUES ($1, $2, $3, $4, NULL)
	ON CONFLICT (tenant_id, app_code) DO UPDATE
	SET status = EXCLUDED.status, active_config_version = EXCLUDED.active_config_version,
	    candidate_config_version = NULL, updated_at = NOW()`,
		tenantConfig.TenantID, tenantConfig.AppCode, tenantConfig.Status, tenantConfig.ConfigVersion); err != nil {
		return Snapshot{}, fmt.Errorf("upsert application %q: %w", tenantConfig.AppName(), err)
	}
	configJSON, err := marshalConfig(snapshot.Config)
	if err != nil {
		return Snapshot{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO application_configs (tenant_id, app_code, version, config_json, checksum, published_at)
VALUES ($1, $2, $3, $4::jsonb, $5, $6)
ON CONFLICT (tenant_id, app_code, version) DO NOTHING`,
		tenantConfig.TenantID, tenantConfig.AppCode, tenantConfig.ConfigVersion, string(configJSON), snapshot.Checksum, snapshot.PublishedAt); err != nil {
		return Snapshot{}, fmt.Errorf("insert application config %q: %w", tenantConfig.AppName(), err)
	}
	if err := syncChannelBindings(ctx, tx, tenantConfig); err != nil {
		return Snapshot{}, err
	}
	keys := []string{activeCacheKey(tenantConfig.TenantID, tenantConfig.AppCode)}
	keys = append(keys, bindingCacheKeys(tenantConfig)...)
	if activeVersion > 0 {
		keys = append(keys, bindingCacheKeys(previousConfig)...)
	}
	payload, err := json.Marshal(ConfigCacheInvalidation{Keys: uniqueKeys(keys)})
	if err != nil {
		return Snapshot{}, fmt.Errorf("marshal configuration cache invalidation: %w", err)
	}
	eventID := fmt.Sprintf("config-cache-invalidate:%s:%d", tenantConfig.AppName(), tenantConfig.ConfigVersion)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO outbox_events (id, request_id, tenant_id, aggregate_key, event_type, payload, created_at)
VALUES ($1, $2, $3, $4, $5, $6::jsonb, NOW())`,
		eventID, eventID, tenantConfig.TenantID, tenantConfig.AppName(), ConfigCacheInvalidationEventType, string(payload)); err != nil {
		return Snapshot{}, fmt.Errorf("enqueue configuration cache invalidation: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Snapshot{}, fmt.Errorf("commit control-plane publish: %w", err)
	}
	return snapshot, nil
}

// GetActive returns the active immutable snapshot for one tenant application.
func (r *PostgresRepository) GetActive(ctx context.Context, tenantID, appCode string) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	tx, err := r.beginTenantTransaction(ctx, tenantID)
	if err != nil {
		return Snapshot{}, fmt.Errorf("begin tenant configuration read: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	row := tx.QueryRowContext(ctx, `
SELECT ac.version, ac.config_json, ac.checksum, ac.published_at
FROM applications a
JOIN application_configs ac
  ON ac.tenant_id = a.tenant_id AND ac.app_code = a.app_code AND ac.version = a.active_config_version
WHERE a.tenant_id = $1 AND a.app_code = $2`,
		tenantID, appCode)
	snapshot, err := scanSnapshot(row)
	if err != nil {
		return Snapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return Snapshot{}, fmt.Errorf("commit tenant configuration read: %w", err)
	}
	return snapshot, nil
}

func (r *PostgresRepository) GetCandidate(ctx context.Context, tenantID, appCode string) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	tx, err := r.beginTenantTransaction(ctx, tenantID)
	if err != nil {
		return Snapshot{}, fmt.Errorf("begin candidate configuration read: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	snapshot, err := scanSnapshot(tx.QueryRowContext(ctx, `
SELECT ac.version, ac.config_json, ac.checksum, ac.published_at
FROM applications a
JOIN application_configs ac
  ON ac.tenant_id=a.tenant_id AND ac.app_code=a.app_code AND ac.version=a.candidate_config_version
WHERE a.tenant_id=$1 AND a.app_code=$2`, tenantID, appCode))
	if errors.Is(err, ErrNotFound) {
		return Snapshot{}, ErrCandidateNotFound
	}
	if err != nil {
		return Snapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return Snapshot{}, fmt.Errorf("commit candidate configuration read: %w", err)
	}
	return snapshot, nil
}

// GetVersion returns one specific immutable configuration version.
func (r *PostgresRepository) GetVersion(ctx context.Context, tenantID, appCode string, version uint64) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if version == 0 {
		return Snapshot{}, fmt.Errorf("configuration version must be positive")
	}
	tx, err := r.beginTenantTransaction(ctx, tenantID)
	if err != nil {
		return Snapshot{}, fmt.Errorf("begin tenant configuration version read: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	row := tx.QueryRowContext(ctx, `
SELECT version, config_json, checksum, published_at
FROM application_configs
WHERE tenant_id = $1 AND app_code = $2 AND version = $3`,
		tenantID, appCode, version)
	snapshot, err := scanSnapshot(row)
	if err != nil {
		return Snapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return Snapshot{}, fmt.Errorf("commit tenant configuration version read: %w", err)
	}
	return snapshot, nil
}

func (r *PostgresRepository) ListVersions(ctx context.Context, tenantID, appCode string, limit int) ([]Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	tx, err := r.beginTenantTransaction(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("begin application version list: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `
SELECT version, config_json, checksum, published_at
FROM application_configs
WHERE tenant_id = $1 AND app_code = $2
ORDER BY version DESC
LIMIT $3`, tenantID, appCode, limit)
	if err != nil {
		return nil, fmt.Errorf("list application versions: %w", err)
	}
	defer rows.Close()
	versions := make([]Snapshot, 0, limit)
	for rows.Next() {
		var version uint64
		var configJSON, checksum []byte
		var publishedAt time.Time
		if err := rows.Scan(&version, &configJSON, &checksum, &publishedAt); err != nil {
			return nil, fmt.Errorf("scan application version: %w", err)
		}
		snapshot, err := decodeSnapshot(version, configJSON, checksum, publishedAt)
		if err != nil {
			return nil, err
		}
		versions = append(versions, snapshot)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate application versions: %w", err)
	}
	if len(versions) == 0 {
		return nil, ErrNotFound
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit application version list: %w", err)
	}
	return versions, nil
}

func (r *PostgresRepository) Stage(ctx context.Context, tenantConfig config.TenantConfig) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if err := validateTenantConfig(tenantConfig); err != nil {
		return Snapshot{}, err
	}
	snapshot, err := newSnapshot(tenantConfig, time.Now().UTC())
	if err != nil {
		return Snapshot{}, err
	}
	tx, err := r.beginTenantTransaction(ctx, tenantConfig.TenantID)
	if err != nil {
		return Snapshot{}, fmt.Errorf("begin candidate configuration stage: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", tenantConfig.AppName()); err != nil {
		return Snapshot{}, fmt.Errorf("lock candidate configuration stage: %w", err)
	}
	var rolloutExists bool
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(SELECT 1 FROM application_rollouts WHERE tenant_id=$1 AND app_code=$2)`,
		tenantConfig.TenantID, tenantConfig.AppCode).Scan(&rolloutExists); err != nil {
		return Snapshot{}, fmt.Errorf("read rollout before candidate stage: %w", err)
	}
	if rolloutExists {
		return Snapshot{}, ErrRolloutInProgress
	}
	var activeVersion, latestVersion uint64
	if err := tx.QueryRowContext(ctx, `
SELECT active_config_version FROM applications WHERE tenant_id = $1 AND app_code = $2`,
		tenantConfig.TenantID, tenantConfig.AppCode).Scan(&activeVersion); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Snapshot{}, ErrNotFound
		}
		return Snapshot{}, fmt.Errorf("read application before candidate stage: %w", err)
	}
	if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(MAX(version), 0) FROM application_configs WHERE tenant_id = $1 AND app_code = $2`,
		tenantConfig.TenantID, tenantConfig.AppCode).Scan(&latestVersion); err != nil {
		return Snapshot{}, fmt.Errorf("read latest candidate version: %w", err)
	}
	if activeVersion == 0 || tenantConfig.ConfigVersion != latestVersion+1 {
		return Snapshot{}, fmt.Errorf("%w: latest=%d requested=%d", ErrVersionConflict, latestVersion, tenantConfig.ConfigVersion)
	}
	configJSON, err := marshalConfig(snapshot.Config)
	if err != nil {
		return Snapshot{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO application_configs (tenant_id, app_code, version, config_json, checksum, published_at)
VALUES ($1, $2, $3, $4::jsonb, $5, $6)`, tenantConfig.TenantID, tenantConfig.AppCode,
		tenantConfig.ConfigVersion, string(configJSON), snapshot.Checksum, snapshot.PublishedAt); err != nil {
		return Snapshot{}, fmt.Errorf("insert candidate application config: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE applications SET candidate_config_version=$3, updated_at=NOW()
WHERE tenant_id=$1 AND app_code=$2`, tenantConfig.TenantID, tenantConfig.AppCode, tenantConfig.ConfigVersion); err != nil {
		return Snapshot{}, fmt.Errorf("set application candidate version: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Snapshot{}, fmt.Errorf("commit candidate configuration stage: %w", err)
	}
	return snapshot, nil
}

func (r *PostgresRepository) GetRollout(ctx context.Context, tenantID, appCode string) (RolloutPolicy, error) {
	if err := ctx.Err(); err != nil {
		return RolloutPolicy{}, err
	}
	tx, err := r.beginTenantTransaction(ctx, tenantID)
	if err != nil {
		return RolloutPolicy{}, fmt.Errorf("begin rollout read: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	rollout, err := scanRollout(tx.QueryRowContext(ctx, `
SELECT tenant_id, app_code, generation, stable_version, candidate_version,
       basis_points, test_user_ids, ingresses, updated_at
FROM application_rollouts
WHERE tenant_id = $1 AND app_code = $2`, tenantID, appCode))
	if err != nil {
		return RolloutPolicy{}, err
	}
	if err := tx.Commit(); err != nil {
		return RolloutPolicy{}, fmt.Errorf("commit rollout read: %w", err)
	}
	return rollout, nil
}

func (r *PostgresRepository) SetRollout(ctx context.Context, tenantID, appCode string, update RolloutUpdate) (RolloutPolicy, error) {
	if err := ctx.Err(); err != nil {
		return RolloutPolicy{}, err
	}
	update, err := normalizeRolloutUpdate(update)
	if err != nil {
		return RolloutPolicy{}, err
	}
	tx, err := r.beginTenantTransaction(ctx, tenantID)
	if err != nil {
		return RolloutPolicy{}, fmt.Errorf("begin rollout update: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	applicationName := tenantID + "/" + appCode
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", applicationName); err != nil {
		return RolloutPolicy{}, fmt.Errorf("lock rollout update: %w", err)
	}
	var stableVersion, candidateVersion, rolloutGeneration uint64
	var candidateVersionNullable sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
SELECT active_config_version, candidate_config_version, rollout_generation
FROM applications
WHERE tenant_id=$1 AND app_code=$2`, tenantID, appCode).Scan(&stableVersion, &candidateVersionNullable, &rolloutGeneration); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return RolloutPolicy{}, ErrNotFound
		}
		return RolloutPolicy{}, fmt.Errorf("read stable and candidate versions: %w", err)
	}
	if !candidateVersionNullable.Valid || candidateVersionNullable.Int64 <= 0 {
		return RolloutPolicy{}, ErrCandidateNotFound
	}
	candidateVersion = uint64(candidateVersionNullable.Int64)
	stable, err := scanSnapshot(tx.QueryRowContext(ctx, `
SELECT version, config_json, checksum, published_at
FROM application_configs
WHERE tenant_id=$1 AND app_code=$2 AND version=$3`, tenantID, appCode, stableVersion))
	if err != nil {
		return RolloutPolicy{}, err
	}
	candidate, err := scanSnapshot(tx.QueryRowContext(ctx, `
SELECT version, config_json, checksum, published_at
FROM application_configs
WHERE tenant_id = $1 AND app_code = $2 AND version = $3`, tenantID, appCode, candidateVersion))
	if err != nil {
		return RolloutPolicy{}, err
	}
	if candidate.Config.Status != config.AgentActive {
		return RolloutPolicy{}, errors.New("rollout candidate must be active")
	}
	if stable.Config.ConfigVersion == candidate.Config.ConfigVersion {
		return RolloutPolicy{}, errors.New("rollout candidate must differ from the stable version")
	}
	if !sameChannelTopology(stable.Config, candidate.Config) {
		return RolloutPolicy{}, errors.New("gray release cannot change channel bindings or credentials")
	}
	if err := validateRolloutIngresses(stable.Config, update.Ingresses); err != nil {
		return RolloutPolicy{}, err
	}
	var currentGeneration uint64
	err = tx.QueryRowContext(ctx, `
SELECT generation FROM application_rollouts WHERE tenant_id = $1 AND app_code = $2 FOR UPDATE`, tenantID, appCode).Scan(&currentGeneration)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if update.ExpectedGeneration != 0 {
			return RolloutPolicy{}, ErrRolloutConflict
		}
		currentGeneration = 0
	case err != nil:
		return RolloutPolicy{}, fmt.Errorf("read rollout generation: %w", err)
	case currentGeneration != update.ExpectedGeneration:
		return RolloutPolicy{}, ErrRolloutConflict
	}
	usersJSON, err := json.Marshal(update.TestUserIDs)
	if err != nil {
		return RolloutPolicy{}, fmt.Errorf("encode rollout test users: %w", err)
	}
	ingressesJSON, err := json.Marshal(update.Ingresses)
	if err != nil {
		return RolloutPolicy{}, fmt.Errorf("encode rollout bindings: %w", err)
	}
	nextGeneration := rolloutGeneration + 1
	if nextGeneration <= currentGeneration {
		nextGeneration = currentGeneration + 1
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE applications SET rollout_generation=$3, updated_at=NOW()
WHERE tenant_id=$1 AND app_code=$2`, tenantID, appCode, nextGeneration); err != nil {
		return RolloutPolicy{}, fmt.Errorf("advance rollout generation: %w", err)
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO application_rollouts (
    tenant_id, app_code, generation, stable_version, candidate_version,
    basis_points, test_user_ids, ingresses, updated_at
) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9)
ON CONFLICT (tenant_id, app_code) DO UPDATE SET
    generation = EXCLUDED.generation,
    stable_version = EXCLUDED.stable_version,
    candidate_version = EXCLUDED.candidate_version,
    basis_points = EXCLUDED.basis_points,
    test_user_ids = EXCLUDED.test_user_ids,
    ingresses = EXCLUDED.ingresses,
    updated_at = EXCLUDED.updated_at`, tenantID, appCode, nextGeneration,
		stable.Config.ConfigVersion, candidate.Config.ConfigVersion, update.BasisPoints,
		string(usersJSON), string(ingressesJSON), now); err != nil {
		return RolloutPolicy{}, fmt.Errorf("persist application rollout: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return RolloutPolicy{}, fmt.Errorf("commit rollout update: %w", err)
	}
	return RolloutPolicy{
		TenantID: tenantID, AppCode: appCode, Generation: nextGeneration,
		StableVersion: stable.Config.ConfigVersion, CandidateVersion: candidate.Config.ConfigVersion,
		BasisPoints: update.BasisPoints, TestUserIDs: update.TestUserIDs, Ingresses: update.Ingresses, UpdatedAt: now,
	}, nil
}

func (r *PostgresRepository) StopRollout(ctx context.Context, tenantID, appCode string, expectedGeneration uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tx, err := r.beginTenantTransaction(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("begin rollout stop: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", tenantID+"/"+appCode); err != nil {
		return fmt.Errorf("lock rollout stop: %w", err)
	}
	result, err := tx.ExecContext(ctx, `
DELETE FROM application_rollouts
WHERE tenant_id = $1 AND app_code = $2 AND generation = $3`, tenantID, appCode, expectedGeneration)
	if err != nil {
		return fmt.Errorf("delete application rollout: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect rollout stop: %w", err)
	}
	if rows != 1 {
		var exists bool
		if scanErr := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM application_rollouts WHERE tenant_id=$1 AND app_code=$2)`, tenantID, appCode).Scan(&exists); scanErr != nil {
			return fmt.Errorf("inspect rollout existence: %w", scanErr)
		}
		if exists {
			return ErrRolloutConflict
		}
		return ErrRolloutNotFound
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit rollout stop: %w", err)
	}
	return nil
}

func (r *PostgresRepository) DiscardCandidate(ctx context.Context, tenantID, appCode string, expectedVersion uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tx, err := r.beginTenantTransaction(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("begin candidate discard: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	applicationName := tenantID + "/" + appCode
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", applicationName); err != nil {
		return fmt.Errorf("lock candidate discard: %w", err)
	}
	var candidate sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
SELECT candidate_config_version FROM applications
WHERE tenant_id=$1 AND app_code=$2 FOR UPDATE`, tenantID, appCode).Scan(&candidate); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("read candidate before discard: %w", err)
	}
	if !candidate.Valid || candidate.Int64 <= 0 {
		return ErrCandidateNotFound
	}
	if expectedVersion == 0 || uint64(candidate.Int64) != expectedVersion {
		return ErrVersionConflict
	}
	var rolloutExists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
SELECT 1 FROM application_rollouts WHERE tenant_id=$1 AND app_code=$2)`, tenantID, appCode).Scan(&rolloutExists); err != nil {
		return fmt.Errorf("read rollout before candidate discard: %w", err)
	}
	if rolloutExists {
		return ErrRolloutInProgress
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE applications SET candidate_config_version=NULL, updated_at=NOW()
WHERE tenant_id=$1 AND app_code=$2`, tenantID, appCode); err != nil {
		return fmt.Errorf("discard application candidate: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit candidate discard: %w", err)
	}
	return nil
}

func (r *PostgresRepository) PromoteCandidate(ctx context.Context, tenantID, appCode string, expectedVersion, expectedGeneration uint64) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	tx, err := r.beginTenantTransaction(ctx, tenantID)
	if err != nil {
		return Snapshot{}, fmt.Errorf("begin candidate promotion: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	applicationName := tenantID + "/" + appCode
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", applicationName); err != nil {
		return Snapshot{}, fmt.Errorf("lock candidate promotion: %w", err)
	}
	var activeVersion uint64
	var candidateVersion sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
SELECT active_config_version, candidate_config_version FROM applications
WHERE tenant_id=$1 AND app_code=$2 FOR UPDATE`, tenantID, appCode).Scan(&activeVersion, &candidateVersion); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Snapshot{}, ErrNotFound
		}
		return Snapshot{}, fmt.Errorf("read application before candidate promotion: %w", err)
	}
	if !candidateVersion.Valid || candidateVersion.Int64 <= 0 {
		return Snapshot{}, ErrCandidateNotFound
	}
	candidateNumber := uint64(candidateVersion.Int64)
	if expectedVersion == 0 || expectedVersion != candidateNumber {
		return Snapshot{}, ErrVersionConflict
	}
	rollout, rolloutErr := scanRollout(tx.QueryRowContext(ctx, `
SELECT tenant_id, app_code, generation, stable_version, candidate_version,
       basis_points, test_user_ids, ingresses, updated_at
FROM application_rollouts
WHERE tenant_id=$1 AND app_code=$2 FOR UPDATE`, tenantID, appCode))
	switch {
	case rolloutErr == nil:
		if rollout.Generation != expectedGeneration || rollout.StableVersion != activeVersion || rollout.CandidateVersion != candidateNumber {
			return Snapshot{}, ErrRolloutConflict
		}
	case errors.Is(rolloutErr, ErrRolloutNotFound):
		if expectedGeneration != 0 {
			return Snapshot{}, ErrRolloutConflict
		}
	default:
		return Snapshot{}, rolloutErr
	}
	stable, err := scanSnapshot(tx.QueryRowContext(ctx, `
SELECT version, config_json, checksum, published_at
FROM application_configs
WHERE tenant_id=$1 AND app_code=$2 AND version=$3`, tenantID, appCode, activeVersion))
	if err != nil {
		return Snapshot{}, err
	}
	candidate, err := scanSnapshot(tx.QueryRowContext(ctx, `
SELECT version, config_json, checksum, published_at
FROM application_configs
WHERE tenant_id=$1 AND app_code=$2 AND version=$3`, tenantID, appCode, candidateNumber))
	if err != nil {
		return Snapshot{}, err
	}
	if candidate.Config.Status != config.AgentActive {
		return Snapshot{}, errors.New("candidate version is not active")
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE applications SET status=$3, active_config_version=$4, candidate_config_version=NULL, updated_at=NOW()
WHERE tenant_id=$1 AND app_code=$2`, tenantID, appCode, candidate.Config.Status, candidate.Config.ConfigVersion); err != nil {
		return Snapshot{}, fmt.Errorf("promote candidate version: %w", err)
	}
	if err := syncChannelBindings(ctx, tx, candidate.Config); err != nil {
		return Snapshot{}, fmt.Errorf("sync channel bindings during promotion: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM application_rollouts WHERE tenant_id=$1 AND app_code=$2`, tenantID, appCode); err != nil {
		return Snapshot{}, fmt.Errorf("clear promoted rollout: %w", err)
	}
	keys := []string{activeCacheKey(tenantID, appCode)}
	keys = append(keys, bindingCacheKeys(stable.Config)...)
	keys = append(keys, bindingCacheKeys(candidate.Config)...)
	payload, err := json.Marshal(ConfigCacheInvalidation{Keys: uniqueKeys(keys)})
	if err != nil {
		return Snapshot{}, fmt.Errorf("marshal promotion cache invalidation: %w", err)
	}
	eventID := fmt.Sprintf("config-cache-invalidate:%s:promote:%d", applicationName, candidateNumber)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO outbox_events (id, request_id, tenant_id, aggregate_key, event_type, payload, created_at)
VALUES ($1,$1,$2,$3,$4,$5::jsonb,NOW())`, eventID, tenantID, applicationName, ConfigCacheInvalidationEventType, string(payload)); err != nil {
		return Snapshot{}, fmt.Errorf("enqueue promotion cache invalidation: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Snapshot{}, fmt.Errorf("commit candidate promotion: %w", err)
	}
	return candidate, nil
}

func marshalChannelAllowlist(allowlist []string) ([]byte, error) {
	if allowlist == nil {
		allowlist = []string{}
	}
	return json.Marshal(allowlist)
}

func scanRollout(row rowScanner) (RolloutPolicy, error) {
	var rollout RolloutPolicy
	var usersJSON, ingressesJSON []byte
	if err := row.Scan(&rollout.TenantID, &rollout.AppCode, &rollout.Generation,
		&rollout.StableVersion, &rollout.CandidateVersion, &rollout.BasisPoints,
		&usersJSON, &ingressesJSON, &rollout.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return RolloutPolicy{}, ErrRolloutNotFound
		}
		return RolloutPolicy{}, fmt.Errorf("scan application rollout: %w", err)
	}
	if err := json.Unmarshal(usersJSON, &rollout.TestUserIDs); err != nil {
		return RolloutPolicy{}, fmt.Errorf("decode rollout test users: %w", err)
	}
	if err := json.Unmarshal(ingressesJSON, &rollout.Ingresses); err != nil {
		return RolloutPolicy{}, fmt.Errorf("decode rollout ingresses: %w", err)
	}
	return rollout, nil
}

// ResolveBinding returns the active configuration that owns one external
// channel binding. Suspended applications do not resolve.
func (r *PostgresRepository) ResolveBinding(ctx context.Context, channel channels.Channel, bindingID string) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if !channel.Supported() {
		return Snapshot{}, fmt.Errorf("unsupported channel %q", channel)
	}
	if strings.TrimSpace(bindingID) == "" {
		return Snapshot{}, fmt.Errorf("external binding ID is required")
	}
	row := r.database.QueryRowContext(ctx, `
SELECT ac.version, ac.config_json, ac.checksum, ac.published_at
FROM channel_bindings cb
JOIN applications a
  ON a.tenant_id = cb.tenant_id AND a.app_code = cb.app_code
JOIN application_configs ac
  ON ac.tenant_id = a.tenant_id AND ac.app_code = a.app_code AND ac.version = a.active_config_version
WHERE cb.channel_type = $1 AND cb.external_binding_id = $2
  AND a.status = 'active'`,
		string(channel), bindingID)
	return scanSnapshot(row)
}

// ListApplications returns every active application snapshot, optionally
// scoped to one tenant, ordered by tenant and app code.
func (r *PostgresRepository) ListApplications(ctx context.Context, tenantID string) ([]Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	query := `
SELECT ac.version, ac.config_json, ac.checksum, ac.published_at, a.tenant_id, a.app_code
FROM applications a
JOIN application_configs ac
  ON ac.tenant_id = a.tenant_id AND ac.app_code = a.app_code AND ac.version = a.active_config_version
WHERE ($1 = '' OR a.tenant_id = $1)
ORDER BY a.tenant_id, a.app_code`
	rows, err := r.database.QueryContext(ctx, query, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list application configurations: %w", err)
	}
	defer rows.Close()

	applications := make([]Snapshot, 0)
	for rows.Next() {
		var version uint64
		var configJSON, checksum []byte
		var publishedAt time.Time
		var rowTenantID, appCode string
		if err := rows.Scan(&version, &configJSON, &checksum, &publishedAt, &rowTenantID, &appCode); err != nil {
			return nil, fmt.Errorf("scan application configuration: %w", err)
		}
		snapshot, err := decodeSnapshot(version, configJSON, checksum, publishedAt)
		if err != nil {
			return nil, fmt.Errorf("decode application %s/%s: %w", rowTenantID, appCode, err)
		}
		applications = append(applications, snapshot)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate application configurations: %w", err)
	}
	return applications, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanSnapshot(row rowScanner) (Snapshot, error) {
	var version uint64
	var configJSON, checksum []byte
	var publishedAt time.Time
	if err := row.Scan(&version, &configJSON, &checksum, &publishedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Snapshot{}, ErrNotFound
		}
		return Snapshot{}, fmt.Errorf("scan tenant configuration: %w", err)
	}
	return decodeSnapshot(version, configJSON, checksum, publishedAt)
}

func decodeSnapshot(version uint64, configJSON, checksum []byte, publishedAt time.Time) (Snapshot, error) {
	var tenantConfig config.TenantConfig
	if err := json.Unmarshal(configJSON, &tenantConfig); err != nil {
		return Snapshot{}, fmt.Errorf("decode tenant configuration JSON: %w", err)
	}
	if tenantConfig.ConfigVersion != version {
		return Snapshot{}, fmt.Errorf("configuration version mismatch: row=%d config=%d", version, tenantConfig.ConfigVersion)
	}
	recomputed, err := newSnapshot(tenantConfig, publishedAt)
	if err != nil {
		return Snapshot{}, err
	}
	if recomputed.Checksum != string(checksum) {
		return Snapshot{}, fmt.Errorf("configuration checksum mismatch")
	}
	return recomputed, nil
}

func marshalConfig(tenantConfig config.TenantConfig) ([]byte, error) {
	encoded, err := json.Marshal(tenantConfig)
	if err != nil {
		return nil, fmt.Errorf("marshal tenant configuration: %w", err)
	}
	return encoded, nil
}

var _ Repository = (*PostgresRepository)(nil)
