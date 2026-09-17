// Package postgres persists Catalog-validated immutable Provider Profiles.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/liuzengh/trpc-agent-service/trpcservice/provider"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

type Repository struct {
	db      *sql.DB
	catalog *provider.Catalog
}

func New(db *sql.DB, catalog *provider.Catalog) *Repository {
	return &Repository{db: db, catalog: catalog}
}

func (r *Repository) PublishModel(ctx context.Context, input provider.ModelProfileSnapshot) (provider.ModelProfileSnapshot, error) {
	value, err := r.catalog.NormalizeModel(input)
	if err != nil {
		return provider.ModelProfileSnapshot{}, err
	}
	options, err := json.Marshal(value.Options)
	if err != nil {
		return provider.ModelProfileSnapshot{}, runtime.ErrInvariantViolation
	}
	generation, err := json.Marshal(value.Generation)
	if err != nil {
		return provider.ModelProfileSnapshot{}, runtime.ErrInvariantViolation
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return provider.ModelProfileSnapshot{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.ExecContext(ctx, `INSERT INTO model_profile(tenant_id,model_profile_id,profile_key,display_name,status)
VALUES($1,$2,$3,$4,$5) ON CONFLICT (tenant_id,model_profile_id) DO NOTHING`, value.TenantID, value.ProfileID, value.ProfileKey, value.DisplayName, value.Status); err != nil {
		return provider.ModelProfileSnapshot{}, translate(err)
	}
	if err = lockIdentity(ctx, tx, "model_profile", value.TenantID, value.ProfileID, value.ProfileKey, value.Status); err != nil {
		return provider.ModelProfileSnapshot{}, err
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO model_profile_revision(tenant_id,model_profile_id,profile_version,schema_version,provider,model_name,endpoint,options,secret_ref,secret_version,generation,content_digest)
VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10,$11::jsonb,$12) ON CONFLICT (tenant_id,model_profile_id,profile_version) DO NOTHING`, value.TenantID, value.ProfileID, value.Version, value.SchemaVersion, value.Provider, value.Model, value.Endpoint, options, nullable(value.SecretRef.Ref), nullableVersion(value.SecretRef.Version), generation, value.ContentDigest)
	if err != nil {
		return provider.ModelProfileSnapshot{}, translate(err)
	}
	if err = verifyImmutableInsert(ctx, tx, result, "model_profile_revision", "model_profile_id", value.TenantID, value.ProfileID, value.Version, value.ContentDigest); err != nil {
		return provider.ModelProfileSnapshot{}, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE model_profile SET current_version=$3,row_version=row_version+1,updated_at=clock_timestamp() WHERE tenant_id=$1 AND model_profile_id=$2 AND (current_version IS NULL OR current_version<$3)`, value.TenantID, value.ProfileID, value.Version); err != nil {
		return provider.ModelProfileSnapshot{}, translate(err)
	}
	if err = writeAudit(ctx, tx, value.TenantID, "model", value.ProfileID, value.Version); err != nil {
		return provider.ModelProfileSnapshot{}, err
	}
	if err = writeCredentialInvalidation(ctx, tx, value.TenantID, "model", value.ProfileID, value.Version); err != nil {
		return provider.ModelProfileSnapshot{}, err
	}
	if err = tx.Commit(); err != nil {
		return provider.ModelProfileSnapshot{}, translate(err)
	}
	return value, nil
}

func (r *Repository) PublishBackend(ctx context.Context, input provider.BackendProfileSnapshot) (provider.BackendProfileSnapshot, error) {
	value, err := r.catalog.NormalizeBackend(input)
	if err != nil {
		return provider.BackendProfileSnapshot{}, err
	}
	configuration, err := json.Marshal(value.Configuration)
	if err != nil {
		return provider.BackendProfileSnapshot{}, runtime.ErrInvariantViolation
	}
	capabilities := make([]string, 0, len(value.Capabilities))
	for capability, enabled := range value.Capabilities {
		if enabled {
			capabilities = append(capabilities, capability)
		}
	}
	sort.Strings(capabilities)
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return provider.BackendProfileSnapshot{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.ExecContext(ctx, `INSERT INTO backend_profile(tenant_id,backend_profile_id,profile_key,display_name,status)
VALUES($1,$2,$3,$4,$5) ON CONFLICT (tenant_id,backend_profile_id) DO NOTHING`, value.TenantID, value.ProfileID, value.ProfileKey, value.DisplayName, value.Status); err != nil {
		return provider.BackendProfileSnapshot{}, translate(err)
	}
	if err = lockIdentity(ctx, tx, "backend_profile", value.TenantID, value.ProfileID, value.ProfileKey, value.Status); err != nil {
		return provider.BackendProfileSnapshot{}, err
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO backend_profile_revision(tenant_id,backend_profile_id,profile_version,schema_version,provider,configuration,credential_ref,credential_version,capabilities,content_digest)
VALUES($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9,$10) ON CONFLICT (tenant_id,backend_profile_id,profile_version) DO NOTHING`, value.TenantID, value.ProfileID, value.Version, value.SchemaVersion, value.Provider, configuration, nullable(value.CredentialRef.Ref), nullableVersion(value.CredentialRef.Version), capabilities, value.ContentDigest)
	if err != nil {
		return provider.BackendProfileSnapshot{}, translate(err)
	}
	if err = verifyImmutableInsert(ctx, tx, result, "backend_profile_revision", "backend_profile_id", value.TenantID, value.ProfileID, value.Version, value.ContentDigest); err != nil {
		return provider.BackendProfileSnapshot{}, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE backend_profile SET current_version=$3,row_version=row_version+1,updated_at=clock_timestamp() WHERE tenant_id=$1 AND backend_profile_id=$2 AND (current_version IS NULL OR current_version<$3)`, value.TenantID, value.ProfileID, value.Version); err != nil {
		return provider.BackendProfileSnapshot{}, translate(err)
	}
	if err = writeAudit(ctx, tx, value.TenantID, "backend", value.ProfileID, value.Version); err != nil {
		return provider.BackendProfileSnapshot{}, err
	}
	if err = writeCredentialInvalidation(ctx, tx, value.TenantID, "backend", value.ProfileID, value.Version); err != nil {
		return provider.BackendProfileSnapshot{}, err
	}
	if err = tx.Commit(); err != nil {
		return provider.BackendProfileSnapshot{}, translate(err)
	}
	return value, nil
}

func (r *Repository) GetModel(ctx context.Context, tenantID, profileID string, version int64) (provider.ModelProfileSnapshot, error) {
	var value provider.ModelProfileSnapshot
	value.TenantID, value.ProfileID, value.Version = tenantID, profileID, version
	var options, generation []byte
	var secretRef sql.NullString
	var secretVersion sql.NullInt64
	err := r.db.QueryRowContext(ctx, `SELECT p.profile_key,p.display_name,p.status,v.schema_version,v.provider,v.model_name,v.endpoint,v.options,v.secret_ref,v.secret_version,v.generation,v.content_digest FROM model_profile p JOIN model_profile_revision v USING(tenant_id,model_profile_id) WHERE p.tenant_id=$1 AND p.model_profile_id=$2 AND v.profile_version=$3`, tenantID, profileID, version).
		Scan(&value.ProfileKey, &value.DisplayName, &value.Status, &value.SchemaVersion, &value.Provider, &value.Model, &value.Endpoint, &options, &secretRef, &secretVersion, &generation, &value.ContentDigest)
	if err != nil {
		return provider.ModelProfileSnapshot{}, translate(err)
	}
	if err := json.Unmarshal(options, &value.Options); err != nil {
		return provider.ModelProfileSnapshot{}, runtime.ErrInvariantViolation
	}
	if err := json.Unmarshal(generation, &value.Generation); err != nil {
		return provider.ModelProfileSnapshot{}, runtime.ErrInvariantViolation
	}
	value.SecretRef = secrets.SecretRef{Ref: secretRef.String, Version: secretVersion.Int64}
	storedDigest := value.ContentDigest
	normalized, err := r.catalog.NormalizeModel(value)
	if err != nil {
		return provider.ModelProfileSnapshot{}, err
	}
	if normalized.ContentDigest != storedDigest {
		return provider.ModelProfileSnapshot{}, runtime.ErrInvariantViolation
	}
	return normalized, nil
}

// PreviousModelCredential returns the newest credential generation strictly
// before a published Profile version. Consumers use it to retire the old
// local generation after receiving the durable invalidation for the new one.
func (r *Repository) PreviousModelCredential(ctx context.Context, tenantID, profileID string, beforeVersion int64) (secrets.SecretRef, int64, error) {
	if r == nil || r.db == nil || tenantID == "" || profileID == "" || beforeVersion < 1 {
		return secrets.SecretRef{}, 0, runtime.ErrInvariantViolation
	}
	var ref sql.NullString
	var credentialVersion, profileVersion sql.NullInt64
	err := r.db.QueryRowContext(ctx, `SELECT secret_ref,secret_version,profile_version
FROM model_profile_revision WHERE tenant_id=$1 AND model_profile_id=$2 AND profile_version<$3
ORDER BY profile_version DESC LIMIT 1`, tenantID, profileID, beforeVersion).Scan(&ref, &credentialVersion, &profileVersion)
	if err != nil {
		return secrets.SecretRef{}, 0, translate(err)
	}
	if ref.String == "" || credentialVersion.Int64 < 1 || profileVersion.Int64 < 1 {
		return secrets.SecretRef{}, 0, runtime.ErrInvariantViolation
	}
	return secrets.SecretRef{Ref: ref.String, Version: credentialVersion.Int64}, profileVersion.Int64, nil
}

func (r *Repository) GetBackend(ctx context.Context, tenantID, profileID string, version int64) (provider.BackendProfileSnapshot, error) {
	var value provider.BackendProfileSnapshot
	value.TenantID, value.ProfileID, value.Version = tenantID, profileID, version
	var configuration, capabilities []byte
	var credentialRef sql.NullString
	var credentialVersion sql.NullInt64
	err := r.db.QueryRowContext(ctx, `SELECT p.profile_key,p.display_name,p.status,v.schema_version,v.provider,v.configuration,v.credential_ref,v.credential_version,to_json(v.capabilities),v.content_digest FROM backend_profile p JOIN backend_profile_revision v USING(tenant_id,backend_profile_id) WHERE p.tenant_id=$1 AND p.backend_profile_id=$2 AND v.profile_version=$3`, tenantID, profileID, version).
		Scan(&value.ProfileKey, &value.DisplayName, &value.Status, &value.SchemaVersion, &value.Provider, &configuration, &credentialRef, &credentialVersion, &capabilities, &value.ContentDigest)
	if err != nil {
		return provider.BackendProfileSnapshot{}, translate(err)
	}
	if err := json.Unmarshal(configuration, &value.Configuration); err != nil {
		return provider.BackendProfileSnapshot{}, runtime.ErrInvariantViolation
	}
	var names []string
	if err := json.Unmarshal(capabilities, &names); err != nil {
		return provider.BackendProfileSnapshot{}, runtime.ErrInvariantViolation
	}
	value.Capabilities = make(provider.CapabilitySet, len(names))
	for _, name := range names {
		value.Capabilities[name] = true
	}
	value.CredentialRef = secrets.SecretRef{Ref: credentialRef.String, Version: credentialVersion.Int64}
	storedDigest := value.ContentDigest
	normalized, err := r.catalog.NormalizeBackend(value)
	if err != nil {
		return provider.BackendProfileSnapshot{}, err
	}
	if normalized.ContentDigest != storedDigest {
		return provider.BackendProfileSnapshot{}, runtime.ErrInvariantViolation
	}
	return normalized, nil
}

func lockIdentity(ctx context.Context, tx *sql.Tx, table, tenantID, profileID, profileKey, status string) error {
	idColumn := "model_profile_id"
	if table == "backend_profile" {
		idColumn = "backend_profile_id"
	}
	query := fmt.Sprintf("SELECT profile_key,status FROM %s WHERE tenant_id=$1 AND %s=$2 FOR UPDATE", table, idColumn)
	var storedKey, storedStatus string
	if err := tx.QueryRowContext(ctx, query, tenantID, profileID).Scan(&storedKey, &storedStatus); err != nil {
		return translate(err)
	}
	if storedKey != profileKey || storedStatus != status || storedStatus == "disabled" {
		return runtime.ErrVersionConflict
	}
	return nil
}

func verifyImmutableInsert(ctx context.Context, tx *sql.Tx, result sql.Result, table, idColumn, tenantID, profileID string, version int64, digest string) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 1 {
		return nil
	}
	query := fmt.Sprintf("SELECT content_digest FROM %s WHERE tenant_id=$1 AND %s=$2 AND profile_version=$3", table, idColumn)
	var stored string
	if err := tx.QueryRowContext(ctx, query, tenantID, profileID, version).Scan(&stored); err != nil {
		return translate(err)
	}
	if stored != digest {
		return runtime.ErrIdempotencyCollision
	}
	return nil
}

func writeAudit(ctx context.Context, tx *sql.Tx, tenantID, kind, profileID string, version int64) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref)
VALUES($1,$2,'audit',$3,$4,$5,$6) ON CONFLICT (tenant_id,kind,idempotency_key) DO NOTHING`, tenantID, fmt.Sprintf("provider-profile:%s:%s:%s:%d", kind, tenantID, profileID, version), profileID, version, fmt.Sprintf("provider-profile:%s:%s:%s:%d", kind, tenantID, profileID, version), fmt.Sprintf("provider-profile://%s/%s/%s/%d", tenantID, kind, profileID, version))
	return translate(err)
}

// writeCredentialInvalidation is deliberately emitted in the same transaction
// as an immutable Profile revision. It is a broadcast hint only: consumers
// reload the exact Profile version before retiring a local generation. The
// payload reference identifies no SecretRef and can never contain a value.
func writeCredentialInvalidation(ctx context.Context, tx *sql.Tx, tenantID, kind, profileID string, version int64) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref)
VALUES($1,$2,'config-invalidation',$3,$4,$5,$6) ON CONFLICT (tenant_id,kind,idempotency_key) DO NOTHING`,
		tenantID, fmt.Sprintf("provider-profile-invalidation:%s:%s:%s:%d", kind, tenantID, profileID, version), profileID, version,
		fmt.Sprintf("provider-profile:%s:%s:%s:%d:invalidate", kind, tenantID, profileID, version),
		fmt.Sprintf("provider-profile://%s/%s/%s/%d", tenantID, kind, profileID, version))
	return translate(err)
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}
func nullableVersion(value int64) any {
	if value < 1 {
		return nil
	}
	return value
}

type sqlStater interface{ SQLState() string }

func translate(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return runtime.ErrNotFound
	}
	var state sqlStater
	if errors.As(err, &state) {
		switch state.SQLState() {
		case "23503", "22023":
			return runtime.ErrInvariantViolation
		case "23505":
			return runtime.ErrVersionConflict
		case "42501":
			return runtime.ErrTenantScope
		}
	}
	return err
}
