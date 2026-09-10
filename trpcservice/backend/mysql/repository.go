// Package mysql provides the MySQL implementation of the Backend
// Profile repository.
package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
)

// BackendRepository persists Backend Profiles and their capability bindings.
type BackendRepository struct {
	db      *sql.DB
	catalog *backend.ProviderCatalog
}

var _ backend.Repository = (*BackendRepository)(nil)

// List returns a stable page of Backend Profiles belonging to one tenant.
func (r *BackendRepository) List(ctx context.Context, tenantID, query, status, cursor string, limit int) ([]*backend.Profile, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if r == nil || r.db == nil {
		return nil, "", ErrStorage
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	offset, err := decodeListCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	rows, err := r.db.QueryContext(ctx, `SELECT profile_id FROM backend_profile WHERE tenant_id = ? ORDER BY profile_id`, tenantID)
	if err != nil {
		return nil, "", ErrStorage
	}
	var profileIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, "", ErrStorage
		}
		profileIDs = append(profileIDs, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, "", ErrStorage
	}
	_ = rows.Close()
	query, status = strings.ToLower(strings.TrimSpace(query)), strings.TrimSpace(status)
	items := make([]*backend.Profile, 0)
	for _, id := range profileIDs {
		value, err := loadBackendProfile(ctx, r.db, r.catalog, tenantID, id, false)
		if err != nil {
			return nil, "", ErrStorage
		}
		if status != "" && string(value.Status) != status {
			continue
		}
		if query != "" && !strings.Contains(strings.ToLower(value.ProfileID+" "+value.ProfileKey+" "+value.DisplayName), query) {
			continue
		}
		items = append(items, value)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ProfileID < items[j].ProfileID })
	if offset >= len(items) {
		return []*backend.Profile{}, "", nil
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	next := ""
	if end < len(items) {
		next = fmt.Sprintf("%d", end)
	}
	return items[offset:end], next, nil
}

func decodeListCursor(cursor string) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	var offset int
	if _, err := fmt.Sscanf(cursor, "%d", &offset); err != nil || offset < 0 {
		return 0, fmt.Errorf("invalid cursor")
	}
	return offset, nil
}

// NewRepository creates a repository that revalidates decoded
// capability bindings against the trusted ProviderCatalog.
func NewRepository(db *sql.DB, catalog *backend.ProviderCatalog) *BackendRepository {
	return &BackendRepository{db: db, catalog: catalog}
}

// Create persists a backend profile and returns its creation event.
func (r *BackendRepository) Create(ctx context.Context, input backend.CreateInput) (*backend.Profile, backend.ChangeEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, backend.ChangeEvent{}, err
	}
	if r == nil || r.db == nil {
		return nil, backend.ChangeEvent{}, ErrStorage
	}
	value, err := backend.NewProfile(input, r.catalog)
	if err != nil {
		return nil, backend.ChangeEvent{}, err
	}
	event, err := backend.PrepareCreatedChange(*value, r.catalog, input.Metadata)
	if err != nil {
		return nil, backend.ChangeEvent{}, err
	}
	tx, err := begin(ctx, r.db)
	if err != nil {
		return nil, backend.ChangeEvent{}, err
	}
	defer rollback(tx)
	if _, err := tx.ExecContext(ctx, `INSERT INTO backend_profile (
			tenant_id, profile_id, profile_key, display_name, description, status, schema_version,
			content_digest, version, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, 'disabled', ?, ?, ?, ?, ?)`, value.TenantID, value.ProfileID, value.ProfileKey,
		value.DisplayName, value.Description, value.SchemaVersion, value.ContentDigest,
		value.Version, value.CreatedAt, value.UpdatedAt); err != nil {
		return nil, backend.ChangeEvent{}, mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
	}
	if err := replaceBackendBindings(ctx, tx, *value); err != nil {
		return nil, backend.ChangeEvent{}, err
	}
	if value.Status != backend.StatusDisabled {
		if _, err := tx.ExecContext(ctx, `UPDATE backend_profile SET status = ? WHERE tenant_id = ? AND profile_id = ?`, string(value.Status), value.TenantID, value.ProfileID); err != nil {
			return nil, backend.ChangeEvent{}, mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
		}
	}
	eventResult, err := tx.ExecContext(ctx, `INSERT INTO backend_profile_change_outbox (
		event_type, tenant_id, profile_id, previous_status, current_status, previous_digest,
		current_digest, actor_type, actor_id, reason, correlation_id, previous_version,
		next_version, occurred_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, string(event.EventType), event.TenantID,
		event.ProfileID, nullableText(string(event.PreviousStatus)), string(event.CurrentStatus),
		nullableText(event.PreviousDigest), event.CurrentDigest, event.ActorType, event.ActorID,
		event.Reason, event.CorrelationID, event.PreviousVersion, event.NextVersion, event.OccurredAt)
	if err != nil {
		return nil, backend.ChangeEvent{}, mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
	}
	var eventID int64
	eventID, err = eventResult.LastInsertId()
	if err != nil {
		return nil, backend.ChangeEvent{}, mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
	}
	stored, err := loadBackendProfile(ctx, tx, r.catalog, value.TenantID, value.ProfileID, false)
	if err != nil {
		return nil, backend.ChangeEvent{}, mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
	}
	committed, err := scanBackendEvent(tx.QueryRowContext(ctx, backendEventSelect+` WHERE event_id = ?`, eventID))
	if err != nil {
		return nil, backend.ChangeEvent{}, mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return nil, backend.ChangeEvent{}, err
	}
	return stored, committed, nil
}

// Get loads a backend profile within the requested tenant.
func (r *BackendRepository) Get(ctx context.Context, tenantID, profileID string) (*backend.Profile, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil || r.db == nil {
		return nil, ErrStorage
	}
	value, err := loadBackendProfile(ctx, r.db, r.catalog, tenantID, profileID, false)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%w: %s", backend.ErrNotFound, profileID)
		}
		return nil, mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
	}
	return value, nil
}

// UpdateConfiguration applies an expected-version configuration update.
func (r *BackendRepository) UpdateConfiguration(ctx context.Context, input backend.UpdateConfigurationInput) (*backend.Profile, backend.ChangeEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, backend.ChangeEvent{}, err
	}
	if r == nil || r.db == nil {
		return nil, backend.ChangeEvent{}, ErrStorage
	}
	tx, err := begin(ctx, r.db)
	if err != nil {
		return nil, backend.ChangeEvent{}, err
	}
	defer rollback(tx)
	current, err := loadBackendProfile(ctx, tx, r.catalog, input.TenantID, input.ProfileID, true)
	if err != nil {
		return nil, backend.ChangeEvent{}, mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
	}
	updated, event, err := backend.PrepareConfigurationChange(*current, input, r.catalog, monotonicNow(current.UpdatedAt))
	if err != nil {
		return nil, backend.ChangeEvent{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE backend_profile SET display_name = ?, description = ?,
		schema_version = ?, content_digest = ?, version = version + 1, updated_at = ?
		WHERE tenant_id = ? AND profile_id = ? AND version = ? AND status <> 'disabled'`,
		updated.DisplayName, updated.Description, updated.SchemaVersion, updated.ContentDigest,
		updated.UpdatedAt, updated.TenantID, updated.ProfileID, input.ExpectedVersion)
	if err != nil {
		return nil, backend.ChangeEvent{}, mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
	}
	if affected, affectedErr := result.RowsAffected(); affectedErr != nil || affected != 1 {
		return nil, backend.ChangeEvent{}, fmt.Errorf("%w: expected %d, got %d", backend.ErrConflict, input.ExpectedVersion, current.Version)
	}
	if err := replaceBackendBindings(ctx, tx, updated); err != nil {
		return nil, backend.ChangeEvent{}, err
	}
	eventResult, err := tx.ExecContext(ctx, `INSERT INTO backend_profile_change_outbox (
		event_type, tenant_id, profile_id, previous_status, current_status, previous_digest,
		current_digest, actor_type, actor_id, reason, correlation_id, previous_version,
		next_version, occurred_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, string(event.EventType), event.TenantID,
		event.ProfileID, nullableText(string(event.PreviousStatus)), string(event.CurrentStatus),
		nullableText(event.PreviousDigest), event.CurrentDigest, event.ActorType, event.ActorID,
		event.Reason, event.CorrelationID, event.PreviousVersion, event.NextVersion, event.OccurredAt)
	if err != nil {
		return nil, backend.ChangeEvent{}, mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
	}
	var eventID int64
	eventID, err = eventResult.LastInsertId()
	if err != nil {
		return nil, backend.ChangeEvent{}, mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
	}
	stored, err := loadBackendProfile(ctx, tx, r.catalog, input.TenantID, input.ProfileID, false)
	if err != nil {
		return nil, backend.ChangeEvent{}, mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
	}
	committed, err := scanBackendEvent(tx.QueryRowContext(ctx, backendEventSelect+` WHERE event_id = ?`, eventID))
	if err != nil {
		return nil, backend.ChangeEvent{}, mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return nil, backend.ChangeEvent{}, err
	}
	return stored, committed, nil
}

// TransitionStatus changes a backend profile status with optimistic concurrency.
func (r *BackendRepository) TransitionStatus(ctx context.Context, input backend.TransitionStatusInput) (*backend.Profile, backend.ChangeEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, backend.ChangeEvent{}, err
	}
	if r == nil || r.db == nil {
		return nil, backend.ChangeEvent{}, ErrStorage
	}
	tx, err := begin(ctx, r.db)
	if err != nil {
		return nil, backend.ChangeEvent{}, err
	}
	defer rollback(tx)
	current, err := loadBackendProfile(ctx, tx, r.catalog, input.TenantID, input.ProfileID, true)
	if err != nil {
		return nil, backend.ChangeEvent{}, mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
	}
	updated, event, err := backend.PrepareStatusChange(*current, input, r.catalog, monotonicNow(current.UpdatedAt))
	if err != nil {
		return nil, backend.ChangeEvent{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE backend_profile SET status = ?, version = version + 1, updated_at = ?
		WHERE tenant_id = ? AND profile_id = ? AND version = ? AND status <> 'disabled'`,
		string(updated.Status), updated.UpdatedAt, input.TenantID, input.ProfileID, input.ExpectedVersion)
	if err != nil {
		return nil, backend.ChangeEvent{}, mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
	}
	if affected, affectedErr := result.RowsAffected(); affectedErr != nil || affected != 1 {
		return nil, backend.ChangeEvent{}, fmt.Errorf("%w: expected %d, got %d", backend.ErrConflict, input.ExpectedVersion, current.Version)
	}
	eventResult, err := tx.ExecContext(ctx, `INSERT INTO backend_profile_change_outbox (
		event_type, tenant_id, profile_id, previous_status, current_status, previous_digest,
		current_digest, actor_type, actor_id, reason, correlation_id, previous_version,
		next_version, occurred_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, string(event.EventType), event.TenantID,
		event.ProfileID, nullableText(string(event.PreviousStatus)), string(event.CurrentStatus),
		nullableText(event.PreviousDigest), event.CurrentDigest, event.ActorType, event.ActorID,
		event.Reason, event.CorrelationID, event.PreviousVersion, event.NextVersion, event.OccurredAt)
	if err != nil {
		return nil, backend.ChangeEvent{}, mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
	}
	eventID, err := eventResult.LastInsertId()
	if err != nil {
		return nil, backend.ChangeEvent{}, mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
	}
	stored, err := loadBackendProfile(ctx, tx, r.catalog, input.TenantID, input.ProfileID, false)
	if err != nil {
		return nil, backend.ChangeEvent{}, mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
	}
	committed, err := scanBackendEvent(tx.QueryRowContext(ctx, backendEventSelect+` WHERE event_id = ?`, eventID))
	if err != nil {
		return nil, backend.ChangeEvent{}, mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return nil, backend.ChangeEvent{}, err
	}
	return stored, committed, nil
}

func replaceBackendBindings(ctx context.Context, q queryer, profile backend.Profile) error {
	for _, binding := range profile.Bindings {
		options, err := encodeJSON(binding.Options)
		if err != nil {
			return ErrStorage
		}
		if len(options) == 0 || string(options) == "null" {
			options = []byte("{}")
		}
		if _, err := q.ExecContext(ctx, `INSERT INTO backend_profile_binding (
				tenant_id, profile_id, capability, provider, endpoint, options, secret_ref
			) VALUES (?, ?, ?, ?, ?, ?, ?)
			ON DUPLICATE KEY UPDATE provider = VALUES(provider), endpoint = VALUES(endpoint), options = VALUES(options), secret_ref = VALUES(secret_ref)`, profile.TenantID, profile.ProfileID, string(binding.Capability),
			binding.Provider, binding.Endpoint, options, binding.SecretRef); err != nil {
			return mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
		}
	}
	if len(profile.Bindings) == 0 {
		if _, err := q.ExecContext(ctx, "DELETE FROM backend_profile_binding WHERE tenant_id = ? AND profile_id = ?", profile.TenantID, profile.ProfileID); err != nil {
			return mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
		}
		return nil
	}
	placeholders := strings.TrimRight(strings.Repeat("?,", len(profile.Bindings)), ",")
	args := make([]any, 0, 2+len(profile.Bindings))
	args = append(args, profile.TenantID, profile.ProfileID)
	for _, binding := range profile.Bindings {
		args = append(args, string(binding.Capability))
	}
	deleteQuery := fmt.Sprintf("DELETE FROM backend_profile_binding WHERE tenant_id = ? AND profile_id = ? AND capability NOT IN (%s)", placeholders)
	if _, err := q.ExecContext(ctx, deleteQuery, args...); err != nil {
		return mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
	}
	return nil
}

const backendRootSelect = `SELECT tenant_id, profile_id, profile_key, display_name,
       description, status, schema_version, content_digest, version,
       created_at, updated_at FROM backend_profile`

const backendBindingsSelect = `SELECT capability, provider, endpoint, options, secret_ref
FROM backend_profile_binding WHERE tenant_id = ? AND profile_id = ? ORDER BY capability`

func loadBackendProfile(ctx context.Context, q queryer, catalog *backend.ProviderCatalog, tenantID, profileID string, forUpdate bool) (*backend.Profile, error) {
	query := backendRootSelect + ` WHERE tenant_id = ? AND profile_id = ?`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	var value backend.Profile
	var status string
	if err := q.QueryRowContext(ctx, query, tenantID, profileID).Scan(
		&value.TenantID, &value.ProfileID, &value.ProfileKey, &value.DisplayName,
		&value.Description, &status, &value.SchemaVersion, &value.ContentDigest,
		&value.Version, &value.CreatedAt, &value.UpdatedAt); err != nil {
		return nil, err
	}
	value.Status = backend.Status(status)
	rows, err := q.QueryContext(ctx, backendBindingsSelect, tenantID, profileID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var bindings []backend.CapabilityBinding
	for rows.Next() {
		var binding backend.CapabilityBinding
		var capability string
		var options []byte
		if err := rows.Scan(&capability, &binding.Provider, &binding.Endpoint, &options, &binding.SecretRef); err != nil {
			return nil, err
		}
		binding.Capability = backend.Capability(capability)
		var decoded map[string]string
		if err := decodeJSON(options, &decoded); err != nil {
			return nil, ErrStorage
		}
		binding.Options = decoded
		bindings = append(bindings, binding)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	value.Bindings = bindings
	value.CreatedAt = asUTC(value.CreatedAt)
	value.UpdatedAt = asUTC(value.UpdatedAt)
	if err := value.Validate(catalog); err != nil {
		return nil, ErrStorage
	}
	clone := value.Clone()
	return &clone, nil
}

const backendEventSelect = `SELECT event_type, tenant_id, profile_id,
       previous_status, current_status, previous_digest, current_digest,
       actor_type, actor_id, reason, correlation_id, previous_version,
       next_version, occurred_at FROM backend_profile_change_outbox`

func scanBackendEvent(row rowScanner) (backend.ChangeEvent, error) {
	var event backend.ChangeEvent
	var eventType, currentStatus string
	var previousStatus, previousDigest sql.NullString
	if err := row.Scan(&eventType, &event.TenantID, &event.ProfileID, &previousStatus,
		&currentStatus, &previousDigest, &event.CurrentDigest, &event.ActorType,
		&event.ActorID, &event.Reason, &event.CorrelationID, &event.PreviousVersion,
		&event.NextVersion, &event.OccurredAt); err != nil {
		return backend.ChangeEvent{}, err
	}
	event.EventType = backend.EventType(eventType)
	if previousStatus.Valid {
		event.PreviousStatus = backend.Status(previousStatus.String)
	}
	event.CurrentStatus = backend.Status(currentStatus)
	if previousDigest.Valid {
		event.PreviousDigest = previousDigest.String
	}
	event.OccurredAt = asUTC(event.OccurredAt)
	return event, nil
}
