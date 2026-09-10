// Package postgres provides the PostgreSQL implementation of the Backend
// Profile repository.
package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
)

// List returns a stable page of Backend Profiles belonging to one tenant.
func (r *BackendRepository) List(ctx context.Context, tenantID, query, status, cursor string, limit int) ([]*backend.Profile, string, error) {
	if r == nil || r.db == nil {
		return nil, "", ErrStorage
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	offset := 0
	if cursor != "" {
		if _, err := fmt.Sscanf(cursor, "%d", &offset); err != nil || offset < 0 {
			return nil, "", fmt.Errorf("invalid cursor")
		}
	}
	rows, err := r.db.QueryContext(ctx, `SELECT profile_id FROM public.backend_profile WHERE tenant_id=$1 ORDER BY profile_id`, tenantID)
	if err != nil {
		return nil, "", ErrStorage
	}
	profileIDs, err := scanProfileIDs(rows)
	if err != nil {
		return nil, "", ErrStorage
	}
	items := make([]*backend.Profile, 0)
	q := strings.ToLower(strings.TrimSpace(query))
	for _, id := range profileIDs {
		value, err := loadBackendProfile(ctx, r.db, r.catalog, tenantID, id, false)
		if err != nil {
			return nil, "", ErrStorage
		}
		if status != "" && string(value.Status) != status {
			continue
		}
		if q != "" && !strings.Contains(strings.ToLower(value.ProfileID+" "+value.ProfileKey+" "+value.DisplayName), q) {
			continue
		}
		items = append(items, value)
	}
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

func scanProfileIDs(rows *sql.Rows) ([]string, error) {
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	return ids, rows.Close()
}

// BackendRepository persists Backend Profiles and their capability bindings.
type BackendRepository struct {
	db      *sql.DB
	catalog *backend.ProviderCatalog
}

var _ backend.Repository = (*BackendRepository)(nil)

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
	bindings, err := encodeBackendBindings(value.Bindings)
	if err != nil {
		return nil, backend.ChangeEvent{}, ErrStorage
	}
	tx, err := begin(ctx, r.db)
	if err != nil {
		return nil, backend.ChangeEvent{}, err
	}
	defer rollback(tx)
	var eventID int64
	err = tx.QueryRowContext(ctx, `
		SELECT public.control_plane_create_backend_profile(
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
			$12, $13, $14, $15, $16, $17, $18, $19, $20, $21
		)`,
		value.TenantID, value.ProfileID, value.ProfileKey, value.DisplayName, value.Description,
		string(value.Status), value.SchemaVersion, value.ContentDigest, value.Version,
		value.CreatedAt, value.UpdatedAt, bindings, event.EventType, event.PreviousStatus,
		event.CurrentStatus, event.PreviousDigest, event.CurrentDigest, event.ActorType,
		event.ActorID, event.Reason, event.CorrelationID,
	).Scan(&eventID)
	if err != nil {
		return nil, backend.ChangeEvent{}, mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
	}
	stored, err := loadBackendProfile(ctx, tx, r.catalog, value.TenantID, value.ProfileID, false)
	if err != nil {
		return nil, backend.ChangeEvent{}, mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
	}
	committed, err := scanBackendEvent(tx.QueryRowContext(ctx, backendEventSelect+` WHERE event_id = $1`, eventID))
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
	bindings, err := encodeBackendBindings(updated.Bindings)
	if err != nil {
		return nil, backend.ChangeEvent{}, ErrStorage
	}
	var eventID int64
	err = tx.QueryRowContext(ctx, `
		SELECT public.control_plane_update_backend_profile(
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
			$12, $13, $14, $15, $16, $17, $18
		)`,
		updated.TenantID, updated.ProfileID, input.ExpectedVersion, updated.DisplayName,
		updated.Description, updated.SchemaVersion, updated.ContentDigest, updated.UpdatedAt,
		bindings, event.EventType, event.PreviousStatus, event.CurrentStatus,
		event.PreviousDigest, event.CurrentDigest, event.ActorType, event.ActorID,
		event.Reason, event.CorrelationID,
	).Scan(&eventID)
	if err != nil {
		return nil, backend.ChangeEvent{}, mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
	}
	stored, err := loadBackendProfile(ctx, tx, r.catalog, input.TenantID, input.ProfileID, false)
	if err != nil {
		return nil, backend.ChangeEvent{}, mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
	}
	committed, err := scanBackendEvent(tx.QueryRowContext(ctx, backendEventSelect+` WHERE event_id = $1`, eventID))
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
	_, _, err = backend.PrepareStatusChange(*current, input, r.catalog, monotonicNow(current.UpdatedAt))
	if err != nil {
		return nil, backend.ChangeEvent{}, err
	}
	var nextVersion int64
	err = tx.QueryRowContext(ctx, `
		SELECT public.transition_backend_profile_status($1, $2, $3, $4, $5, $6, $7, $8)
	`, input.TenantID, input.ProfileID, input.ExpectedVersion, string(input.NextStatus),
		strings.TrimSpace(input.Metadata.ActorType), strings.TrimSpace(input.Metadata.ActorID),
		strings.TrimSpace(input.Metadata.Reason), strings.TrimSpace(input.Metadata.CorrelationID)).Scan(&nextVersion)
	if err != nil {
		return nil, backend.ChangeEvent{}, mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
	}
	stored, err := loadBackendProfile(ctx, tx, r.catalog, input.TenantID, input.ProfileID, false)
	if err != nil {
		return nil, backend.ChangeEvent{}, mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
	}
	committed, err := scanBackendEvent(tx.QueryRowContext(ctx, backendEventSelect+` WHERE tenant_id = $1 AND profile_id = $2 AND next_version = $3 ORDER BY event_id DESC LIMIT 1`, input.TenantID, input.ProfileID, nextVersion))
	if err != nil {
		return nil, backend.ChangeEvent{}, mapDBError(ctx, err, backend.ErrNotFound, backend.ErrDuplicateKey, backend.ErrConflict, backend.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return nil, backend.ChangeEvent{}, err
	}
	return stored, committed, nil
}

const backendRootSelect = `SELECT tenant_id, profile_id, profile_key, display_name,
       description, status, schema_version, content_digest, version,
       created_at, updated_at FROM public.backend_profile`

const backendBindingsSelect = `SELECT capability, provider, endpoint, options, secret_ref
FROM public.backend_profile_binding WHERE tenant_id = $1 AND profile_id = $2 ORDER BY capability`

func loadBackendProfile(ctx context.Context, q queryer, catalog *backend.ProviderCatalog, tenantID, profileID string, forUpdate bool) (*backend.Profile, error) {
	query := backendRootSelect + ` WHERE tenant_id = $1 AND profile_id = $2`
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
       next_version, occurred_at FROM public.backend_profile_change_outbox`

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
