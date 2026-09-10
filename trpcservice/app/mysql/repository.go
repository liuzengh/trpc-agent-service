// Package mysql provides the MySQL implementation of the Agent App
// repository.
package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
)

// AppRepository persists App roots, mutable drafts and immutable published
// revisions. It is named after the app domain rather than the external Agent
// framework so the storage implementation does not blur those ownerships.
type AppRepository struct {
	db *sql.DB
}

var _ appmodel.Repository = (*AppRepository)(nil)

// List returns a stable page of Apps belonging to one tenant.
func (r *AppRepository) List(ctx context.Context, tenantID, query, status, cursor string, limit int) ([]*appmodel.App, string, error) {
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
	rows, err := r.db.QueryContext(ctx, agentAppSelect+` WHERE tenant_id = ? ORDER BY app_id`, tenantID)
	if err != nil {
		return nil, "", mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	defer rows.Close()
	query, status = strings.ToLower(strings.TrimSpace(query)), strings.TrimSpace(status)
	items := make([]*appmodel.App, 0)
	for rows.Next() {
		var value appmodel.App
		var state string
		var current, canary sql.NullInt64
		if err := rows.Scan(&value.TenantID, &value.AppID, &value.AppKey, &value.DisplayName, &value.Description, &state, &current, &canary, &value.Version, &value.CreatedAt, &value.UpdatedAt); err != nil {
			return nil, "", ErrStorage
		}
		value.Status, value.CurrentRevision, value.CanaryRevision = appmodel.Status(state), nullableInt(current), nullableInt(canary)
		if status != "" && string(value.Status) != status {
			continue
		}
		if query != "" && !strings.Contains(strings.ToLower(value.AppID+" "+value.AppKey+" "+value.DisplayName), query) {
			continue
		}
		if err := value.Validate(); err != nil {
			return nil, "", ErrStorage
		}
		value.CreatedAt, value.UpdatedAt = asUTC(value.CreatedAt), asUTC(value.UpdatedAt)
		clone := value.Clone()
		items = append(items, &clone)
	}
	if err := rows.Err(); err != nil {
		return nil, "", ErrStorage
	}
	return pageApps(items, offset, limit)
}

// ListRevisions returns revisions for one App using stable numeric ordering.
func (r *AppRepository) ListRevisions(ctx context.Context, tenantID, appID, query, status, cursor string, limit int) ([]*appmodel.Revision, string, error) {
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
	rows, err := r.db.QueryContext(ctx, `SELECT revision FROM agent_app_revision WHERE tenant_id = ? AND app_id = ? ORDER BY revision`, tenantID, appID)
	if err != nil {
		return nil, "", mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	var revisions []int64
	for rows.Next() {
		var revision int64
		if err := rows.Scan(&revision); err != nil {
			_ = rows.Close()
			return nil, "", ErrStorage
		}
		revisions = append(revisions, revision)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, "", ErrStorage
	}
	_ = rows.Close()
	query, status = strings.ToLower(strings.TrimSpace(query)), strings.TrimSpace(status)
	items := make([]*appmodel.Revision, 0)
	for _, revision := range revisions {
		value, err := loadAgentRevision(ctx, r.db, tenantID, appID, revision, false)
		if err != nil {
			return nil, "", mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
		}
		if status != "" && string(value.State) != status {
			continue
		}
		if query != "" && !strings.Contains(strings.ToLower(value.Description+" "+value.Instruction+" "+value.GlobalInstruction), query) {
			continue
		}
		items = append(items, value)
	}
	return pageRevisions(items, offset, limit)
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
func pageApps(items []*appmodel.App, offset, limit int) ([]*appmodel.App, string, error) {
	sort.Slice(items, func(i, j int) bool { return items[i].AppID < items[j].AppID })
	if offset >= len(items) {
		return []*appmodel.App{}, "", nil
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
func pageRevisions(items []*appmodel.Revision, offset, limit int) ([]*appmodel.Revision, string, error) {
	sort.Slice(items, func(i, j int) bool { return items[i].Revision < items[j].Revision })
	if offset >= len(items) {
		return []*appmodel.Revision{}, "", nil
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

// NewAppRepository creates an App repository over a MySQL pool.
func NewAppRepository(db *sql.DB) *AppRepository { return &AppRepository{db: db} }

// Create persists a new agent application.
func (r *AppRepository) Create(ctx context.Context, input appmodel.CreateInput) (*appmodel.App, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil || r.db == nil {
		return nil, ErrStorage
	}
	value, err := appmodel.NewApp(input)
	if err != nil {
		return nil, err
	}
	tx, err := begin(ctx, r.db)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	_, err = tx.ExecContext(ctx, `INSERT INTO agent_app (
		tenant_id, app_id, app_key, display_name, description, status, current_revision,
		version, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, value.TenantID, value.AppID, value.AppKey,
		value.DisplayName, value.Description, string(value.Status), value.CurrentRevision,
		value.Version, value.CreatedAt, value.UpdatedAt)
	if err != nil {
		return nil, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	stored, err := loadAgentApp(ctx, tx, value.TenantID, value.AppID, false)
	if err != nil {
		return nil, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return nil, err
	}
	return stored, nil
}

// Get loads an agent application within a tenant.
func (r *AppRepository) Get(ctx context.Context, tenantID, appID string) (*appmodel.App, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil || r.db == nil {
		return nil, ErrStorage
	}
	value, err := loadAgentApp(ctx, r.db, tenantID, appID, false)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%w: tenant %s app %s", appmodel.ErrNotFound, tenantID, appID)
		}
		return nil, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	return value, nil
}

// UpdateMetadata applies an expected-version metadata update.
func (r *AppRepository) UpdateMetadata(ctx context.Context, input appmodel.UpdateMetadataInput) (*appmodel.App, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil || r.db == nil {
		return nil, ErrStorage
	}
	tx, err := begin(ctx, r.db)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	current, err := loadAgentApp(ctx, tx, input.TenantID, input.AppID, true)
	if err != nil {
		return nil, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if current.Status == appmodel.StatusDisabled {
		return nil, appmodel.ErrDisabled
	}
	if current.Version != input.ExpectedVersion {
		return nil, fmt.Errorf("%w: expected %d, got %d", appmodel.ErrConflict, input.ExpectedVersion, current.Version)
	}
	updated := current.Clone()
	updated.DisplayName = strings.TrimSpace(input.DisplayName)
	updated.Description = strings.TrimSpace(input.Description)
	updated.Version++
	updated.UpdatedAt = monotonicNow(current.UpdatedAt)
	if err := updated.Validate(); err != nil {
		return nil, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE agent_app SET display_name = ?, description = ?,
		version = version + 1, updated_at = ? WHERE tenant_id = ? AND app_id = ? AND version = ? AND status <> 'disabled'`,
		updated.DisplayName, updated.Description, updated.UpdatedAt, input.TenantID, input.AppID, input.ExpectedVersion)
	if err != nil {
		return nil, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if affected, affectedErr := result.RowsAffected(); affectedErr != nil || affected != 1 {
		return nil, fmt.Errorf("%w: expected %d, got %d", appmodel.ErrConflict, input.ExpectedVersion, current.Version)
	}
	stored, err := loadAgentApp(ctx, tx, input.TenantID, input.AppID, false)
	if err != nil {
		return nil, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return nil, err
	}
	return stored, nil
}

// CreateDraft persists a draft revision.
func (r *AppRepository) CreateDraft(ctx context.Context, input appmodel.CreateDraftInput) (*appmodel.Revision, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil || r.db == nil {
		return nil, ErrStorage
	}
	tx, err := begin(ctx, r.db)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	app, err := loadAgentApp(ctx, tx, input.TenantID, input.AppID, true)
	if err != nil {
		return nil, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if err := mutableAgentApp(app, input.ExpectedAppVersion); err != nil {
		return nil, err
	}
	var revisionNumber int64
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(revision), 0) + 1 FROM agent_app_revision
		WHERE tenant_id = ? AND app_id = ?
	`, input.TenantID, input.AppID).Scan(&revisionNumber); err != nil {
		return nil, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	draft, err := appmodel.NewRevision(appmodel.CreateRevisionInput{
		TenantID: input.TenantID, AppID: input.AppID, Revision: revisionNumber,
		Kind: input.Kind, SchemaVersion: input.SchemaVersion, Configuration: input.Configuration,
	})
	if err != nil {
		return nil, err
	}
	generation, runtime, _, err := encodeAgentRevisionParts(*draft)
	if err != nil {
		return nil, ErrStorage
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO agent_app_revision (
		tenant_id, app_id, revision, state, draft_version, agent_kind, schema_version,
		description, instruction, global_instruction, model_profile_id, generation_config,
		runtime_policy, content_digest, published_at, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, draft.TenantID, draft.AppID,
		draft.Revision, string(draft.State), draft.DraftVersion, string(draft.Kind), draft.SchemaVersion,
		draft.Description, draft.Instruction, draft.GlobalInstruction, draft.ModelProfileID,
		generation, runtime, nil, nil, draft.CreatedAt, draft.UpdatedAt)
	if err != nil {
		return nil, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if err := replaceRevisionTools(ctx, tx, *draft); err != nil {
		return nil, err
	}
	stored, err := loadAgentRevision(ctx, tx, input.TenantID, input.AppID, revisionNumber, false)
	if err != nil {
		return nil, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return nil, err
	}
	return stored, nil
}

// UpdateDraft applies an expected-version draft update.
func (r *AppRepository) UpdateDraft(ctx context.Context, input appmodel.UpdateDraftInput) (*appmodel.Revision, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil || r.db == nil {
		return nil, ErrStorage
	}
	tx, err := begin(ctx, r.db)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	app, err := loadAgentApp(ctx, tx, input.TenantID, input.AppID, true)
	if err != nil {
		return nil, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if err := mutableAgentApp(app, input.ExpectedAppVersion); err != nil {
		return nil, err
	}
	current, err := loadAgentRevision(ctx, tx, input.TenantID, input.AppID, input.Revision, true)
	if err != nil {
		return nil, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if current.State != appmodel.RevisionStateDraft {
		return nil, appmodel.ErrImmutableRevision
	}
	if current.DraftVersion != input.ExpectedDraftVersion {
		return nil, fmt.Errorf("%w: expected %d, got %d", appmodel.ErrConflict, input.ExpectedDraftVersion, current.DraftVersion)
	}
	candidate, err := appmodel.NewRevision(appmodel.CreateRevisionInput{
		TenantID: input.TenantID, AppID: input.AppID, Revision: input.Revision,
		Kind: current.Kind, SchemaVersion: current.SchemaVersion, Configuration: input.Configuration,
	})
	if err != nil {
		return nil, err
	}
	candidate.DraftVersion = current.DraftVersion + 1
	candidate.CreatedAt = current.CreatedAt
	candidate.UpdatedAt = monotonicNow(current.UpdatedAt)
	if err := candidate.Validate(); err != nil {
		return nil, err
	}
	generation, runtime, _, err := encodeAgentRevisionParts(*candidate)
	if err != nil {
		return nil, ErrStorage
	}
	result, err := tx.ExecContext(ctx, `UPDATE agent_app_revision SET
		draft_version = ?, description = ?, instruction = ?, global_instruction = ?, model_profile_id = ?,
		generation_config = ?, runtime_policy = ?, updated_at = ?
		WHERE tenant_id = ? AND app_id = ? AND revision = ? AND draft_version = ? AND state = 'draft'`,
		candidate.DraftVersion, candidate.Description, candidate.Instruction, candidate.GlobalInstruction,
		candidate.ModelProfileID, generation, runtime, candidate.UpdatedAt, candidate.TenantID, candidate.AppID,
		candidate.Revision, input.ExpectedDraftVersion)
	if err != nil {
		return nil, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if affected, affectedErr := result.RowsAffected(); affectedErr != nil || affected != 1 {
		return nil, fmt.Errorf("%w: expected %d, got %d", appmodel.ErrConflict, input.ExpectedDraftVersion, current.DraftVersion)
	}
	if err := replaceRevisionTools(ctx, tx, *candidate); err != nil {
		return nil, err
	}
	stored, err := loadAgentRevision(ctx, tx, input.TenantID, input.AppID, input.Revision, false)
	if err != nil {
		return nil, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return nil, err
	}
	return stored, nil
}

// GetRevision loads a specific application revision.
func (r *AppRepository) GetRevision(ctx context.Context, tenantID, appID string, revision int64) (*appmodel.Revision, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil || r.db == nil {
		return nil, ErrStorage
	}
	value, err := loadAgentRevision(ctx, r.db, tenantID, appID, revision, false)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%w: tenant %s app %s revision %d", appmodel.ErrNotFound, tenantID, appID, revision)
		}
		return nil, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	return value, nil
}

// Publish makes a draft revision active and returns its change event.
func (r *AppRepository) Publish(ctx context.Context, input appmodel.PublishInput) (*appmodel.App, *appmodel.Revision, appmodel.ChangeEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, appmodel.ChangeEvent{}, err
	}
	if r == nil || r.db == nil {
		return nil, nil, appmodel.ChangeEvent{}, ErrStorage
	}
	if err := validatePublishInput(input); err != nil {
		return nil, nil, appmodel.ChangeEvent{}, err
	}
	tx, err := begin(ctx, r.db)
	if err != nil {
		return nil, nil, appmodel.ChangeEvent{}, err
	}
	defer rollback(tx)
	currentApp, draft, err := loadPublishState(ctx, tx, input)
	if err != nil {
		return nil, nil, appmodel.ChangeEvent{}, err
	}
	now := monotonicNow(maxTime(currentApp.UpdatedAt, draft.UpdatedAt))
	published, err := draft.Publish(now)
	if err != nil {
		return nil, nil, appmodel.ChangeEvent{}, err
	}
	updatedApp := currentApp.Clone()
	previousStatus := updatedApp.Status
	previousRevision := cloneAgentInt64(updatedApp.CurrentRevision)
	updatedApp.CurrentRevision = agentInt64(input.Revision)
	updatedApp.CanaryRevision = nil
	if updatedApp.Status == appmodel.StatusDraft {
		updatedApp.Status = appmodel.StatusActive
	}
	updatedApp.Version++
	updatedApp.UpdatedAt = now
	if err := updatedApp.Validate(); err != nil {
		return nil, nil, appmodel.ChangeEvent{}, err
	}
	event := appmodel.ChangeEvent{
		EventType: appmodel.ChangePublished, TenantID: updatedApp.TenantID, AppID: updatedApp.AppID,
		PreviousRevision: previousRevision, CurrentRevision: cloneAgentInt64(updatedApp.CurrentRevision),
		ContentDigest: published.ContentDigest, PreviousStatus: previousStatus, CurrentStatus: updatedApp.Status,
		ActorType: strings.TrimSpace(input.Metadata.ActorType), ActorID: strings.TrimSpace(input.Metadata.ActorID),
		Reason: strings.TrimSpace(input.Metadata.Reason), CorrelationID: strings.TrimSpace(input.Metadata.CorrelationID),
		PreviousVersion: currentApp.Version, NextVersion: updatedApp.Version, OccurredAt: now,
	}
	eventID, err := persistPublishedAgent(ctx, tx, input, published, updatedApp, event)
	if err != nil {
		return nil, nil, appmodel.ChangeEvent{}, err
	}
	storedApp, err := loadAgentApp(ctx, tx, input.TenantID, input.AppID, false)
	if err != nil {
		return nil, nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	storedRevision, err := loadAgentRevision(ctx, tx, input.TenantID, input.AppID, input.Revision, false)
	if err != nil {
		return nil, nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	committed, err := scanAgentEvent(tx.QueryRowContext(ctx, agentEventSelect+` WHERE event_id = ?`, eventID))
	if err != nil {
		return nil, nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return nil, nil, appmodel.ChangeEvent{}, err
	}
	return storedApp, storedRevision, committed, nil
}

func persistPublishedAgent(ctx context.Context, tx *sql.Tx, input appmodel.PublishInput, published appmodel.Revision, updatedApp appmodel.App, event appmodel.ChangeEvent) (int64, error) {
	result, err := tx.ExecContext(ctx, `UPDATE agent_app_revision SET state = 'published',
		content_digest = ?, published_at = ?, updated_at = ?
		WHERE tenant_id = ? AND app_id = ? AND revision = ? AND draft_version = ? AND state = 'draft'`,
		published.ContentDigest, published.PublishedAt, published.UpdatedAt, input.TenantID, input.AppID,
		input.Revision, input.ExpectedDraftVersion)
	if err != nil {
		return 0, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if affected, affectedErr := result.RowsAffected(); affectedErr != nil || affected != 1 {
		return 0, fmt.Errorf("%w: expected draft version %d", appmodel.ErrConflict, input.ExpectedDraftVersion)
	}
	result, err = tx.ExecContext(ctx, `UPDATE agent_app SET status = ?, current_revision = ?, canary_revision = NULL, version = ?, updated_at = ?
		WHERE tenant_id = ? AND app_id = ? AND version = ?`, string(updatedApp.Status), input.Revision,
		updatedApp.Version, updatedApp.UpdatedAt, input.TenantID, input.AppID, input.ExpectedAppVersion)
	if err != nil {
		return 0, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if affected, affectedErr := result.RowsAffected(); affectedErr != nil || affected != 1 {
		return 0, fmt.Errorf("%w: expected app version %d", appmodel.ErrConflict, input.ExpectedAppVersion)
	}
	return insertAgentEvent(ctx, tx, event)
}

// SetCanary selects or clears a published candidate revision.
//
//nolint:gocyclo // The transaction validates and persists one complete control-plane mutation.
func (r *AppRepository) SetCanary(ctx context.Context, input appmodel.SetCanaryInput) (*appmodel.App, appmodel.ChangeEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	if r == nil || r.db == nil {
		return nil, appmodel.ChangeEvent{}, ErrStorage
	}
	if err := validateAgentMetadata(input.Metadata); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	if !input.TenantActive {
		return nil, appmodel.ChangeEvent{}, fmt.Errorf("%w: tenant must be active", appmodel.ErrInvalid)
	}
	tx, err := begin(ctx, r.db)
	if err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	defer rollback(tx)
	current, err := loadAgentApp(ctx, tx, input.TenantID, input.AppID, true)
	if err != nil {
		return nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if err := mutableAgentApp(current, input.ExpectedAppVersion); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	if current.Status != appmodel.StatusActive {
		return nil, appmodel.ChangeEvent{}, fmt.Errorf("%w: canary requires an active app", appmodel.ErrInvalid)
	}
	if sameAgentRevision(current.CanaryRevision, input.CandidateRevision) {
		return nil, appmodel.ChangeEvent{}, fmt.Errorf("%w: canary revision is unchanged", appmodel.ErrInvalid)
	}
	var candidate *appmodel.Revision
	if input.CandidateRevision != nil {
		if *input.CandidateRevision < 1 || current.CurrentRevision == nil || *input.CandidateRevision == *current.CurrentRevision {
			return nil, appmodel.ChangeEvent{}, fmt.Errorf("%w: invalid canary revision", appmodel.ErrInvalid)
		}
		candidate, err = loadAgentRevision(ctx, tx, input.TenantID, input.AppID, *input.CandidateRevision, false)
		if err != nil {
			return nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
		}
		if candidate.State != appmodel.RevisionStatePublished {
			return nil, appmodel.ChangeEvent{}, fmt.Errorf("%w: canary revision must be published", appmodel.ErrInvalid)
		}
	}
	now := monotonicNow(current.UpdatedAt)
	updated := current.Clone()
	updated.CanaryRevision = cloneAgentInt64(input.CandidateRevision)
	updated.Version++
	updated.UpdatedAt = now
	if err := updated.Validate(); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	digest := ""
	if candidate != nil {
		digest = candidate.ContentDigest
	}
	eventType := appmodel.ChangeCanaryStopped
	if input.CandidateRevision != nil {
		eventType = appmodel.ChangeCanaryStarted
	}
	event := appmodel.ChangeEvent{
		EventType: eventType, TenantID: updated.TenantID, AppID: updated.AppID,
		PreviousRevision: cloneAgentInt64(current.CanaryRevision), CurrentRevision: cloneAgentInt64(updated.CanaryRevision),
		ContentDigest: digest, PreviousStatus: current.Status, CurrentStatus: updated.Status,
		ActorType: strings.TrimSpace(input.Metadata.ActorType), ActorID: strings.TrimSpace(input.Metadata.ActorID),
		Reason: strings.TrimSpace(input.Metadata.Reason), CorrelationID: strings.TrimSpace(input.Metadata.CorrelationID),
		PreviousVersion: current.Version, NextVersion: updated.Version, OccurredAt: now,
	}
	var candidateRevision any
	if input.CandidateRevision != nil {
		candidateRevision = *input.CandidateRevision
	}
	result, err := tx.ExecContext(ctx, `UPDATE agent_app SET canary_revision = ?, version = ?, updated_at = ?
		WHERE tenant_id = ? AND app_id = ? AND version = ?`, candidateRevision, updated.Version, updated.UpdatedAt, input.TenantID, input.AppID, input.ExpectedAppVersion)
	if err != nil {
		return nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if affected, affectedErr := result.RowsAffected(); affectedErr != nil || affected != 1 {
		return nil, appmodel.ChangeEvent{}, fmt.Errorf("%w: expected app version %d", appmodel.ErrConflict, input.ExpectedAppVersion)
	}
	eventID, err := insertAgentEvent(ctx, tx, event)
	if err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	stored, err := loadAgentApp(ctx, tx, input.TenantID, input.AppID, false)
	if err != nil {
		return nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	committed, err := scanAgentEvent(tx.QueryRowContext(ctx, agentEventSelect+` WHERE event_id = ?`, eventID))
	if err != nil {
		return nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	if err := ctx.Err(); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	return stored, committed, nil
}

func insertAgentEvent(ctx context.Context, tx *sql.Tx, event appmodel.ChangeEvent) (int64, error) {
	eventResult, err := tx.ExecContext(ctx, `INSERT INTO agent_app_change_outbox (
		event_type, tenant_id, app_id, previous_status, current_status, previous_revision,
		current_revision, content_digest, actor_type, actor_id, reason, correlation_id,
		previous_version, next_version, occurred_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, string(event.EventType), event.TenantID,
		event.AppID, nullableText(string(event.PreviousStatus)), string(event.CurrentStatus), event.PreviousRevision,
		event.CurrentRevision, event.ContentDigest, event.ActorType, event.ActorID, event.Reason,
		event.CorrelationID, event.PreviousVersion, event.NextVersion, event.OccurredAt)
	if err != nil {
		return 0, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	eventID, err := eventResult.LastInsertId()
	if err != nil {
		return 0, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	return eventID, nil
}

func validatePublishInput(input appmodel.PublishInput) error {
	if err := validateAgentMetadata(input.Metadata); err != nil {
		return err
	}
	if !input.TenantActive {
		return fmt.Errorf("%w: tenant must be active", appmodel.ErrInvalid)
	}
	return nil
}

func loadPublishState(ctx context.Context, tx *sql.Tx, input appmodel.PublishInput) (*appmodel.App, *appmodel.Revision, error) {
	if err := assertTenantActive(ctx, tx, input.TenantID); err != nil {
		return nil, nil, err
	}
	currentApp, err := loadAgentApp(ctx, tx, input.TenantID, input.AppID, true)
	if err != nil {
		return nil, nil, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if err := mutableAgentApp(currentApp, input.ExpectedAppVersion); err != nil {
		return nil, nil, err
	}
	draft, err := loadAgentRevision(ctx, tx, input.TenantID, input.AppID, input.Revision, true)
	if err != nil {
		return nil, nil, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if draft.State != appmodel.RevisionStateDraft {
		return nil, nil, appmodel.ErrImmutableRevision
	}
	if draft.DraftVersion != input.ExpectedDraftVersion {
		return nil, nil, fmt.Errorf("%w: expected %d, got %d", appmodel.ErrConflict, input.ExpectedDraftVersion, draft.DraftVersion)
	}
	return currentApp, draft, nil
}

// Rollback restores an earlier published revision.
func (r *AppRepository) Rollback(ctx context.Context, input appmodel.RollbackInput) (*appmodel.App, appmodel.ChangeEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	if r == nil || r.db == nil {
		return nil, appmodel.ChangeEvent{}, ErrStorage
	}
	if err := validateAgentMetadata(input.Metadata); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	tx, err := begin(ctx, r.db)
	if err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	defer rollback(tx)
	current, err := loadAgentApp(ctx, tx, input.TenantID, input.AppID, true)
	if err != nil {
		return nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if err := mutableAgentApp(current, input.ExpectedAppVersion); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	target, err := loadAgentRevision(ctx, tx, input.TenantID, input.AppID, input.TargetRevision, true)
	if err != nil {
		return nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if target.State != appmodel.RevisionStatePublished {
		return nil, appmodel.ChangeEvent{}, fmt.Errorf("%w: rollback target must be published", appmodel.ErrInvalid)
	}
	if current.CurrentRevision == nil || *current.CurrentRevision == input.TargetRevision {
		return nil, appmodel.ChangeEvent{}, fmt.Errorf("%w: rollback must change the current revision", appmodel.ErrInvalid)
	}
	now := monotonicNow(current.UpdatedAt)
	updated := current.Clone()
	previousRevision := cloneAgentInt64(updated.CurrentRevision)
	updated.CurrentRevision = agentInt64(input.TargetRevision)
	updated.CanaryRevision = nil
	updated.Version++
	updated.UpdatedAt = now
	if err := updated.Validate(); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	event := appmodel.ChangeEvent{
		EventType: appmodel.ChangeRolledBack, TenantID: updated.TenantID, AppID: updated.AppID,
		PreviousRevision: previousRevision, CurrentRevision: cloneAgentInt64(updated.CurrentRevision),
		ContentDigest: target.ContentDigest, PreviousStatus: updated.Status, CurrentStatus: updated.Status,
		ActorType: strings.TrimSpace(input.Metadata.ActorType), ActorID: strings.TrimSpace(input.Metadata.ActorID),
		Reason: strings.TrimSpace(input.Metadata.Reason), CorrelationID: strings.TrimSpace(input.Metadata.CorrelationID),
		PreviousVersion: current.Version, NextVersion: updated.Version, OccurredAt: now,
	}
	eventID, err := persistAgentRollback(ctx, tx, input, updated, event)
	if err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	stored, err := loadAgentApp(ctx, tx, input.TenantID, input.AppID, false)
	if err != nil {
		return nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	committed, err := scanAgentEvent(tx.QueryRowContext(ctx, agentEventSelect+` WHERE event_id = ?`, eventID))
	if err != nil {
		return nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	return stored, committed, nil
}

func persistAgentRollback(ctx context.Context, tx *sql.Tx, input appmodel.RollbackInput, updated appmodel.App, event appmodel.ChangeEvent) (int64, error) {
	result, err := tx.ExecContext(ctx, `UPDATE agent_app SET current_revision = ?, canary_revision = NULL, version = ?, updated_at = ?
		WHERE tenant_id = ? AND app_id = ? AND version = ?`, input.TargetRevision, updated.Version, updated.UpdatedAt,
		input.TenantID, input.AppID, input.ExpectedAppVersion)
	if err != nil {
		return 0, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if affected, affectedErr := result.RowsAffected(); affectedErr != nil || affected != 1 {
		return 0, fmt.Errorf("%w: expected app version %d", appmodel.ErrConflict, input.ExpectedAppVersion)
	}
	return insertAgentEvent(ctx, tx, event)
}

// TransitionStatus changes an application status with optimistic concurrency.
func (r *AppRepository) TransitionStatus(ctx context.Context, input appmodel.TransitionStatusInput) (*appmodel.App, appmodel.ChangeEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	if r == nil || r.db == nil {
		return nil, appmodel.ChangeEvent{}, ErrStorage
	}
	if err := validateAgentMetadata(input.Metadata); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	tx, err := begin(ctx, r.db)
	if err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	defer rollback(tx)
	current, err := loadAgentApp(ctx, tx, input.TenantID, input.AppID, true)
	if err != nil {
		return nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if err := mutableAgentApp(current, input.ExpectedVersion); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	if !current.CanTransitionTo(input.NextStatus) {
		return nil, appmodel.ChangeEvent{}, fmt.Errorf("%w: %s -> %s", appmodel.ErrInvalidTransition, current.Status, input.NextStatus)
	}
	now := monotonicNow(current.UpdatedAt)
	updated := current.Clone()
	previousRevision := cloneAgentInt64(updated.CurrentRevision)
	previousStatus := updated.Status
	updated.Status = input.NextStatus
	updated.Version++
	updated.UpdatedAt = now
	if err := updated.Validate(); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	digest := ""
	if current.CurrentRevision != nil {
		revision, revisionErr := loadAgentRevision(ctx, tx, input.TenantID, input.AppID, *current.CurrentRevision, true)
		if revisionErr != nil {
			return nil, appmodel.ChangeEvent{}, mapDBError(ctx, revisionErr, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
		}
		digest = revision.ContentDigest
	}
	event := appmodel.ChangeEvent{
		EventType: agentStatusEventType(input.NextStatus), TenantID: updated.TenantID, AppID: updated.AppID,
		PreviousRevision: previousRevision, CurrentRevision: cloneAgentInt64(updated.CurrentRevision),
		ContentDigest: digest, PreviousStatus: previousStatus, CurrentStatus: updated.Status,
		ActorType: strings.TrimSpace(input.Metadata.ActorType), ActorID: strings.TrimSpace(input.Metadata.ActorID),
		Reason: strings.TrimSpace(input.Metadata.Reason), CorrelationID: strings.TrimSpace(input.Metadata.CorrelationID),
		PreviousVersion: current.Version, NextVersion: updated.Version, OccurredAt: now,
	}
	result, err := tx.ExecContext(ctx, `UPDATE agent_app SET status = ?, version = ?, updated_at = ?
		WHERE tenant_id = ? AND app_id = ? AND version = ?`, string(updated.Status), updated.Version, updated.UpdatedAt,
		input.TenantID, input.AppID, input.ExpectedVersion)
	if err != nil {
		return nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if affected, affectedErr := result.RowsAffected(); affectedErr != nil || affected != 1 {
		return nil, appmodel.ChangeEvent{}, fmt.Errorf("%w: expected app version %d", appmodel.ErrConflict, input.ExpectedVersion)
	}
	eventResult, err := tx.ExecContext(ctx, `INSERT INTO agent_app_change_outbox (
		event_type, tenant_id, app_id, previous_status, current_status, previous_revision,
		current_revision, content_digest, actor_type, actor_id, reason, correlation_id,
		previous_version, next_version, occurred_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, string(event.EventType), event.TenantID,
		event.AppID, nullableText(string(event.PreviousStatus)), string(event.CurrentStatus), event.PreviousRevision,
		event.CurrentRevision, nullableText(event.ContentDigest), event.ActorType, event.ActorID, event.Reason,
		event.CorrelationID, event.PreviousVersion, event.NextVersion, event.OccurredAt)
	if err != nil {
		return nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	var eventID int64
	eventID, err = eventResult.LastInsertId()
	if err != nil {
		return nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	stored, err := loadAgentApp(ctx, tx, input.TenantID, input.AppID, false)
	if err != nil {
		return nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	committed, err := scanAgentEvent(tx.QueryRowContext(ctx, agentEventSelect+` WHERE event_id = ?`, eventID))
	if err != nil {
		return nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	return stored, committed, nil
}

func replaceRevisionTools(ctx context.Context, q queryer, revision appmodel.Revision) error {
	if _, err := q.ExecContext(ctx, "DELETE FROM agent_app_revision_tool WHERE tenant_id = ? AND app_id = ? AND revision = ?", revision.TenantID, revision.AppID, revision.Revision); err != nil {
		return mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	for _, tool := range revision.Tools {
		if _, err := q.ExecContext(ctx, `INSERT INTO agent_app_revision_tool (tenant_id, app_id, revision, tool_id, required) VALUES (?, ?, ?, ?, ?)`, revision.TenantID, revision.AppID, revision.Revision, tool.ToolID, tool.Required); err != nil {
			return mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
		}
	}
	return nil
}

const agentAppSelect = `SELECT tenant_id, app_id, app_key, display_name, description,
       status, current_revision, canary_revision, version, created_at, updated_at
FROM agent_app`

func loadAgentApp(ctx context.Context, q queryer, tenantID, appID string, forUpdate bool) (*appmodel.App, error) {
	query := agentAppSelect + ` WHERE tenant_id = ? AND app_id = ?`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	var value appmodel.App
	var status string
	var currentRevision, canaryRevision sql.NullInt64
	if err := q.QueryRowContext(ctx, query, tenantID, appID).Scan(&value.TenantID, &value.AppID,
		&value.AppKey, &value.DisplayName, &value.Description, &status, &currentRevision, &canaryRevision,
		&value.Version, &value.CreatedAt, &value.UpdatedAt); err != nil {
		return nil, err
	}
	value.Status = appmodel.Status(status)
	value.CurrentRevision = nullableInt(currentRevision)
	value.CanaryRevision = nullableInt(canaryRevision)
	value.CreatedAt = asUTC(value.CreatedAt)
	value.UpdatedAt = asUTC(value.UpdatedAt)
	if err := value.Validate(); err != nil {
		return nil, ErrStorage
	}
	clone := value.Clone()
	return &clone, nil
}

const agentRevisionSelect = `SELECT tenant_id, app_id, revision, state, draft_version,
       agent_kind, schema_version, description, instruction, global_instruction,
       model_profile_id, generation_config, runtime_policy, content_digest,
       published_at, created_at, updated_at
FROM agent_app_revision`

func loadAgentRevision(ctx context.Context, q queryer, tenantID, appID string, revision int64, forUpdate bool) (*appmodel.Revision, error) {
	query := agentRevisionSelect + ` WHERE tenant_id = ? AND app_id = ? AND revision = ?`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	var value appmodel.Revision
	var state, kind string
	var generation, runtime []byte
	var digest sql.NullString
	var publishedAt sql.NullTime
	if err := q.QueryRowContext(ctx, query, tenantID, appID, revision).Scan(
		&value.TenantID, &value.AppID, &value.Revision, &state, &value.DraftVersion,
		&kind, &value.SchemaVersion, &value.Description, &value.Instruction,
		&value.GlobalInstruction, &value.ModelProfileID, &generation, &runtime,
		&digest, &publishedAt, &value.CreatedAt, &value.UpdatedAt); err != nil {
		return nil, err
	}
	value.State = appmodel.RevisionState(state)
	value.Kind = appmodel.Kind(kind)
	if digest.Valid {
		value.ContentDigest = digest.String
	}
	if publishedAt.Valid {
		value.PublishedAt = timePointer(asUTC(publishedAt.Time))
	}
	if err := decodeAgentRevisionParts(generation, runtime, &value); err != nil {
		return nil, ErrStorage
	}
	toolsRows, err := q.QueryContext(ctx, `
		SELECT tool_id, required FROM agent_app_revision_tool
		WHERE tenant_id = ? AND app_id = ? AND revision = ? ORDER BY tool_id
	`, tenantID, appID, revision)
	if err != nil {
		return nil, err
	}
	defer func() { _ = toolsRows.Close() }()
	for toolsRows.Next() {
		var tool appmodel.ToolAuthorization
		if err := toolsRows.Scan(&tool.ToolID, &tool.Required); err != nil {
			return nil, err
		}
		value.Tools = append(value.Tools, tool)
	}
	if err := toolsRows.Err(); err != nil {
		return nil, err
	}
	value.CreatedAt = asUTC(value.CreatedAt)
	value.UpdatedAt = asUTC(value.UpdatedAt)
	if err := value.Validate(); err != nil {
		return nil, ErrStorage
	}
	clone := value.Clone()
	return &clone, nil
}

const agentEventSelect = `SELECT event_type, tenant_id, app_id,
       previous_status, current_status, previous_revision, current_revision,
       content_digest, actor_type, actor_id, reason, correlation_id,
       previous_version, next_version, occurred_at
FROM agent_app_change_outbox`

func scanAgentEvent(row rowScanner) (appmodel.ChangeEvent, error) {
	var event appmodel.ChangeEvent
	var eventType, currentStatus string
	var previousStatus sql.NullString
	var previousRevision, currentRevision sql.NullInt64
	var digest sql.NullString
	if err := row.Scan(&eventType, &event.TenantID, &event.AppID, &previousStatus, &currentStatus,
		&previousRevision, &currentRevision, &digest, &event.ActorType, &event.ActorID,
		&event.Reason, &event.CorrelationID, &event.PreviousVersion, &event.NextVersion,
		&event.OccurredAt); err != nil {
		return appmodel.ChangeEvent{}, err
	}
	event.EventType = appmodel.ChangeEventType(eventType)
	if previousStatus.Valid {
		event.PreviousStatus = appmodel.Status(previousStatus.String)
	}
	event.CurrentStatus = appmodel.Status(currentStatus)
	event.PreviousRevision = nullableInt64(previousRevision)
	event.CurrentRevision = nullableInt64(currentRevision)
	if digest.Valid {
		event.ContentDigest = digest.String
	}
	event.OccurredAt = asUTC(event.OccurredAt)
	return event, nil
}

func mutableAgentApp(app *appmodel.App, expected int64) error {
	if app.Status == appmodel.StatusDisabled {
		return appmodel.ErrDisabled
	}
	if expected != app.Version {
		return fmt.Errorf("%w: expected %d, got %d", appmodel.ErrConflict, expected, app.Version)
	}
	return nil
}

func assertTenantActive(ctx context.Context, q queryer, tenantID string) error {
	var status string
	if err := q.QueryRowContext(ctx, `SELECT status FROM tenant WHERE tenant_id = ? FOR SHARE`, tenantID).Scan(&status); err != nil {
		return mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if status != "active" {
		return fmt.Errorf("%w: tenant must be active", appmodel.ErrInvalid)
	}
	return nil
}

func validateAgentMetadata(metadata appmodel.ChangeMetadata) error {
	metadata.ActorType = strings.TrimSpace(metadata.ActorType)
	metadata.ActorID = strings.TrimSpace(metadata.ActorID)
	metadata.Reason = strings.TrimSpace(metadata.Reason)
	metadata.CorrelationID = strings.TrimSpace(metadata.CorrelationID)
	if metadata.ActorType == "" || metadata.ActorID == "" || metadata.Reason == "" || metadata.CorrelationID == "" {
		return fmt.Errorf("%w: change metadata requires actor, reason, and correlation ID", appmodel.ErrInvalid)
	}
	if len([]rune(metadata.Reason)) > 1000 {
		return fmt.Errorf("%w: change reason must contain at most 1000 characters", appmodel.ErrInvalid)
	}
	return nil
}

func agentStatusEventType(status appmodel.Status) appmodel.ChangeEventType {
	switch status {
	case appmodel.StatusSuspended:
		return appmodel.ChangeSuspended
	case appmodel.StatusActive:
		return appmodel.ChangeResumed
	default:
		return appmodel.ChangeDisabled
	}
}

func cloneAgentInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func sameAgentRevision(left, right *int64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func agentInt64(value int64) *int64 { return &value }

func nullableInt64(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	copy := value.Int64
	return &copy
}

func timePointer(value time.Time) *time.Time {
	copy := value
	return &copy
}

func maxTime(left, right time.Time) time.Time {
	if right.After(left) {
		return right
	}
	return left
}
