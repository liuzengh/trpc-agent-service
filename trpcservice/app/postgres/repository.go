// Package postgres provides the PostgreSQL implementation of the Agent App
// repository.
package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
)

// List returns a stable page of Apps belonging to one tenant.
func (r *AppRepository) List(ctx context.Context, tenantID, query, status, cursor string, limit int) ([]*appmodel.App, string, error) {
	if err := r.checkList(ctx); err != nil {
		return nil, "", err
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
	rows, err := r.db.QueryContext(ctx, agentAppSelect+` WHERE tenant_id = $1 ORDER BY app_id`, tenantID)
	if err != nil {
		return nil, "", mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	defer rows.Close()
	q := strings.ToLower(strings.TrimSpace(query))
	items := make([]*appmodel.App, 0)
	for rows.Next() {
		var v appmodel.App
		var st string
		var cur, can sql.NullInt64
		if err := rows.Scan(&v.TenantID, &v.AppID, &v.AppKey, &v.DisplayName, &v.Description, &st, &cur, &can, &v.Version, &v.CreatedAt, &v.UpdatedAt); err != nil {
			return nil, "", ErrStorage
		}
		v.Status = appmodel.Status(st)
		v.CurrentRevision = nullableInt(cur)
		v.CanaryRevision = nullableInt(can)
		v.CreatedAt = asUTC(v.CreatedAt)
		v.UpdatedAt = asUTC(v.UpdatedAt)
		if status != "" && st != status {
			continue
		}
		if q != "" && !strings.Contains(strings.ToLower(v.AppID+" "+v.AppKey+" "+v.DisplayName), q) {
			continue
		}
		if err := v.Validate(); err != nil {
			return nil, "", ErrStorage
		}
		clone := v.Clone()
		items = append(items, &clone)
	}
	if err := rows.Err(); err != nil {
		return nil, "", ErrStorage
	}
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

// ListRevisions returns a stable page of revisions belonging to one App.
func (r *AppRepository) ListRevisions(ctx context.Context, tenantID, appID, query, status, cursor string, limit int) ([]*appmodel.Revision, string, error) {
	if err := r.checkList(ctx); err != nil {
		return nil, "", err
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
	rows, err := r.db.QueryContext(ctx, `SELECT revision FROM public.agent_app_revision WHERE tenant_id=$1 AND app_id=$2 ORDER BY revision`, tenantID, appID)
	if err != nil {
		return nil, "", mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	revisions, err := scanRevisionNumbers(rows)
	if err != nil {
		return nil, "", ErrStorage
	}
	items := make([]*appmodel.Revision, 0)
	q := strings.ToLower(strings.TrimSpace(query))
	for _, number := range revisions {
		v, err := loadAgentRevision(ctx, r.db, tenantID, appID, number, false)
		if err != nil {
			return nil, "", ErrStorage
		}
		if status != "" && string(v.State) != status {
			continue
		}
		if q != "" && !strings.Contains(strings.ToLower(v.Description+" "+v.Instruction+" "+v.GlobalInstruction), q) {
			continue
		}
		items = append(items, v)
	}
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

func scanRevisionNumbers(rows *sql.Rows) ([]int64, error) {
	var revisions []int64
	for rows.Next() {
		var number int64
		if err := rows.Scan(&number); err != nil {
			_ = rows.Close()
			return nil, err
		}
		revisions = append(revisions, number)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	return revisions, rows.Close()
}

// AppRepository persists App roots, mutable drafts and immutable published
// revisions. It is named after the app domain rather than the external Agent
// framework so the storage implementation does not blur those ownerships.
type AppRepository struct {
	db *sql.DB
}

var _ appmodel.Repository = (*AppRepository)(nil)

// NewAppRepository creates an App repository over a PostgreSQL pool.
func NewAppRepository(db *sql.DB) *AppRepository { return &AppRepository{db: db} }

func (r *AppRepository) checkList(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || r.db == nil {
		return ErrStorage
	}
	return nil
}

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
	_, err = tx.ExecContext(ctx, `
		SELECT public.control_plane_create_agent_app($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`, value.TenantID, value.AppID, value.AppKey, value.DisplayName, value.Description,
		string(value.Status), value.Version, value.CreatedAt, value.UpdatedAt)
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
	// Keep preflight reads unlocked; the SECURITY DEFINER function acquires
	// tenant and app together in the canonical order before writing.
	current, err := loadAgentApp(ctx, tx, input.TenantID, input.AppID, false)
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
	_, err = tx.ExecContext(ctx, `
		SELECT public.control_plane_update_agent_metadata($1, $2, $3, $4, $5, $6)
	`, input.TenantID, input.AppID, input.ExpectedVersion, updated.DisplayName,
		updated.Description, updated.UpdatedAt)
	if err != nil {
		return nil, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
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
		SELECT COALESCE(MAX(revision), 0) + 1 FROM public.agent_app_revision
		WHERE tenant_id = $1 AND app_id = $2
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
	generation, runtime, tools, err := encodeAgentRevisionParts(*draft)
	if err != nil {
		return nil, ErrStorage
	}
	_, err = tx.ExecContext(ctx, `
		SELECT public.control_plane_create_agent_revision(
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
		)
	`, draft.TenantID, draft.AppID, draft.Revision, draft.DraftVersion, string(draft.Kind),
		draft.SchemaVersion, draft.Description, draft.Instruction, draft.GlobalInstruction,
		draft.ModelProfileID, generation, runtime, tools, draft.CreatedAt, draft.UpdatedAt,
		string(draft.State))
	if err != nil {
		return nil, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
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
	generation, runtime, tools, err := encodeAgentRevisionParts(*candidate)
	if err != nil {
		return nil, ErrStorage
	}
	_, err = tx.ExecContext(ctx, `
		SELECT public.control_plane_update_agent_draft(
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
		)
	`, candidate.TenantID, candidate.AppID, candidate.Revision, input.ExpectedDraftVersion,
		candidate.DraftVersion, candidate.Description, candidate.Instruction, candidate.GlobalInstruction,
		candidate.ModelProfileID, generation, runtime, tools, candidate.UpdatedAt, string(candidate.State),
		string(candidate.Kind), candidate.SchemaVersion)
	if err != nil {
		return nil, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
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
	var eventID int64
	err = tx.QueryRowContext(ctx, `
		SELECT public.control_plane_publish_agent_app(
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
		)
	`, input.TenantID, input.AppID, input.Revision, input.ExpectedAppVersion, input.ExpectedDraftVersion,
		published.ContentDigest, published.PublishedAt, published.UpdatedAt, updatedApp.Status,
		updatedApp.CurrentRevision, updatedApp.Version, updatedApp.UpdatedAt, event.PreviousStatus,
		event.CurrentStatus, event.PreviousRevision, event.CurrentRevision, event.ActorType,
		event.ActorID, event.Reason, event.CorrelationID).Scan(&eventID)
	if err != nil {
		return nil, nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	storedApp, err := loadAgentApp(ctx, tx, input.TenantID, input.AppID, false)
	if err != nil {
		return nil, nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	storedRevision, err := loadAgentRevision(ctx, tx, input.TenantID, input.AppID, input.Revision, false)
	if err != nil {
		return nil, nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	committed, err := scanAgentEvent(tx.QueryRowContext(ctx, agentEventSelect+` WHERE event_id = $1`, eventID))
	if err != nil {
		return nil, nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return nil, nil, appmodel.ChangeEvent{}, err
	}
	return storedApp, storedRevision, committed, nil
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
	var eventID int64
	err = tx.QueryRowContext(ctx, `
		SELECT public.control_plane_rollback_agent_app(
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
		)
	`, input.TenantID, input.AppID, input.TargetRevision, input.ExpectedAppVersion,
		updated.CurrentRevision, updated.Version, updated.UpdatedAt, event.ContentDigest,
		event.PreviousRevision, event.CurrentRevision, event.PreviousStatus, event.CurrentStatus,
		event.ActorType, event.ActorID, event.Reason, event.CorrelationID).Scan(&eventID)
	if err != nil {
		return nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	stored, err := loadAgentApp(ctx, tx, input.TenantID, input.AppID, false)
	if err != nil {
		return nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	committed, err := scanAgentEvent(tx.QueryRowContext(ctx, agentEventSelect+` WHERE event_id = $1`, eventID))
	if err != nil {
		return nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	return stored, committed, nil
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
	// Keep the preflight read unlocked; the SECURITY DEFINER function acquires
	// tenant and app together in the canonical order before writing.
	current, err := loadAgentApp(ctx, tx, input.TenantID, input.AppID, false)
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
		var getErr error
		candidate, getErr = loadAgentRevision(ctx, tx, input.TenantID, input.AppID, *input.CandidateRevision, false)
		if getErr != nil {
			return nil, appmodel.ChangeEvent{}, mapDBError(ctx, getErr, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
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
	var eventID int64
	err = tx.QueryRowContext(ctx, `SELECT public.control_plane_set_agent_app_canary($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`, input.TenantID, input.AppID, candidateRevision, *current.CurrentRevision, input.ExpectedAppVersion, updated.Version, updated.UpdatedAt, event.ContentDigest, event.PreviousRevision, event.CurrentRevision, event.EventType, event.ActorType, event.ActorID, event.Reason, event.CorrelationID).Scan(&eventID)
	if err != nil {
		return nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	stored, err := loadAgentApp(ctx, tx, input.TenantID, input.AppID, false)
	if err != nil {
		return nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	committed, err := scanAgentEvent(tx.QueryRowContext(ctx, agentEventSelect+` WHERE event_id = $1`, eventID))
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

func sameAgentRevision(left, right *int64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
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
	var eventID int64
	err = tx.QueryRowContext(ctx, `
		SELECT public.control_plane_transition_agent_app_status(
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
		)
	`, input.TenantID, input.AppID, input.ExpectedVersion, string(input.NextStatus),
		updated.UpdatedAt, event.PreviousRevision, event.CurrentRevision, event.ContentDigest,
		event.ActorType, event.ActorID, event.Reason, event.CorrelationID).Scan(&eventID)
	if err != nil {
		return nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	stored, err := loadAgentApp(ctx, tx, input.TenantID, input.AppID, false)
	if err != nil {
		return nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	committed, err := scanAgentEvent(tx.QueryRowContext(ctx, agentEventSelect+` WHERE event_id = $1`, eventID))
	if err != nil {
		return nil, appmodel.ChangeEvent{}, mapDBError(ctx, err, appmodel.ErrNotFound, appmodel.ErrDuplicateKey, appmodel.ErrConflict, appmodel.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	return stored, committed, nil
}

const agentAppSelect = `SELECT tenant_id, app_id, app_key, display_name, description,
       status, current_revision, canary_revision, version, created_at, updated_at
FROM public.agent_app`

func loadAgentApp(ctx context.Context, q queryer, tenantID, appID string, forUpdate bool) (*appmodel.App, error) {
	query := agentAppSelect + ` WHERE tenant_id = $1 AND app_id = $2`
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
FROM public.agent_app_revision`

func loadAgentRevision(ctx context.Context, q queryer, tenantID, appID string, revision int64, forUpdate bool) (*appmodel.Revision, error) {
	query := agentRevisionSelect + ` WHERE tenant_id = $1 AND app_id = $2 AND revision = $3`
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
		SELECT tool_id, required FROM public.agent_app_revision_tool
		WHERE tenant_id = $1 AND app_id = $2 AND revision = $3 ORDER BY tool_id
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
FROM public.agent_app_change_outbox`

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
	if err := q.QueryRowContext(ctx, `SELECT status FROM public.tenant WHERE tenant_id = $1 FOR SHARE`, tenantID).Scan(&status); err != nil {
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
