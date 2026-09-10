// Package inmemory provides the single-process Agent App repository.
package inmemory

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
)

// List returns a stable page of Apps belonging to one tenant.
func (r *InMemoryRepository) List(ctx context.Context, tenantID, query, status, cursor string, limit int) ([]*appmodel.App, string, error) {
	if err := checkContext(ctx); err != nil {
		return nil, "", err
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	offset, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	if err := r.mu.rlock(ctx); err != nil {
		return nil, "", err
	}
	defer r.mu.runlock()
	query, status = strings.ToLower(strings.TrimSpace(query)), strings.TrimSpace(status)
	items := make([]*appmodel.App, 0)
	for scope, value := range r.apps {
		if scope.tenantID != tenantID || (status != "" && string(value.Status) != status) {
			continue
		}
		if query != "" && !strings.Contains(strings.ToLower(value.AppID+" "+value.AppKey+" "+value.DisplayName), query) {
			continue
		}
		items = append(items, cloneApp(value))
	}
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
		next = encodeCursor(end)
	}
	return items[offset:end], next, nil
}

// ListRevisions returns revisions for one App using stable numeric ordering.
func (r *InMemoryRepository) ListRevisions(ctx context.Context, tenantID, appID, query, status, cursor string, limit int) ([]*appmodel.Revision, string, error) {
	if err := checkContext(ctx); err != nil {
		return nil, "", err
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	offset, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	if err := r.mu.rlock(ctx); err != nil {
		return nil, "", err
	}
	defer r.mu.runlock()
	values := r.revisions[appScope{tenantID: tenantID, appID: appID}]
	items := make([]*appmodel.Revision, 0, len(values))
	status = strings.TrimSpace(status)
	query = strings.ToLower(strings.TrimSpace(query))
	for _, value := range values {
		if status != "" && string(value.State) != status {
			continue
		}
		if query != "" && !strings.Contains(strings.ToLower(value.Description+" "+value.Instruction+" "+value.GlobalInstruction), query) {
			continue
		}
		items = append(items, cloneRevision(value))
	}
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
		next = encodeCursor(end)
	}
	return items[offset:end], next, nil
}

func decodeCursor(cursor string) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	var offset int
	if _, err := fmt.Sscanf(cursor, "%d", &offset); err != nil || offset < 0 {
		return 0, fmt.Errorf("invalid cursor")
	}
	return offset, nil
}
func encodeCursor(offset int) string { return fmt.Sprintf("%d", offset) }

type appScope struct {
	tenantID string
	appID    string
}

type keyScope struct {
	tenantID string
	appKey   string
}

// InMemoryRepository is a thread-safe, tenant-scoped development repository.
// It does not provide durability or cross-process consistency.
type InMemoryRepository struct {
	mu        contextRWMutex
	apps      map[appScope]*appmodel.App
	byKey     map[keyScope]string
	revisions map[appScope]map[int64]*appmodel.Revision
	next      map[appScope]int64
}

// NewInMemoryRepository creates an empty repository.
func NewInMemoryRepository() *InMemoryRepository {
	return &InMemoryRepository{
		apps:      make(map[appScope]*appmodel.App),
		byKey:     make(map[keyScope]string),
		revisions: make(map[appScope]map[int64]*appmodel.Revision),
		next:      make(map[appScope]int64),
	}
}

// NewRepository is the concise constructor for the InMemory implementation.
func NewRepository() *InMemoryRepository { return NewInMemoryRepository() }

var _ appmodel.Repository = (*InMemoryRepository)(nil)

// Create stores a new agent application in memory.
func (r *InMemoryRepository) Create(ctx context.Context, input appmodel.CreateInput) (*appmodel.App, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	app, err := appmodel.NewApp(input)
	if err != nil {
		return nil, err
	}
	if err := r.mu.lock(ctx); err != nil {
		return nil, err
	}
	defer r.mu.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	scope := appScope{tenantID: app.TenantID, appID: app.AppID}
	key := keyScope{tenantID: app.TenantID, appKey: app.AppKey}
	if _, exists := r.apps[scope]; exists {
		return nil, fmt.Errorf("%w: %s", appmodel.ErrDuplicateKey, app.AppID)
	}
	if _, exists := r.byKey[key]; exists {
		return nil, fmt.Errorf("%w: %s", appmodel.ErrDuplicateKey, app.AppKey)
	}
	copy := app.Clone()
	r.apps[scope] = &copy
	r.byKey[key] = app.AppID
	r.revisions[scope] = make(map[int64]*appmodel.Revision)
	return cloneApp(app), nil
}

// Get loads an agent application within the requested tenant.
func (r *InMemoryRepository) Get(ctx context.Context, tenantID, appID string) (*appmodel.App, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := r.mu.rlock(ctx); err != nil {
		return nil, err
	}
	defer r.mu.runlock()
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	app, err := r.getLocked(tenantID, appID)
	if err != nil {
		return nil, err
	}
	return cloneApp(app), nil
}

// UpdateMetadata applies an expected-version application metadata update.
func (r *InMemoryRepository) UpdateMetadata(ctx context.Context, input appmodel.UpdateMetadataInput) (*appmodel.App, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := r.mu.lock(ctx); err != nil {
		return nil, err
	}
	defer r.mu.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	app, err := r.mutableAppLocked(input.TenantID, input.AppID, input.ExpectedVersion)
	if err != nil {
		return nil, err
	}
	updated := app.Clone()
	updated.DisplayName = strings.TrimSpace(input.DisplayName)
	updated.Description = strings.TrimSpace(input.Description)
	updated.Version++
	updated.UpdatedAt = nextTime(app.UpdatedAt)
	if err := updated.Validate(); err != nil {
		return nil, err
	}
	r.storeAppLocked(&updated)
	return cloneApp(&updated), nil
}

// CreateDraft stores a draft revision for an agent application.
func (r *InMemoryRepository) CreateDraft(ctx context.Context, input appmodel.CreateDraftInput) (*appmodel.Revision, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := r.mu.lock(ctx); err != nil {
		return nil, err
	}
	defer r.mu.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if _, err := r.mutableAppLocked(input.TenantID, input.AppID, input.ExpectedAppVersion); err != nil {
		return nil, err
	}
	scope := appScope{tenantID: input.TenantID, appID: input.AppID}
	number := r.next[scope] + 1
	draft, err := appmodel.NewRevision(appmodel.CreateRevisionInput{
		TenantID: input.TenantID, AppID: input.AppID, Revision: number,
		Kind: input.Kind, SchemaVersion: input.SchemaVersion, Configuration: input.Configuration,
	})
	if err != nil {
		return nil, err
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	copy := draft.Clone()
	r.revisions[scope][number] = &copy
	r.next[scope] = number
	return cloneRevision(draft), nil
}

// UpdateDraft applies an expected-version draft update in memory.
func (r *InMemoryRepository) UpdateDraft(ctx context.Context, input appmodel.UpdateDraftInput) (*appmodel.Revision, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := r.mu.lock(ctx); err != nil {
		return nil, err
	}
	defer r.mu.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if _, err := r.mutableAppLocked(input.TenantID, input.AppID, input.ExpectedAppVersion); err != nil {
		return nil, err
	}
	existing, err := r.revisionLocked(input.TenantID, input.AppID, input.Revision)
	if err != nil {
		return nil, err
	}
	if existing.State != appmodel.RevisionStateDraft {
		return nil, appmodel.ErrImmutableRevision
	}
	if input.ExpectedDraftVersion != existing.DraftVersion {
		return nil, conflict(input.ExpectedDraftVersion, existing.DraftVersion)
	}
	candidate, err := appmodel.NewRevision(appmodel.CreateRevisionInput{
		TenantID: input.TenantID, AppID: input.AppID, Revision: input.Revision,
		Kind: existing.Kind, SchemaVersion: existing.SchemaVersion, Configuration: input.Configuration,
	})
	if err != nil {
		return nil, err
	}
	candidate.DraftVersion = existing.DraftVersion + 1
	candidate.CreatedAt = existing.CreatedAt
	candidate.UpdatedAt = nextTime(existing.UpdatedAt)
	if err := candidate.Validate(); err != nil {
		return nil, err
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	copy := candidate.Clone()
	r.revisions[appScope{tenantID: input.TenantID, appID: input.AppID}][input.Revision] = &copy
	return cloneRevision(candidate), nil
}

// GetRevision loads a specific in-memory application revision.
func (r *InMemoryRepository) GetRevision(ctx context.Context, tenantID, appID string, revision int64) (*appmodel.Revision, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := r.mu.rlock(ctx); err != nil {
		return nil, err
	}
	defer r.mu.runlock()
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	value, err := r.revisionLocked(tenantID, appID, revision)
	if err != nil {
		return nil, err
	}
	return cloneRevision(value), nil
}

// Publish makes a draft revision active in memory.
func (r *InMemoryRepository) Publish(ctx context.Context, input appmodel.PublishInput) (*appmodel.App, *appmodel.Revision, appmodel.ChangeEvent, error) {
	if err := checkContext(ctx); err != nil {
		return nil, nil, appmodel.ChangeEvent{}, err
	}
	if err := validateChange(input.Metadata); err != nil {
		return nil, nil, appmodel.ChangeEvent{}, err
	}
	if !input.TenantActive {
		return nil, nil, appmodel.ChangeEvent{}, fmt.Errorf("%w: tenant must be active", appmodel.ErrInvalid)
	}
	if err := r.mu.lock(ctx); err != nil {
		return nil, nil, appmodel.ChangeEvent{}, err
	}
	defer r.mu.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, nil, appmodel.ChangeEvent{}, err
	}
	app, err := r.mutableAppLocked(input.TenantID, input.AppID, input.ExpectedAppVersion)
	if err != nil {
		return nil, nil, appmodel.ChangeEvent{}, err
	}
	draft, err := r.revisionLocked(input.TenantID, input.AppID, input.Revision)
	if err != nil {
		return nil, nil, appmodel.ChangeEvent{}, err
	}
	if draft.State != appmodel.RevisionStateDraft {
		return nil, nil, appmodel.ChangeEvent{}, appmodel.ErrImmutableRevision
	}
	if input.ExpectedDraftVersion != draft.DraftVersion {
		return nil, nil, appmodel.ChangeEvent{}, conflict(input.ExpectedDraftVersion, draft.DraftVersion)
	}
	now := nextTime(maxTime(app.UpdatedAt, draft.UpdatedAt))
	published, err := draft.Publish(now)
	if err != nil {
		return nil, nil, appmodel.ChangeEvent{}, err
	}
	updated := app.Clone()
	previousStatus := updated.Status
	previousRevision := cloneInt64(updated.CurrentRevision)
	updated.CurrentRevision = int64Pointer(input.Revision)
	updated.CanaryRevision = nil
	if updated.Status == appmodel.StatusDraft {
		updated.Status = appmodel.StatusActive
	}
	updated.Version++
	updated.UpdatedAt = now
	if err := updated.Validate(); err != nil {
		return nil, nil, appmodel.ChangeEvent{}, err
	}
	event := newEvent(appmodel.ChangePublished, &updated, previousStatus, previousRevision, published.ContentDigest, input.Metadata, app.Version, now)
	if err := checkContext(ctx); err != nil {
		return nil, nil, appmodel.ChangeEvent{}, err
	}
	r.storeAppLocked(&updated)
	copy := published.Clone()
	r.revisions[appScope{tenantID: input.TenantID, appID: input.AppID}][input.Revision] = &copy
	return cloneApp(&updated), cloneRevision(&published), event.Clone(), nil
}

// Rollback restores an earlier published revision in memory.
func (r *InMemoryRepository) Rollback(ctx context.Context, input appmodel.RollbackInput) (*appmodel.App, appmodel.ChangeEvent, error) {
	if err := checkContext(ctx); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	if err := validateChange(input.Metadata); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	if err := r.mu.lock(ctx); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	defer r.mu.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	app, err := r.mutableAppLocked(input.TenantID, input.AppID, input.ExpectedAppVersion)
	if err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	target, err := r.revisionLocked(input.TenantID, input.AppID, input.TargetRevision)
	if err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	if target.State != appmodel.RevisionStatePublished {
		return nil, appmodel.ChangeEvent{}, fmt.Errorf("%w: rollback target must be published", appmodel.ErrInvalid)
	}
	if app.CurrentRevision == nil || *app.CurrentRevision == input.TargetRevision {
		return nil, appmodel.ChangeEvent{}, fmt.Errorf("%w: rollback must change the current revision", appmodel.ErrInvalid)
	}
	now := nextTime(app.UpdatedAt)
	updated := app.Clone()
	previous := cloneInt64(updated.CurrentRevision)
	updated.CurrentRevision = int64Pointer(input.TargetRevision)
	updated.CanaryRevision = nil
	updated.Version++
	updated.UpdatedAt = now
	if err := updated.Validate(); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	event := newEvent(appmodel.ChangeRolledBack, &updated, app.Status, previous, target.ContentDigest, input.Metadata, app.Version, now)
	if err := checkContext(ctx); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	r.storeAppLocked(&updated)
	return cloneApp(&updated), event.Clone(), nil
}

// SetCanary selects or clears a published candidate revision for all future
// executions of one tenant-scoped App. Existing execution snapshots remain
// unchanged.
func (r *InMemoryRepository) SetCanary(ctx context.Context, input appmodel.SetCanaryInput) (*appmodel.App, appmodel.ChangeEvent, error) {
	if err := checkContext(ctx); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	if err := validateChange(input.Metadata); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	if !input.TenantActive {
		return nil, appmodel.ChangeEvent{}, fmt.Errorf("%w: tenant must be active", appmodel.ErrInvalid)
	}
	if err := r.mu.lock(ctx); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	defer r.mu.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	app, err := r.mutableAppLocked(input.TenantID, input.AppID, input.ExpectedAppVersion)
	if err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	if app.Status != appmodel.StatusActive {
		return nil, appmodel.ChangeEvent{}, fmt.Errorf("%w: canary requires an active app", appmodel.ErrInvalid)
	}
	var candidate *appmodel.Revision
	if input.CandidateRevision != nil {
		if *input.CandidateRevision < 1 {
			return nil, appmodel.ChangeEvent{}, fmt.Errorf("%w: candidate revision must be positive", appmodel.ErrInvalid)
		}
		var getErr error
		candidate, getErr = r.revisionLocked(input.TenantID, input.AppID, *input.CandidateRevision)
		if getErr != nil {
			return nil, appmodel.ChangeEvent{}, getErr
		}
		if candidate.State != appmodel.RevisionStatePublished {
			return nil, appmodel.ChangeEvent{}, fmt.Errorf("%w: canary revision must be published", appmodel.ErrInvalid)
		}
		if app.CurrentRevision == nil || *app.CurrentRevision == *input.CandidateRevision {
			return nil, appmodel.ChangeEvent{}, fmt.Errorf("%w: canary revision must differ from current revision", appmodel.ErrInvalid)
		}
	}
	previous := cloneInt64(app.CanaryRevision)
	if sameRevision(previous, input.CandidateRevision) {
		return nil, appmodel.ChangeEvent{}, fmt.Errorf("%w: canary revision is unchanged", appmodel.ErrInvalid)
	}
	now := nextTime(app.UpdatedAt)
	updated := app.Clone()
	updated.CanaryRevision = cloneInt64(input.CandidateRevision)
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
	if updated.CanaryRevision != nil {
		eventType = appmodel.ChangeCanaryStarted
	}
	event := newCanaryEvent(eventType, &updated, app.CanaryRevision, digest, input.Metadata, app.Version, now)
	r.storeAppLocked(&updated)
	return cloneApp(&updated), event.Clone(), nil
}

func sameRevision(left, right *int64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

// TransitionStatus changes an application status with optimistic concurrency.
func (r *InMemoryRepository) TransitionStatus(ctx context.Context, input appmodel.TransitionStatusInput) (*appmodel.App, appmodel.ChangeEvent, error) {
	if err := checkContext(ctx); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	if err := validateChange(input.Metadata); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	if err := r.mu.lock(ctx); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	defer r.mu.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	app, err := r.mutableAppLocked(input.TenantID, input.AppID, input.ExpectedVersion)
	if err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	if !app.CanTransitionTo(input.NextStatus) {
		return nil, appmodel.ChangeEvent{}, fmt.Errorf("%w: %s -> %s", appmodel.ErrInvalidTransition, app.Status, input.NextStatus)
	}
	now := nextTime(app.UpdatedAt)
	updated := app.Clone()
	previousStatus := updated.Status
	updated.Status = input.NextStatus
	updated.Version++
	updated.UpdatedAt = now
	if err := updated.Validate(); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	digest := ""
	if updated.CurrentRevision != nil {
		current, err := r.revisionLocked(input.TenantID, input.AppID, *updated.CurrentRevision)
		if err != nil {
			return nil, appmodel.ChangeEvent{}, err
		}
		digest = current.ContentDigest
	}
	event := newEvent(statusEventType(input.NextStatus), &updated, previousStatus, cloneInt64(app.CurrentRevision), digest, input.Metadata, app.Version, now)
	if err := checkContext(ctx); err != nil {
		return nil, appmodel.ChangeEvent{}, err
	}
	r.storeAppLocked(&updated)
	return cloneApp(&updated), event.Clone(), nil
}

func (r *InMemoryRepository) getLocked(tenantID, appID string) (*appmodel.App, error) {
	app, ok := r.apps[appScope{tenantID: tenantID, appID: appID}]
	if !ok {
		return nil, fmt.Errorf("%w: tenant %s app %s", appmodel.ErrNotFound, tenantID, appID)
	}
	return app, nil
}

func (r *InMemoryRepository) mutableAppLocked(tenantID, appID string, expected int64) (*appmodel.App, error) {
	app, err := r.getLocked(tenantID, appID)
	if err != nil {
		return nil, err
	}
	if app.Status == appmodel.StatusDisabled {
		return nil, appmodel.ErrDisabled
	}
	if expected != app.Version {
		return nil, conflict(expected, app.Version)
	}
	return app, nil
}

func (r *InMemoryRepository) revisionLocked(tenantID, appID string, revision int64) (*appmodel.Revision, error) {
	scope := appScope{tenantID: tenantID, appID: appID}
	if _, err := r.getLocked(tenantID, appID); err != nil {
		return nil, err
	}
	value, ok := r.revisions[scope][revision]
	if !ok {
		return nil, fmt.Errorf("%w: tenant %s app %s revision %d", appmodel.ErrNotFound, tenantID, appID, revision)
	}
	return value, nil
}

func (r *InMemoryRepository) storeAppLocked(app *appmodel.App) {
	copy := app.Clone()
	r.apps[appScope{tenantID: app.TenantID, appID: app.AppID}] = &copy
}

func validateChange(metadata appmodel.ChangeMetadata) error {
	reason := strings.TrimSpace(metadata.Reason)
	if strings.TrimSpace(metadata.ActorType) == "" || strings.TrimSpace(metadata.ActorID) == "" || reason == "" || strings.TrimSpace(metadata.CorrelationID) == "" {
		return fmt.Errorf("%w: change metadata requires actor, reason, and correlation ID", appmodel.ErrInvalid)
	}
	if len([]rune(reason)) > 1000 {
		return fmt.Errorf("%w: change reason must contain at most 1000 characters", appmodel.ErrInvalid)
	}
	return nil
}

func newEvent(eventType appmodel.ChangeEventType, app *appmodel.App, previousStatus appmodel.Status, previousRevision *int64, digest string, metadata appmodel.ChangeMetadata, previousVersion int64, at time.Time) appmodel.ChangeEvent {
	return appmodel.ChangeEvent{
		EventType: eventType, TenantID: app.TenantID, AppID: app.AppID,
		PreviousRevision: cloneInt64(previousRevision), CurrentRevision: cloneInt64(app.CurrentRevision),
		ContentDigest: digest, PreviousStatus: previousStatus, CurrentStatus: app.Status,
		ActorType: strings.TrimSpace(metadata.ActorType), ActorID: strings.TrimSpace(metadata.ActorID),
		Reason: strings.TrimSpace(metadata.Reason), CorrelationID: strings.TrimSpace(metadata.CorrelationID),
		PreviousVersion: previousVersion, NextVersion: app.Version, OccurredAt: at,
	}
}

func newCanaryEvent(eventType appmodel.ChangeEventType, app *appmodel.App, previousRevision *int64, digest string, metadata appmodel.ChangeMetadata, previousVersion int64, at time.Time) appmodel.ChangeEvent {
	event := newEvent(eventType, app, app.Status, previousRevision, digest, metadata, previousVersion, at)
	event.CurrentRevision = cloneInt64(app.CanaryRevision)
	return event
}

func statusEventType(next appmodel.Status) appmodel.ChangeEventType {
	switch next {
	case appmodel.StatusSuspended:
		return appmodel.ChangeSuspended
	case appmodel.StatusActive:
		return appmodel.ChangeResumed
	default:
		return appmodel.ChangeDisabled
	}
}

func conflict(expected, actual int64) error {
	return fmt.Errorf("%w: expected %d, got %d", appmodel.ErrConflict, expected, actual)
}

func checkContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func cloneApp(app *appmodel.App) *appmodel.App {
	if app == nil {
		return nil
	}
	copy := app.Clone()
	return &copy
}

func cloneRevision(revision *appmodel.Revision) *appmodel.Revision {
	if revision == nil {
		return nil
	}
	copy := revision.Clone()
	return &copy
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func int64Pointer(value int64) *int64 { return &value }

func maxTime(left, right time.Time) time.Time {
	if right.After(left) {
		return right
	}
	return left
}

func nextTime(previous time.Time) time.Time {
	now := time.Now().UTC()
	if now.Before(previous) {
		return previous
	}
	return now
}
