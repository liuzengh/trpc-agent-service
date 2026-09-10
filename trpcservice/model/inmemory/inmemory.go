// Package inmemory provides the single-process Model Profile repository.
package inmemory

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
)

// List returns a stable page of Model Profiles in one tenant.
func (r *InMemoryRepository) List(ctx context.Context, tenantID, query, status, cursor string, limit int) ([]*modelprofile.Profile, string, error) {
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
	if err := r.rLock(ctx); err != nil {
		return nil, "", err
	}
	defer r.rUnlock()
	query, status = strings.ToLower(strings.TrimSpace(query)), strings.TrimSpace(status)
	items := make([]*modelprofile.Profile, 0)
	for scope, value := range r.byID {
		if scope.tenantID != tenantID || (status != "" && string(value.Status) != status) {
			continue
		}
		if query != "" && !strings.Contains(strings.ToLower(value.ProfileID+" "+value.ProfileKey+" "+value.DisplayName), query) {
			continue
		}
		items = append(items, cloneProfile(value))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ProfileID < items[j].ProfileID })
	if offset >= len(items) {
		return []*modelprofile.Profile{}, "", nil
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

type profileScope struct {
	tenantID  string
	profileID string
}

type keyScope struct {
	tenantID   string
	profileKey string
}

// InMemoryRepository is a tenant-scoped repository for development and tests.
// It does not provide durability or cross-process consistency.
type InMemoryRepository struct {
	mu      contextRWMutex
	catalog *modelprofile.ProviderCatalog
	byID    map[profileScope]*modelprofile.Profile
	byKey   map[keyScope]string
}

// NewInMemoryRepository creates an empty repository using catalog as the
// trusted schema boundary for every write.
func NewInMemoryRepository(catalog *modelprofile.ProviderCatalog) *InMemoryRepository {
	return &InMemoryRepository{
		catalog: catalog,
		byID:    make(map[profileScope]*modelprofile.Profile),
		byKey:   make(map[keyScope]string),
	}
}

// NewRepository is the concise constructor for the InMemory implementation.
func NewRepository(catalog *modelprofile.ProviderCatalog) *InMemoryRepository {
	return NewInMemoryRepository(catalog)
}

var _ modelprofile.Repository = (*InMemoryRepository)(nil)

// Create validates and atomically stores a Profile and its created event.
func (r *InMemoryRepository) Create(ctx context.Context, input modelprofile.CreateInput) (*modelprofile.Profile, modelprofile.ChangeEvent, error) {
	if err := checkContext(ctx); err != nil {
		return nil, modelprofile.ChangeEvent{}, err
	}
	profile, err := modelprofile.NewProfile(input, r.catalog)
	if err != nil {
		return nil, modelprofile.ChangeEvent{}, err
	}
	event, err := modelprofile.PrepareCreatedChange(*profile, r.catalog, input.Metadata)
	if err != nil {
		return nil, modelprofile.ChangeEvent{}, err
	}
	if err := r.lock(ctx); err != nil {
		return nil, modelprofile.ChangeEvent{}, err
	}
	defer r.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, modelprofile.ChangeEvent{}, err
	}
	idScope := profileScope{tenantID: profile.TenantID, profileID: profile.ProfileID}
	keyScope := keyScope{tenantID: profile.TenantID, profileKey: profile.ProfileKey}
	if _, exists := r.byID[idScope]; exists {
		return nil, modelprofile.ChangeEvent{}, fmt.Errorf("%w: generated profile identity collision", modelprofile.ErrDuplicateKey)
	}
	if _, exists := r.byKey[keyScope]; exists {
		return nil, modelprofile.ChangeEvent{}, fmt.Errorf("%w: profile key already exists in tenant", modelprofile.ErrDuplicateKey)
	}
	if err := checkContext(ctx); err != nil {
		return nil, modelprofile.ChangeEvent{}, err
	}
	stored := profile.Clone()
	r.byID[idScope] = &stored
	r.byKey[keyScope] = profile.ProfileID
	return cloneProfile(profile), event.Clone(), nil
}

// Get returns a defensive copy scoped by tenant and Profile identity.
func (r *InMemoryRepository) Get(ctx context.Context, tenantID, profileID string) (*modelprofile.Profile, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := r.rLock(ctx); err != nil {
		return nil, err
	}
	defer r.rUnlock()
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	profile, exists := r.byID[profileScope{tenantID: tenantID, profileID: profileID}]
	if !exists {
		return nil, modelprofile.ErrNotFound
	}
	return cloneProfile(profile), nil
}

// UpdateConfiguration atomically replaces mutable configuration and emits an event.
func (r *InMemoryRepository) UpdateConfiguration(ctx context.Context, input modelprofile.UpdateConfigurationInput) (*modelprofile.Profile, modelprofile.ChangeEvent, error) {
	if err := checkContext(ctx); err != nil {
		return nil, modelprofile.ChangeEvent{}, err
	}
	if err := r.lock(ctx); err != nil {
		return nil, modelprofile.ChangeEvent{}, err
	}
	defer r.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, modelprofile.ChangeEvent{}, err
	}
	scope := profileScope{tenantID: input.TenantID, profileID: input.ProfileID}
	current, exists := r.byID[scope]
	if !exists {
		return nil, modelprofile.ChangeEvent{}, modelprofile.ErrNotFound
	}
	updated, event, err := modelprofile.PrepareConfigurationChange(*current, input, r.catalog, nextTime(current.UpdatedAt))
	if err != nil {
		return nil, modelprofile.ChangeEvent{}, err
	}
	if err := checkContext(ctx); err != nil {
		return nil, modelprofile.ChangeEvent{}, err
	}
	stored := updated.Clone()
	r.byID[scope] = &stored
	return cloneProfile(&updated), event.Clone(), nil
}

// TransitionStatus atomically applies a lifecycle transition and emits an event.
func (r *InMemoryRepository) TransitionStatus(ctx context.Context, input modelprofile.TransitionStatusInput) (*modelprofile.Profile, modelprofile.ChangeEvent, error) {
	if err := checkContext(ctx); err != nil {
		return nil, modelprofile.ChangeEvent{}, err
	}
	if err := r.lock(ctx); err != nil {
		return nil, modelprofile.ChangeEvent{}, err
	}
	defer r.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, modelprofile.ChangeEvent{}, err
	}
	scope := profileScope{tenantID: input.TenantID, profileID: input.ProfileID}
	current, exists := r.byID[scope]
	if !exists {
		return nil, modelprofile.ChangeEvent{}, modelprofile.ErrNotFound
	}
	updated, event, err := modelprofile.PrepareStatusChange(*current, input, r.catalog, nextTime(current.UpdatedAt))
	if err != nil {
		return nil, modelprofile.ChangeEvent{}, err
	}
	if err := checkContext(ctx); err != nil {
		return nil, modelprofile.ChangeEvent{}, err
	}
	stored := updated.Clone()
	r.byID[scope] = &stored
	return cloneProfile(&updated), event.Clone(), nil
}

func cloneProfile(profile *modelprofile.Profile) *modelprofile.Profile {
	if profile == nil {
		return nil
	}
	clone := profile.Clone()
	return &clone
}

func nextTime(previous time.Time) time.Time {
	now := time.Now().UTC()
	if now.After(previous) {
		return now
	}
	return previous.Add(time.Nanosecond)
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func (r *InMemoryRepository) lock(ctx context.Context) error  { return r.mu.lock(ctx) }
func (r *InMemoryRepository) unlock()                         { r.mu.unlock() }
func (r *InMemoryRepository) rLock(ctx context.Context) error { return r.mu.rlock(ctx) }
func (r *InMemoryRepository) rUnlock()                        { r.mu.runlock() }
