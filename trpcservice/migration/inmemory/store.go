package inmemory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Store struct {
	mu           sync.Mutex
	migrations   map[string]migration.Migration
	activeDomain map[string]string
	batches      map[string]migration.Batch
	intents      map[string]migration.PhaseIntent
}

func New() *Store {
	return &Store{migrations: make(map[string]migration.Migration), activeDomain: make(map[string]string),
		batches: make(map[string]migration.Batch), intents: make(map[string]migration.PhaseIntent)}
}

func (s *Store) Create(ctx context.Context, in migration.CreateRequest) (migration.Migration, error) {
	if err := ctx.Err(); err != nil {
		return migration.Migration{}, err
	}
	created, err := migration.NewMigration(in)
	if err != nil {
		return migration.Migration{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := migrationKey(in.TenantID, in.MigrationID)
	if existing, ok := s.migrations[key]; ok {
		if sameCreation(existing, created) {
			return cloneMigration(existing), nil
		}
		return migration.Migration{}, runtime.ErrIdempotencyCollision
	}
	domainKey := in.TenantID + "\x00" + in.Domain
	if _, exists := s.activeDomain[domainKey]; exists {
		return migration.Migration{}, runtime.ErrVersionConflict
	}
	s.migrations[key] = created
	s.activeDomain[domainKey] = in.MigrationID
	return cloneMigration(created), nil
}

func (s *Store) Get(ctx context.Context, tenantID, migrationID string) (migration.Migration, error) {
	if err := ctx.Err(); err != nil {
		return migration.Migration{}, err
	}
	if tenantID == "" || migrationID == "" {
		return migration.Migration{}, runtime.ErrTenantScope
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.migrations[migrationKey(tenantID, migrationID)]
	if !ok {
		return migration.Migration{}, runtime.ErrNotFound
	}
	return cloneMigration(value), nil
}

func (s *Store) List(ctx context.Context, tenantID, domain string) ([]migration.Migration, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if tenantID == "" || domain == "" {
		return nil, runtime.ErrTenantScope
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]migration.Migration, 0)
	for _, value := range s.migrations {
		if value.TenantID == tenantID && value.Domain == domain {
			result = append(result, cloneMigration(value))
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].MigrationID > result[j].MigrationID
		}
		return result[i].CreatedAt.After(result[j].CreatedAt)
	})
	return result, nil
}

func (s *Store) Transition(ctx context.Context, in migration.TransitionRequest) (migration.Migration, error) {
	if err := ctx.Err(); err != nil {
		return migration.Migration{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := migrationKey(in.TenantID, in.MigrationID)
	current, ok := s.migrations[key]
	if !ok {
		return migration.Migration{}, runtime.ErrNotFound
	}
	next, err := migration.ApplyTransition(current, in)
	if err != nil {
		return migration.Migration{}, err
	}
	s.migrations[key] = next
	if next.State == migration.StateCleanup {
		delete(s.activeDomain, next.TenantID+"\x00"+next.Domain)
	}
	return cloneMigration(next), nil
}

func (s *Store) CommitBatch(ctx context.Context, in migration.BatchRequest) (migration.BatchResult, error) {
	if err := ctx.Err(); err != nil {
		return migration.BatchResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	batchKey := migrationKey(in.TenantID, in.MigrationID) + "\x00" + in.BatchID
	if existing, ok := s.batches[batchKey]; ok {
		if !migration.SameBatch(existing, in) {
			return migration.BatchResult{}, runtime.ErrIdempotencyCollision
		}
		current, ok := s.migrations[migrationKey(in.TenantID, in.MigrationID)]
		if !ok {
			return migration.BatchResult{}, runtime.ErrNotFound
		}
		return cloneBatchResult(migration.BatchResult{Migration: current, Batch: existing}), nil
	}
	key := migrationKey(in.TenantID, in.MigrationID)
	current, ok := s.migrations[key]
	if !ok {
		return migration.BatchResult{}, runtime.ErrNotFound
	}
	next, batch, err := migration.ApplyBatch(current, in)
	if err != nil {
		return migration.BatchResult{}, err
	}
	result := migration.BatchResult{Migration: next, Batch: batch}
	s.migrations[key] = next
	s.batches[batchKey] = batch
	return cloneBatchResult(result), nil
}

func (s *Store) RecordVerification(ctx context.Context, in migration.VerificationRequest) (migration.Migration, error) {
	if err := ctx.Err(); err != nil {
		return migration.Migration{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := migrationKey(in.TenantID, in.MigrationID)
	current, ok := s.migrations[key]
	if !ok {
		return migration.Migration{}, runtime.ErrNotFound
	}
	next, err := migration.ApplyVerification(current, in)
	if err != nil {
		return migration.Migration{}, err
	}
	s.migrations[key] = next
	return cloneMigration(next), nil
}

func (s *Store) Pause(ctx context.Context, in migration.ControlRequest) (migration.Migration, error) {
	return s.control(ctx, in, migration.ApplyPause)
}

func (s *Store) Resume(ctx context.Context, in migration.ControlRequest) (migration.Migration, error) {
	return s.control(ctx, in, migration.ApplyResume)
}

func (s *Store) Abort(ctx context.Context, in migration.ControlRequest) (migration.Migration, error) {
	return s.control(ctx, in, migration.ApplyAbort)
}

func (s *Store) BeginPhaseIntent(ctx context.Context, in migration.TransitionRequest) (migration.PhaseIntent, error) {
	if err := ctx.Err(); err != nil {
		return migration.PhaseIntent{}, err
	}
	created, err := migration.NewPhaseIntent(in)
	if err != nil {
		return migration.PhaseIntent{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := intentKey(created.TenantID, created.MigrationID, created.IntentID)
	if existing, ok := s.intents[key]; ok {
		if existing.RequestDigest != created.RequestDigest {
			return migration.PhaseIntent{}, runtime.ErrIdempotencyCollision
		}
		return existing, nil
	}
	s.intents[key] = created
	return created, nil
}

func (s *Store) CompletePhaseIntent(ctx context.Context, in migration.PhaseIntent, result migration.Migration, completedAt time.Time) (migration.PhaseIntent, error) {
	if err := ctx.Err(); err != nil {
		return migration.PhaseIntent{}, err
	}
	if completedAt.IsZero() || result.TenantID != in.TenantID || result.MigrationID != in.MigrationID ||
		result.State != in.Request.To || result.Version != in.Request.ExpectedVersion+1 {
		return migration.PhaseIntent{}, runtime.ErrInvariantViolation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := intentKey(in.TenantID, in.MigrationID, in.IntentID)
	stored, ok := s.intents[key]
	if !ok {
		return migration.PhaseIntent{}, runtime.ErrNotFound
	}
	if stored.RequestDigest != in.RequestDigest {
		return migration.PhaseIntent{}, runtime.ErrIdempotencyCollision
	}
	if stored.Status == migration.PhaseIntentCompleted {
		if stored.ResultVersion != result.Version {
			return migration.PhaseIntent{}, runtime.ErrVersionConflict
		}
		return stored, nil
	}
	stored.Status = migration.PhaseIntentCompleted
	stored.ResultVersion = result.Version
	stored.CompletedAt = completedAt.UTC()
	s.intents[key] = stored
	return stored, nil
}

func (s *Store) ListPendingPhaseIntents(ctx context.Context, tenantID, migrationID string) ([]migration.PhaseIntent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if tenantID == "" || migrationID == "" {
		return nil, runtime.ErrTenantScope
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]migration.PhaseIntent, 0)
	for _, intent := range s.intents {
		if intent.TenantID == tenantID && intent.MigrationID == migrationID && intent.Status == migration.PhaseIntentPending {
			result = append(result, intent)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].IntentID < result[j].IntentID
		}
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result, nil
}

func (s *Store) control(ctx context.Context, in migration.ControlRequest, apply func(migration.Migration, migration.ControlRequest) (migration.Migration, error)) (migration.Migration, error) {
	if err := ctx.Err(); err != nil {
		return migration.Migration{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := migrationKey(in.TenantID, in.MigrationID)
	current, ok := s.migrations[key]
	if !ok {
		return migration.Migration{}, runtime.ErrNotFound
	}
	next, err := apply(current, in)
	if err != nil {
		return migration.Migration{}, err
	}
	s.migrations[key] = next
	if next.State == migration.StateAborted {
		delete(s.activeDomain, next.TenantID+"\x00"+next.Domain)
	}
	return cloneMigration(next), nil
}

func migrationKey(tenantID, migrationID string) string { return tenantID + "\x00" + migrationID }
func intentKey(tenantID, migrationID, intentID string) string {
	return migrationKey(tenantID, migrationID) + "\x00" + intentID
}

func sameCreation(left, right migration.Migration) bool {
	return left.TenantID == right.TenantID && left.MigrationID == right.MigrationID && left.Domain == right.Domain &&
		left.Epoch == right.Epoch && left.Source == right.Source && left.Target == right.Target && left.CreatedAt.Equal(right.CreatedAt)
}

func cloneMigration(value migration.Migration) migration.Migration       { return value }
func cloneBatchResult(value migration.BatchResult) migration.BatchResult { return value }

var _ migration.Repository = (*Store)(nil)
var _ migration.PhaseIntentStore = (*Store)(nil)
