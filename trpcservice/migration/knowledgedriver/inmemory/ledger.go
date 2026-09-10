// Package inmemory implements the Knowledge migration ledger for contracts.
package inmemory

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Ledger struct {
	mu    sync.Mutex
	items map[string]knowledgedriver.Mutation
}

func New() *Ledger { return &Ledger{items: make(map[string]knowledgedriver.Mutation)} }

func (l *Ledger) Record(ctx context.Context, in knowledgedriver.RecordRequest) (knowledgedriver.Mutation, error) {
	if err := ctx.Err(); err != nil {
		return knowledgedriver.Mutation{}, err
	}
	if in.Direction == "" {
		in.Direction = knowledgedriver.DirectionForward
	}
	if !validRecord(in) {
		return knowledgedriver.Mutation{}, runtime.ErrInvariantViolation
	}
	value := knowledgedriver.Mutation{TenantID: in.TenantID, MigrationID: in.MigrationID, MutationID: in.MutationID,
		Epoch: in.Epoch, Direction: in.Direction, Key: in.Key, Operation: in.Operation, SourceRevision: in.SourceRevision,
		MutationDigest: in.MutationDigest, State: knowledgedriver.MutationPending,
		CreatedAt: in.CreatedAt.UTC().Truncate(time.Microsecond), NotBefore: in.CreatedAt.UTC().Truncate(time.Microsecond), Version: 1}
	l.mu.Lock()
	defer l.mu.Unlock()
	key := mutationKey(value.TenantID, value.MigrationID, value.MutationID)
	if existing, ok := l.items[key]; ok {
		if same(existing, value) {
			return existing, nil
		}
		return knowledgedriver.Mutation{}, runtime.ErrIdempotencyCollision
	}
	l.items[key] = value
	return value, nil
}

func (l *Ledger) Claim(ctx context.Context, in knowledgedriver.ClaimRequest) ([]knowledgedriver.Mutation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if in.TenantID == "" || in.MigrationID == "" || in.WorkerID == "" || in.Limit < 1 || in.Now.IsZero() || in.Lease <= 0 {
		return nil, runtime.ErrInvariantViolation
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	keys := make([]string, 0)
	for key, item := range l.items {
		if item.TenantID != in.TenantID || item.MigrationID != in.MigrationID || item.State == knowledgedriver.MutationApplied || item.NotBefore.After(in.Now) {
			continue
		}
		if item.State == knowledgedriver.MutationApplying && item.LeaseUntil.After(in.Now) {
			continue
		}
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		left, right := l.items[keys[i]], l.items[keys[j]]
		if left.CreatedAt.Equal(right.CreatedAt) {
			return keys[i] < keys[j]
		}
		return left.CreatedAt.Before(right.CreatedAt)
	})
	if len(keys) > in.Limit {
		keys = keys[:in.Limit]
	}
	result := make([]knowledgedriver.Mutation, 0, len(keys))
	for _, key := range keys {
		item := l.items[key]
		item.State = knowledgedriver.MutationApplying
		item.Attempt++
		item.LeaseOwner = in.WorkerID
		item.LeaseUntil = in.Now.Add(in.Lease).UTC()
		item.Version++
		l.items[key] = item
		result = append(result, item)
	}
	return result, nil
}

func (l *Ledger) MarkApplied(ctx context.Context, in knowledgedriver.CompleteRequest) (knowledgedriver.Mutation, error) {
	if err := ctx.Err(); err != nil {
		return knowledgedriver.Mutation{}, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	key := mutationKey(in.TenantID, in.MigrationID, in.MutationID)
	item, ok := l.items[key]
	if !ok {
		return knowledgedriver.Mutation{}, runtime.ErrNotFound
	}
	if item.Key != in.Key || item.State != knowledgedriver.MutationApplying || item.LeaseOwner != in.WorkerID || item.Version != in.ExpectedVersion ||
		in.TargetRevision < item.SourceRevision || !digest(in.TargetDigest) || in.At.IsZero() || in.At.Before(item.CreatedAt) || in.At.After(item.LeaseUntil) {
		return knowledgedriver.Mutation{}, runtime.ErrVersionConflict
	}
	item.State = knowledgedriver.MutationApplied
	item.TargetRevision = in.TargetRevision
	item.TargetDigest = in.TargetDigest
	item.AppliedAt = in.At.UTC()
	item.LeaseOwner = ""
	item.LeaseUntil = time.Time{}
	item.LastErrorClass = ""
	item.Version++
	l.items[key] = item
	return item, nil
}

func (l *Ledger) MarkRetry(ctx context.Context, in knowledgedriver.RetryRequest) (knowledgedriver.Mutation, error) {
	if err := ctx.Err(); err != nil {
		return knowledgedriver.Mutation{}, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	key := mutationKey(in.TenantID, in.MigrationID, in.MutationID)
	item, ok := l.items[key]
	if !ok {
		return knowledgedriver.Mutation{}, runtime.ErrNotFound
	}
	if item.Key != in.Key || item.State != knowledgedriver.MutationApplying || item.LeaseOwner != in.WorkerID || item.Version != in.ExpectedVersion ||
		in.ErrorClass == "" || len(in.ErrorClass) > 64 || in.At.IsZero() || in.At.After(item.LeaseUntil) || in.NotBefore.Before(in.At) {
		return knowledgedriver.Mutation{}, runtime.ErrVersionConflict
	}
	item.State = knowledgedriver.MutationPending
	item.NotBefore = in.NotBefore.UTC()
	item.LastErrorClass = in.ErrorClass
	item.LeaseOwner = ""
	item.LeaseUntil = time.Time{}
	item.Version++
	l.items[key] = item
	return item, nil
}

func (l *Ledger) Outstanding(ctx context.Context, tenantID, migrationID string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if tenantID == "" || migrationID == "" {
		return 0, runtime.ErrTenantScope
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	var count int64
	for _, item := range l.items {
		if item.TenantID == tenantID && item.MigrationID == migrationID && item.State != knowledgedriver.MutationApplied {
			count++
		}
	}
	return count, nil
}

func validRecord(in knowledgedriver.RecordRequest) bool {
	return in.TenantID != "" && in.MigrationID != "" && in.MutationID != "" && in.Epoch >= 1 &&
		in.Key.TenantID == in.TenantID && in.Key.KnowledgeID != "" && in.Key.KnowledgeVersion >= 1 && in.Key.ChunkID != "" &&
		(in.Direction == knowledgedriver.DirectionForward || in.Direction == knowledgedriver.DirectionReverse) &&
		(in.Operation == knowledgedriver.OperationUpsert || in.Operation == knowledgedriver.OperationDelete) &&
		in.SourceRevision >= 1 && digest(in.MutationDigest) && !in.CreatedAt.IsZero()
}
func mutationKey(tenant, migration, mutation string) string {
	return strings.Join([]string{tenant, migration, mutation}, "\x00")
}
func same(left, right knowledgedriver.Mutation) bool {
	return left.TenantID == right.TenantID && left.MigrationID == right.MigrationID && left.MutationID == right.MutationID && left.Epoch == right.Epoch && left.Direction == right.Direction && left.Key == right.Key && left.Operation == right.Operation && left.SourceRevision == right.SourceRevision && left.MutationDigest == right.MutationDigest && left.CreatedAt.Equal(right.CreatedAt)
}
func digest(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

var _ knowledgedriver.MutationLedger = (*Ledger)(nil)
var _ knowledgedriver.Recorder = (*Ledger)(nil)
