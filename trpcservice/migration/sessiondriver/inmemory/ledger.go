// Package inmemory implements the session migration mutation ledger for contracts.
package inmemory

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/sessiondriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
)

type Ledger struct {
	mu    sync.Mutex
	items map[string]sessiondriver.Mutation
}

func New() *Ledger { return &Ledger{items: make(map[string]sessiondriver.Mutation)} }

func (l *Ledger) Record(ctx context.Context, in sessiondriver.RecordRequest) (sessiondriver.Mutation, error) {
	if err := ctx.Err(); err != nil {
		return sessiondriver.Mutation{}, err
	}
	if in.Direction == "" {
		in.Direction = sessiondriver.DirectionForward
	}
	if in.TenantID == "" || in.MigrationID == "" || in.MutationID == "" || in.Epoch < 1 ||
		(in.Direction != sessiondriver.DirectionForward && in.Direction != sessiondriver.DirectionReverse) ||
		in.SessionKey.TenantID != in.TenantID || in.AgentAppID == "" || in.SessionID == "" || in.SourceVersion < 1 ||
		!digest(in.MutationDigest) || in.CreatedAt.IsZero() {
		return sessiondriver.Mutation{}, runtime.ErrInvariantViolation
	}
	value := sessiondriver.Mutation{TenantID: in.TenantID, MigrationID: in.MigrationID,
		MutationID: in.MutationID, Epoch: in.Epoch, Direction: in.Direction, SessionKey: in.SessionKey,
		SourceVersion: in.SourceVersion, MutationDigest: in.MutationDigest,
		State: sessiondriver.MutationPending, CreatedAt: in.CreatedAt.UTC(), Version: 1}
	l.mu.Lock()
	defer l.mu.Unlock()
	key := mutationKey(value.TenantID, value.MigrationID, value.SessionKey, value.MutationID)
	if existing, ok := l.items[key]; ok {
		if sameRecord(existing, value) {
			return existing, nil
		}
		return sessiondriver.Mutation{}, runtime.ErrIdempotencyCollision
	}
	l.items[key] = value
	return value, nil
}

func (l *Ledger) Claim(ctx context.Context, in sessiondriver.ClaimRequest) ([]sessiondriver.Mutation, error) {
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
		if item.TenantID != in.TenantID || item.MigrationID != in.MigrationID || item.State == sessiondriver.MutationApplied || item.NotBefore.After(in.Now) {
			continue
		}
		if item.State == sessiondriver.MutationApplying && item.LeaseUntil.After(in.Now) {
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
	result := make([]sessiondriver.Mutation, 0, len(keys))
	for _, key := range keys {
		item := l.items[key]
		item.State = sessiondriver.MutationApplying
		item.Attempt++
		item.LeaseOwner = in.WorkerID
		item.LeaseUntil = in.Now.Add(in.Lease).UTC()
		item.Version++
		l.items[key] = item
		result = append(result, item)
	}
	return result, nil
}

func (l *Ledger) MarkApplied(ctx context.Context, in sessiondriver.CompleteRequest) (sessiondriver.Mutation, error) {
	if err := ctx.Err(); err != nil {
		return sessiondriver.Mutation{}, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	key := mutationKey(in.TenantID, in.MigrationID, in.SessionKey, in.MutationID)
	item, ok := l.items[key]
	if !ok {
		return sessiondriver.Mutation{}, runtime.ErrNotFound
	}
	if item.State != sessiondriver.MutationApplying || item.LeaseOwner != in.WorkerID || item.Version != in.ExpectedVersion ||
		in.TargetVersion < item.SourceVersion || !digest(in.TargetDigest) || in.At.IsZero() || in.At.Before(item.CreatedAt) || in.At.After(item.LeaseUntil) {
		return sessiondriver.Mutation{}, runtime.ErrVersionConflict
	}
	item.State = sessiondriver.MutationApplied
	item.TargetVersion, item.TargetDigest = in.TargetVersion, in.TargetDigest
	item.AppliedAt = in.At.UTC()
	item.LeaseOwner, item.LeaseUntil, item.LastErrorClass = "", time.Time{}, ""
	item.Version++
	l.items[key] = item
	return item, nil
}

func (l *Ledger) MarkRetry(ctx context.Context, in sessiondriver.RetryRequest) (sessiondriver.Mutation, error) {
	if err := ctx.Err(); err != nil {
		return sessiondriver.Mutation{}, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	key := mutationKey(in.TenantID, in.MigrationID, in.SessionKey, in.MutationID)
	item, ok := l.items[key]
	if !ok {
		return sessiondriver.Mutation{}, runtime.ErrNotFound
	}
	if item.State != sessiondriver.MutationApplying || item.LeaseOwner != in.WorkerID || item.Version != in.ExpectedVersion ||
		in.ErrorClass == "" || len(in.ErrorClass) > 64 || in.At.IsZero() || in.At.After(item.LeaseUntil) || in.NotBefore.Before(in.At) {
		return sessiondriver.Mutation{}, runtime.ErrVersionConflict
	}
	item.State = sessiondriver.MutationPending
	item.NotBefore, item.LastErrorClass = in.NotBefore.UTC(), in.ErrorClass
	item.LeaseOwner, item.LeaseUntil = "", time.Time{}
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
		if item.TenantID == tenantID && item.MigrationID == migrationID && item.State != sessiondriver.MutationApplied {
			count++
		}
	}
	return count, nil
}

func mutationKey(tenantID, migrationID string, key sessionstore.SessionKey, mutationID string) string {
	return keyOf(tenantID, migrationID, key.AgentAppID, key.SessionID, mutationID)
}

func keyOf(tenantID, migrationID, agentAppID, sessionID, mutationID string) string {
	return strings.Join([]string{tenantID, migrationID, agentAppID, sessionID, mutationID}, "\x00")
}

func sameRecord(left, right sessiondriver.Mutation) bool {
	return left.TenantID == right.TenantID && left.MigrationID == right.MigrationID && left.MutationID == right.MutationID &&
		left.Epoch == right.Epoch && left.SessionKey == right.SessionKey && left.SourceVersion == right.SourceVersion &&
		left.Direction == right.Direction && left.MutationDigest == right.MutationDigest && left.CreatedAt.Equal(right.CreatedAt)
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

var _ sessiondriver.MutationLedger = (*Ledger)(nil)
var _ sessiondriver.Recorder = (*Ledger)(nil)
