// Package inmemory provides an in-memory purge store for contract and unit
// tests only. It mirrors the guarded batch state machine without the SQL.
package inmemory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit/purge"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// Store is an in-memory purge store for tests.
type Store struct {
	mu           sync.Mutex
	retention    time.Duration
	events       []audit.Event
	batches      map[string]purge.Batch
	certificates int
}

func New(retention time.Duration) *Store {
	return &Store{retention: retention, batches: map[string]purge.Batch{}}
}

func (s *Store) Seed(events ...audit.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, events...)
}

func (s *Store) EffectiveRetention(_ context.Context, _, _ string) (time.Duration, error) {
	return s.retention, nil
}

func (s *Store) DueCandidates(_ context.Context, now time.Time) ([]purge.Candidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	set := map[string]bool{}
	for _, e := range s.events {
		if !e.OccurredAt.Before(now.Add(-s.retention)) {
			continue
		}
		set[e.TenantID+"|"+classOf(e)] = true
	}
	var candidates []purge.Candidate
	for key := range set {
		parts := strings.SplitN(key, "|", 2)
		candidates = append(candidates, purge.Candidate{TenantID: parts[0], Class: parts[1]})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].TenantID != candidates[j].TenantID {
			return candidates[i].TenantID < candidates[j].TenantID
		}
		return candidates[i].Class < candidates[j].Class
	})
	return candidates, nil
}

func (s *Store) ActiveBatches(_ context.Context) ([]purge.Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var batches []purge.Batch
	for _, b := range s.batches {
		switch b.State {
		case purge.StatePlanned, purge.StateApproved, purge.StateExecuting, purge.StateFailed:
			batches = append(batches, b)
		}
	}
	sort.Slice(batches, func(i, j int) bool {
		if batches[i].TenantID != batches[j].TenantID {
			return batches[i].TenantID < batches[j].TenantID
		}
		return batches[i].BatchID < batches[j].BatchID
	})
	return batches, nil
}

func (s *Store) Plan(ctx context.Context, input purge.PlanInput) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !input.Now.IsZero() && input.CutoffAt.After(input.Now.Add(-s.retention)) {
		return "", runtime.ErrInvariantViolation
	}
	batchID := stableID(input.TenantID, input.Class, input.CutoffAt)
	if _, ok := s.batches[batchID]; ok {
		return batchID, nil
	}
	count, digest := s.candidate(input.TenantID, input.Class, input.CutoffAt)
	maxBatch := input.MaxBatchSize
	if maxBatch <= 0 {
		maxBatch = 50000
	}
	if count > maxBatch {
		return "", runtime.ErrInvariantViolation
	}
	s.batches[batchID] = purge.Batch{TenantID: input.TenantID, BatchID: batchID, State: purge.StatePlanned,
		CutoffAt: input.CutoffAt, Class: input.Class, PlannedCount: count, PlannedDigest: digest,
		TTLUntil: time.Now().Add(input.TTL)}
	return batchID, nil
}

func (s *Store) Approve(_ context.Context, tenantID, batchID, approver, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.batches[batchID]
	if !ok || b.TenantID != tenantID || !purge.TransitionOK(b.State, purge.StateApproved) {
		return runtime.ErrInvariantViolation
	}
	b.State = purge.StateApproved
	s.batches[batchID] = b
	return nil
}

func (s *Store) Execute(_ context.Context, tenantID, batchID, owner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.batches[batchID]
	if !ok || b.TenantID != tenantID {
		return runtime.ErrInvariantViolation
	}
	if b.State == purge.StateCompleted {
		return nil
	}
	if !purge.TransitionOK(b.State, purge.StateExecuting) {
		return runtime.ErrInvariantViolation
	}
	b.State = purge.StateExecuting
	b.ClaimOwner = owner
	b.ClaimUntil = time.Now().Add(time.Minute)
	// Divergence check: recompute the candidate set and compare the plan.
	count, digest := s.candidate(tenantID, b.Class, b.CutoffAt)
	if count != b.PlannedCount || digest != b.PlannedDigest {
		b.State = purge.StateFailed
		b.LastError = "divergence"
		b.DeleteAttempt++
		b.NotBefore = time.Now().Add(time.Minute)
		s.batches[batchID] = b
		return runtime.ErrInvariantViolation
	}
	s.deleteCandidates(tenantID, b.Class, b.CutoffAt)
	b.State = purge.StateCompleted
	b.DeletedCount = count
	s.batches[batchID] = b
	s.certificates++
	return nil
}

func (s *Store) Quarantine(_ context.Context, tenantID, batchID, owner, errClass string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.batches[batchID]
	if !ok || b.TenantID != tenantID || !purge.TransitionOK(b.State, purge.StateQuarantined) {
		return runtime.ErrInvariantViolation
	}
	b.State = purge.StateQuarantined
	b.LastError = errClass
	s.batches[batchID] = b
	return nil
}

func (s *Store) Get(_ context.Context, tenantID, batchID string) (purge.Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.batches[batchID]
	if !ok || b.TenantID != tenantID {
		return purge.Batch{}, runtime.ErrNotFound
	}
	return b, nil
}

func (s *Store) Gauge(_ context.Context, _ time.Time) (purge.Gauge, error) {
	return purge.Gauge{}, nil
}

// Certificates returns the number of completed destruction certificates.
func (s *Store) Certificates() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.certificates
}

// SetBatch allows a test to stage a batch in a specific state.
func (s *Store) SetBatch(b purge.Batch) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches[b.BatchID] = b
}

func (s *Store) candidate(tenantID, class string, cutoff time.Time) (int64, string) {
	hasher := sha256.New()
	var ids []string
	for _, e := range s.events {
		if e.TenantID != tenantID || classOf(e) != class || !e.OccurredAt.Before(cutoff) {
			continue
		}
		ids = append(ids, e.TenantID+":"+e.AuditID)
	}
	sort.Strings(ids)
	for _, id := range ids {
		hasher.Write([]byte(id))
		hasher.Write([]byte{'\n'})
	}
	return int64(len(ids)), hex.EncodeToString(hasher.Sum(nil))
}

func (s *Store) deleteCandidates(tenantID, class string, cutoff time.Time) {
	kept := s.events[:0]
	for _, e := range s.events {
		if e.TenantID == tenantID && classOf(e) == class && e.OccurredAt.Before(cutoff) {
			continue
		}
		kept = append(kept, e)
	}
	s.events = kept
}

func classOf(e audit.Event) string {
	switch {
	case strings.HasPrefix(e.Action, "governance."), strings.HasPrefix(e.Action, "artifact.quarantine"),
		strings.HasPrefix(e.Action, "tool_confirmation"):
		return "security"
	case strings.HasPrefix(e.Action, "usage."):
		return "billing"
	default:
		return "default"
	}
}

func stableID(tenantID, class string, cutoff time.Time) string {
	hasher := sha256.New()
	hasher.Write([]byte(fmt.Sprintf("audit-purge-v1\x1f%s\x1f%s\x1f%d", tenantID, class, cutoff.Unix())))
	return "apb_" + hex.EncodeToString(hasher.Sum(nil))
}

var _ purge.Store = (*Store)(nil)
