// Package inmemory implements the business audit retention store for
// contracts, mirroring the SQL watermark gate.
package inmemory

import (
	"context"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit/purgebusiness"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type eventRow struct {
	tenantID, auditID string
	occurredAt        time.Time
}

type outboxRow struct {
	tenantID, outboxID string
	kind               string
	state              string
	createdAt          time.Time
}

type Store struct {
	mu       sync.Mutex
	events   map[string]eventRow
	outbox   map[string]outboxRow
	batches  map[string]purgebusiness.Batch
	nextPlan func(tenantID string, cutoff time.Time) string
}

func New() *Store {
	return &Store{
		events:  make(map[string]eventRow),
		outbox:  make(map[string]outboxRow),
		batches: make(map[string]purgebusiness.Batch),
	}
}

// SeedEvent and SeedOutbox let a contract test stage facts before planning.
func (s *Store) SeedEvent(tenantID, auditID string, occurredAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events[tenantID+"\x00"+auditID] = eventRow{tenantID, auditID, occurredAt.UTC()}
}

func (s *Store) SeedOutbox(tenantID, outboxID, state string, createdAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outbox[tenantID+"\x00"+outboxID] = outboxRow{tenantID, outboxID, "audit", state, createdAt.UTC()}
}

func (s *Store) Plan(_ context.Context, in purgebusiness.PlanInput) (string, error) {
	if in.TenantID == "" || in.CutoffAt.IsZero() || in.Actor == "" || in.Reason == "" || in.Now.IsZero() {
		return "", runtime.ErrInvariantViolation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	batchID := in.TenantID + "\x00" + in.CutoffAt.UTC().Format(time.RFC3339Nano)
	if _, ok := s.batches[batchID]; ok {
		return batchID, nil
	}
	watermark := s.watermarkLocked(in.TenantID)
	safe := in.CutoffAt.UTC()
	if !watermark.IsZero() && watermark.Before(safe) {
		safe = watermark
	}
	var events, outbox int64
	for _, e := range s.events {
		if e.tenantID == in.TenantID && e.occurredAt.Before(safe) {
			events++
		}
	}
	for _, o := range s.outbox {
		if o.tenantID == in.TenantID && o.kind == "audit" && o.state == "published" && o.createdAt.Before(safe) {
			outbox++
		}
	}
	s.batches[batchID] = purgebusiness.Batch{
		TenantID: in.TenantID, BatchID: batchID, State: purgebusiness.StatePlanned,
		CutoffAt: in.CutoffAt.UTC(), WatermarkAt: watermark, SafeCutoffAt: safe,
		PlannedEvents: events, PlannedOutbox: outbox, PlannedDigest: digestOf(events, outbox, watermark),
		NotBefore: in.Now.UTC(), CreatedAt: in.Now.UTC(), Version: 1,
	}
	return batchID, nil
}

func (s *Store) Execute(_ context.Context, tenantID, batchID, owner string, maxBatchSize int64) (string, error) {
	if tenantID == "" || batchID == "" || owner == "" || maxBatchSize < 1 || maxBatchSize > 1_000_000 {
		return "", runtime.ErrInvariantViolation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	batch, ok := s.batches[batchID]
	if !ok || batch.TenantID != tenantID {
		return "", runtime.ErrNotFound
	}
	if batch.State == purgebusiness.StateCompleted {
		return "completed", nil
	}
	if batch.State != purgebusiness.StatePlanned && batch.State != purgebusiness.StateExecuting && batch.State != purgebusiness.StateFailed {
		return "", runtime.ErrVersionConflict
	}
	if batch.NotBefore.After(time.Now().UTC()) {
		return "not_before", nil
	}
	watermark := s.watermarkLocked(tenantID)
	safe := batch.CutoffAt
	if !watermark.IsZero() && watermark.Before(safe) {
		safe = watermark
	}
	if !safe.Equal(batch.SafeCutoffAt) {
		batch.State = purgebusiness.StateFailed
		batch.LastError = "watermark_drift"
		batch.DeleteAttempt++
		batch.Version++
		s.batches[batchID] = batch
		return "watermark_drift", nil
	}
	var events, outbox int64
	var eventKeys, outboxKeys []string
	for key, e := range s.events {
		if e.tenantID == tenantID && e.occurredAt.Before(safe) {
			eventKeys = append(eventKeys, key)
			events++
		}
	}
	for key, o := range s.outbox {
		if o.tenantID == tenantID && o.kind == "audit" && o.state == "published" && o.createdAt.Before(safe) {
			outboxKeys = append(outboxKeys, key)
			outbox++
		}
	}
	if events != batch.PlannedEvents || outbox != batch.PlannedOutbox {
		batch.State = purgebusiness.StateFailed
		batch.LastError = "divergence"
		batch.DeleteAttempt++
		batch.Version++
		s.batches[batchID] = batch
		return "divergence", nil
	}
	for _, key := range eventKeys {
		delete(s.events, key)
	}
	for _, key := range outboxKeys {
		delete(s.outbox, key)
	}
	batch.State = purgebusiness.StateCompleted
	batch.DeletedEvents = events
	batch.DeletedOutbox = outbox
	batch.Version++
	s.batches[batchID] = batch
	return "completed", nil
}

func (s *Store) Quarantine(_ context.Context, tenantID, batchID, owner string) error {
	if tenantID == "" || batchID == "" || owner == "" {
		return runtime.ErrInvariantViolation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	batch, ok := s.batches[batchID]
	if !ok || batch.TenantID != tenantID {
		return runtime.ErrNotFound
	}
	if batch.State == purgebusiness.StateQuarantined {
		return nil
	}
	if batch.State != purgebusiness.StateFailed && batch.State != purgebusiness.StateExecuting {
		return runtime.ErrVersionConflict
	}
	batch.State = purgebusiness.StateQuarantined
	batch.ClaimOwner = owner
	batch.Version++
	s.batches[batchID] = batch
	return nil
}

func (s *Store) Get(_ context.Context, tenantID, batchID string) (purgebusiness.Batch, error) {
	if tenantID == "" || batchID == "" {
		return purgebusiness.Batch{}, runtime.ErrInvariantViolation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	batch, ok := s.batches[batchID]
	if !ok || batch.TenantID != tenantID {
		return purgebusiness.Batch{}, runtime.ErrNotFound
	}
	return batch, nil
}

func (s *Store) ActiveBatches(_ context.Context) ([]purgebusiness.Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []purgebusiness.Batch
	for _, batch := range s.batches {
		if batch.State == purgebusiness.StatePlanned || batch.State == purgebusiness.StateExecuting || batch.State == purgebusiness.StateFailed {
			result = append(result, batch)
		}
	}
	return result, nil
}

func (s *Store) DueTenants(_ context.Context, cutoff time.Time) ([]string, error) {
	if cutoff.IsZero() {
		return nil, runtime.ErrInvariantViolation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := make(map[string]struct{})
	for _, e := range s.events {
		if e.occurredAt.Before(cutoff) {
			seen[e.tenantID] = struct{}{}
		}
	}
	for _, o := range s.outbox {
		if o.kind == "audit" && o.state == "published" && o.createdAt.Before(cutoff) {
			seen[o.tenantID] = struct{}{}
		}
	}
	result := make([]string, 0, len(seen))
	for tenant := range seen {
		result = append(result, tenant)
	}
	return result, nil
}

func (s *Store) watermarkLocked(tenantID string) time.Time {
	var watermark time.Time
	for _, o := range s.outbox {
		if o.tenantID == tenantID && o.kind == "audit" && o.state != "published" {
			if watermark.IsZero() || o.createdAt.Before(watermark) {
				watermark = o.createdAt
			}
		}
	}
	return watermark
}

func digestOf(events, outbox int64, watermark time.Time) string {
	if events == 0 && outbox == 0 {
		return ""
	}
	// A deterministic placeholder digest; contracts only assert equality, not
	// the algorithm.
	return "digest:" + time.Time{}.Add(time.Duration(events*1000000+outbox)).Format(time.RFC3339Nano) + watermark.Format(time.RFC3339Nano)
}

var _ purgebusiness.Store = (*Store)(nil)
