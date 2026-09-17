package inmemory

import (
	"context"
	"reflect"
	"strconv"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Store struct {
	mu                           sync.Mutex
	policies                     map[string]governance.PolicySnapshot
	pricing                      map[string]governance.PricingSnapshot
	reservations                 map[string]governance.Reservation
	coordinates                  map[string]string
	usage                        map[string]string
	decisions                    map[string]governance.Decision
	maxCost, maxTokens           int64
	reservedCost, reservedTokens int64
}

func New(maxCost, maxTokens int64) *Store {
	return &Store{policies: make(map[string]governance.PolicySnapshot), pricing: make(map[string]governance.PricingSnapshot), reservations: make(map[string]governance.Reservation),
		coordinates: make(map[string]string), usage: make(map[string]string), decisions: make(map[string]governance.Decision), maxCost: maxCost, maxTokens: maxTokens}
}

func key(tenant string, version int64) string {
	return tenant + "\x00" + strconv.FormatInt(version, 10)
}

func (s *Store) PublishPolicy(value governance.PolicySnapshot) error {
	digest, _, err := governance.PolicyDigest(value.Policy)
	if err != nil {
		return err
	}
	if value.TenantID == "" || value.Version < 1 || value.ContentDigest != digest {
		return runtime.ErrInvariantViolation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(value.TenantID, value.Version)
	if existing, ok := s.policies[k]; ok {
		if existing.ContentDigest == value.ContentDigest {
			return nil
		}
		return runtime.ErrIdempotencyCollision
	}
	s.policies[k] = value
	return nil
}
func (s *Store) PublishPricing(value governance.PricingSnapshot) error {
	digest, _, err := governance.PricingDigest(value.Pricing)
	if err != nil {
		return err
	}
	if value.TenantID == "" || value.Version < 1 || value.ContentDigest != digest {
		return runtime.ErrInvariantViolation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(value.TenantID, value.Version)
	if existing, ok := s.pricing[k]; ok {
		if existing.ContentDigest == value.ContentDigest {
			return nil
		}
		return runtime.ErrIdempotencyCollision
	}
	s.pricing[k] = value
	return nil
}
func (s *Store) GetPolicy(ctx context.Context, tenant string, version int64) (governance.PolicySnapshot, error) {
	if err := ctx.Err(); err != nil {
		return governance.PolicySnapshot{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.policies[key(tenant, version)]
	if !ok {
		return governance.PolicySnapshot{}, runtime.ErrNotFound
	}
	return value, nil
}
func (s *Store) GetPricing(ctx context.Context, tenant string, version int64) (governance.PricingSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return governance.PricingSnapshot{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.pricing[key(tenant, version)]
	if !ok {
		return governance.PricingSnapshot{}, runtime.ErrNotFound
	}
	return value, nil
}
func (s *Store) Reserve(ctx context.Context, in governance.ReserveRequest) (governance.Reservation, error) {
	if err := ctx.Err(); err != nil {
		return governance.Reservation{}, err
	}
	id, err := governance.StableReservationID(in)
	if err != nil {
		return governance.Reservation{}, err
	}
	if in.PolicyVersion < 1 || in.MaxCostMicros < 0 || in.MaxTokens < 0 {
		return governance.Reservation{}, runtime.ErrInvariantViolation
	}
	coordinate := in.TenantID + "\x00" + in.RequestID + "\x00" + in.ResourceID + "\x00" + in.AttemptClass
	s.mu.Lock()
	defer s.mu.Unlock()
	if existingID, ok := s.coordinates[coordinate]; ok {
		existing := s.reservations[existingID]
		if existing.ReservedCostMicros != in.MaxCostMicros || existing.ReservedTokens != in.MaxTokens || existing.PolicyVersion != in.PolicyVersion || existing.PricingVersion != in.PricingVersion {
			return governance.Reservation{}, runtime.ErrIdempotencyCollision
		}
		return existing, nil
	}
	if (s.maxCost > 0 && (in.MaxCostMicros == 0 || in.MaxCostMicros > s.maxCost-s.reservedCost)) || (s.maxTokens > 0 && (in.MaxTokens == 0 || in.MaxTokens > s.maxTokens-s.reservedTokens)) {
		return governance.Reservation{}, runtime.ErrCapabilityUnsupported
	}
	value := governance.Reservation{ReservationID: id, TenantID: in.TenantID, RequestID: in.RequestID, ResourceID: in.ResourceID, AttemptClass: in.AttemptClass,
		PolicyVersion: in.PolicyVersion, PricingVersion: in.PricingVersion, ReservedCostMicros: in.MaxCostMicros, ReservedTokens: in.MaxTokens, State: governance.ReservationReserved, Version: 1}
	s.reservations[id], s.coordinates[coordinate] = value, id
	s.reservedCost += in.MaxCostMicros
	s.reservedTokens += in.MaxTokens
	return value, nil
}
func (s *Store) Settle(ctx context.Context, in governance.SettleRequest) (governance.Reservation, error) {
	if err := ctx.Err(); err != nil {
		return governance.Reservation{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.reservations[in.ReservationID]
	if !ok || value.TenantID != in.TenantID || value.RequestID != in.RequestID {
		return governance.Reservation{}, runtime.ErrNotFound
	}
	usageKey := in.TenantID + "\x00" + in.RequestID + "\x00" + in.Stage + "\x00" + in.UsageKind
	if prior, ok := s.usage[usageKey]; ok {
		if prior != in.ReservationID {
			return governance.Reservation{}, runtime.ErrIdempotencyCollision
		}
		return value, nil
	}
	if value.State != governance.ReservationReserved || value.Version != in.ExpectedVersion || in.ActualCostMicros < 0 || in.ActualCostMicros > value.ReservedCostMicros || in.Usage.InputTokens < 0 || in.Usage.OutputTokens < 0 || (value.ReservedTokens > 0 && in.Usage.InputTokens+in.Usage.OutputTokens > value.ReservedTokens) {
		return governance.Reservation{}, runtime.ErrVersionConflict
	}
	s.reservedCost -= value.ReservedCostMicros - in.ActualCostMicros
	s.reservedTokens -= value.ReservedTokens - (in.Usage.InputTokens + in.Usage.OutputTokens)
	value.State, value.ActualCostMicros, value.InputTokens, value.OutputTokens, value.Version = governance.ReservationSettled, in.ActualCostMicros, in.Usage.InputTokens, in.Usage.OutputTokens, value.Version+1
	s.reservations[value.ReservationID], s.usage[usageKey] = value, value.ReservationID
	return value, nil
}
func (s *Store) Refund(ctx context.Context, tenant, id string, expected int64, _ string) (governance.Reservation, error) {
	if err := ctx.Err(); err != nil {
		return governance.Reservation{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.reservations[id]
	if !ok || value.TenantID != tenant {
		return governance.Reservation{}, runtime.ErrNotFound
	}
	if value.State == governance.ReservationRefunded {
		return value, nil
	}
	if value.State != governance.ReservationReserved || value.Version != expected {
		return governance.Reservation{}, runtime.ErrVersionConflict
	}
	s.reservedCost -= value.ReservedCostMicros
	s.reservedTokens -= value.ReservedTokens
	value.State, value.Version = governance.ReservationRefunded, value.Version+1
	s.reservations[id] = value
	return value, nil
}
func (s *Store) GetReservation(ctx context.Context, tenant, id string) (governance.Reservation, error) {
	if err := ctx.Err(); err != nil {
		return governance.Reservation{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.reservations[id]
	if !ok || value.TenantID != tenant {
		return governance.Reservation{}, runtime.ErrNotFound
	}
	return value, nil
}
func (s *Store) RecordDecision(ctx context.Context, value governance.Decision) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if value.TenantID == "" || value.DecisionID == "" || value.RequestID == "" {
		return runtime.ErrInvariantViolation
	}
	value.RuleIDs = append([]string{}, value.RuleIDs...)
	s.mu.Lock()
	defer s.mu.Unlock()
	k := value.TenantID + "\x00" + value.DecisionID
	if prior, ok := s.decisions[k]; ok && !reflect.DeepEqual(prior, value) {
		return runtime.ErrIdempotencyCollision
	}
	s.decisions[k] = value
	return nil
}

var _ governance.Repository = (*Store)(nil)
var _ governance.Ledger = (*Store)(nil)
var _ governance.DecisionRecorder = (*Store)(nil)
