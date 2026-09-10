package inmemory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Store struct {
	mu         sync.Mutex
	routes     map[string]ingress.BindingRoute
	candidates map[string]ingress.CandidateRecord
}

func New() *Store {
	return &Store{routes: make(map[string]ingress.BindingRoute), candidates: make(map[string]ingress.CandidateRecord)}
}

func (s *Store) PutBindingRoute(ctx context.Context, route ingress.BindingRoute) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if route.OpaqueBindingID == "" || route.Channel == "" || route.RouteKeyDigest == "" || route.TenantID == "" ||
		route.AgentAppID == "" || route.ChannelBindingID == "" || route.ExternalAccountID == "" || route.TenantVersion < 1 || route.BindingVersion < 1 ||
		route.SecretRef.Ref == "" || route.SecretRef.Version < 1 || route.IdentitySecretRef.Ref == "" || route.IdentitySecretRef.Version < 1 ||
		route.SessionSecretRef.Ref == "" || route.SessionSecretRef.Version < 1 {
		return runtime.ErrInvariantViolation
	}
	key := route.Channel + "\x00" + route.RouteKeyDigest
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.routes[key]; ok {
		if old == route {
			return nil
		}
		if old.OpaqueBindingID != route.OpaqueBindingID || old.TenantID != route.TenantID ||
			old.ChannelBindingID != route.ChannelBindingID || route.BindingVersion <= old.BindingVersion {
			return runtime.ErrIdempotencyCollision
		}
	}
	for oldKey, old := range s.routes {
		if oldKey != key && old.OpaqueBindingID == route.OpaqueBindingID {
			return runtime.ErrIdempotencyCollision
		}
	}
	s.routes[key] = route
	return nil
}

func (s *Store) ResolveBindingRoute(ctx context.Context, channel, routeDigest string) (ingress.BindingRoute, error) {
	if err := ctx.Err(); err != nil {
		return ingress.BindingRoute{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	route, ok := s.routes[channel+"\x00"+routeDigest]
	if !ok {
		return ingress.BindingRoute{}, runtime.ErrNotFound
	}
	return route, nil
}

func (s *Store) IssueCandidate(ctx context.Context, record ingress.CandidateRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if record.TokenDigest == "" || record.OpaqueBindingID == "" || record.State != ingress.CandidateIssued || record.Version != 0 || !record.ExpiresAt.After(record.IssuedAt) {
		return runtime.ErrInvariantViolation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.candidates[record.TokenDigest]; ok {
		return runtime.ErrIdempotencyCollision
	}
	s.candidates[record.TokenDigest] = record
	return nil
}

func (s *Store) AcquireCandidate(ctx context.Context, tokenDigest string, bindingVersion int64, now time.Time) (ingress.CandidateRecord, ingress.BindingRoute, error) {
	if err := ctx.Err(); err != nil {
		return ingress.CandidateRecord{}, ingress.BindingRoute{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.candidates[tokenDigest]
	if !ok {
		return ingress.CandidateRecord{}, ingress.BindingRoute{}, runtime.ErrNotFound
	}
	if record.State != ingress.CandidateIssued || record.BindingVersion != bindingVersion || !record.ExpiresAt.After(now) {
		return ingress.CandidateRecord{}, ingress.BindingRoute{}, runtime.ErrVersionConflict
	}
	route, ok := s.routeByOpaque(record.OpaqueBindingID)
	if !ok || route.BindingVersion != bindingVersion || !route.Enabled {
		return ingress.CandidateRecord{}, ingress.BindingRoute{}, runtime.ErrVersionMismatch
	}
	record.State, record.Version = ingress.CandidateVerifierAcquired, record.Version+1
	s.candidates[tokenDigest] = record
	return record, route, nil
}

func (s *Store) MarkCandidateVerified(ctx context.Context, tokenDigest string, version int64, receiptDigest, identityDigest string, verifiedAt time.Time) (ingress.CandidateRecord, error) {
	if err := ctx.Err(); err != nil {
		return ingress.CandidateRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.candidates[tokenDigest]
	if !ok || record.State != ingress.CandidateVerifierAcquired || record.Version != version || receiptDigest == "" || identityDigest == "" || verifiedAt.IsZero() {
		return ingress.CandidateRecord{}, runtime.ErrVersionConflict
	}
	record.State, record.Version = ingress.CandidateVerified, record.Version+1
	record.ReceiptDigest, record.ProtocolIdentityDigest, record.VerifiedAt = receiptDigest, identityDigest, verifiedAt
	s.candidates[tokenDigest] = record
	return record, nil
}

func (s *Store) PromoteCandidate(ctx context.Context, tokenDigest string, bindingVersion int64, receiptDigest, identityDigest string, verifiedAt, now time.Time) (ingress.CandidateRecord, ingress.BindingRoute, error) {
	if err := ctx.Err(); err != nil {
		return ingress.CandidateRecord{}, ingress.BindingRoute{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.candidates[tokenDigest]
	if !ok || record.State != ingress.CandidateVerified || record.BindingVersion != bindingVersion ||
		record.ReceiptDigest != receiptDigest || record.ProtocolIdentityDigest != identityDigest ||
		record.VerifiedAt.UTC().UnixMicro() != verifiedAt.UTC().UnixMicro() || !record.ExpiresAt.After(now) {
		return ingress.CandidateRecord{}, ingress.BindingRoute{}, runtime.ErrVersionConflict
	}
	route, ok := s.routeByOpaque(record.OpaqueBindingID)
	if !ok || !route.Enabled || route.BindingVersion != bindingVersion {
		return ingress.CandidateRecord{}, ingress.BindingRoute{}, runtime.ErrVersionMismatch
	}
	record.State, record.Version = ingress.CandidatePromoted, record.Version+1
	s.candidates[tokenDigest] = record
	return record, route, nil
}

func (s *Store) BurnCandidate(ctx context.Context, tokenDigest string, version int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.candidates[tokenDigest]
	if !ok || record.Version != version || (record.State != ingress.CandidateIssued && record.State != ingress.CandidateVerifierAcquired) {
		return runtime.ErrVersionConflict
	}
	record.State, record.Version = ingress.CandidateBurned, record.Version+1
	s.candidates[tokenDigest] = record
	return nil
}

func (s *Store) BurnExpiredCandidates(ctx context.Context, now time.Time, limit int) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if now.IsZero() {
		return 0, runtime.ErrInvariantViolation
	}
	if limit <= 0 {
		limit = 100
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	type expiredCandidate struct {
		key       string
		expiresAt time.Time
	}
	candidates := make([]expiredCandidate, 0, len(s.candidates))
	for key, record := range s.candidates {
		if !record.ExpiresAt.After(now) && (record.State == ingress.CandidateIssued ||
			record.State == ingress.CandidateVerifierAcquired || record.State == ingress.CandidateVerified) {
			candidates = append(candidates, expiredCandidate{key: key, expiresAt: record.ExpiresAt})
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].expiresAt.Equal(candidates[j].expiresAt) {
			return candidates[i].key < candidates[j].key
		}
		return candidates[i].expiresAt.Before(candidates[j].expiresAt)
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	for _, candidate := range candidates {
		record := s.candidates[candidate.key]
		record.State, record.Version = ingress.CandidateBurned, record.Version+1
		record.ReceiptDigest, record.ProtocolIdentityDigest, record.VerifiedAt = "", "", time.Time{}
		s.candidates[candidate.key] = record
	}
	return len(candidates), nil
}

func (s *Store) routeByOpaque(opaqueID string) (ingress.BindingRoute, bool) {
	for _, route := range s.routes {
		if route.OpaqueBindingID == opaqueID {
			return route, true
		}
	}
	return ingress.BindingRoute{}, false
}

var _ ingress.Store = (*Store)(nil)
