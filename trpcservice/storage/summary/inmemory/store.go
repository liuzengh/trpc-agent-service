package inmemory

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/summary"
)

type record struct {
	body          summary.Body
	supersededBy  string
	supersededAt  time.Time
	claimOwner    string
	claimUntil    time.Time
	deleteAttempt int
	version       int64
}
type Store struct {
	mu         sync.Mutex
	values     map[string]*record
	watermarks map[string]uint64
}

func New() *Store { return &Store{values: map[string]*record{}, watermarks: map[string]uint64{}} }
func key(value summary.Key) string {
	return value.TenantID + "\x00" + value.AgentAppID + "\x00" + value.SessionID + "\x00" + value.SummaryID
}
func sessionKey(value summary.Key) string {
	return value.TenantID + "\x00" + value.AgentAppID + "\x00" + value.SessionID
}

// SetWatermark is a test-only stand-in for the committed session_summary
// boundary that PostgreSQL checks under the same transaction/lock.
func (s *Store) SetWatermark(value summary.Key, base uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.watermarks[key(value)] = base
}
func (s *Store) Put(ctx context.Context, in summary.Body) (summary.Body, error) {
	if err := ctx.Err(); err != nil {
		return summary.Body{}, err
	}
	value, err := summary.ValidateBody(in)
	if err != nil {
		return summary.Body{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if old := s.values[key(value.Key)]; old != nil {
		if old.body.ContentRef != value.ContentRef || old.body.ContentDigest != value.ContentDigest || !bytes.Equal(old.body.Content, value.Content) {
			return summary.Body{}, runtime.ErrIdempotencyCollision
		}
		return clone(old.body), nil
	}
	s.values[key(value.Key)] = &record{body: value}
	return clone(value), nil
}
func (s *Store) Get(ctx context.Context, in summary.Key) (summary.Body, error) {
	if err := ctx.Err(); err != nil {
		return summary.Body{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	value := s.values[key(in)]
	if value == nil {
		return summary.Body{}, runtime.ErrNotFound
	}
	return clone(value.body), nil
}
func (s *Store) Supersede(ctx context.Context, source summary.Key, replacement string, at time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if replacement == "" || at.IsZero() {
		return runtime.ErrInvalidEnvelope
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old := s.values[key(source)]
	next := s.values[key(summary.Key{TenantID: source.TenantID, AgentAppID: source.AgentAppID, SessionID: source.SessionID, SummaryID: replacement})]
	if old == nil || next == nil {
		return runtime.ErrNotFound
	}
	if old.supersededBy != "" {
		if old.supersededBy == replacement {
			return nil
		}
		return runtime.ErrVersionConflict
	}
	if s.watermarks[key(next.body.Key)] <= s.watermarks[key(source)] {
		return runtime.ErrVersionMismatch
	}
	old.supersededBy = replacement
	old.supersededAt = at.UTC()
	old.version++
	return nil
}
func (s *Store) ClaimSuperseded(ctx context.Context, now time.Time, owner string, ttl time.Duration, limit int) ([]summary.ClaimedBody, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if owner == "" || ttl <= 0 || limit <= 0 {
		return nil, runtime.ErrInvariantViolation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.values))
	for k := range s.values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]summary.ClaimedBody, 0, limit)
	for _, k := range keys {
		value := s.values[k]
		if value.supersededBy == "" || (!value.claimUntil.IsZero() && value.claimUntil.After(now)) {
			continue
		}
		value.claimOwner = owner
		value.claimUntil = now.Add(ttl)
		value.version++
		out = append(out, claimed(value))
		if len(out) == limit {
			break
		}
	}
	return out, nil
}
func (s *Store) FinishDelete(ctx context.Context, in summary.ClaimedBody) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	value := s.values[key(in.Key)]
	if value == nil {
		return runtime.ErrNotFound
	}
	if value.claimOwner != in.ClaimOwner || value.version != in.Version {
		return runtime.ErrVersionConflict
	}
	delete(s.values, key(in.Key))
	return nil
}
func (s *Store) DeferDelete(ctx context.Context, in summary.ClaimedBody, until time.Time, class string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if until.IsZero() || class == "" {
		return runtime.ErrInvalidEnvelope
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	value := s.values[key(in.Key)]
	if value == nil {
		return runtime.ErrNotFound
	}
	if value.claimOwner != in.ClaimOwner || value.version != in.Version {
		return runtime.ErrVersionConflict
	}
	value.claimUntil = until.UTC()
	value.deleteAttempt++
	value.version++
	return nil
}
func claimed(value *record) summary.ClaimedBody {
	return summary.ClaimedBody{Body: clone(value.body), SupersededBy: value.supersededBy, ClaimOwner: value.claimOwner, ClaimUntil: value.claimUntil, DeleteAttempt: value.deleteAttempt, Version: value.version}
}
func clone(value summary.Body) summary.Body {
	value.Content = append([]byte(nil), value.Content...)
	return value
}

var _ summary.Store = (*Store)(nil)
