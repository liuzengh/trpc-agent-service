package wecommcp

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

var ErrStateConflict = errors.New("WeCom MCP state conflict; operator review required")

type PollKey struct{ TenantID, BindingID, ChatHash string }
type Checkpoint struct {
	ConfigHash string    `json:"config_hash"`
	Through    time.Time `json:"through_at"`
	Version    int64     `json:"version"`
	Floor      time.Time `json:"floor_at"`
}
type CheckpointView struct {
	ChatHash string `json:"chat_hash"`
	Checkpoint
}
type DeliveryKey struct{ TenantID, BindingID, OutboundID string }
type DeliveryState struct {
	InputHash, Status string
	UpdatedAt         time.Time
}

// Store is shared by receivers/senders. Attempts have no lease expiry: an
// interrupted non-idempotent send must not become automatically sendable again.
type Store interface {
	Checkpoint(context.Context, PollKey, string, time.Time) (Checkpoint, error)
	Advance(context.Context, PollKey, Checkpoint, time.Time) error
	Seen(context.Context, PollKey, string) (bool, error)
	MarkSeen(context.Context, PollKey, string) error
	RecordRejection(context.Context, PollKey, RejectedMessage) (bool, error)
	ListRejections(context.Context, string, string, int) ([]RejectedMessage, error)
	ListCheckpoints(context.Context, string, string) ([]CheckpointView, error)
	RecoverCheckpoint(context.Context, controlplane.ChannelBinding, string, int64, string, time.Time, bool) (Checkpoint, error)
	BeginDelivery(context.Context, DeliveryKey, string) (DeliveryState, bool, error)
	FinishDelivery(context.Context, DeliveryKey, string, string) error
	Ready(context.Context) error
}

func NewStore(repository controlplane.Repository) (Store, error) {
	if repository == nil {
		return nil, errors.New("WeCom MCP repository required")
	}
	if provider, ok := repository.(interface{ SQLDB() *sql.DB }); ok {
		if provider.SQLDB() == nil {
			return nil, errors.New("WeCom MCP database required")
		}
		return &PostgresStore{db: provider.SQLDB()}, nil
	}
	return NewMemoryStore(), nil
}

type MemoryStore struct {
	mu          sync.Mutex
	checkpoints map[PollKey]Checkpoint
	seen        map[PollKey]map[string]bool
	deliveries  map[DeliveryKey]DeliveryState
	rejections  map[PollKey]map[string]RejectedMessage
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{checkpoints: map[PollKey]Checkpoint{}, seen: map[PollKey]map[string]bool{}, deliveries: map[DeliveryKey]DeliveryState{}, rejections: map[PollKey]map[string]RejectedMessage{}}
}
func (s *MemoryStore) Checkpoint(ctx context.Context, key PollKey, hash string, start time.Time) (Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return Checkpoint{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.checkpoints[key]
	if !ok {
		value = Checkpoint{ConfigHash: hash, Through: start, Version: 1, Floor: start}
		s.checkpoints[key] = value
	}
	if value.ConfigHash != hash {
		return Checkpoint{}, ErrStateConflict
	}
	return value, nil
}
func (s *MemoryStore) Advance(ctx context.Context, key PollKey, previous Checkpoint, through time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.checkpoints[key]
	if !ok || current.ConfigHash != previous.ConfigHash || current.Version != previous.Version || through.Before(current.Through) {
		return ErrStateConflict
	}
	current.Through = through
	current.Version++
	s.checkpoints[key] = current
	return nil
}
func (s *MemoryStore) Seen(ctx context.Context, key PollKey, id string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seen[key][id], nil
}
func (s *MemoryStore) MarkSeen(ctx context.Context, key PollKey, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen[key] == nil {
		s.seen[key] = map[string]bool{}
	}
	s.seen[key][id] = true
	return nil
}
func (s *MemoryStore) BeginDelivery(ctx context.Context, key DeliveryKey, hash string) (DeliveryState, bool, error) {
	if err := ctx.Err(); err != nil {
		return DeliveryState{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if value, ok := s.deliveries[key]; ok {
		if value.InputHash != hash {
			return value, false, ErrStateConflict
		}
		return value, false, nil
	}
	value := DeliveryState{hash, "attempting", time.Now().UTC()}
	s.deliveries[key] = value
	return value, true, nil
}
func (s *MemoryStore) FinishDelivery(ctx context.Context, key DeliveryKey, hash, status string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.deliveries[key]
	if !ok || value.InputHash != hash || value.Status != "attempting" || (status != "sent" && status != "rejected" && status != "unknown") {
		return ErrStateConflict
	}
	value.Status = status
	value.UpdatedAt = time.Now().UTC()
	s.deliveries[key] = value
	return nil
}
func (s *MemoryStore) Ready(ctx context.Context) error { return ctx.Err() }
