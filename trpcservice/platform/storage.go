package platform

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"sync"
	"time"
)

// SessionState is the materialized, tenant-scoped view of a Session.
type SessionState struct {
	Session
	SummarySourceSequence uint64    `json:"summary_source_sequence"`
	Summary               string    `json:"summary"`
	EventCount            int       `json:"event_count"`
	ProjectionSequence    uint64    `json:"projection_sequence"`
	UpdatedAt             time.Time `json:"updated_at"`
}

type MemoryRecord struct {
	ID           string    `json:"id"`
	TenantID     string    `json:"tenant_id"`
	SessionID    string    `json:"session_id"`
	Key          string    `json:"key"`
	Value        string    `json:"value"`
	UpdatedAt    time.Time `json:"updated_at"`
	FencingToken uint64    `json:"fencing_token,omitempty"`
}

type BackendHealth struct {
	Backend string    `json:"backend"`
	Status  string    `json:"status"`
	Message string    `json:"message,omitempty"`
	Checked time.Time `json:"checked_at"`
}

// DataStore is the Stage 2 storage boundary. Implementations must preserve
// event immutability, tenant isolation, idempotency, and sequence ordering.
type SessionStore interface {
	StorageAdapter
	GetSessionState(context.Context, string, string) (SessionState, error)
}

type MemoryStore interface {
	ListMemory(context.Context, string, string) ([]MemoryRecord, error)
	PutMemory(context.Context, MemoryRecord) error
}

type DataStore interface {
	SessionStore
	MemoryStore
	Health(context.Context) BackendHealth
}

type unavailableStore struct{ backend string }

func (s *unavailableStore) failure() error {
	return errors.New("platform: " + s.backend + " storage unavailable")
}
func (s *unavailableStore) GetSession(context.Context, string, string) (Session, error) {
	return Session{}, s.failure()
}
func (s *unavailableStore) AppendSessionEvent(context.Context, SessionEvent) error {
	return s.failure()
}
func (s *unavailableStore) ListSessionEvents(context.Context, string, string, uint64) ([]SessionEvent, error) {
	return nil, s.failure()
}
func (s *unavailableStore) GetSessionState(context.Context, string, string) (SessionState, error) {
	return SessionState{}, s.failure()
}
func (s *unavailableStore) ListMemory(context.Context, string, string) ([]MemoryRecord, error) {
	return nil, s.failure()
}
func (s *unavailableStore) PutMemory(context.Context, MemoryRecord) error { return s.failure() }
func (s *unavailableStore) Health(context.Context) BackendHealth {
	return BackendHealth{Backend: s.backend, Status: "unavailable", Checked: time.Now().UTC()}
}

type memoryEventKey struct{ tenant, session, key string }

// InMemoryStore is the deterministic reference implementation used by local
// development and tests.
type InMemoryStore struct {
	mu        sync.RWMutex
	events    map[string][]SessionEvent
	byKey     map[memoryEventKey]SessionEvent
	memory    map[string]MemoryRecord
	backend   string
	fences    map[string]uint64
	artifacts map[string]Artifact
	knowledge map[string]KnowledgeRecord
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{events: map[string][]SessionEvent{}, byKey: map[memoryEventKey]SessionEvent{}, memory: map[string]MemoryRecord{}, backend: "inmemory", fences: map[string]uint64{}, artifacts: map[string]Artifact{}, knowledge: map[string]KnowledgeRecord{}}
}

func storageSessionKey(tenant, session string) string { return tenant + "\x00" + session }
func storageMemoryKey(tenant, session, key string) string {
	return tenant + "\x00" + session + "\x00" + key
}

func (s *InMemoryStore) GetSession(ctx context.Context, tenant, session string) (Session, error) {
	state, err := s.GetSessionState(ctx, tenant, session)
	return state.Session, err
}

func (s *InMemoryStore) GetSessionState(ctx context.Context, tenant, session string) (SessionState, error) {
	events, err := s.ListSessionEvents(ctx, tenant, session, 0)
	if err != nil {
		return SessionState{}, err
	}
	return materializeSession(tenant, session, events)
}

func materializeSession(tenant, session string, events []SessionEvent) (SessionState, error) {
	if len(events) == 0 {
		return SessionState{}, ErrNotFound
	}
	state := SessionState{Session: Session{ID: session, TenantID: tenant}, EventCount: len(events)}
	for _, event := range events {
		if event.Sequence != state.ProjectionSequence+1 {
			return SessionState{}, errors.New("platform: session projection sequence gap")
		}
		state.ProjectionSequence = event.Sequence
		state.Sequence = event.Sequence
		state.UpdatedAt = event.OccurredAt
		if event.Type == "summary" {
			state.Summary = string(event.Payload)
			var projection summaryProjection
			if json.Unmarshal(event.Payload, &projection) == nil && projection.SourceSequence > 0 {
				if projection.SourceSequence >= event.Sequence {
					return SessionState{}, errors.New("platform: invalid summary checkpoint")
				}
				state.Summary, state.SummarySourceSequence = projection.Text, projection.SourceSequence
			}
		}
	}
	return state, nil
}

func (s *InMemoryStore) AppendSessionEvent(ctx context.Context, event SessionEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if event.TenantID == "" || event.SessionID == "" || event.IdempotencyKey == "" {
		return errors.New("platform: invalid session event")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stream := storageSessionKey(event.TenantID, event.SessionID)
	if event.FencingToken > 0 && event.FencingToken < s.fences[stream] && !importingMigration(ctx) {
		return ErrStaleFencingToken
	}
	if event.FencingToken > s.fences[stream] {
		s.fences[stream] = event.FencingToken
	}
	key := memoryEventKey{event.TenantID, event.SessionID, event.IdempotencyKey}
	if prior, ok := s.byKey[key]; ok {
		if prior.Type == event.Type && string(prior.Payload) == string(event.Payload) {
			return nil
		}
		return ErrDuplicateEvent
	}
	event.Sequence = uint64(len(s.events[stream]) + 1)
	if event.ID == "" {
		event.ID = event.TenantID + ":" + event.SessionID + ":" + itoa(event.Sequence)
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}
	event.Payload = append([]byte(nil), event.Payload...)
	s.events[stream] = append(s.events[stream], event)
	s.byKey[key] = event
	return nil
}

func (s *InMemoryStore) ListSessionEvents(ctx context.Context, tenant, session string, after uint64) ([]SessionEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	stream := s.events[storageSessionKey(tenant, session)]
	result := make([]SessionEvent, 0, len(stream))
	for _, event := range stream {
		if event.Sequence > after {
			event.Payload = append([]byte(nil), event.Payload...)
			result = append(result, event)
		}
	}
	return result, nil
}

func (s *InMemoryStore) ListMemory(ctx context.Context, tenant, session string) ([]MemoryRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := []MemoryRecord{}
	prefix := storageSessionKey(tenant, session) + "\x00"
	for key, item := range s.memory {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			result = append(result, item)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, nil
}

func (s *InMemoryStore) PutMemory(ctx context.Context, item MemoryRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if item.TenantID == "" || item.SessionID == "" || item.Key == "" {
		return errors.New("platform: invalid memory record")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stream := storageSessionKey(item.TenantID, item.SessionID)
	if item.FencingToken > 0 && item.FencingToken < s.fences[stream] && !importingMigration(ctx) {
		return ErrStaleFencingToken
	}
	if item.FencingToken > s.fences[stream] {
		s.fences[stream] = item.FencingToken
	}
	if item.ID == "" {
		item.ID = item.TenantID + ":" + item.SessionID + ":" + item.Key
	}
	if item.UpdatedAt.IsZero() {
		item.UpdatedAt = time.Now().UTC()
	}
	s.memory[storageMemoryKey(item.TenantID, item.SessionID, item.Key)] = item
	return nil
}

func (s *InMemoryStore) Health(ctx context.Context) BackendHealth {
	if err := ctx.Err(); err != nil {
		return BackendHealth{Backend: s.backend, Status: "unavailable", Checked: time.Now().UTC()}
	}
	return BackendHealth{Backend: s.backend, Status: "healthy", Checked: time.Now().UTC()}
}

func itoa(v uint64) string {
	const digits = "0123456789"
	if v == 0 {
		return "0"
	}
	buf := [20]byte{}
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = digits[v%10]
		v /= 10
	}
	return string(buf[i:])
}
func migrationChecksum(events []SessionEvent, memory []MemoryRecord) string {
	h := sha256.New()
	writeChecksumField(h, []byte("stage2-v1"))
	for _, event := range events {
		writeChecksumField(h, []byte(event.TenantID))
		writeChecksumField(h, []byte(event.SessionID))
		writeChecksumField(h, []byte(event.ID))
		writeChecksumField(h, []byte(strconv.FormatUint(event.Sequence, 10)))
		writeChecksumField(h, []byte(event.Type))
		writeChecksumField(h, event.Payload)
		writeChecksumField(h, []byte(event.IdempotencyKey))
		writeChecksumField(h, []byte(event.OccurredAt.Format(time.RFC3339Nano)))
	}
	sort.Slice(memory, func(i, j int) bool {
		if memory[i].SessionID == memory[j].SessionID {
			return memory[i].Key < memory[j].Key
		}
		return memory[i].SessionID < memory[j].SessionID
	})
	for _, item := range memory {
		writeChecksumField(h, []byte(item.TenantID))
		writeChecksumField(h, []byte(item.SessionID))
		writeChecksumField(h, []byte(item.ID))
		writeChecksumField(h, []byte(item.Key))
		writeChecksumField(h, []byte(item.Value))
		writeChecksumField(h, []byte(item.UpdatedAt.Format(time.RFC3339Nano)))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func writeChecksumField(h interface{ Write([]byte) (int, error) }, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = h.Write(length[:])
	_, _ = h.Write(value)
}
