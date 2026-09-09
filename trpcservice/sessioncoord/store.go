package sessioncoord

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

// ErrOutOfOrder asks a distributed consumer to retry after an earlier Inbox sequence.
var ErrOutOfOrder = errors.New("sessioncoord: inbox sequence is out of order")

// Event is one committed session event.
type Event struct {
	EventID, InboxID, Type, Payload, TraceID string
	Seq                                      uint64
	CreatedAt                                time.Time
}

// Summary is a monotonically published session summary.
type Summary struct {
	Version, CutoffEventSeq uint64
	Content                 string
	UpdatedAt               time.Time
}

// Memory is an idempotent memory projection from a source event.
type Memory struct {
	MemoryID, SourceEventID string
	SourceEventSeq, Version uint64
	Status, Content         string
	UpdatedAt               time.Time
}

// Head is the current session concurrency state.
type Head struct {
	LastEventSeq, LastFence, StateVersion uint64
	State                                 map[string]string
}

// TurnWrite atomically persists an event and its state delta under one fence.
type TurnWrite struct {
	Key                                           gateway.SessionKey
	Fence                                         uint64
	InboxSeq                                      uint64
	InboxID, EventID, EventType, Payload, TraceID string
	StateDelta                                    map[string]string
}

// CommittedTurnReader lets a retry recover a previously committed assistant
// event before invoking the model again. Implementations return (false,nil)
// when no event exists for the inbox ID.
type CommittedTurnReader interface {
	CommittedTurn(context.Context, gateway.SessionKey, string) (Event, bool, error)
}

// WriteStore is the platform persistence contract required for multi-node safety.
type WriteStore interface {
	FenceAdvancer
	FenceValidator
	ValidateTurn(context.Context, gateway.SessionKey, uint64) error
	CommitTurn(context.Context, TurnWrite) (uint64, error)
	PublishSummary(context.Context, gateway.SessionKey, uint64, Summary) error
	UpsertMemory(context.Context, gateway.SessionKey, uint64, Memory) error
	PublishOutbox(context.Context, gateway.SessionKey, uint64, gateway.OutboundMessage) error
}

// WithFence keeps takeover serialized with one external Session mutation.
// Implementations must hold the same ownership boundary used by AdvanceFence
// until operation returns, closing the validate-then-write race across nodes.
func (store *MemoryWriteStore) WithFence(ctx context.Context, key gateway.SessionKey, fence uint64, operation func(context.Context) error) error {
	if operation == nil {
		return errors.New("sessioncoord: fenced operation is required")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.data(key).head.LastFence != fence {
		return ErrStaleFence
	}
	return operation(ctx)
}

// ValidateFence checks exact current ownership without changing state.
func (store *MemoryWriteStore) ValidateFence(_ context.Context, key gateway.SessionKey, fence uint64) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.data(key).head.LastFence != fence {
		return ErrStaleFence
	}
	return nil
}

// ValidateTurn prevents a later Inbox sequence from reaching Runner/Tool
// side effects before the prior turn has committed.
func (store *MemoryWriteStore) ValidateTurn(_ context.Context, key gateway.SessionKey, inboxSeq uint64) error {
	if inboxSeq == 0 {
		return errors.New("sessioncoord: inbox sequence is required")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if inboxSeq != store.data(key).head.LastEventSeq+1 {
		return ErrOutOfOrder
	}
	return nil
}

type sessionData struct {
	head           Head
	events         []Event
	summary        *Summary
	memories       map[string]Memory
	memorySources  map[string]string
	committedInbox map[string]uint64
}

// MemoryWriteStore is an atomic reference implementation used by tests.
type MemoryWriteStore struct {
	mu       sync.Mutex
	sessions map[gateway.SessionKey]*sessionData
	outbox   map[string]gateway.OutboundMessage
	now      func() time.Time
}

// NewMemoryWriteStore creates an empty fenced store.
func NewMemoryWriteStore() *MemoryWriteStore {
	return &MemoryWriteStore{sessions: make(map[gateway.SessionKey]*sessionData), outbox: make(map[string]gateway.OutboundMessage), now: time.Now}
}

func (store *MemoryWriteStore) data(key gateway.SessionKey) *sessionData {
	data := store.sessions[key]
	if data == nil {
		data = &sessionData{head: Head{State: make(map[string]string)}, memories: make(map[string]Memory), memorySources: make(map[string]string), committedInbox: make(map[string]uint64)}
		store.sessions[key] = data
	}
	return data
}

// AdvanceFence invalidates every older owner before the new Worker runs.
func (store *MemoryWriteStore) AdvanceFence(_ context.Context, key gateway.SessionKey, token uint64) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	data := store.data(key)
	if token <= data.head.LastFence {
		return ErrStaleFence
	}
	data.head.LastFence = token
	return nil
}

func (store *MemoryWriteStore) CurrentFence(_ context.Context, key gateway.SessionKey) (uint64, error) {
	if store == nil || key.TenantID == "" || key.AppID == "" || key.UserID == "" || key.SessionID == "" {
		return 0, errors.New("sessioncoord: complete session key is required")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.data(key).head.LastFence, nil
}

// CommitTurn atomically verifies fence and commits an event and its state delta.
func (store *MemoryWriteStore) CommitTurn(_ context.Context, write TurnWrite) (uint64, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	data := store.data(write.Key)
	if write.Fence != data.head.LastFence {
		return 0, ErrStaleFence
	}
	if write.InboxID == "" || write.EventID == "" || write.InboxSeq == 0 {
		return 0, errors.New("sessioncoord: inbox sequence, inbox ID, and event ID are required")
	}
	if seq, exists := data.committedInbox[write.InboxID]; exists {
		return seq, nil
	}
	if write.InboxSeq != data.head.LastEventSeq+1 {
		return 0, ErrOutOfOrder
	}
	seq := data.head.LastEventSeq + 1
	now := store.now().UTC()
	event := Event{EventID: write.EventID, InboxID: write.InboxID, Type: write.EventType, Payload: write.Payload, TraceID: write.TraceID, Seq: seq, CreatedAt: now}
	newState := make(map[string]string, len(data.head.State)+len(write.StateDelta))
	for key, value := range data.head.State {
		newState[key] = value
	}
	for key, value := range write.StateDelta {
		newState[key] = value
	}
	data.events = append(data.events, event)
	data.head.LastEventSeq = seq
	data.head.StateVersion++
	data.head.State = newState
	data.committedInbox[write.InboxID] = seq
	return seq, nil
}

func (store *MemoryWriteStore) CommittedTurn(_ context.Context, key gateway.SessionKey, inboxID string) (Event, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if inboxID == "" {
		return Event{}, false, errors.New("sessioncoord: inbox ID is required")
	}
	data := store.data(key)
	seq, ok := data.committedInbox[inboxID]
	if !ok || seq == 0 || int(seq) > len(data.events) {
		return Event{}, false, nil
	}
	return data.events[seq-1], true, nil
}

// PublishOutbox persists the reply only after the turn and derived writes succeed.
func (store *MemoryWriteStore) PublishOutbox(_ context.Context, key gateway.SessionKey, fence uint64, outbound gateway.OutboundMessage) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	data := store.data(key)
	if fence != data.head.LastFence {
		return ErrStaleFence
	}
	if outbound.DedupeKey == "" || outbound.SourceInboxID == "" || outbound.SourceEventID == "" {
		return errors.New("sessioncoord: outbox dedupe and source IDs are required")
	}
	if outbound.TenantID != key.TenantID || outbound.AppID != key.AppID || outbound.UserID != key.UserID || outbound.SessionID != key.SessionID {
		return errors.New("sessioncoord: outbox scope does not match session")
	}
	seq, committed := data.committedInbox[outbound.SourceInboxID]
	if !committed {
		return errors.New("sessioncoord: outbox source inbox is not committed")
	}
	if seq == 0 || data.events[seq-1].EventID != outbound.SourceEventID {
		return errors.New("sessioncoord: outbox source event does not match committed turn")
	}
	outboxKey := outbound.TenantID + "/" + outbound.DedupeKey
	if _, exists := store.outbox[outboxKey]; exists {
		return nil
	}
	outbound.CreatedAt = store.now().UTC()
	store.outbox[outboxKey] = outbound
	return nil
}

// PublishSummary uses cutoff/version CAS and rejects stale fences/jobs.
func (store *MemoryWriteStore) PublishSummary(_ context.Context, key gateway.SessionKey, fence uint64, summary Summary) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	data := store.data(key)
	if fence != data.head.LastFence {
		return ErrStaleFence
	}
	if summary.Version == 0 || summary.CutoffEventSeq == 0 || summary.Content == "" {
		return errors.New("sessioncoord: summary version, cutoff, and content are required")
	}
	if summary.CutoffEventSeq > data.head.LastEventSeq {
		return errors.New("sessioncoord: summary cutoff exceeds session head")
	}
	if data.summary != nil {
		if summary.Version == data.summary.Version && summary.CutoffEventSeq == data.summary.CutoffEventSeq && summary.Content == data.summary.Content {
			return nil
		}
		if summary.CutoffEventSeq <= data.summary.CutoffEventSeq || summary.Version <= data.summary.Version {
			return errors.New("sessioncoord: stale summary")
		}
	}
	summary.UpdatedAt = store.now().UTC()
	data.summary = &summary
	return nil
}

// UpsertMemory is idempotent by source event and rejects stale fences.
func (store *MemoryWriteStore) UpsertMemory(_ context.Context, key gateway.SessionKey, fence uint64, memory Memory) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	data := store.data(key)
	if fence != data.head.LastFence {
		return ErrStaleFence
	}
	if memory.MemoryID == "" || memory.SourceEventID == "" || memory.SourceEventSeq == 0 || memory.Version == 0 {
		return errors.New("sessioncoord: memory identity, source, and version are required")
	}
	if existingID := data.memorySources[memory.SourceEventID]; existingID != "" {
		if existingID == memory.MemoryID {
			return nil
		}
		return errors.New("sessioncoord: source event already mapped")
	}
	if current, exists := data.memories[memory.MemoryID]; exists && memory.Version <= current.Version {
		return errors.New("sessioncoord: stale memory version")
	}
	memory.UpdatedAt = store.now().UTC()
	data.memories[memory.MemoryID] = memory
	data.memorySources[memory.SourceEventID] = memory.MemoryID
	return nil
}

// Snapshot returns defensive copies for diagnostics and tests.
func (store *MemoryWriteStore) Snapshot(key gateway.SessionKey) (Head, []Event, *Summary, []Memory) {
	store.mu.Lock()
	defer store.mu.Unlock()
	data := store.data(key)
	head := data.head
	head.State = make(map[string]string, len(data.head.State))
	for k, v := range data.head.State {
		head.State[k] = v
	}
	events := append([]Event(nil), data.events...)
	var summary *Summary
	if data.summary != nil {
		copy := *data.summary
		summary = &copy
	}
	memories := make([]Memory, 0, len(data.memories))
	for _, value := range data.memories {
		memories = append(memories, value)
	}
	return head, events, summary, memories
}

// Outbox returns a tenant-scoped durable reply.
func (store *MemoryWriteStore) Outbox(tenantID, dedupeKey string) (gateway.OutboundMessage, bool) {
	store.mu.Lock()
	defer store.mu.Unlock()
	value, ok := store.outbox[tenantID+"/"+dedupeKey]
	return value, ok
}
