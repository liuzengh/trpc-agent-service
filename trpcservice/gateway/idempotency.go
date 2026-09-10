package gateway

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

var (
	// ErrIdempotencyCapacity reports that a bounded in-process store cannot
	// retain another key. It is intentionally separate from duplicate input.
	ErrIdempotencyCapacity = errors.New("idempotency store capacity exhausted")
)

const (
	defaultIdempotencyTTL        = 10 * time.Minute
	defaultIdempotencyMaxEntries = 4096
)

// IdempotencyState is the lifecycle state of one accepted execution key.
type IdempotencyState string

const (
	// IdempotencyPending marks a request whose execution is still in progress.
	IdempotencyPending IdempotencyState = "pending"
	// IdempotencyCompleted marks a request with a stored successful response.
	IdempotencyCompleted IdempotencyState = "completed"
	// IdempotencyFailed marks a request whose execution failed.
	IdempotencyFailed IdempotencyState = "failed"
)

// IdempotencyConfig bounds the process-local execution de-duplication store.
type IdempotencyConfig struct {
	TTL        time.Duration
	MaxEntries int
	Now        func() time.Time
}

type idempotencyEntry struct {
	state     IdempotencyState
	events    []DispatchEvent
	expiresAt time.Time
}

// IdempotencyStore prevents duplicate execution for one trusted principal and
// external message ID. It deliberately makes no persistence or cluster claim.
type IdempotencyStore struct {
	mu         sync.Mutex
	ttl        time.Duration
	maxEntries int
	now        func() time.Time
	entries    map[string]*idempotencyEntry
	closed     bool
}

// IdempotencyClaim owns a newly pending key until Complete or Fail is called.
type IdempotencyClaim struct {
	store *IdempotencyStore
	key   string
	once  sync.Once
}

// NewIdempotencyStore creates a bounded in-process idempotency store.
func NewIdempotencyStore(config IdempotencyConfig) (*IdempotencyStore, error) {
	if config.TTL == 0 {
		config.TTL = defaultIdempotencyTTL
	}
	if config.TTL < 0 {
		return nil, fmt.Errorf("%w: idempotency TTL cannot be negative", ErrInvalid)
	}
	if config.MaxEntries == 0 {
		config.MaxEntries = defaultIdempotencyMaxEntries
	}
	if config.MaxEntries < 1 {
		return nil, fmt.Errorf("%w: idempotency capacity must be positive", ErrInvalid)
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &IdempotencyStore{ttl: config.TTL, maxEntries: config.MaxEntries, now: config.Now, entries: make(map[string]*idempotencyEntry)}, nil
}

// Ready reports whether the store accepts new claims.
func (store *IdempotencyStore) Ready() bool {
	if store == nil {
		return false
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return !store.closed
}

// Begin reserves a normalized message key. A completed duplicate returns its
// stored redacted events; a pending duplicate returns ErrDuplicateMessage.
func (store *IdempotencyStore) Begin(ctx context.Context, principal Principal, message InboundMessage) (*IdempotencyClaim, []DispatchEvent, error) {
	if store == nil {
		return nil, nil, ErrNotReady
	}
	if ctx == nil {
		return nil, nil, fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if err := principal.Validate(); err != nil {
		return nil, nil, ErrUnauthenticated
	}
	normalized, err := message.Normalize()
	if err != nil {
		return nil, nil, err
	}
	if normalized.ExternalMessageID == "" {
		return nil, nil, fmt.Errorf("%w: external message ID is required for idempotency", ErrInvalid)
	}
	key, err := makeIdempotencyKey(principal, normalized)
	if err != nil {
		return nil, nil, err
	}
	now := store.nowUTC()
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return nil, nil, ErrClosed
	}
	store.pruneLocked(now)
	if entry := store.entries[key]; entry != nil {
		switch entry.state {
		case IdempotencyPending:
			return nil, nil, ErrDuplicateMessage
		case IdempotencyCompleted:
			return nil, cloneDispatchEvents(entry.events), nil
		case IdempotencyFailed:
			delete(store.entries, key)
		}
	}
	if len(store.entries) >= store.maxEntries {
		return nil, nil, ErrIdempotencyCapacity
	}
	store.entries[key] = &idempotencyEntry{state: IdempotencyPending, expiresAt: now.Add(store.ttl)}
	return &IdempotencyClaim{store: store, key: key}, nil, nil
}

// Complete stores the final redacted events for future duplicate requests.
func (claim *IdempotencyClaim) Complete(events []DispatchEvent) error {
	if claim == nil || claim.store == nil {
		return nil
	}
	claim.once.Do(func() {
		claim.store.mu.Lock()
		defer claim.store.mu.Unlock()
		if entry := claim.store.entries[claim.key]; entry != nil && entry.state == IdempotencyPending {
			entry.state = IdempotencyCompleted
			entry.events = cloneDispatchEvents(events)
			entry.expiresAt = claim.store.nowUTC().Add(claim.store.ttl)
		}
	})
	return nil
}

// Fail lets a failed pending key be retried after the current request ends.
func (claim *IdempotencyClaim) Fail() error {
	if claim == nil || claim.store == nil {
		return nil
	}
	claim.once.Do(func() {
		claim.store.mu.Lock()
		defer claim.store.mu.Unlock()
		if entry := claim.store.entries[claim.key]; entry != nil && entry.state == IdempotencyPending {
			entry.state = IdempotencyFailed
			entry.events = nil
			entry.expiresAt = claim.store.nowUTC().Add(claim.store.ttl)
		}
	})
	return nil
}

// Close prevents new claims and drops all process-local state.
func (store *IdempotencyStore) Close() error {
	if store == nil {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.closed = true
	store.entries = make(map[string]*idempotencyEntry)
	return nil
}

func (store *IdempotencyStore) pruneLocked(now time.Time) {
	for key, entry := range store.entries {
		if entry == nil || !now.Before(entry.expiresAt) {
			delete(store.entries, key)
		}
	}
}

func (store *IdempotencyStore) nowUTC() time.Time {
	now := store.now().UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return now
}

func makeIdempotencyKey(principal Principal, message InboundMessage) (string, error) {
	if err := principal.Validate(); err != nil {
		return "", ErrUnauthenticated
	}
	parts := []string{string(principal.Kind()), principal.TenantID(), principal.AppID(), principal.SubjectID(), string(message.ConversationKind), message.ExternalUserID, message.ExternalPeerID, message.ExternalChatID, message.ExternalThreadID, message.ExternalMessageID}
	if principal.Kind() == PrincipalChannel {
		target, ok := principal.RoutingTarget()
		if !ok {
			return "", ErrUnauthenticated
		}
		parts = append(parts, string(target.Channel), target.BindingID, fmt.Sprintf("%d", target.BindingVersion))
	}
	var builder strings.Builder
	for _, part := range parts {
		builder.WriteString(fmt.Sprintf("%d:", len([]byte(part))))
		builder.WriteString(part)
	}
	return builder.String(), nil
}

func cloneDispatchEvents(events []DispatchEvent) []DispatchEvent {
	if len(events) == 0 {
		return nil
	}
	clone := make([]DispatchEvent, len(events))
	copy(clone, events)
	return clone
}
