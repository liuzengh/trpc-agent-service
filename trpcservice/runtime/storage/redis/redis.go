// Package redis provides a tenant-scoped Redis runtime and memory store.
//
// Each tenant is represented by one versioned JSON document. Mutations use
// WATCH/MULTI so event sequencing, idempotency claims, leases and memory CAS
// remain atomic when multiple workers share the same Redis server.
package redis

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	sessionstorage "github.com/XnLemon/trpc-agent-service/trpcservice/storage/session"
	"github.com/google/uuid"
	redisclient "github.com/redis/go-redis/v9"
)

const (
	stateVersion    = 1
	maxWatchRetries = 8
)

// Config configures a Redis-backed runtime store. Password is transient
// connection input and must come from a trusted secret boundary.
type Config struct {
	Addr         string
	Password     string
	DB           int
	KeyPrefix    string
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PoolSize     int
}

// Store implements the tenant-scoped runtime capabilities and MemoryStore over Redis.
type Store struct {
	client    redisclient.UniversalClient
	keyPrefix string
	closeOnce sync.Once
	closeErr  error
	owned     bool
}

type state struct {
	Version             int                                        `json:"version"`
	Sessions            map[string]sessionstorage.Session          `json:"sessions,omitempty"`
	Events              map[string]runtimestorage.MessageEvent     `json:"events,omitempty"`
	Messages            map[string]string                          `json:"messages,omitempty"`
	Histories           map[string][]sessionstorage.EventPayload   `json:"histories,omitempty"`
	Replies             map[string]runtimestorage.ReplyOutbox      `json:"replies,omitempty"`
	Correlations        map[string]runtimestorage.ReplyCorrelation `json:"correlations,omitempty"`
	Memories            map[string]runtimestorage.MemoryRecord     `json:"memories,omitempty"`
	MemoryIndexHandoffs map[string]int64                           `json:"memory_index_handoffs,omitempty"`
}

// New creates a store using a caller-owned Redis client.
func New(client redisclient.UniversalClient, keyPrefix string) (*Store, error) {
	if client == nil {
		return nil, runtimestorage.ErrInvalid
	}
	keyPrefix = strings.TrimSpace(keyPrefix)
	if keyPrefix == "" {
		keyPrefix = "trpc:runtime:v1"
	}
	return &Store{client: client, keyPrefix: keyPrefix}, nil
}

// NewFromURL creates and pings a Redis client from a redis:// URL. The URL is
// never included in returned errors or logs.
func NewFromURL(ctx context.Context, rawURL string) (*Store, error) {
	if ctx == nil {
		return nil, runtimestorage.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	options, err := redisclient.ParseURL(strings.TrimSpace(rawURL))
	if err != nil || options == nil || options.Addr == "" {
		return nil, runtimestorage.ErrInvalid
	}
	client := redisclient.NewClient(options)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, mapRedisError(ctx, err)
	}
	store, err := New(client, "trpc:runtime:v1")
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	store.owned = true
	return store, nil
}

// NewFromConfig creates and pings an owned client from explicit connection
// settings. Password is accepted only as resolved runtime input.
func NewFromConfig(ctx context.Context, config Config) (*Store, error) {
	if ctx == nil || strings.TrimSpace(config.Addr) == "" || config.DB < 0 {
		return nil, runtimestorage.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	options := &redisclient.Options{Addr: strings.TrimSpace(config.Addr), Password: config.Password, DB: config.DB}
	if config.DialTimeout > 0 {
		options.DialTimeout = config.DialTimeout
	}
	if config.ReadTimeout > 0 {
		options.ReadTimeout = config.ReadTimeout
	}
	if config.WriteTimeout > 0 {
		options.WriteTimeout = config.WriteTimeout
	}
	if config.PoolSize > 0 {
		options.PoolSize = config.PoolSize
	}
	client := redisclient.NewClient(options)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, mapRedisError(ctx, err)
	}
	store, err := New(client, config.KeyPrefix)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	store.owned = true
	return store, nil
}

// Ping checks readiness without exposing provider errors.
func (s *Store) Ping(ctx context.Context) error {
	if err := s.check(ctx); err != nil {
		return err
	}
	return mapRedisError(ctx, s.client.Ping(ctx).Err())
}

func (s *Store) check(ctx context.Context) error {
	if ctx == nil {
		return runtimestorage.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.client == nil {
		return runtimestorage.ErrStorage
	}
	return nil
}

func (s *Store) key(tenantID string) string {
	return s.keyPrefix + ":" + hex.EncodeToString([]byte(tenantID))
}

func emptyState() state {
	return state{Version: stateVersion, Sessions: map[string]sessionstorage.Session{}, Events: map[string]runtimestorage.MessageEvent{}, Messages: map[string]string{}, Histories: map[string][]sessionstorage.EventPayload{}, Replies: map[string]runtimestorage.ReplyOutbox{}, Correlations: map[string]runtimestorage.ReplyCorrelation{}, Memories: map[string]runtimestorage.MemoryRecord{}, MemoryIndexHandoffs: map[string]int64{}}
}

func normalizeState(value state) state {
	if value.Version == 0 {
		value.Version = stateVersion
	}
	if value.Sessions == nil {
		value.Sessions = map[string]sessionstorage.Session{}
	}
	if value.Events == nil {
		value.Events = map[string]runtimestorage.MessageEvent{}
	}
	if value.Messages == nil {
		value.Messages = map[string]string{}
	}
	if value.Histories == nil {
		value.Histories = map[string][]sessionstorage.EventPayload{}
	}
	if value.Replies == nil {
		value.Replies = map[string]runtimestorage.ReplyOutbox{}
	}
	if value.Correlations == nil {
		value.Correlations = map[string]runtimestorage.ReplyCorrelation{}
	}
	if value.Memories == nil {
		value.Memories = map[string]runtimestorage.MemoryRecord{}
	}
	if value.MemoryIndexHandoffs == nil {
		value.MemoryIndexHandoffs = map[string]int64{}
	}
	return value
}

func (s *Store) load(ctx context.Context, tenantID string) (state, error) {
	result, err := s.client.Get(ctx, s.key(tenantID)).Bytes()
	if errors.Is(err, redisclient.Nil) {
		return emptyState(), nil
	}
	if err != nil {
		return state{}, mapRedisError(ctx, err)
	}
	var value state
	if err := json.Unmarshal(result, &value); err != nil || value.Version != stateVersion {
		return state{}, runtimestorage.ErrStorage
	}
	return normalizeState(value), nil
}

func (s *Store) mutate(ctx context.Context, tenantID string, fn func(*state) error) error {
	if err := s.check(ctx); err != nil {
		return err
	}
	if err := runtimestorage.ValidateTenant(tenantID); err != nil {
		return err
	}
	key := s.key(tenantID)
	for attempt := 0; attempt < maxWatchRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := s.client.Watch(ctx, func(tx *redisclient.Tx) error {
			value, err := s.loadFromTx(ctx, tx, key)
			if err != nil {
				return err
			}
			if err := fn(&value); err != nil {
				return err
			}
			encoded, err := json.Marshal(value)
			if err != nil {
				return runtimestorage.ErrStorage
			}
			_, err = tx.TxPipelined(ctx, func(pipe redisclient.Pipeliner) error {
				pipe.Set(ctx, key, encoded, 0)
				return nil
			})
			return err
		}, key)
		if err == nil {
			return nil
		}
		if errors.Is(err, redisclient.TxFailedErr) {
			continue
		}
		return mapRedisError(ctx, err)
	}
	return runtimestorage.ErrConflict
}

func (s *Store) loadFromTx(ctx context.Context, tx *redisclient.Tx, key string) (state, error) {
	result, err := tx.Get(ctx, key).Bytes()
	if errors.Is(err, redisclient.Nil) {
		return emptyState(), nil
	}
	if err != nil {
		return state{}, mapRedisError(ctx, err)
	}
	var value state
	if err := json.Unmarshal(result, &value); err != nil || value.Version != stateVersion {
		return state{}, runtimestorage.ErrStorage
	}
	return normalizeState(value), nil
}

func mapRedisError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	for _, sentinel := range []error{runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid, runtimestorage.ErrIllegalTransition, runtimestorage.ErrStorage} {
		if errors.Is(err, sentinel) {
			return err
		}
	}
	return runtimestorage.ErrStorage
}

func scopedKey(parts ...string) string {
	var b strings.Builder
	for _, part := range parts {
		b.WriteString(strconv.Itoa(len(part)))
		b.WriteByte(':')
		b.WriteString(part)
	}
	return b.String()
}

func replyKey(replyID string, segment int) string    { return scopedKey(replyID, strconv.Itoa(segment)) }
func messageKey(bindingID, externalID string) string { return scopedKey(bindingID, externalID) }

func cloneMap(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}
	data, err := json.Marshal(input)
	if err != nil {
		return nil
	}
	var output map[string]any
	if json.Unmarshal(data, &output) != nil {
		return nil
	}
	return output
}

func cloneSession(v sessionstorage.Session) sessionstorage.Session {
	v.State = cloneMap(v.State)
	return v
}
func cloneEvent(v runtimestorage.MessageEvent) runtimestorage.MessageEvent {
	if v.LeaseExpiresAt != nil {
		x := *v.LeaseExpiresAt
		v.LeaseExpiresAt = &x
	}
	return v
}
func clonePayload(v sessionstorage.EventPayload) sessionstorage.EventPayload {
	v.Payload = append([]byte(nil), v.Payload...)
	return v
}
func cloneReply(v runtimestorage.ReplyOutbox) runtimestorage.ReplyOutbox {
	if v.LeaseExpiresAt != nil {
		x := *v.LeaseExpiresAt
		v.LeaseExpiresAt = &x
	}
	return v
}
func cloneMemory(v runtimestorage.MemoryRecord) runtimestorage.MemoryRecord {
	v.Topics = append([]string(nil), v.Topics...)
	v.Metadata = cloneMap(v.Metadata)
	v.Embedding = append([]float64(nil), v.Embedding...)
	if v.DeletedAt != nil {
		x := *v.DeletedAt
		v.DeletedAt = &x
	}
	return v
}

func jsonEqual(left, right []byte) bool {
	var a, b any
	return json.Unmarshal(left, &a) == nil && json.Unmarshal(right, &b) == nil && reflect.DeepEqual(a, b)
}

// GetReplyCorrelation returns the tenant-scoped request correlation for an event.
func (s *Store) GetReplyCorrelation(ctx context.Context, tenantID, eventID string) (runtimestorage.ReplyCorrelation, error) {
	if err := s.check(ctx); err != nil {
		return runtimestorage.ReplyCorrelation{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || eventID == "" {
		return runtimestorage.ReplyCorrelation{}, runtimestorage.ErrInvalid
	}
	value, err := s.load(ctx, tenantID)
	if err != nil {
		return runtimestorage.ReplyCorrelation{}, err
	}
	result, ok := value.Correlations[eventID]
	if !ok {
		return runtimestorage.ReplyCorrelation{}, runtimestorage.ErrNotFound
	}
	result.TraceParent = observability.NormalizeTraceParent(result.TraceParent)
	return result, nil
}

// GetSession returns one tenant-scoped session.
func (s *Store) GetSession(ctx context.Context, tenantID, sessionID string) (sessionstorage.Session, error) {
	if err := s.check(ctx); err != nil {
		return sessionstorage.Session{}, err
	}
	if runtimestorage.ValidateSession(tenantID, sessionID) != nil {
		return sessionstorage.Session{}, runtimestorage.ErrInvalid
	}
	value, err := s.load(ctx, tenantID)
	if err != nil {
		return sessionstorage.Session{}, err
	}
	result, ok := value.Sessions[sessionID]
	if !ok {
		return sessionstorage.Session{}, runtimestorage.ErrNotFound
	}
	return cloneSession(result), nil
}

// CreateSession creates an active tenant-scoped session.
func (s *Store) CreateSession(ctx context.Context, tenantID, sessionID string, stateValue map[string]any) (sessionstorage.Session, error) {
	if err := s.check(ctx); err != nil {
		return sessionstorage.Session{}, err
	}
	if runtimestorage.ValidateSession(tenantID, sessionID) != nil || (stateValue != nil && cloneMap(stateValue) == nil) {
		return sessionstorage.Session{}, runtimestorage.ErrInvalid
	}
	result := sessionstorage.Session{TenantID: tenantID, SessionID: sessionID, Status: runtimestorage.SessionActive, Version: 1, State: cloneMap(stateValue), CreatedAt: time.Now().UTC()}
	result.UpdatedAt = result.CreatedAt
	err := s.mutate(ctx, tenantID, func(value *state) error {
		if _, ok := value.Sessions[sessionID]; ok {
			return runtimestorage.ErrDuplicate
		}
		value.Sessions[sessionID] = result
		return nil
	})
	if err != nil {
		return sessionstorage.Session{}, err
	}
	return cloneSession(result), nil
}

// UpdateSessionState applies an optimistic-concurrency session update.
func (s *Store) UpdateSessionState(ctx context.Context, tenantID, sessionID string, expectedVersion int64, stateValue map[string]any) (sessionstorage.Session, error) {
	if err := s.check(ctx); err != nil {
		return sessionstorage.Session{}, err
	}
	if runtimestorage.ValidateSession(tenantID, sessionID) != nil || (stateValue != nil && cloneMap(stateValue) == nil) {
		return sessionstorage.Session{}, runtimestorage.ErrInvalid
	}
	var result sessionstorage.Session
	err := s.mutate(ctx, tenantID, func(value *state) error {
		current, ok := value.Sessions[sessionID]
		if !ok {
			return runtimestorage.ErrNotFound
		}
		if current.Version != expectedVersion {
			return runtimestorage.ErrConflict
		}
		current.Version++
		current.State = cloneMap(stateValue)
		current.UpdatedAt = time.Now().UTC()
		value.Sessions[sessionID] = current
		result = cloneSession(current)
		return nil
	})
	return result, err
}

// DeleteSession removes a session and its dependent runtime records.
func (s *Store) DeleteSession(ctx context.Context, tenantID, sessionID string) error {
	if err := s.check(ctx); err != nil {
		return err
	}
	if runtimestorage.ValidateSession(tenantID, sessionID) != nil {
		return runtimestorage.ErrInvalid
	}
	return s.mutate(ctx, tenantID, func(value *state) error {
		if _, ok := value.Sessions[sessionID]; !ok {
			return runtimestorage.ErrNotFound
		}
		delete(value.Sessions, sessionID)
		delete(value.Histories, sessionID)
		for id, event := range value.Events {
			if event.SessionID != sessionID {
				continue
			}
			delete(value.Events, id)
			delete(value.Messages, messageKey(event.BindingID, event.ExternalMessageID))
			for key, reply := range value.Replies {
				if reply.EventID == event.EventID {
					delete(value.Replies, key)
				}
			}
		}
		return nil
	})
}

// RecordMessage records or returns an idempotent inbound message event.
func (s *Store) RecordMessage(ctx context.Context, input runtimestorage.MessageEventInput) (runtimestorage.MessageEvent, bool, error) {
	if err := s.check(ctx); err != nil {
		return runtimestorage.MessageEvent{}, false, err
	}
	if runtimestorage.ValidateSession(input.TenantID, input.SessionID) != nil || input.BindingID == "" || input.ExternalMessageID == "" || input.EventID == "" || runtimestorage.ValidateReplyTarget(input.ReplyTarget) != nil || (input.ReplyTarget != (runtimestorage.ReplyTarget{}) && input.ReplyTarget.BindingID != input.BindingID) {
		return runtimestorage.MessageEvent{}, false, runtimestorage.ErrInvalid
	}
	var result runtimestorage.MessageEvent
	duplicate := false
	err := s.mutate(ctx, input.TenantID, func(value *state) error {
		if id, ok := value.Messages[messageKey(input.BindingID, input.ExternalMessageID)]; ok {
			result = cloneEvent(value.Events[id])
			duplicate = true
			return nil
		}
		if _, ok := value.Events[input.EventID]; ok {
			return runtimestorage.ErrDuplicate
		}
		sess, ok := value.Sessions[input.SessionID]
		if !ok {
			return runtimestorage.ErrNotFound
		}
		sess.Version++
		sess.UpdatedAt = time.Now().UTC()
		value.Sessions[input.SessionID] = sess
		now := time.Now().UTC()
		result = runtimestorage.MessageEvent{TenantID: input.TenantID, EventID: input.EventID, SessionID: input.SessionID, BindingID: input.BindingID, ExternalMessageID: input.ExternalMessageID, IdempotencyKey: input.IdempotencyKey, EventSeq: sess.Version, Status: runtimestorage.EventReceived, ReplyTarget: input.ReplyTarget, CreatedAt: now, UpdatedAt: now}
		value.Events[input.EventID] = result
		value.Messages[messageKey(input.BindingID, input.ExternalMessageID)] = input.EventID
		return nil
	})
	return result, duplicate, err
}

func validateMessageTransition(t runtimestorage.MessageTransition) error {
	if runtimestorage.ValidateTenant(t.TenantID) != nil || t.EventID == "" || t.Owner == "" {
		return runtimestorage.ErrInvalid
	}
	if !runtimestorage.ValidateMessageTransition(t.From, t.To) {
		return runtimestorage.ErrIllegalTransition
	}
	if t.To == runtimestorage.EventRunning && t.LeaseDuration <= 0 {
		return runtimestorage.ErrInvalid
	}
	return nil
}

// GetMessage returns one tenant-scoped inbound message event.
func (s *Store) GetMessage(ctx context.Context, tenantID, eventID string) (runtimestorage.MessageEvent, error) {
	if err := s.check(ctx); err != nil {
		return runtimestorage.MessageEvent{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || eventID == "" {
		return runtimestorage.MessageEvent{}, runtimestorage.ErrInvalid
	}
	value, err := s.load(ctx, tenantID)
	if err != nil {
		return runtimestorage.MessageEvent{}, err
	}
	result, ok := value.Events[eventID]
	if !ok {
		return runtimestorage.MessageEvent{}, runtimestorage.ErrNotFound
	}
	return cloneEvent(result), nil
}

// TransitionMessage advances an inbound event through its fenced lifecycle.
func (s *Store) TransitionMessage(ctx context.Context, transition runtimestorage.MessageTransition) (runtimestorage.MessageEvent, error) {
	if err := s.check(ctx); err != nil {
		return runtimestorage.MessageEvent{}, err
	}
	if err := validateMessageTransition(transition); err != nil {
		return runtimestorage.MessageEvent{}, err
	}
	var result runtimestorage.MessageEvent
	err := s.mutate(ctx, transition.TenantID, func(value *state) error {
		current, ok := value.Events[transition.EventID]
		if !ok {
			return runtimestorage.ErrNotFound
		}
		if current.Status != transition.From {
			return runtimestorage.ErrConflict
		}
		now := time.Now().UTC()
		if transition.From == runtimestorage.EventRunning {
			if transition.To == runtimestorage.EventExecutionReconciling {
				if current.LeaseExpiresAt == nil || current.LeaseExpiresAt.After(now) {
					return runtimestorage.ErrConflict
				}
			} else if current.LeaseOwner != transition.Owner || transition.FencingToken == 0 || current.FencingToken != transition.FencingToken || current.LeaseExpiresAt == nil || !current.LeaseExpiresAt.After(now) {
				return runtimestorage.ErrConflict
			}
		}
		if transition.To == runtimestorage.EventRunning {
			deadline := now.Add(transition.LeaseDuration)
			current.LeaseOwner = transition.Owner
			current.LeaseExpiresAt = &deadline
		} else {
			current.LeaseOwner = ""
			current.LeaseExpiresAt = nil
		}
		current.Status = transition.To
		if transition.ReplyID != "" {
			current.ReplyID = transition.ReplyID
		}
		if transition.SegmentCount > 0 {
			current.SegmentCount = transition.SegmentCount
		}
		current.FencingToken++
		current.UpdatedAt = now
		value.Events[transition.EventID] = current
		result = cloneEvent(current)
		return nil
	})
	return result, err
}

func validatePayload(value sessionstorage.EventPayload) error {
	if runtimestorage.ValidateSession(value.TenantID, value.SessionID) != nil || value.EventID == "" || len(value.Payload) == 0 || !json.Valid(value.Payload) {
		return runtimestorage.ErrInvalid
	}
	return nil
}

// AppendEventPayload appends or idempotently replays one session event payload.
func (s *Store) AppendEventPayload(ctx context.Context, payload sessionstorage.EventPayload) (sessionstorage.EventPayload, error) {
	if err := s.check(ctx); err != nil {
		return sessionstorage.EventPayload{}, err
	}
	if err := validatePayload(payload); err != nil {
		return sessionstorage.EventPayload{}, err
	}
	var result sessionstorage.EventPayload
	err := s.mutate(ctx, payload.TenantID, func(value *state) error {
		if _, ok := value.Sessions[payload.SessionID]; !ok {
			return runtimestorage.ErrNotFound
		}
		entries := value.Histories[payload.SessionID]
		for _, existing := range entries {
			if existing.EventID != payload.EventID {
				continue
			}
			if !jsonEqual(existing.Payload, payload.Payload) {
				return runtimestorage.ErrConflict
			}
			result = clonePayload(existing)
			return nil
		}
		payload.HistorySeq = int64(len(entries) + 1)
		payload.CreatedAt = time.Now().UTC()
		value.Histories[payload.SessionID] = append(entries, clonePayload(payload))
		result = clonePayload(payload)
		return nil
	})
	return result, err
}

// ListEventPayloads returns a session's event history in sequence order.
func (s *Store) ListEventPayloads(ctx context.Context, tenantID, sessionID string) ([]sessionstorage.EventPayload, error) {
	if err := s.check(ctx); err != nil {
		return nil, err
	}
	if runtimestorage.ValidateSession(tenantID, sessionID) != nil {
		return nil, runtimestorage.ErrInvalid
	}
	value, err := s.load(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	if _, ok := value.Sessions[sessionID]; !ok {
		return nil, runtimestorage.ErrNotFound
	}
	entries := value.Histories[sessionID]
	result := make([]sessionstorage.EventPayload, len(entries))
	for i, item := range entries {
		result[i] = clonePayload(item)
	}
	return result, nil
}

func validateReplySegment(value runtimestorage.ReplyOutbox) error {
	if runtimestorage.ValidateTenant(value.TenantID) != nil || value.ReplyID == "" || value.EventID == "" || value.SegmentIndex < 0 || value.SegmentCount <= value.SegmentIndex || runtimestorage.ValidateReplyTarget(value.ReplyTarget) != nil {
		return runtimestorage.ErrInvalid
	}
	return nil
}
func prepareReply(value runtimestorage.ReplyOutbox) (runtimestorage.ReplyOutbox, error) {
	if value.Status == "" {
		value.Status = runtimestorage.ReplyPending
	}
	if err := validateReplySegment(value); err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	if value.Status != runtimestorage.ReplyPending {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	return value, nil
}
func sameReply(a, b runtimestorage.ReplyOutbox) bool {
	return a.EventID == b.EventID && a.SegmentCount == b.SegmentCount && a.Payload == b.Payload && a.ReplyTarget == b.ReplyTarget
}

// EnqueueReply materializes one pending reply segment.
func (s *Store) EnqueueReply(ctx context.Context, input runtimestorage.ReplyOutbox) (runtimestorage.ReplyOutbox, error) {
	value, err := prepareReply(input)
	if err != nil {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	var result runtimestorage.ReplyOutbox
	err = s.mutate(ctx, value.TenantID, func(current *state) error {
		event, ok := current.Events[value.EventID]
		if !ok {
			return runtimestorage.ErrNotFound
		}
		if event.ReplyTarget != value.ReplyTarget {
			return runtimestorage.ErrConflict
		}
		key := replyKey(value.ReplyID, value.SegmentIndex)
		if existing, ok := current.Replies[key]; ok {
			if !sameReply(existing, value) {
				return runtimestorage.ErrConflict
			}
			result = cloneReply(existing)
			return nil
		}
		now := time.Now().UTC()
		value.CreatedAt, value.UpdatedAt = now, now
		current.Replies[key] = value
		result = cloneReply(value)
		return nil
	})
	return result, err
}

func validateReplyBatch(values []runtimestorage.ReplyOutbox) (runtimestorage.ReplyOutbox, error) {
	if len(values) == 0 {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	first, err := prepareReply(values[0])
	if err != nil {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	seen := map[int]bool{}
	for _, raw := range values {
		value, err := prepareReply(raw)
		if err != nil || value.TenantID != first.TenantID || value.ReplyID != first.ReplyID || value.EventID != first.EventID || value.SegmentCount != first.SegmentCount || value.ReplyTarget != first.ReplyTarget || seen[value.SegmentIndex] {
			return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
		}
		seen[value.SegmentIndex] = true
	}
	if len(seen) != first.SegmentCount {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	for i := 0; i < first.SegmentCount; i++ {
		if !seen[i] {
			return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
		}
	}
	return first, nil
}

// EnqueueReplies materializes a complete reply batch atomically.
func (s *Store) EnqueueReplies(ctx context.Context, values []runtimestorage.ReplyOutbox) ([]runtimestorage.ReplyOutbox, error) {
	return s.enqueueReplies(ctx, runtimestorage.ReplyCorrelation{}, values)
}

// EnqueueRepliesWithCorrelation atomically stores correlation and reply segments.
func (s *Store) EnqueueRepliesWithCorrelation(ctx context.Context, correlation runtimestorage.ReplyCorrelation, values []runtimestorage.ReplyOutbox) ([]runtimestorage.ReplyOutbox, error) {
	if correlation.TenantID == "" || correlation.EventID == "" || correlation.RequestID == "" {
		return nil, runtimestorage.ErrInvalid
	}
	correlation.TraceParent = observability.NormalizeTraceParent(correlation.TraceParent)
	return s.enqueueReplies(ctx, correlation, values)
}
func (s *Store) enqueueReplies(ctx context.Context, correlation runtimestorage.ReplyCorrelation, values []runtimestorage.ReplyOutbox) ([]runtimestorage.ReplyOutbox, error) {
	if err := s.check(ctx); err != nil {
		return nil, err
	}
	first, err := validateReplyBatch(values)
	if err != nil {
		return nil, err
	}
	normalized := make([]runtimestorage.ReplyOutbox, len(values))
	for i, raw := range values {
		var prepareErr error
		normalized[i], prepareErr = prepareReply(raw)
		if prepareErr != nil {
			return nil, prepareErr
		}
	}
	var result []runtimestorage.ReplyOutbox
	err = s.mutate(ctx, first.TenantID, func(current *state) error {
		event, ok := current.Events[first.EventID]
		if !ok {
			return runtimestorage.ErrNotFound
		}
		if event.ReplyTarget != first.ReplyTarget {
			return runtimestorage.ErrConflict
		}
		if correlation.RequestID != "" {
			if correlation.TenantID != first.TenantID || correlation.EventID != first.EventID {
				return runtimestorage.ErrInvalid
			}
			if existing, ok := current.Correlations[first.EventID]; ok && existing != correlation {
				return runtimestorage.ErrConflict
			}
		}
		for _, value := range normalized {
			if existing, ok := current.Replies[replyKey(value.ReplyID, value.SegmentIndex)]; ok && !sameReply(existing, value) {
				return runtimestorage.ErrConflict
			}
		}
		now := time.Now().UTC()
		result = make([]runtimestorage.ReplyOutbox, 0, len(normalized))
		for _, value := range normalized {
			key := replyKey(value.ReplyID, value.SegmentIndex)
			if existing, ok := current.Replies[key]; ok {
				result = append(result, cloneReply(existing))
				continue
			}
			value.CreatedAt, value.UpdatedAt = now, now
			current.Replies[key] = value
			result = append(result, cloneReply(value))
		}
		if correlation.RequestID != "" {
			current.Correlations[first.EventID] = correlation
		}
		return nil
	})
	return result, err
}

// GetReply returns one tenant-scoped reply segment.
func (s *Store) GetReply(ctx context.Context, tenantID, replyID string, segment int) (runtimestorage.ReplyOutbox, error) {
	if err := s.check(ctx); err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || replyID == "" || segment < 0 {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	value, err := s.load(ctx, tenantID)
	if err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	result, ok := value.Replies[replyKey(replyID, segment)]
	if !ok {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrNotFound
	}
	return cloneReply(result), nil
}

// ListReplyCandidates returns pending, retryable, and expired sending segments.
func (s *Store) ListReplyCandidates(ctx context.Context, tenantID string) ([]runtimestorage.ReplyOutbox, error) {
	if err := s.check(ctx); err != nil {
		return nil, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil {
		return nil, runtimestorage.ErrInvalid
	}
	value, err := s.load(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	result := make([]runtimestorage.ReplyOutbox, 0, len(value.Replies))
	for _, item := range value.Replies {
		result = append(result, cloneReply(item))
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].ReplyID == result[j].ReplyID {
			return result[i].SegmentIndex < result[j].SegmentIndex
		}
		return result[i].ReplyID < result[j].ReplyID
	})
	return result, nil
}

// ClaimReply claims one reply segment with an expiring fencing lease.
func (s *Store) ClaimReply(ctx context.Context, tenantID, replyID string, segment int, owner string, duration time.Duration) (runtimestorage.ReplyOutbox, error) {
	if err := s.check(ctx); err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || replyID == "" || segment < 0 || owner == "" || duration <= 0 {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	var result runtimestorage.ReplyOutbox
	err := s.mutate(ctx, tenantID, func(value *state) error {
		current, ok := value.Replies[replyKey(replyID, segment)]
		if !ok {
			return runtimestorage.ErrNotFound
		}
		now := time.Now().UTC()
		expired := current.Status == runtimestorage.ReplySending && current.LeaseExpiresAt != nil && !current.LeaseExpiresAt.After(now)
		if current.Status != runtimestorage.ReplyPending && current.Status != runtimestorage.ReplyRetryable && !expired {
			return runtimestorage.ErrConflict
		}
		deadline := now.Add(duration)
		current.Status, current.Attempts, current.FencingToken, current.LeaseOwner, current.LeaseExpiresAt, current.UpdatedAt = runtimestorage.ReplySending, current.Attempts+1, current.FencingToken+1, owner, &deadline, now
		value.Replies[replyKey(replyID, segment)] = current
		result = cloneReply(current)
		return nil
	})
	return result, err
}

// RecordReplyReceipt persists a provider acknowledgement without releasing
// the current sending lease. The worker later owns the sent transition.
func (s *Store) RecordReplyReceipt(ctx context.Context, receipt runtimestorage.ReplyReceipt) (runtimestorage.ReplyOutbox, error) {
	if err := s.check(ctx); err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	if runtimestorage.ValidateTenant(receipt.TenantID) != nil || receipt.ReplyID == "" || receipt.SegmentIndex < 0 || receipt.Owner == "" || receipt.FencingToken <= 0 || strings.TrimSpace(receipt.ProviderID) == "" {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	var result runtimestorage.ReplyOutbox
	err := s.mutate(ctx, receipt.TenantID, func(value *state) error {
		key := replyKey(receipt.ReplyID, receipt.SegmentIndex)
		current, ok := value.Replies[key]
		if !ok {
			return runtimestorage.ErrNotFound
		}
		now := time.Now().UTC()
		if current.Status != runtimestorage.ReplySending || current.LeaseOwner != receipt.Owner || current.FencingToken != receipt.FencingToken || current.LeaseExpiresAt == nil || !current.LeaseExpiresAt.After(now) || current.ProviderMessageID != "" && current.ProviderMessageID != receipt.ProviderID {
			return runtimestorage.ErrConflict
		}
		if current.ProviderMessageID == "" {
			current.ProviderMessageID = receipt.ProviderID
			current.UpdatedAt = now
			value.Replies[key] = current
		}
		result = cloneReply(current)
		return nil
	})
	return result, err
}

// TransitionReply advances a reply segment through its fenced lifecycle.
func (s *Store) TransitionReply(ctx context.Context, transition runtimestorage.ReplyTransition) (runtimestorage.ReplyOutbox, error) {
	if err := s.check(ctx); err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	if runtimestorage.ValidateTenant(transition.TenantID) != nil || transition.ReplyID == "" || transition.SegmentIndex < 0 || transition.Owner == "" {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	if !runtimestorage.ValidateTransition(transition.From, transition.To) {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrIllegalTransition
	}
	var result runtimestorage.ReplyOutbox
	err := s.mutate(ctx, transition.TenantID, func(value *state) error {
		key := replyKey(transition.ReplyID, transition.SegmentIndex)
		current, ok := value.Replies[key]
		if !ok {
			return runtimestorage.ErrNotFound
		}
		now := time.Now().UTC()
		if current.Status != transition.From || (current.LeaseOwner != "" && current.LeaseOwner != transition.Owner) || (transition.FencingToken != 0 && current.FencingToken != transition.FencingToken) || (current.Status == runtimestorage.ReplySending && (current.LeaseExpiresAt == nil || !current.LeaseExpiresAt.After(now))) {
			return runtimestorage.ErrConflict
		}
		current.Status = transition.To
		current.LeaseOwner = transition.Owner
		current.FencingToken++
		if transition.To == runtimestorage.ReplySending {
			current.Attempts++
			if transition.LeaseDuration > 0 {
				deadline := now.Add(transition.LeaseDuration)
				current.LeaseExpiresAt = &deadline
			}
		} else {
			current.LeaseOwner = ""
			current.LeaseExpiresAt = nil
		}
		if transition.ProviderID != "" {
			current.ProviderMessageID = transition.ProviderID
		}
		current.LastErrorClass, current.UpdatedAt = transition.ErrorClass, now
		value.Replies[key] = current
		result = cloneReply(current)
		return nil
	})
	return result, err
}

// PutMemory creates or updates one durable tenant-scoped memory record.
func (s *Store) PutMemory(ctx context.Context, input runtimestorage.MemoryInput) (runtimestorage.MemoryRecord, error) {
	if err := s.check(ctx); err != nil {
		return runtimestorage.MemoryRecord{}, err
	}
	if runtimestorage.ValidateTenant(input.TenantID) != nil || !runtimestorage.ValidateText(input.UserID, 256, true) || !runtimestorage.ValidateText(input.Content, 0, true) || !runtimestorage.ValidateText(input.MemoryID, 256, false) || !runtimestorage.ValidateText(input.SessionID, 256, false) || !runtimestorage.ValidateEmbedding(input.Embedding) || (input.Metadata != nil && cloneMap(input.Metadata) == nil) {
		return runtimestorage.MemoryRecord{}, runtimestorage.ErrInvalid
	}
	if input.MemoryID == "" {
		input.MemoryID = "mem_" + uuid.NewString()
	}
	if input.Topics == nil {
		input.Topics = []string{}
	}
	if input.Metadata == nil {
		input.Metadata = map[string]any{}
	}
	if input.Embedding == nil {
		input.Embedding = []float64{}
	}
	var result runtimestorage.MemoryRecord
	err := s.mutate(ctx, input.TenantID, func(value *state) error {
		now := time.Now().UTC()
		current, ok := value.Memories[input.MemoryID]
		if ok {
			current.Content, current.Topics, current.Metadata, current.Embedding = input.Content, append([]string(nil), input.Topics...), cloneMap(input.Metadata), append([]float64(nil), input.Embedding...)
			current.UserID, current.SessionID, current.Version, current.UpdatedAt, current.DeletedAt = input.UserID, input.SessionID, current.Version+1, now, nil
			value.Memories[input.MemoryID] = current
			result = cloneMemory(current)
			return nil
		}
		current = runtimestorage.MemoryRecord{TenantID: input.TenantID, MemoryID: input.MemoryID, UserID: input.UserID, SessionID: input.SessionID, Content: input.Content, Topics: append([]string(nil), input.Topics...), Metadata: cloneMap(input.Metadata), Embedding: append([]float64(nil), input.Embedding...), Version: 1, CreatedAt: now, UpdatedAt: now}
		value.Memories[input.MemoryID] = current
		result = cloneMemory(current)
		return nil
	})
	return result, err
}

// GetMemory returns one non-deleted tenant-scoped memory record.
func (s *Store) GetMemory(ctx context.Context, tenantID, memoryID string) (runtimestorage.MemoryRecord, error) {
	if err := s.check(ctx); err != nil {
		return runtimestorage.MemoryRecord{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || memoryID == "" {
		return runtimestorage.MemoryRecord{}, runtimestorage.ErrInvalid
	}
	value, err := s.load(ctx, tenantID)
	if err != nil {
		return runtimestorage.MemoryRecord{}, err
	}
	result, ok := value.Memories[memoryID]
	if !ok || result.DeletedAt != nil {
		return runtimestorage.MemoryRecord{}, runtimestorage.ErrNotFound
	}
	return cloneMemory(result), nil
}

// ListMemories returns a tenant user's non-deleted memories.
func (s *Store) ListMemories(ctx context.Context, tenantID, userID string, limit int) ([]runtimestorage.MemoryRecord, error) {
	if err := s.check(ctx); err != nil {
		return nil, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || strings.TrimSpace(userID) == "" || limit < 0 {
		return nil, runtimestorage.ErrInvalid
	}
	value, err := s.load(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	result := make([]runtimestorage.MemoryRecord, 0)
	for _, item := range value.Memories {
		if item.UserID == userID && item.DeletedAt == nil {
			result = append(result, cloneMemory(item))
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].UpdatedAt.Equal(result[j].UpdatedAt) {
			return result[i].MemoryID < result[j].MemoryID
		}
		return result[i].UpdatedAt.After(result[j].UpdatedAt)
	})
	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

// SearchMemories searches a tenant user's memory content by text terms.
func (s *Store) SearchMemories(ctx context.Context, tenantID, userID, query string, limit int) ([]runtimestorage.MemorySearchResult, error) {
	if err := s.check(ctx); err != nil {
		return nil, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || strings.TrimSpace(userID) == "" || strings.TrimSpace(query) == "" || limit < 0 {
		return nil, runtimestorage.ErrInvalid
	}
	terms := strings.Fields(strings.ToLower(query))
	value, err := s.load(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	result := make([]runtimestorage.MemorySearchResult, 0)
	for _, item := range value.Memories {
		if item.UserID != userID || item.DeletedAt != nil {
			continue
		}
		hits := 0
		text := strings.ToLower(item.Content)
		for _, term := range terms {
			if strings.Contains(text, term) {
				hits++
			}
		}
		if hits > 0 {
			result = append(result, runtimestorage.MemorySearchResult{Memory: cloneMemory(item), Score: float64(hits) / float64(len(terms))})
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Score == result[j].Score {
			return result[i].Memory.MemoryID < result[j].Memory.MemoryID
		}
		return result[i].Score > result[j].Score
	})
	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

// DeleteMemory tombstones one tenant-scoped memory record.
func (s *Store) DeleteMemory(ctx context.Context, tenantID, memoryID string) error {
	if err := s.check(ctx); err != nil {
		return err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || memoryID == "" {
		return runtimestorage.ErrInvalid
	}
	return s.mutate(ctx, tenantID, func(value *state) error {
		current, ok := value.Memories[memoryID]
		if !ok || current.DeletedAt != nil {
			return runtimestorage.ErrNotFound
		}
		now := time.Now().UTC()
		current.DeletedAt, current.UpdatedAt, current.Version = &now, now, current.Version+1
		value.Memories[memoryID] = current
		delete(value.MemoryIndexHandoffs, scopedKey(memoryID))
		return nil
	})
}

// EnqueueMemoryIndex records a durable index handoff for a memory version.
func (s *Store) EnqueueMemoryIndex(ctx context.Context, value runtimestorage.MemoryRecord) error {
	if err := s.check(ctx); err != nil {
		return err
	}
	if runtimestorage.ValidateTenant(value.TenantID) != nil || value.MemoryID == "" || value.Version < 1 {
		return runtimestorage.ErrInvalid
	}
	return s.mutate(ctx, value.TenantID, func(current *state) error {
		stored, ok := current.Memories[value.MemoryID]
		if !ok || stored.DeletedAt != nil {
			return runtimestorage.ErrNotFound
		}
		if stored.Version != value.Version {
			return runtimestorage.ErrConflict
		}
		current.MemoryIndexHandoffs[scopedKey(value.MemoryID)] = value.Version
		return nil
	})
}

// WaitForMemoryIndex verifies that a memory version has been handed off.
func (s *Store) WaitForMemoryIndex(ctx context.Context, tenantID, memoryID string, version int64) error {
	if err := s.check(ctx); err != nil {
		return err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || memoryID == "" || version < 1 {
		return runtimestorage.ErrInvalid
	}
	value, err := s.load(ctx, tenantID)
	if err != nil {
		return err
	}
	current, ok := value.Memories[memoryID]
	if !ok || current.DeletedAt != nil {
		return runtimestorage.ErrNotFound
	}
	if current.Version < version || value.MemoryIndexHandoffs[scopedKey(memoryID)] < version {
		return runtimestorage.ErrConflict
	}
	return nil
}

// Close is idempotent. A caller-owned client remains open.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		if s.owned {
			if closer, ok := s.client.(interface{ Close() error }); ok {
				s.closeErr = mapRedisError(context.Background(), closer.Close())
			}
		}
	})
	return s.closeErr
}

var _ sessionstorage.SessionStateStore = (*Store)(nil)
var _ sessionstorage.EventHistoryStore = (*Store)(nil)
var _ runtimestorage.MessageStore = (*Store)(nil)
var _ runtimestorage.ReplyStore = (*Store)(nil)
var _ runtimestorage.MemoryStore = (*Store)(nil)
var _ runtimestorage.ReplyBatchEnqueuer = (*Store)(nil)
var _ runtimestorage.ReplyBatchCorrelationEnqueuer = (*Store)(nil)
var _ runtimestorage.ReplyCorrelationStore = (*Store)(nil)
var _ runtimestorage.ReplyReceiptRecorder = (*Store)(nil)
