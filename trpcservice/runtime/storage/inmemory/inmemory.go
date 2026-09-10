// Package inmemory provides a concurrency-safe runtime store for tests and local development.
package inmemory

import (
	"context"
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	sessionstorage "github.com/XnLemon/trpc-agent-service/trpcservice/storage/session"
)

// Store is a concurrency-safe in-memory implementation of the runtime store.
type Store struct {
	mu           *sync.RWMutex
	sessions     map[string]sessionstorage.Session
	events       map[string]runtimestorage.MessageEvent
	histories    map[string][]sessionstorage.EventPayload
	messages     map[string]string
	replies      map[string]runtimestorage.ReplyOutbox
	correlations map[string]runtimestorage.ReplyCorrelation
	memories     map[string]runtimestorage.MemoryRecord
	summaries    map[string]runtimestorage.SummaryRecord
	knowledge    map[string]runtimestorage.KnowledgeDocument
	artifacts    map[string]runtimestorage.ArtifactRecord
	audits       map[string][]runtimestorage.AuditRecord
	vectors      map[string]runtimestorage.VectorRecord
	objects      map[string]runtimestorage.ObjectInfo
	objectData   map[string][]byte
	attachments  map[string]storedAttachment
	indexQueue   chan runtimestorage.MemoryRecord
	indexDone    chan struct{}
	indexMu      *sync.RWMutex
	closeOnce    *sync.Once
	lifecycle    *backendLifecycle
}

// Backend owns one in-memory state graph that can be shared by multiple
// tenant workers in tests, modeling cross-node visibility of a shared store.
type Backend struct {
	store     *Store
	lifecycle *backendLifecycle
	closeOnce sync.Once
}

type backendLifecycle struct {
	mu      sync.Mutex
	refs    int
	closed  bool
	done    chan struct{}
	indexMu *sync.RWMutex
}

func (l *backendLifecycle) retain() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return false
	}
	l.refs++
	return true
}

func (l *backendLifecycle) release() {
	l.mu.Lock()
	if l.refs > 0 {
		l.refs--
	}
	if l.refs != 0 || l.closed {
		l.mu.Unlock()
		return
	}
	l.closed = true
	l.mu.Unlock()
	l.indexMu.Lock()
	close(l.done)
	l.indexMu.Unlock()
}

// NewBackend creates an isolated shared in-memory backend.
func NewBackend() *Backend {
	lifecycle := &backendLifecycle{refs: 1, done: make(chan struct{}), indexMu: &sync.RWMutex{}}
	store := newStore(lifecycle)
	return &Backend{store: store, lifecycle: lifecycle}
}

// NewWithBackend creates a store view over an existing shared backend. Each
// view owns its close operation while the backend keeps shared state and one
// asynchronous index worker alive until the final view closes.
func NewWithBackend(backend *Backend) *Store {
	if backend == nil {
		return New()
	}
	if backend.store == nil {
		backend.lifecycle = &backendLifecycle{refs: 1, done: make(chan struct{}), indexMu: &sync.RWMutex{}}
		backend.store = newStore(backend.lifecycle)
	}
	if backend.lifecycle == nil || !backend.lifecycle.retain() {
		// Keep closed-backend views attached to the same state graph. They
		// cannot enqueue new index work, but reads remain consistent with
		// other views instead of silently switching to an unrelated store.
		view := *backend.store
		view.closeOnce = &sync.Once{}
		return &view
	}
	view := *backend.store
	view.closeOnce = &sync.Once{}
	return &view
}

// Close releases the backend owner. Views remain usable until the final view
// and the backend owner have both been closed.
func (backend *Backend) Close() error {
	if backend == nil || backend.lifecycle == nil {
		return nil
	}
	backend.closeOnce.Do(backend.lifecycle.release)
	return nil
}

// New creates an empty runtime store.
func New() *Store {
	lifecycle := &backendLifecycle{refs: 1, done: make(chan struct{}), indexMu: &sync.RWMutex{}}
	return newStore(lifecycle)
}

func newStore(lifecycle *backendLifecycle) *Store {
	store := &Store{
		mu:       &sync.RWMutex{},
		sessions: map[string]sessionstorage.Session{}, events: map[string]runtimestorage.MessageEvent{},
		histories: map[string][]sessionstorage.EventPayload{}, messages: map[string]string{},
		replies: map[string]runtimestorage.ReplyOutbox{}, correlations: map[string]runtimestorage.ReplyCorrelation{},
		memories: map[string]runtimestorage.MemoryRecord{}, summaries: map[string]runtimestorage.SummaryRecord{},
		knowledge: map[string]runtimestorage.KnowledgeDocument{}, artifacts: map[string]runtimestorage.ArtifactRecord{},
		audits: map[string][]runtimestorage.AuditRecord{}, vectors: map[string]runtimestorage.VectorRecord{},
		objects: map[string]runtimestorage.ObjectInfo{}, objectData: map[string][]byte{}, attachments: map[string]storedAttachment{},
		indexQueue: make(chan runtimestorage.MemoryRecord, 128), indexDone: lifecycle.done, indexMu: lifecycle.indexMu, lifecycle: lifecycle, closeOnce: &sync.Once{},
	}
	go store.indexWorker()
	return store
}

// GetReplyCorrelation loads a durable execution-to-reply correlation.
func (s *Store) GetReplyCorrelation(ctx context.Context, tenantID, eventID string) (runtimestorage.ReplyCorrelation, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.ReplyCorrelation{}, err
	}
	if tenantID == "" || eventID == "" {
		return runtimestorage.ReplyCorrelation{}, runtimestorage.ErrInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.correlations[key(tenantID, eventID)]
	if !ok {
		return runtimestorage.ReplyCorrelation{}, runtimestorage.ErrNotFound
	}
	value.TraceParent = observability.NormalizeTraceParent(value.TraceParent)
	return value, nil
}

// GetSession returns a tenant-scoped session snapshot.
func (s *Store) GetSession(ctx context.Context, tenantID, sessionID string) (sessionstorage.Session, error) {
	if err := check(ctx); err != nil {
		return sessionstorage.Session{}, err
	}
	if err := runtimestorage.ValidateSession(tenantID, sessionID); err != nil {
		return sessionstorage.Session{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.sessions[key(tenantID, sessionID)]
	if !ok {
		return sessionstorage.Session{}, runtimestorage.ErrNotFound
	}
	return cloneSession(value), nil
}

// CreateSession creates a tenant-scoped session with an initial state.
func (s *Store) CreateSession(ctx context.Context, tenantID, sessionID string, state map[string]any) (sessionstorage.Session, error) {
	if err := check(ctx); err != nil {
		return sessionstorage.Session{}, err
	}
	if err := runtimestorage.ValidateSession(tenantID, sessionID); err != nil {
		return sessionstorage.Session{}, err
	}
	now := time.Now().UTC()
	value := sessionstorage.Session{TenantID: tenantID, SessionID: sessionID, Status: runtimestorage.SessionActive, Version: 1, State: cloneMap(state), CreatedAt: now, UpdatedAt: now}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.sessions[key(tenantID, sessionID)]; ok {
		return sessionstorage.Session{}, runtimestorage.ErrDuplicate
	}
	s.sessions[key(tenantID, sessionID)] = value
	return cloneSession(value), nil
}

// UpdateSessionState applies an expected-version state update.
func (s *Store) UpdateSessionState(ctx context.Context, tenantID, sessionID string, expectedVersion int64, state map[string]any) (sessionstorage.Session, error) {
	if err := check(ctx); err != nil {
		return sessionstorage.Session{}, err
	}
	if err := runtimestorage.ValidateSession(tenantID, sessionID); err != nil {
		return sessionstorage.Session{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(tenantID, sessionID)
	value, ok := s.sessions[k]
	if !ok {
		return sessionstorage.Session{}, runtimestorage.ErrNotFound
	}
	if value.Version != expectedVersion {
		return sessionstorage.Session{}, runtimestorage.ErrConflict
	}
	value.Version++
	value.State = cloneMap(state)
	value.UpdatedAt = time.Now().UTC()
	s.sessions[k] = value
	return cloneSession(value), nil
}

// DeleteSession removes a tenant-scoped session and its related history.
func (s *Store) DeleteSession(ctx context.Context, tenantID, sessionID string) error {
	if err := check(ctx); err != nil {
		return err
	}
	if err := runtimestorage.ValidateSession(tenantID, sessionID); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.sessions[key(tenantID, sessionID)]; !ok {
		return runtimestorage.ErrNotFound
	}
	delete(s.sessions, key(tenantID, sessionID))
	delete(s.histories, key(tenantID, sessionID))
	for summaryKey, summary := range s.summaries {
		if summary.TenantID == tenantID && summary.SessionID == sessionID {
			delete(s.summaries, summaryKey)
		}
	}
	for eventKey, event := range s.events {
		if event.TenantID != tenantID || event.SessionID != sessionID {
			continue
		}
		delete(s.events, eventKey)
		delete(s.messages, key(tenantID, event.BindingID, event.ExternalMessageID))
		for replyKey, reply := range s.replies {
			if reply.TenantID == tenantID && reply.EventID == event.EventID {
				delete(s.replies, replyKey)
			}
		}
		for attachmentKey, attachmentValue := range s.attachments {
			if attachmentValue.eventID == event.EventID && strings.HasPrefix(attachmentKey, key(tenantID)) {
				attachmentValue.eventID = ""
				s.attachments[attachmentKey] = attachmentValue
			}
		}
	}
	return nil
}

// RecordMessage stores an idempotent inbound message event.
func (s *Store) RecordMessage(ctx context.Context, input runtimestorage.MessageEventInput) (runtimestorage.MessageEvent, bool, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.MessageEvent{}, false, err
	}
	if runtimestorage.ValidateSession(input.TenantID, input.SessionID) != nil || input.BindingID == "" || input.ExternalMessageID == "" || input.EventID == "" || runtimestorage.ValidateReplyTarget(input.ReplyTarget) != nil || (input.ReplyTarget != (runtimestorage.ReplyTarget{}) && input.ReplyTarget.BindingID != input.BindingID) {
		return runtimestorage.MessageEvent{}, false, runtimestorage.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unique := key(input.TenantID, input.BindingID, input.ExternalMessageID)
	if existingID, ok := s.messages[unique]; ok {
		return cloneEvent(s.events[existingID]), true, nil
	}
	if _, ok := s.events[key(input.TenantID, input.EventID)]; ok {
		return runtimestorage.MessageEvent{}, false, runtimestorage.ErrDuplicate
	}
	sessionKey := key(input.TenantID, input.SessionID)
	sess, ok := s.sessions[sessionKey]
	if !ok {
		return runtimestorage.MessageEvent{}, false, runtimestorage.ErrNotFound
	}
	sess.Version++
	sess.UpdatedAt = time.Now().UTC()
	s.sessions[sessionKey] = sess
	event := runtimestorage.MessageEvent{TenantID: input.TenantID, EventID: input.EventID, SessionID: input.SessionID, BindingID: input.BindingID, ExternalMessageID: input.ExternalMessageID, IdempotencyKey: input.IdempotencyKey, EventSeq: sess.Version, Status: runtimestorage.EventReceived, ReplyTarget: input.ReplyTarget, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	s.events[key(input.TenantID, input.EventID)] = event
	s.messages[unique] = key(input.TenantID, input.EventID)
	return cloneEvent(event), false, nil
}

// GetMessage returns a tenant-scoped message event.
func (s *Store) GetMessage(ctx context.Context, tenantID, eventID string) (runtimestorage.MessageEvent, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.MessageEvent{}, err
	}
	if err := runtimestorage.ValidateTenant(tenantID); err != nil || eventID == "" {
		return runtimestorage.MessageEvent{}, runtimestorage.ErrInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.events[key(tenantID, eventID)]
	if !ok {
		return runtimestorage.MessageEvent{}, runtimestorage.ErrNotFound
	}
	return cloneEvent(value), nil
}

// TransitionMessage applies a validated message lifecycle transition.
func (s *Store) TransitionMessage(ctx context.Context, transition runtimestorage.MessageTransition) (runtimestorage.MessageEvent, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.MessageEvent{}, err
	}
	if err := validateMessageTransition(transition); err != nil {
		return runtimestorage.MessageEvent{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(transition.TenantID, transition.EventID)
	value, ok := s.events[k]
	if !ok {
		return runtimestorage.MessageEvent{}, runtimestorage.ErrNotFound
	}
	if value.Status != transition.From {
		return runtimestorage.MessageEvent{}, runtimestorage.ErrConflict
	}
	if err := validateMessageLease(value, transition); err != nil {
		return runtimestorage.MessageEvent{}, err
	}
	applyMessageTransition(&value, transition)
	s.events[k] = value
	return cloneEvent(value), nil
}

func validateMessageTransition(transition runtimestorage.MessageTransition) error {
	if runtimestorage.ValidateTenant(transition.TenantID) != nil || transition.EventID == "" || transition.Owner == "" {
		return runtimestorage.ErrInvalid
	}
	if !runtimestorage.ValidateMessageTransition(transition.From, transition.To) {
		return runtimestorage.ErrIllegalTransition
	}
	return nil
}

func validateMessageLease(value runtimestorage.MessageEvent, transition runtimestorage.MessageTransition) error {
	if transition.To == runtimestorage.EventRunning && transition.LeaseDuration <= 0 {
		return runtimestorage.ErrInvalid
	}
	if transition.From != runtimestorage.EventRunning {
		return nil
	}
	if transition.To == runtimestorage.EventExecutionReconciling {
		if value.LeaseExpiresAt == nil || value.LeaseExpiresAt.After(time.Now().UTC()) {
			return runtimestorage.ErrConflict
		}
		return nil
	}
	if value.LeaseOwner != transition.Owner || transition.FencingToken == 0 || value.FencingToken != transition.FencingToken || (value.LeaseExpiresAt != nil && !value.LeaseExpiresAt.After(time.Now().UTC())) {
		return runtimestorage.ErrConflict
	}
	return nil
}

func applyMessageTransition(value *runtimestorage.MessageEvent, transition runtimestorage.MessageTransition) {
	if transition.To == runtimestorage.EventRunning {
		deadline := time.Now().UTC().Add(transition.LeaseDuration)
		value.LeaseOwner = transition.Owner
		value.LeaseExpiresAt = &deadline
	} else {
		value.LeaseOwner = ""
		value.LeaseExpiresAt = nil
	}
	value.Status = transition.To
	if transition.ReplyID != "" {
		value.ReplyID = transition.ReplyID
	}
	if transition.SegmentCount > 0 {
		value.SegmentCount = transition.SegmentCount
	}
	value.FencingToken++
	value.UpdatedAt = time.Now().UTC()
}

// AppendEventPayload adds an ordered payload to a tenant session history.
func (s *Store) AppendEventPayload(ctx context.Context, payload sessionstorage.EventPayload) (sessionstorage.EventPayload, error) {
	if err := check(ctx); err != nil {
		return sessionstorage.EventPayload{}, err
	}
	if err := validatePayload(payload); err != nil {
		return sessionstorage.EventPayload{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(payload.TenantID, payload.SessionID)
	if _, ok := s.sessions[k]; !ok {
		return sessionstorage.EventPayload{}, runtimestorage.ErrNotFound
	}
	entries := s.histories[k]
	for _, existing := range entries {
		if existing.EventID != payload.EventID {
			continue
		}
		if !jsonEqual(existing.Payload, payload.Payload) {
			return sessionstorage.EventPayload{}, runtimestorage.ErrConflict
		}
		return clonePayload(existing), nil
	}
	payload.HistorySeq = int64(len(entries) + 1)
	payload.CreatedAt = time.Now().UTC()
	payload = clonePayload(payload)
	s.histories[k] = append(entries, payload)
	return clonePayload(payload), nil
}

// ListEventPayloads returns ordered payloads for a tenant session.
func (s *Store) ListEventPayloads(ctx context.Context, tenantID, sessionID string) ([]sessionstorage.EventPayload, error) {
	if err := check(ctx); err != nil {
		return nil, err
	}
	if err := runtimestorage.ValidateSession(tenantID, sessionID); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	k := key(tenantID, sessionID)
	if _, ok := s.sessions[k]; !ok {
		return nil, runtimestorage.ErrNotFound
	}
	entries := s.histories[k]
	result := make([]sessionstorage.EventPayload, len(entries))
	for i, value := range entries {
		result[i] = clonePayload(value)
	}
	return result, nil
}

// EnqueueReply stores one durable reply segment idempotently.
func (s *Store) EnqueueReply(ctx context.Context, value runtimestorage.ReplyOutbox) (runtimestorage.ReplyOutbox, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	value, err := prepareReply(value)
	if err != nil {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.validateReplyEvent(value); err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	k := replyKey(value.TenantID, value.ReplyID, value.SegmentIndex)
	if existing, ok := s.replies[k]; ok {
		if !sameReplyContract(existing, value) {
			return runtimestorage.ReplyOutbox{}, runtimestorage.ErrConflict
		}
		return cloneReply(existing), nil
	}
	s.replies[k] = value
	return cloneReply(value), nil
}

func prepareReply(value runtimestorage.ReplyOutbox) (runtimestorage.ReplyOutbox, error) {
	normalized, err := runtimestorage.NormalizeReplyOutbox(value)
	if err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	if err := validateReplySegment(normalized); err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	if normalized.Status == "" {
		normalized.Status = runtimestorage.ReplyPending
	}
	if normalized.Status != runtimestorage.ReplyPending {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	now := time.Now().UTC()
	normalized.CreatedAt = now
	normalized.UpdatedAt = now
	return normalized, nil
}

func validateReplySegment(value runtimestorage.ReplyOutbox) error {
	if runtimestorage.ValidateTenant(value.TenantID) != nil || value.ReplyID == "" || value.EventID == "" || value.SegmentIndex < 0 || value.SegmentCount <= value.SegmentIndex || runtimestorage.ValidateReplyTarget(value.ReplyTarget) != nil {
		return runtimestorage.ErrInvalid
	}
	return nil
}

func (s *Store) validateReplyEvent(value runtimestorage.ReplyOutbox) error {
	event, ok := s.events[key(value.TenantID, value.EventID)]
	if !ok {
		return runtimestorage.ErrNotFound
	}
	if event.ReplyTarget != value.ReplyTarget {
		return runtimestorage.ErrConflict
	}
	return nil
}

func sameReplyContract(existing, value runtimestorage.ReplyOutbox) bool {
	return existing.EventID == value.EventID &&
		existing.SegmentCount == value.SegmentCount &&
		existing.Payload == value.Payload &&
		existing.Kind == value.Kind &&
		existing.Attachment == value.Attachment &&
		existing.Fallback == value.Fallback &&
		existing.ReplyTarget == value.ReplyTarget
}

// EnqueueReplies validates a complete reply before committing any new segment.
// This prevents a failed multi-segment materialization from exposing a
// deliverable prefix to a worker.
//
//nolint:gocyclo // The atomic in-memory write validates and materializes the complete batch.
func (s *Store) EnqueueReplies(ctx context.Context, values []runtimestorage.ReplyOutbox) ([]runtimestorage.ReplyOutbox, error) {
	return s.enqueueReplies(ctx, runtimestorage.ReplyCorrelation{}, values)
}

// EnqueueRepliesWithCorrelation atomically persists execution correlation and
// the complete reply segment batch.
func (s *Store) EnqueueRepliesWithCorrelation(ctx context.Context, correlation runtimestorage.ReplyCorrelation, values []runtimestorage.ReplyOutbox) ([]runtimestorage.ReplyOutbox, error) {
	if correlation.TenantID == "" || correlation.EventID == "" || correlation.RequestID == "" {
		return nil, runtimestorage.ErrInvalid
	}
	correlation.TraceParent = observability.NormalizeTraceParent(correlation.TraceParent)
	return s.enqueueReplies(ctx, correlation, values)
}

func (s *Store) enqueueReplies(ctx context.Context, correlation runtimestorage.ReplyCorrelation, values []runtimestorage.ReplyOutbox) ([]runtimestorage.ReplyOutbox, error) {
	if err := check(ctx); err != nil {
		return nil, err
	}
	var err error
	values, err = normalizeReplyBatch(values)
	if err != nil {
		return nil, err
	}
	first, _, err := validateReplyBatch(values)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	event, ok := s.events[key(first.TenantID, first.EventID)]
	if !ok {
		return nil, runtimestorage.ErrNotFound
	}
	if event.ReplyTarget != first.ReplyTarget {
		return nil, runtimestorage.ErrConflict
	}
	if correlation.RequestID != "" {
		if correlation.TenantID != first.TenantID || correlation.EventID != first.EventID {
			return nil, runtimestorage.ErrInvalid
		}
		if existing, ok := s.correlations[key(correlation.TenantID, correlation.EventID)]; ok && existing != correlation {
			return nil, runtimestorage.ErrConflict
		}
	}
	if err := validateExistingReplies(s.replies, values); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	result := make([]runtimestorage.ReplyOutbox, 0, len(values))
	for _, value := range values {
		k := replyKey(value.TenantID, value.ReplyID, value.SegmentIndex)
		if existing, ok := s.replies[k]; ok {
			result = append(result, cloneReply(existing))
			continue
		}
		value.Status = runtimestorage.ReplyPending
		value.CreatedAt, value.UpdatedAt = now, now
		s.replies[k] = value
		result = append(result, cloneReply(value))
	}
	if correlation.RequestID != "" {
		s.correlations[key(correlation.TenantID, correlation.EventID)] = correlation
	}
	return result, nil
}

func validateReplyBatch(values []runtimestorage.ReplyOutbox) (runtimestorage.ReplyOutbox, map[int]struct{}, error) {
	if len(values) == 0 {
		return runtimestorage.ReplyOutbox{}, nil, runtimestorage.ErrInvalid
	}
	first := values[0]
	seen := make(map[int]struct{}, len(values))
	for _, value := range values {
		if runtimestorage.ValidateTenant(value.TenantID) != nil || value.ReplyID == "" || value.EventID == "" || value.SegmentIndex < 0 || value.SegmentCount <= value.SegmentIndex || value.Status != "" && value.Status != runtimestorage.ReplyPending || runtimestorage.ValidateReplyTarget(value.ReplyTarget) != nil || value.TenantID != first.TenantID || value.ReplyID != first.ReplyID || value.EventID != first.EventID || value.SegmentCount != first.SegmentCount || value.ReplyTarget != first.ReplyTarget {
			return runtimestorage.ReplyOutbox{}, nil, runtimestorage.ErrInvalid
		}
		if _, duplicate := seen[value.SegmentIndex]; duplicate {
			return runtimestorage.ReplyOutbox{}, nil, runtimestorage.ErrInvalid
		}
		seen[value.SegmentIndex] = struct{}{}
	}
	if len(seen) != first.SegmentCount {
		return runtimestorage.ReplyOutbox{}, nil, runtimestorage.ErrInvalid
	}
	for index := 0; index < first.SegmentCount; index++ {
		if _, present := seen[index]; !present {
			return runtimestorage.ReplyOutbox{}, nil, runtimestorage.ErrInvalid
		}
	}
	return first, seen, nil
}

func normalizeReplyBatch(values []runtimestorage.ReplyOutbox) ([]runtimestorage.ReplyOutbox, error) {
	normalized := make([]runtimestorage.ReplyOutbox, 0, len(values))
	for _, value := range values {
		reply, err := runtimestorage.NormalizeReplyOutbox(value)
		if err != nil {
			return nil, runtimestorage.ErrInvalid
		}
		normalized = append(normalized, reply)
	}
	return normalized, nil
}

func validateExistingReplies(replies map[string]runtimestorage.ReplyOutbox, values []runtimestorage.ReplyOutbox) error {
	for _, value := range values {
		if existing, ok := replies[replyKey(value.TenantID, value.ReplyID, value.SegmentIndex)]; ok && (existing.EventID != value.EventID || existing.SegmentCount != value.SegmentCount || existing.Payload != value.Payload || existing.Kind != value.Kind || existing.Attachment != value.Attachment || existing.Fallback != value.Fallback || existing.ReplyTarget != value.ReplyTarget) {
			return runtimestorage.ErrConflict
		}
	}
	return nil
}

// GetReply returns one tenant-scoped durable reply segment.
func (s *Store) GetReply(ctx context.Context, tenantID, replyID string, segment int) (runtimestorage.ReplyOutbox, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || replyID == "" || segment < 0 {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.replies[replyKey(tenantID, replyID, segment)]
	if !ok {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrNotFound
	}
	return cloneReply(value), nil
}

// ListReplyCandidates returns pending or reclaimable reply segments.
func (s *Store) ListReplyCandidates(ctx context.Context, tenantID string) ([]runtimestorage.ReplyOutbox, error) {
	if err := check(ctx); err != nil {
		return nil, err
	}
	if err := runtimestorage.ValidateTenant(tenantID); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]runtimestorage.ReplyOutbox, 0)
	for _, value := range s.replies {
		if value.TenantID != tenantID {
			continue
		}
		result = append(result, cloneReply(value))
	}
	return result, nil
}

// ClaimReply leases a pending reply segment to a delivery worker.
func (s *Store) ClaimReply(ctx context.Context, tenantID, replyID string, segment int, owner string, leaseDuration time.Duration) (runtimestorage.ReplyOutbox, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || replyID == "" || owner == "" || leaseDuration <= 0 {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := replyKey(tenantID, replyID, segment)
	value, ok := s.replies[k]
	if !ok {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrNotFound
	}
	leaseExpired := value.Status == runtimestorage.ReplySending && value.LeaseExpiresAt != nil && !value.LeaseExpiresAt.After(time.Now().UTC())
	if value.Status != runtimestorage.ReplyPending && value.Status != runtimestorage.ReplyRetryable && !leaseExpired {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrConflict
	}
	deadline := time.Now().UTC().Add(leaseDuration)
	value.Status = runtimestorage.ReplySending
	value.Attempts++
	value.FencingToken++
	value.LeaseOwner = owner
	value.LeaseExpiresAt = &deadline
	value.UpdatedAt = time.Now().UTC()
	s.replies[k] = value
	return cloneReply(value), nil
}

// RecordReplyReceipt persists a provider acknowledgement without releasing
// the current sending lease. The worker later owns the sent transition.
func (s *Store) RecordReplyReceipt(ctx context.Context, receipt runtimestorage.ReplyReceipt) (runtimestorage.ReplyOutbox, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	if runtimestorage.ValidateTenant(receipt.TenantID) != nil || receipt.ReplyID == "" || receipt.SegmentIndex < 0 || receipt.Owner == "" || receipt.FencingToken <= 0 || strings.TrimSpace(receipt.ProviderID) == "" {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := replyKey(receipt.TenantID, receipt.ReplyID, receipt.SegmentIndex)
	value, ok := s.replies[key]
	if !ok {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrNotFound
	}
	if value.Status != runtimestorage.ReplySending || value.LeaseOwner != receipt.Owner || value.FencingToken != receipt.FencingToken || value.LeaseExpiresAt == nil || !value.LeaseExpiresAt.After(time.Now().UTC()) || value.ProviderMessageID != "" && value.ProviderMessageID != receipt.ProviderID {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrConflict
	}
	if value.ProviderMessageID == "" {
		value.ProviderMessageID = receipt.ProviderID
		value.UpdatedAt = time.Now().UTC()
		s.replies[key] = value
	}
	return cloneReply(value), nil
}

// TransitionReply applies a fenced reply delivery transition.
func (s *Store) TransitionReply(ctx context.Context, transition runtimestorage.ReplyTransition) (runtimestorage.ReplyOutbox, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	if runtimestorage.ValidateTenant(transition.TenantID) != nil || transition.ReplyID == "" || transition.Owner == "" {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	if !runtimestorage.ValidateTransition(transition.From, transition.To) {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrIllegalTransition
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := replyKey(transition.TenantID, transition.ReplyID, transition.SegmentIndex)
	value, ok := s.replies[k]
	if !ok {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrNotFound
	}
	if value.Status != transition.From || (value.LeaseOwner != "" && value.LeaseOwner != transition.Owner) || (transition.FencingToken != 0 && value.FencingToken != transition.FencingToken) {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrConflict
	}
	if value.Status == runtimestorage.ReplySending && value.LeaseExpiresAt != nil && !value.LeaseExpiresAt.After(time.Now().UTC()) {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrConflict
	}
	value.Status = transition.To
	value.LeaseOwner = transition.Owner
	value.FencingToken++
	if transition.To == runtimestorage.ReplySending {
		value.Attempts++
		if transition.LeaseDuration > 0 {
			deadline := time.Now().UTC().Add(transition.LeaseDuration)
			value.LeaseExpiresAt = &deadline
		}
	}
	if transition.ProviderID != "" {
		value.ProviderMessageID = transition.ProviderID
	}
	value.LastErrorClass = transition.ErrorClass
	value.UpdatedAt = time.Now().UTC()
	s.replies[k] = value
	return cloneReply(value), nil
}

// Close stops the asynchronous memory index worker. It is idempotent.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	if s.closeOnce == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		if s.lifecycle != nil {
			s.lifecycle.release()
		}
	})
	return nil
}
func check(ctx context.Context) error {
	if ctx == nil {
		return runtimestorage.ErrInvalid
	}
	return ctx.Err()
}
func key(parts ...string) string {
	var out strings.Builder
	for _, p := range parts {
		out.WriteString(strconv.Itoa(len(p)))
		out.WriteByte(':')
		out.WriteString(p)
	}
	return out.String()
}
func replyKey(tenant, reply string, segment int) string {
	return key(tenant, reply, strconv.Itoa(segment))
}
func cloneMap(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil
	}
	var output map[string]any
	if err := json.Unmarshal(encoded, &output); err != nil {
		return nil
	}
	return output
}
func cloneSession(value sessionstorage.Session) sessionstorage.Session {
	value.State = cloneMap(value.State)
	return value
}
func cloneEvent(value runtimestorage.MessageEvent) runtimestorage.MessageEvent {
	if value.LeaseExpiresAt != nil {
		copy := *value.LeaseExpiresAt
		value.LeaseExpiresAt = &copy
	}
	return value
}
func clonePayload(value sessionstorage.EventPayload) sessionstorage.EventPayload {
	value.Payload = append([]byte(nil), value.Payload...)
	return value
}
func validatePayload(value sessionstorage.EventPayload) error {
	if runtimestorage.ValidateSession(value.TenantID, value.SessionID) != nil || value.EventID == "" || len(value.Payload) == 0 || !json.Valid(value.Payload) {
		return runtimestorage.ErrInvalid
	}
	return nil
}
func jsonEqual(left, right []byte) bool {
	var leftValue, rightValue any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return false
	}
	return reflect.DeepEqual(leftValue, rightValue)
}
func cloneReply(value runtimestorage.ReplyOutbox) runtimestorage.ReplyOutbox {
	if value.LeaseExpiresAt != nil {
		copy := *value.LeaseExpiresAt
		value.LeaseExpiresAt = &copy
	}
	return value
}

var _ sessionstorage.SessionStateStore = (*Store)(nil)
var _ sessionstorage.EventHistoryStore = (*Store)(nil)
var _ runtimestorage.MessageStore = (*Store)(nil)
var _ runtimestorage.ReplyStore = (*Store)(nil)
var _ runtimestorage.ReplyReceiptRecorder = (*Store)(nil)
