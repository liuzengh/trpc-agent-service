package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

var (
	// ErrSessionNotFound indicates that a tenant cannot access the requested
	// session state.
	ErrSessionNotFound = errors.New("session not found")
	// ErrOutboxEventNotFound indicates that an event is missing or belongs to a
	// different tenant.
	ErrOutboxEventNotFound = errors.New("outbox event not found")
	// ErrStaleFencingToken indicates that an execution attempt has a fencing token
	// smaller than or equal to the session's recorded revision.
	ErrStaleFencingToken = errors.New("stale fencing token")
	// ErrSessionOwnedByAnotherUser prevents history claiming from overwriting
	// an ownership fact that has already been established.
	ErrSessionOwnedByAnotherUser = errors.New("session is owned by another platform user")
)

// ExecutionRecord is the minimum durable evidence produced by a completed
// message execution. Details are redacted before they enter the audit store.
type ExecutionRecord struct {
	TenantID            string
	AppCode             string
	SessionKey          string
	MessageID           string
	Channel             string
	BindingID           string
	ConversationID      string
	ConversationScope   string
	ExternalUserID      string
	ActorExternalUserID string
	ActorPlatformUserID string
	TriggerType         string
	TraceID             string
	Action              string
	Result              string
	AuditDetail         string
	OutboxType          string
	ModelUsage          *ModelUsage
	OutboxPayload       []byte
	OutboxRequestID     string
	SubjectID           string
	OwnerPlatformUserID string
	ExecutionTrace      *AgentExecutionTrace
	FencingToken        uint64
}

// Session is the recoverable tenant-scoped state index for one conversation.
type Session struct {
	TenantID            string
	AppCode             string
	SessionKey          string
	LastMessageID       string
	Revision            uint64
	SubjectID           string
	OwnerPlatformUserID string
	Status              string
	ArchivedAt          *time.Time
	UpdatedAt           time.Time
	Conversations       []SessionConversation
}

// SessionConversation maps one external conversation to the channel-neutral
// platform Session that owns its Agent transcript.
type SessionConversation struct {
	TenantID       string
	AppCode        string
	SessionKey     string
	Channel        string
	BindingID      string
	ConversationID string
	ExternalUserID string
	Scope          string
	StartedAt      time.Time
	UpdatedAt      time.Time
	EndedAt        *time.Time
}

// SessionRoute is the stable input used to resolve an inbound conversation
// into one channel-neutral Session.
type SessionRoute struct {
	TenantID            string
	AppCode             string
	Channel             string
	BindingID           string
	ConversationID      string
	ExternalUserID      string
	SubjectID           string
	OwnerPlatformUserID string
	Scope               string
}

// InboundMessageRoute is the durable, content-free projection that explains
// where a framework user message came from and who acted in that message.
type InboundMessageRoute struct {
	TenantID            string
	AppCode             string
	Channel             string
	MessageID           string
	SessionKey          string
	BindingID           string
	ConversationID      string
	Scope               string
	ActorExternalUserID string
	ActorPlatformUserID string
	TriggerType         string
	CreatedAt           time.Time
}

// SessionManager owns the mapping from external conversations and subjects to
// platform Sessions. Removing a channel binding never removes these historical
// mappings or the transcript they reference.
type SessionManager interface {
	ResolveSession(context.Context, SessionRoute, string) (string, error)
	ArchiveSession(context.Context, string, string) error
}

// SessionSwitchRequest atomically moves one external conversation to a fresh
// platform Session and enqueues the user-visible acknowledgement. RequestID is
// the provider message ID and therefore also makes command replay idempotent.
type SessionSwitchRequest struct {
	Route         SessionRoute
	SessionKey    string
	RequestID     string
	OutboxType    string
	OutboxPayload []byte
}

type SessionSwitchResult struct {
	SessionKey string
	Replayed   bool
}

// SessionCommandStore is deliberately separate from SessionManager so custom
// Session backends that only serve normal routing do not silently claim to
// support transactional platform commands.
type SessionCommandStore interface {
	SwitchSession(context.Context, SessionSwitchRequest) (SessionSwitchResult, error)
}

// SessionOwnershipStore owns the explicit lifecycle transitions that cannot
// be inferred safely from message routing.
type SessionOwnershipStore interface {
	EndChannelIdentityRoutes(ctx context.Context, tenantID, channel, bindingID, externalUserID string) error
	ListClaimableSessions(ctx context.Context, tenantID, channel, bindingID, externalUserID string, limit int) ([]Session, error)
	ClaimSession(ctx context.Context, tenantID, sessionKey, platformUserID string) error
	ListInboundMessageRoutes(ctx context.Context, tenantID, sessionKey string) ([]InboundMessageRoute, error)
}

// SessionMessageRouteStore supports page-local source lookups for transcript
// messages. Keeping this separate from SessionOwnershipStore lets custom state
// stores remain compatible while the production stores avoid loading every
// historical route for a long session.
type SessionMessageRouteStore interface {
	ListInboundMessageRoutesByMessageIDs(ctx context.Context, tenantID, sessionKey string, messageIDs []string) ([]InboundMessageRoute, error)
}

// AuditEvent is a minimal, redacted operation record addressable by tenant and
// trace ID. It intentionally excludes raw message bodies and credentials.
type AuditEvent struct {
	ID         string
	TenantID   string
	TraceID    string
	RequestID  string
	Channel    string
	UserID     string
	SessionID  string
	AgentName  string
	ToolName   string
	Action     string
	Result     string
	Decision   string
	LatencyMS  int64
	ErrorType  string
	CostMicros int64
	Detail     string
	CreatedAt  time.Time
}

// OutboxEvent is a durable event waiting for an asynchronous publisher.
type OutboxEvent struct {
	ID                string
	RequestID         string
	TenantID          string
	AggregateKey      string
	Type              string
	Payload           []byte
	CreatedAt         time.Time
	AvailableAt       time.Time
	DeliveredAt       *time.Time
	DeliveryOwner     string
	LeaseExpiresAt    *time.Time
	DeliveryReceipt   string
	DeliveryAttempts  int
	LastDeliveryError string
}

// StateStore persists the session, audit, and outbox effects of one execution
// atomically. Production implementations must perform RecordExecution in one
// database transaction.
type StateStore interface {
	SessionManager
	SessionExecutionLeaser
	RecordExecution(ctx context.Context, record ExecutionRecord) (OutboxEvent, error)
	RecordExecutionTrace(context.Context, ExecutionTraceRecord) error
	GetExecutionTrace(context.Context, string, string, string, string) (ExecutionTraceRecord, error)
	ListExecutionTraces(context.Context, string, []ExecutionTraceRef) ([]ExecutionTraceRecord, error)
	GetSession(ctx context.Context, tenantID, sessionKey string) (Session, error)
	ListAudit(ctx context.Context, tenantID, traceID string) ([]AuditEvent, error)
	ListPendingOutbox(ctx context.Context, tenantID string, limit int) ([]OutboxEvent, error)
	MarkOutboxDelivered(ctx context.Context, tenantID, eventID string) error
}

type AuditRecorder interface {
	RecordAudit(context.Context, AuditEvent) error
}

type AuditRetentionStore interface {
	PurgeAuditBefore(context.Context, string, time.Time) (int64, error)
}

// SessionLister exposes session reads required by the current console.
type SessionLister interface {
	// ListSessions returns the most recent sessions for one tenant in update
	// order, newest first, capped at limit.
	ListSessions(ctx context.Context, tenantID string, limit int) ([]Session, error)
}

// ApplicationSessionLister enumerates the complete platform Session catalog
// for one tenant application. Online backend migration uses this catalog as
// the authoritative set of framework Session keys to backfill and verify.
type ApplicationSessionLister interface {
	ListApplicationSessions(context.Context, string, string) ([]Session, error)
}

// MemoryStateStore is a per-instance deterministic implementation for tests and
// local development. It is not a durable replacement for PostgreSQL.
type MemoryStateStore struct {
	mu                 sync.RWMutex
	sessions           map[string]Session
	sessionLeases      map[string]SessionExecutionLease
	audits             []AuditEvent
	outbox             map[string]OutboxEvent
	conversations      map[string]SessionConversation
	conversationActive map[string]string
	inboundRoutes      map[string]InboundMessageRoute
	now                func() time.Time
	audit              AuditContentDigester
	usage              map[string]ModelUsage
	traces             map[string]ExecutionTraceRecord
}

// NewMemoryStateStore constructs an isolated state store.
func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{
		sessions:           make(map[string]Session),
		sessionLeases:      make(map[string]SessionExecutionLease),
		outbox:             make(map[string]OutboxEvent),
		conversations:      make(map[string]SessionConversation),
		conversationActive: make(map[string]string),
		inboundRoutes:      make(map[string]InboundMessageRoute),
		now:                time.Now,
		audit:              memoryAuditDigester,
		usage:              make(map[string]ModelUsage),
		traces:             make(map[string]ExecutionTraceRecord),
	}
}

// NewMemoryStateStoreWithAuditKey constructs a test store with an explicit key.
func NewMemoryStateStoreWithAuditKey(key []byte) (*MemoryStateStore, error) {
	digester, err := NewAuditContentDigester(key)
	if err != nil {
		return nil, err
	}
	return &MemoryStateStore{
		sessions:           make(map[string]Session),
		sessionLeases:      make(map[string]SessionExecutionLease),
		outbox:             make(map[string]OutboxEvent),
		conversations:      make(map[string]SessionConversation),
		conversationActive: make(map[string]string),
		inboundRoutes:      make(map[string]InboundMessageRoute),
		now:                time.Now,
		audit:              digester,
		usage:              make(map[string]ModelUsage),
		traces:             make(map[string]ExecutionTraceRecord),
	}, nil
}

// RecordExecution atomically updates session progress, records redacted audit
// evidence, and creates an Outbox event.
func (s *MemoryStateStore) RecordExecution(ctx context.Context, record ExecutionRecord) (OutboxEvent, error) {
	if err := ctx.Err(); err != nil {
		return OutboxEvent{}, err
	}
	if err := validateExecutionRecord(record); err != nil {
		return OutboxEvent{}, err
	}
	eventID, err := newStateID()
	if err != nil {
		return OutboxEvent{}, err
	}
	auditID, err := newStateID()
	if err != nil {
		return OutboxEvent{}, err
	}

	now := s.now().UTC()
	sessionKey := tenantSessionKey(record.TenantID, record.SessionKey)
	event := OutboxEvent{
		ID:           eventID,
		TenantID:     record.TenantID,
		AggregateKey: record.SessionKey,
		Type:         record.OutboxType,
		Payload:      append([]byte(nil), record.OutboxPayload...),
		RequestID:    record.OutboxRequestID,
		CreatedAt:    now,
		AvailableAt:  now,
	}
	audit := auditEventFromExecution(auditID, record, s.audit.Digest(record.AuditDetail), now)

	s.mu.Lock()
	defer s.mu.Unlock()
	session := s.sessions[sessionKey]
	if record.FencingToken > 0 {
		lease := s.sessionLeases[sessionKey]
		if lease.FencingToken != record.FencingToken || !lease.LeaseUntil.After(now) {
			return OutboxEvent{}, ErrStaleFencingToken
		}
	}
	session.TenantID = record.TenantID
	session.AppCode = record.AppCode
	session.SessionKey = record.SessionKey
	session.LastMessageID = record.MessageID
	if session.SubjectID == "" {
		session.SubjectID = record.SubjectID
	}
	if session.OwnerPlatformUserID == "" && record.OwnerPlatformUserID != "" {
		session.OwnerPlatformUserID = record.OwnerPlatformUserID
	}
	session.Status = "active"
	session.ArchivedAt = nil
	session.Revision++
	session.UpdatedAt = now
	if strings.TrimSpace(record.ConversationID) != "" {
		conversation := SessionConversation{
			TenantID: record.TenantID, AppCode: record.AppCode, SessionKey: record.SessionKey,
			Channel: record.Channel, BindingID: record.BindingID, ConversationID: record.ConversationID,
			ExternalUserID: record.ExternalUserID, Scope: record.ConversationScope, StartedAt: now, UpdatedAt: now,
		}
		routeKey := sessionConversationRouteKey(conversation)
		key := sessionConversationHistoryKey(conversation)
		if existing, ok := s.conversations[key]; ok {
			conversation.StartedAt = existing.StartedAt
			conversation.ExternalUserID = existing.ExternalUserID
		}
		s.conversations[key] = conversation
		s.conversationActive[routeKey] = record.SessionKey
	}
	if strings.TrimSpace(record.TriggerType) != "" {
		route := InboundMessageRoute{
			TenantID: record.TenantID, AppCode: record.AppCode, Channel: record.Channel,
			MessageID: record.MessageID, SessionKey: record.SessionKey, BindingID: record.BindingID,
			ConversationID: record.ConversationID, Scope: record.ConversationScope,
			ActorExternalUserID: record.ActorExternalUserID, ActorPlatformUserID: record.ActorPlatformUserID,
			TriggerType: record.TriggerType, CreatedAt: now,
		}
		s.inboundRoutes[inboundMessageRouteKey(route.TenantID, route.Channel, route.BindingID, route.MessageID)] = route
	}
	if record.ModelUsage != nil {
		s.usage[record.TenantID+"\x00"+record.Channel+"\x00"+record.BindingID+"\x00"+record.MessageID] = *record.ModelUsage
	}
	if record.ExecutionTrace != nil {
		s.traces[executionTraceKey(record.TenantID, record.Channel, record.BindingID, record.MessageID)] = cloneExecutionTraceRecord(ExecutionTraceRecord{
			TenantID:  record.TenantID,
			AppCode:   record.AppCode,
			Channel:   record.Channel,
			BindingID: record.BindingID,
			MessageID: record.MessageID,
			TraceID:   record.TraceID,
			Trace:     *record.ExecutionTrace,
		})
	}
	s.sessions[sessionKey] = session
	s.audits = append(s.audits, audit)
	s.outbox[event.ID] = event
	return cloneOutboxEvent(event), nil
}

func (s *MemoryStateStore) ResolveSession(ctx context.Context, route SessionRoute, preferred string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := validateSessionRoute(route, preferred); err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	conversation := SessionConversation{
		TenantID: route.TenantID, AppCode: route.AppCode, Channel: route.Channel,
		BindingID: route.BindingID, ConversationID: route.ConversationID,
		ExternalUserID: route.ExternalUserID, Scope: route.Scope,
	}
	routeKey := sessionConversationRouteKey(conversation)
	if activeSession := s.conversationActive[routeKey]; activeSession != "" {
		if route.Scope == "group" {
			key := tenantSessionKey(route.TenantID, activeSession)
			if existing, ok := s.sessions[key]; ok {
				existing.SubjectID = route.SubjectID
				existing.OwnerPlatformUserID = ""
				s.sessions[key] = existing
			}
		}
		return activeSession, nil
	}

	sessionKey := preferred
	conversation.SessionKey = sessionKey
	conversation.StartedAt = s.now().UTC()
	conversation.UpdatedAt = conversation.StartedAt
	s.conversations[sessionConversationHistoryKey(conversation)] = conversation
	s.conversationActive[routeKey] = sessionKey
	key := tenantSessionKey(route.TenantID, sessionKey)
	if _, exists := s.sessions[key]; !exists {
		s.sessions[key] = Session{
			TenantID: route.TenantID, AppCode: route.AppCode, SessionKey: sessionKey,
			SubjectID: route.SubjectID, OwnerPlatformUserID: route.OwnerPlatformUserID,
			Status: "active", UpdatedAt: conversation.UpdatedAt,
		}
	}
	return sessionKey, nil
}

func (s *MemoryStateStore) SwitchSession(ctx context.Context, request SessionSwitchRequest) (SessionSwitchResult, error) {
	if err := ctx.Err(); err != nil {
		return SessionSwitchResult{}, err
	}
	if err := validateSessionSwitchRequest(request); err != nil {
		return SessionSwitchResult{}, err
	}
	eventID, err := newStateID()
	if err != nil {
		return SessionSwitchResult{}, err
	}
	now := s.now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, event := range s.outbox {
		if event.TenantID == request.Route.TenantID && event.RequestID == request.RequestID {
			if event.Type != request.OutboxType {
				return SessionSwitchResult{}, fmt.Errorf("session command request ID already belongs to %q", event.Type)
			}
			return SessionSwitchResult{SessionKey: event.AggregateKey, Replayed: true}, nil
		}
	}
	conversation := SessionConversation{
		TenantID: request.Route.TenantID, AppCode: request.Route.AppCode,
		Channel: request.Route.Channel, BindingID: request.Route.BindingID,
		ConversationID: request.Route.ConversationID, ExternalUserID: request.Route.ExternalUserID,
		Scope: request.Route.Scope,
	}
	routeKey := sessionConversationRouteKey(conversation)
	if previous := s.conversationActive[routeKey]; previous != "" {
		for key, current := range s.conversations {
			if current.TenantID != request.Route.TenantID || current.SessionKey != previous || current.EndedAt != nil || sessionConversationRouteKey(current) != routeKey {
				continue
			}
			endedAt := now
			current.EndedAt = &endedAt
			current.UpdatedAt = now
			s.conversations[key] = current
		}
		activeElsewhere := false
		for _, current := range s.conversations {
			if current.TenantID == request.Route.TenantID && current.SessionKey == previous && current.EndedAt == nil {
				activeElsewhere = true
				break
			}
		}
		if !activeElsewhere {
			key := tenantSessionKey(request.Route.TenantID, previous)
			if old, ok := s.sessions[key]; ok {
				archivedAt := now
				old.Status = "archived"
				old.ArchivedAt = &archivedAt
				old.UpdatedAt = now
				s.sessions[key] = old
			}
		}
	}
	newSession := Session{
		TenantID: request.Route.TenantID, AppCode: request.Route.AppCode,
		SessionKey: request.SessionKey, SubjectID: request.Route.SubjectID,
		OwnerPlatformUserID: request.Route.OwnerPlatformUserID,
		Status:              "active", UpdatedAt: now,
	}
	s.sessions[tenantSessionKey(request.Route.TenantID, request.SessionKey)] = newSession
	conversation.SessionKey = request.SessionKey
	conversation.StartedAt, conversation.UpdatedAt = now, now
	s.conversations[sessionConversationHistoryKey(conversation)] = conversation
	s.conversationActive[routeKey] = request.SessionKey
	s.outbox[eventID] = OutboxEvent{
		ID: eventID, RequestID: request.RequestID, TenantID: request.Route.TenantID,
		AggregateKey: request.SessionKey, Type: request.OutboxType,
		Payload: append([]byte(nil), request.OutboxPayload...), CreatedAt: now, AvailableAt: now,
	}
	return SessionSwitchResult{SessionKey: request.SessionKey}, nil
}

func (s *MemoryStateStore) ArchiveSession(ctx context.Context, tenantID, sessionKey string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UTC()
	for key, conversation := range s.conversations {
		if conversation.TenantID != tenantID || conversation.SessionKey != sessionKey || conversation.EndedAt != nil {
			continue
		}
		conversation.EndedAt = &now
		conversation.UpdatedAt = now
		s.conversations[key] = conversation
		delete(s.conversationActive, sessionConversationRouteKey(conversation))
	}
	key := tenantSessionKey(tenantID, sessionKey)
	session := s.sessions[key]
	session.TenantID = tenantID
	session.SessionKey = sessionKey
	session.Status = "archived"
	session.ArchivedAt = &now
	session.UpdatedAt = now
	s.sessions[key] = session
	return nil
}

func (s *MemoryStateStore) ArchiveIdleSessions(ctx context.Context, before time.Time, limit int) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if before.IsZero() || limit <= 0 {
		return 0, fmt.Errorf("archive cutoff and positive limit are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var archived int64
	for key, session := range s.sessions {
		if archived == int64(limit) {
			break
		}
		if session.Status != "active" || !session.UpdatedAt.Before(before) {
			continue
		}
		now := s.now().UTC()
		session.Status, session.ArchivedAt, session.UpdatedAt = "archived", &now, now
		s.sessions[key] = session
		for conversationKey, conversation := range s.conversations {
			if conversation.TenantID != session.TenantID || conversation.SessionKey != session.SessionKey || conversation.EndedAt != nil {
				continue
			}
			conversation.EndedAt = &now
			conversation.UpdatedAt = now
			s.conversations[conversationKey] = conversation
			delete(s.conversationActive, sessionConversationRouteKey(conversation))
		}
		archived++
	}
	return archived, nil
}

// GetSession returns one tenant-scoped recoverable session index.
func (s *MemoryStateStore) GetSession(ctx context.Context, tenantID, sessionKey string) (Session, error) {
	if err := ctx.Err(); err != nil {
		return Session{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	session, found := s.sessions[tenantSessionKey(tenantID, sessionKey)]
	if !found {
		return Session{}, ErrSessionNotFound
	}
	session.Conversations = s.sessionConversationsLocked(tenantID, sessionKey)
	return session, nil
}

// ListSessions implements SessionLister for the in-memory store: newest first.
func (s *MemoryStateStore) ListSessions(ctx context.Context, tenantID string, limit int) ([]Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("session list tenant ID is required")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("session list limit must be positive")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	sessions := make([]Session, 0)
	for _, session := range s.sessions {
		if session.TenantID != tenantID {
			continue
		}
		session.Conversations = s.sessionConversationsLocked(tenantID, session.SessionKey)
		sessions = append(sessions, session)
	}
	sort.Slice(sessions, func(i, j int) bool {
		if sessions[i].UpdatedAt.Equal(sessions[j].UpdatedAt) {
			return sessions[i].SessionKey < sessions[j].SessionKey
		}
		return sessions[i].UpdatedAt.After(sessions[j].UpdatedAt)
	})
	if len(sessions) > limit {
		sessions = sessions[:limit]
	}
	return sessions, nil
}

func (s *MemoryStateStore) ListApplicationSessions(ctx context.Context, tenantID, appCode string) ([]Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(appCode) == "" {
		return nil, errors.New("application Session catalog requires tenant and application")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Session, 0)
	for _, current := range s.sessions {
		if current.TenantID != tenantID || current.AppCode != appCode {
			continue
		}
		current.Conversations = s.sessionConversationsLocked(tenantID, current.SessionKey)
		result = append(result, current)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].UpdatedAt.Equal(result[j].UpdatedAt) {
			return result[i].SessionKey < result[j].SessionKey
		}
		return result[i].UpdatedAt.Before(result[j].UpdatedAt)
	})
	return result, nil
}

func (s *MemoryStateStore) EndChannelIdentityRoutes(ctx context.Context, tenantID, channel, bindingID, externalUserID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(channel) == "" || strings.TrimSpace(bindingID) == "" || strings.TrimSpace(externalUserID) == "" {
		return fmt.Errorf("channel identity route is incomplete")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UTC()
	for key, conversation := range s.conversations {
		if conversation.TenantID != tenantID || conversation.Channel != channel || conversation.BindingID != bindingID || conversation.ExternalUserID != externalUserID || conversation.EndedAt != nil {
			continue
		}
		conversation.EndedAt = &now
		conversation.UpdatedAt = now
		s.conversations[key] = conversation
		delete(s.conversationActive, sessionConversationRouteKey(conversation))
	}
	return nil
}

func (s *MemoryStateStore) ListClaimableSessions(ctx context.Context, tenantID, channel, bindingID, externalUserID string, limit int) ([]Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(channel) == "" || strings.TrimSpace(bindingID) == "" || strings.TrimSpace(externalUserID) == "" || limit <= 0 {
		return nil, fmt.Errorf("claimable session query is incomplete")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	matched := make(map[string]Session)
	for _, conversation := range s.conversations {
		if conversation.TenantID != tenantID || conversation.Channel != channel || conversation.BindingID != bindingID || conversation.ExternalUserID != externalUserID || conversation.Scope != "direct" {
			continue
		}
		session, ok := s.sessions[tenantSessionKey(tenantID, conversation.SessionKey)]
		if !ok || session.OwnerPlatformUserID != "" {
			continue
		}
		session.Conversations = s.sessionConversationsLocked(tenantID, session.SessionKey)
		matched[session.SessionKey] = session
	}
	result := make([]Session, 0, len(matched))
	for _, session := range matched {
		result = append(result, session)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].UpdatedAt.After(result[j].UpdatedAt) })
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (s *MemoryStateStore) ClaimSession(ctx context.Context, tenantID, sessionKey, platformUserID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(sessionKey) == "" || strings.TrimSpace(platformUserID) == "" {
		return fmt.Errorf("session claim is incomplete")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := tenantSessionKey(tenantID, sessionKey)
	session, ok := s.sessions[key]
	if !ok {
		return ErrSessionNotFound
	}
	if session.OwnerPlatformUserID != "" && session.OwnerPlatformUserID != platformUserID {
		return ErrSessionOwnedByAnotherUser
	}
	session.OwnerPlatformUserID = platformUserID
	session.UpdatedAt = s.now().UTC()
	s.sessions[key] = session
	return nil
}

func (s *MemoryStateStore) ListInboundMessageRoutes(ctx context.Context, tenantID, sessionKey string) ([]InboundMessageRoute, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(sessionKey) == "" {
		return nil, fmt.Errorf("message route tenant and session are required")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]InboundMessageRoute, 0)
	for _, route := range s.inboundRoutes {
		if route.TenantID == tenantID && route.SessionKey == sessionKey {
			result = append(result, route)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].MessageID < result[j].MessageID
		}
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result, nil
}

func (s *MemoryStateStore) ListInboundMessageRoutesByMessageIDs(ctx context.Context, tenantID, sessionKey string, messageIDs []string) ([]InboundMessageRoute, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(sessionKey) == "" {
		return nil, fmt.Errorf("message route tenant and session are required")
	}
	wanted := make(map[string]struct{}, len(messageIDs))
	for _, messageID := range messageIDs {
		if messageID = strings.TrimSpace(messageID); messageID != "" {
			wanted[messageID] = struct{}{}
		}
	}
	if len(wanted) == 0 {
		return []InboundMessageRoute{}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]InboundMessageRoute, 0, len(wanted))
	for _, route := range s.inboundRoutes {
		if route.TenantID != tenantID || route.SessionKey != sessionKey {
			continue
		}
		if _, ok := wanted[route.MessageID]; ok {
			result = append(result, route)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].MessageID < result[j].MessageID
		}
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result, nil
}

func (s *MemoryStateStore) sessionConversationsLocked(tenantID, sessionKey string) []SessionConversation {
	conversations := make([]SessionConversation, 0)
	for _, conversation := range s.conversations {
		if conversation.TenantID == tenantID && conversation.SessionKey == sessionKey {
			conversations = append(conversations, conversation)
		}
	}
	sort.Slice(conversations, func(i, j int) bool {
		if conversations[i].UpdatedAt.Equal(conversations[j].UpdatedAt) {
			return sessionConversationHistoryKey(conversations[i]) < sessionConversationHistoryKey(conversations[j])
		}
		return conversations[i].UpdatedAt.After(conversations[j].UpdatedAt)
	})
	return conversations
}

func validateSessionRoute(route SessionRoute, preferred string) error {
	if strings.TrimSpace(route.TenantID) == "" || strings.TrimSpace(route.AppCode) == "" || strings.TrimSpace(route.Channel) == "" || strings.TrimSpace(route.ConversationID) == "" || strings.TrimSpace(route.SubjectID) == "" || strings.TrimSpace(preferred) == "" {
		return fmt.Errorf("session route is incomplete")
	}
	if route.Scope != "direct" && route.Scope != "group" {
		return fmt.Errorf("session route scope must be direct or group")
	}
	return nil
}

func validateSessionSwitchRequest(request SessionSwitchRequest) error {
	if err := validateSessionRoute(request.Route, request.SessionKey); err != nil {
		return err
	}
	if strings.TrimSpace(request.RequestID) == "" {
		return fmt.Errorf("session command request ID is required")
	}
	if strings.TrimSpace(request.OutboxType) == "" || len(request.OutboxPayload) == 0 {
		return fmt.Errorf("session command outbox reply is required")
	}
	return nil
}

func sessionConversationRouteKey(conversation SessionConversation) string {
	return strings.Join([]string{
		conversation.TenantID, conversation.AppCode, conversation.Channel,
		conversation.BindingID, conversation.ConversationID,
	}, "\x00")
}

func sessionConversationHistoryKey(conversation SessionConversation) string {
	return sessionConversationRouteKey(conversation) + "\x00" + conversation.SessionKey
}

func inboundMessageRouteKey(tenantID, channel, bindingID, messageID string) string {
	return tenantID + "\x00" + channel + "\x00" + bindingID + "\x00" + messageID
}

// ListAudit returns the audit evidence visible to one tenant and trace.
func (s *MemoryStateStore) ListAudit(ctx context.Context, tenantID, traceID string) ([]AuditEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	events := make([]AuditEvent, 0)
	for _, event := range s.audits {
		if event.TenantID == tenantID && (traceID == "" || event.TraceID == traceID) {
			events = append(events, event)
		}
	}
	return events, nil
}

func (s *MemoryStateStore) RecordAudit(ctx context.Context, event AuditEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(event.TenantID) == "" || strings.TrimSpace(event.TraceID) == "" || strings.TrimSpace(event.Action) == "" {
		return fmt.Errorf("audit tenant, trace, and action are required")
	}
	if event.ID == "" {
		id, err := newStateID()
		if err != nil {
			return err
		}
		event.ID = id
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = s.now().UTC()
	}
	event.Detail = s.audit.Digest(event.Detail)
	s.mu.Lock()
	s.audits = append(s.audits, event)
	s.mu.Unlock()
	return nil
}

func (s *MemoryStateStore) PurgeAuditBefore(ctx context.Context, tenantID string, before time.Time) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := s.audits[:0]
	var removed int64
	for _, event := range s.audits {
		if event.TenantID == tenantID && event.CreatedAt.Before(before) {
			removed++
			continue
		}
		kept = append(kept, event)
	}
	s.audits = kept
	return removed, nil
}

// ListPendingOutbox returns undelivered tenant events in insertion order.
func (s *MemoryStateStore) ListPendingOutbox(ctx context.Context, tenantID string, limit int) ([]OutboxEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("outbox tenant ID is required")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("outbox limit must be positive")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	events := make([]OutboxEvent, 0, limit)
	for _, event := range s.outbox {
		if event.TenantID != tenantID || event.DeliveredAt != nil || event.AvailableAt.After(s.now().UTC()) {
			continue
		}
		events = append(events, cloneOutboxEvent(event))
		if len(events) == limit {
			break
		}
	}
	return events, nil
}

// MarkOutboxDelivered completes one tenant-owned outbox event.
func (s *MemoryStateStore) MarkOutboxDelivered(ctx context.Context, tenantID, eventID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	event, found := s.outbox[eventID]
	if !found || event.TenantID != tenantID {
		return ErrOutboxEventNotFound
	}
	if event.DeliveredAt == nil {
		deliveredAt := s.now().UTC()
		event.DeliveredAt = &deliveredAt
		s.outbox[eventID] = event
	}
	return nil
}

// Redact removes common credential forms before data becomes durable audit text.
func Redact(detail string) string {
	return log.Redact(detail)
}

func auditEventFromExecution(id string, record ExecutionRecord, detail string, now time.Time) AuditEvent {
	cost := int64(0)
	if record.ModelUsage != nil && record.ModelUsage.Known {
		cost = record.ModelUsage.CostMicros
	}
	requestID := record.OutboxRequestID
	if strings.TrimSpace(requestID) == "" {
		requestID = record.MessageID
	}
	return AuditEvent{
		ID:         id,
		TenantID:   record.TenantID,
		TraceID:    record.TraceID,
		RequestID:  requestID,
		Channel:    record.Channel,
		UserID:     record.SubjectID,
		SessionID:  record.SessionKey,
		AgentName:  "assistant",
		Action:     record.Action,
		Result:     record.Result,
		Decision:   record.Result,
		CostMicros: cost,
		Detail:     detail,
		CreatedAt:  now,
	}
}

func validateExecutionRecord(record ExecutionRecord) error {
	if strings.TrimSpace(record.TenantID) == "" || strings.TrimSpace(record.AppCode) == "" {
		return fmt.Errorf("execution tenant and application are required")
	}
	if strings.TrimSpace(record.SessionKey) == "" || !strings.HasPrefix(record.SessionKey, record.TenantID+"/") {
		return fmt.Errorf("execution session key is not scoped to tenant")
	}
	if strings.TrimSpace(record.MessageID) == "" || strings.TrimSpace(record.Channel) == "" || strings.TrimSpace(record.BindingID) == "" || strings.TrimSpace(record.TraceID) == "" {
		return fmt.Errorf("execution message, channel, binding, and trace IDs are required")
	}
	if strings.TrimSpace(record.Action) == "" || strings.TrimSpace(record.Result) == "" {
		return fmt.Errorf("execution audit action and result are required")
	}
	if strings.TrimSpace(record.OutboxType) == "" {
		return fmt.Errorf("execution outbox type is required")
	}
	if record.ExecutionTrace != nil {
		if err := validateExecutionTraceRecord(ExecutionTraceRecord{
			TenantID:  record.TenantID,
			AppCode:   record.AppCode,
			Channel:   record.Channel,
			BindingID: record.BindingID,
			MessageID: record.MessageID,
			TraceID:   record.TraceID,
			Trace:     *record.ExecutionTrace,
		}); err != nil {
			return err
		}
	}
	return nil
}

func tenantSessionKey(tenantID, sessionKey string) string {
	return fmt.Sprintf("%d:%s%d:%s", len(tenantID), tenantID, len(sessionKey), sessionKey)
}

func cloneOutboxEvent(event OutboxEvent) OutboxEvent {
	cloned := event
	cloned.Payload = append([]byte(nil), event.Payload...)
	if event.DeliveredAt != nil {
		deliveredAt := *event.DeliveredAt
		cloned.DeliveredAt = &deliveredAt
	}
	if event.LeaseExpiresAt != nil {
		leaseExpiresAt := *event.LeaseExpiresAt
		cloned.LeaseExpiresAt = &leaseExpiresAt
	}
	return cloned
}

func newStateID() (string, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", fmt.Errorf("generate state ID: %w", err)
	}
	return id.String(), nil
}

// ModelUsage is the priced provider token result for one execution.
type ModelUsage struct {
	ProviderID         string
	ModelName          string
	Known              bool
	PromptTokens       int
	CachedPromptTokens int
	CompletionTokens   int
	TotalTokens        int
	CostMicros         int64
}
