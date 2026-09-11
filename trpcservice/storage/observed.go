package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
)

// ObservedStateStore decorates the durable state boundary without changing its
// transactional or optional-interface semantics. Telemetry carries only the
// tenant, backend and stable operation name; it never contains keys, payloads,
// SQL, audit detail, receipts, principals or message content.
type ObservedStateStore struct {
	delegate StateStore
	observer metrics.StoreObserver
	backend  string
}

func NewObservedStateStore(delegate StateStore, observer metrics.StoreObserver, backend string) (*ObservedStateStore, error) {
	if delegate == nil {
		return nil, fmt.Errorf("observed state store delegate is required")
	}
	if observer == nil {
		return nil, fmt.Errorf("observed state store observer is required")
	}
	if backend == "" {
		return nil, fmt.Errorf("observed state store backend is required")
	}
	return &ObservedStateStore{delegate: delegate, observer: observer, backend: backend}, nil
}

func (s *ObservedStateStore) start(ctx context.Context, tenantID, operation string) (context.Context, func(error)) {
	return s.observer.StartStore(ctx, metrics.StoreAttributes{TenantID: tenantID, Backend: s.backend, Operation: operation})
}

func (s *ObservedStateStore) RecordExecution(ctx context.Context, record ExecutionRecord) (OutboxEvent, error) {
	ctx, finish := s.start(ctx, record.TenantID, "record_execution")
	event, err := s.delegate.RecordExecution(ctx, record)
	finish(err)
	return event, err
}

func (s *ObservedStateStore) RecordExecutionTrace(ctx context.Context, record ExecutionTraceRecord) error {
	ctx, finish := s.start(ctx, record.TenantID, "record_execution_trace")
	err := s.delegate.RecordExecutionTrace(ctx, record)
	finish(err)
	return err
}

func (s *ObservedStateStore) GetExecutionTrace(ctx context.Context, tenantID, channel, bindingID, messageID string) (ExecutionTraceRecord, error) {
	ctx, finish := s.start(ctx, tenantID, "get_execution_trace")
	record, err := s.delegate.GetExecutionTrace(ctx, tenantID, channel, bindingID, messageID)
	finish(err)
	return record, err
}

func (s *ObservedStateStore) ListExecutionTraces(ctx context.Context, tenantID string, refs []ExecutionTraceRef) ([]ExecutionTraceRecord, error) {
	ctx, finish := s.start(ctx, tenantID, "list_execution_traces")
	records, err := s.delegate.ListExecutionTraces(ctx, tenantID, refs)
	finish(err)
	return records, err
}

func (s *ObservedStateStore) GetSession(ctx context.Context, tenantID, sessionKey string) (Session, error) {
	ctx, finish := s.start(ctx, tenantID, "get_session")
	session, err := s.delegate.GetSession(ctx, tenantID, sessionKey)
	finish(err)
	return session, err
}

func (s *ObservedStateStore) SwitchSession(ctx context.Context, request SessionSwitchRequest) (SessionSwitchResult, error) {
	store, ok := s.delegate.(SessionCommandStore)
	if !ok {
		return SessionSwitchResult{}, fmt.Errorf("observed state store delegate does not support session commands")
	}
	ctx, finish := s.start(ctx, request.Route.TenantID, "switch_session")
	result, err := store.SwitchSession(ctx, request)
	finish(err)
	return result, err
}

func (s *ObservedStateStore) ListAudit(ctx context.Context, tenantID, traceID string) ([]AuditEvent, error) {
	ctx, finish := s.start(ctx, tenantID, "list_audit")
	events, err := s.delegate.ListAudit(ctx, tenantID, traceID)
	finish(err)
	return events, err
}

func (s *ObservedStateStore) ListPendingOutbox(ctx context.Context, tenantID string, limit int) ([]OutboxEvent, error) {
	ctx, finish := s.start(ctx, tenantID, "list_pending_outbox")
	events, err := s.delegate.ListPendingOutbox(ctx, tenantID, limit)
	finish(err)
	return events, err
}

func (s *ObservedStateStore) OutboxBacklog(ctx context.Context, tenantID, eventType string) (OutboxBacklog, error) {
	store, ok := s.delegate.(OutboxBacklogStore)
	if !ok {
		return OutboxBacklog{}, fmt.Errorf("observed state store delegate does not support outbox backlog")
	}
	ctx, finish := s.start(ctx, tenantID, "read_outbox_backlog")
	backlog, err := store.OutboxBacklog(ctx, tenantID, eventType)
	finish(err)
	return backlog, err
}

func (s *ObservedStateStore) MarkOutboxDelivered(ctx context.Context, tenantID, eventID string) error {
	ctx, finish := s.start(ctx, tenantID, "mark_outbox_delivered")
	err := s.delegate.MarkOutboxDelivered(ctx, tenantID, eventID)
	finish(err)
	return err
}

func (s *ObservedStateStore) deliveryStore() (OutboxDeliveryStore, error) {
	store, ok := s.delegate.(OutboxDeliveryStore)
	if !ok {
		return nil, fmt.Errorf("observed state store delegate does not support outbox delivery")
	}
	return store, nil
}

func (s *ObservedStateStore) ClaimPendingOutbox(ctx context.Context, tenantID, owner string, lease time.Duration, limit int) ([]OutboxEvent, error) {
	store, err := s.deliveryStore()
	if err != nil {
		return nil, err
	}
	ctx, finish := s.start(ctx, tenantID, "claim_pending_outbox")
	events, err := store.ClaimPendingOutbox(ctx, tenantID, owner, lease, limit)
	finish(err)
	return events, err
}

func (s *ObservedStateStore) ClaimPendingOutboxByType(ctx context.Context, tenantID, owner string, lease time.Duration, limit int, eventType string) ([]OutboxEvent, error) {
	store, err := s.deliveryStore()
	if err != nil {
		return nil, err
	}
	ctx, finish := s.start(ctx, tenantID, "claim_pending_outbox")
	events, err := store.ClaimPendingOutboxByType(ctx, tenantID, owner, lease, limit, eventType)
	finish(err)
	return events, err
}

func (s *ObservedStateStore) RenewOutboxDelivery(ctx context.Context, tenantID, eventID, owner string, lease time.Duration) error {
	store, err := s.deliveryStore()
	if err != nil {
		return err
	}
	ctx, finish := s.start(ctx, tenantID, "renew_outbox_delivery")
	err = store.RenewOutboxDelivery(ctx, tenantID, eventID, owner, lease)
	finish(err)
	return err
}

func (s *ObservedStateStore) CompleteOutboxDelivery(ctx context.Context, tenantID, eventID, owner, receipt string) error {
	store, err := s.deliveryStore()
	if err != nil {
		return err
	}
	ctx, finish := s.start(ctx, tenantID, "complete_outbox_delivery")
	err = store.CompleteOutboxDelivery(ctx, tenantID, eventID, owner, receipt)
	finish(err)
	return err
}

func (s *ObservedStateStore) FailOutboxDelivery(ctx context.Context, tenantID, eventID, owner string, cause error) error {
	store, err := s.deliveryStore()
	if err != nil {
		return err
	}
	ctx, finish := s.start(ctx, tenantID, "fail_outbox_delivery")
	err = store.FailOutboxDelivery(ctx, tenantID, eventID, owner, cause)
	finish(err)
	return err
}

func (s *ObservedStateStore) FindOutboxByRequestID(ctx context.Context, tenantID, requestID string) (OutboxEvent, error) {
	store, err := s.deliveryStore()
	if err != nil {
		return OutboxEvent{}, err
	}
	ctx, finish := s.start(ctx, tenantID, "find_outbox_by_request")
	event, err := store.FindOutboxByRequestID(ctx, tenantID, requestID)
	finish(err)
	return event, err
}

func (s *ObservedStateStore) FindOutboxByRequestIDs(ctx context.Context, tenantID string, requestIDs []string) ([]OutboxEvent, error) {
	store, ok := s.delegate.(OutboxRequestBatchFinder)
	if !ok {
		return nil, fmt.Errorf("observed state store delegate does not support batch outbox lookup")
	}
	ctx, finish := s.start(ctx, tenantID, "find_outbox_by_requests")
	events, err := store.FindOutboxByRequestIDs(ctx, tenantID, requestIDs)
	finish(err)
	return events, err
}

func (s *ObservedStateStore) RecordAudit(ctx context.Context, event AuditEvent) error {
	recorder, ok := s.delegate.(AuditRecorder)
	if !ok {
		return fmt.Errorf("observed state store delegate does not support audit recording")
	}
	ctx, finish := s.start(ctx, event.TenantID, "record_audit")
	err := recorder.RecordAudit(ctx, event)
	finish(err)
	return err
}

func (s *ObservedStateStore) PurgeAuditBefore(ctx context.Context, tenantID string, before time.Time) (int64, error) {
	store, ok := s.delegate.(AuditRetentionStore)
	if !ok {
		return 0, fmt.Errorf("observed state store delegate does not support audit retention")
	}
	ctx, finish := s.start(ctx, tenantID, "purge_audit")
	count, err := store.PurgeAuditBefore(ctx, tenantID, before)
	finish(err)
	return count, err
}

func (s *ObservedStateStore) PurgeDeliveredOutboxBefore(ctx context.Context, tenantID string, before time.Time, limit int) (int64, error) {
	store, ok := s.delegate.(OutboxRetentionStore)
	if !ok {
		return 0, fmt.Errorf("observed state store delegate does not support outbox retention")
	}
	ctx, finish := s.start(ctx, tenantID, "purge_delivered_outbox")
	count, err := store.PurgeDeliveredOutboxBefore(ctx, tenantID, before, limit)
	finish(err)
	return count, err
}

func (s *ObservedStateStore) ListSessions(ctx context.Context, tenantID string, limit int) ([]Session, error) {
	store, ok := s.delegate.(SessionLister)
	if !ok {
		return nil, fmt.Errorf("observed state store delegate does not support session listing")
	}
	ctx, finish := s.start(ctx, tenantID, "list_sessions")
	sessions, err := store.ListSessions(ctx, tenantID, limit)
	finish(err)
	return sessions, err
}

func (s *ObservedStateStore) ListApplicationSessions(ctx context.Context, tenantID, appCode string) ([]Session, error) {
	store, ok := s.delegate.(ApplicationSessionLister)
	if !ok {
		return nil, fmt.Errorf("observed state store delegate does not support application Session listing")
	}
	ctx, finish := s.start(ctx, tenantID, "list_application_sessions")
	sessions, err := store.ListApplicationSessions(ctx, tenantID, appCode)
	finish(err)
	return sessions, err
}

func (s *ObservedStateStore) ListInboundMessageRoutesByMessageIDs(ctx context.Context, tenantID, sessionKey string, messageIDs []string) ([]InboundMessageRoute, error) {
	store, ok := s.delegate.(SessionMessageRouteStore)
	if !ok {
		return nil, fmt.Errorf("observed state store delegate does not support page-local message routes")
	}
	ctx, finish := s.start(ctx, tenantID, "list_inbound_message_routes_by_ids")
	routes, err := store.ListInboundMessageRoutesByMessageIDs(ctx, tenantID, sessionKey, messageIDs)
	finish(err)
	return routes, err
}

func (s *ObservedStateStore) ResolveSession(ctx context.Context, route SessionRoute, preferred string) (string, error) {
	store, ok := s.delegate.(SessionManager)
	if !ok {
		return "", fmt.Errorf("observed state store delegate does not support session routing")
	}
	ctx, finish := s.start(ctx, route.TenantID, "resolve_session")
	session, err := store.ResolveSession(ctx, route, preferred)
	finish(err)
	return session, err
}

func (s *ObservedStateStore) ArchiveSession(ctx context.Context, tenantID, sessionKey string) error {
	store, ok := s.delegate.(SessionManager)
	if !ok {
		return fmt.Errorf("observed state store delegate does not support session routing")
	}
	ctx, finish := s.start(ctx, tenantID, "archive_session")
	err := store.ArchiveSession(ctx, tenantID, sessionKey)
	finish(err)
	return err
}

func (s *ObservedStateStore) sessionOwnershipStore() (SessionOwnershipStore, error) {
	store, ok := s.delegate.(SessionOwnershipStore)
	if !ok {
		return nil, fmt.Errorf("observed state store delegate does not support session ownership")
	}
	return store, nil
}

func (s *ObservedStateStore) EndChannelIdentityRoutes(ctx context.Context, tenantID, channel, bindingID, externalUserID string) error {
	store, err := s.sessionOwnershipStore()
	if err != nil {
		return err
	}
	ctx, finish := s.start(ctx, tenantID, "end_channel_identity_routes")
	err = store.EndChannelIdentityRoutes(ctx, tenantID, channel, bindingID, externalUserID)
	finish(err)
	return err
}

func (s *ObservedStateStore) ListClaimableSessions(ctx context.Context, tenantID, channel, bindingID, externalUserID string, limit int) ([]Session, error) {
	store, err := s.sessionOwnershipStore()
	if err != nil {
		return nil, err
	}
	ctx, finish := s.start(ctx, tenantID, "list_claimable_sessions")
	sessions, err := store.ListClaimableSessions(ctx, tenantID, channel, bindingID, externalUserID, limit)
	finish(err)
	return sessions, err
}

func (s *ObservedStateStore) ClaimSession(ctx context.Context, tenantID, sessionKey, platformUserID string) error {
	store, err := s.sessionOwnershipStore()
	if err != nil {
		return err
	}
	ctx, finish := s.start(ctx, tenantID, "claim_session")
	err = store.ClaimSession(ctx, tenantID, sessionKey, platformUserID)
	finish(err)
	return err
}

func (s *ObservedStateStore) ListInboundMessageRoutes(ctx context.Context, tenantID, sessionKey string) ([]InboundMessageRoute, error) {
	store, err := s.sessionOwnershipStore()
	if err != nil {
		return nil, err
	}
	ctx, finish := s.start(ctx, tenantID, "list_inbound_message_routes")
	routes, err := store.ListInboundMessageRoutes(ctx, tenantID, sessionKey)
	finish(err)
	return routes, err
}

func (s *ObservedStateStore) ArchiveIdleSessions(ctx context.Context, before time.Time, limit int) (int64, error) {
	store, ok := s.delegate.(IdleSessionArchiver)
	if !ok {
		return 0, fmt.Errorf("observed state store delegate does not support idle-session archival")
	}
	ctx, finish := s.start(ctx, "", "archive_idle_sessions")
	count, err := store.ArchiveIdleSessions(ctx, before, limit)
	finish(err)
	return count, err
}

func (s *ObservedStateStore) sessionExecutionLeaser() (SessionExecutionLeaser, error) {
	store, ok := s.delegate.(SessionExecutionLeaser)
	if !ok {
		return nil, fmt.Errorf("observed state store delegate does not support session execution leases")
	}
	return store, nil
}

func (s *ObservedStateStore) AcquireSessionExecutionLease(ctx context.Context, tenantID, sessionKey, ownerID string, ttl time.Duration) (SessionExecutionLease, error) {
	store, err := s.sessionExecutionLeaser()
	if err != nil {
		return SessionExecutionLease{}, err
	}
	ctx, finish := s.start(ctx, tenantID, "acquire_session_execution_lease")
	lease, err := store.AcquireSessionExecutionLease(ctx, tenantID, sessionKey, ownerID, ttl)
	finish(err)
	return lease, err
}

func (s *ObservedStateStore) RenewSessionExecutionLease(ctx context.Context, lease SessionExecutionLease, ttl time.Duration) (SessionExecutionLease, error) {
	store, err := s.sessionExecutionLeaser()
	if err != nil {
		return SessionExecutionLease{}, err
	}
	ctx, finish := s.start(ctx, lease.TenantID, "renew_session_execution_lease")
	renewed, err := store.RenewSessionExecutionLease(ctx, lease, ttl)
	finish(err)
	return renewed, err
}

func (s *ObservedStateStore) ReleaseSessionExecutionLease(ctx context.Context, lease SessionExecutionLease) error {
	store, err := s.sessionExecutionLeaser()
	if err != nil {
		return err
	}
	ctx, finish := s.start(ctx, lease.TenantID, "release_session_execution_lease")
	err = store.ReleaseSessionExecutionLease(ctx, lease)
	finish(err)
	return err
}

// ObservedExecutionDedupStore instruments ownership fencing and preserves the
// dashboard read seam.
type ObservedExecutionDedupStore struct {
	delegate ExecutionDedupStore
	observer metrics.StoreObserver
	backend  string
}

func NewObservedExecutionDedupStore(delegate ExecutionDedupStore, observer metrics.StoreObserver, backend string) (*ObservedExecutionDedupStore, error) {
	if delegate == nil || observer == nil || backend == "" {
		return nil, fmt.Errorf("observed execution dedup dependencies are required")
	}
	return &ObservedExecutionDedupStore{delegate: delegate, observer: observer, backend: backend}, nil
}

func (s *ObservedExecutionDedupStore) start(ctx context.Context, tenantID, operation string) (context.Context, func(error)) {
	return s.observer.StartStore(ctx, metrics.StoreAttributes{TenantID: tenantID, Backend: s.backend, Operation: operation})
}

func (s *ObservedExecutionDedupStore) Begin(ctx context.Context, tenantID, appCode, channel, bindingID, messageID, traceID string, window time.Duration) (BeginResult, error) {
	ctx, finish := s.start(ctx, tenantID, "claim_execution")
	result, err := s.delegate.Begin(ctx, tenantID, appCode, channel, bindingID, messageID, traceID, window)
	finish(err)
	return result, err
}

func (s *ObservedExecutionDedupStore) Abort(ctx context.Context, tenantID, channel, bindingID, messageID, traceID string) error {
	ctx, finish := s.start(ctx, tenantID, "abort_execution_claim")
	err := s.delegate.Abort(ctx, tenantID, channel, bindingID, messageID, traceID)
	finish(err)
	return err
}

func (s *ObservedExecutionDedupStore) Fail(ctx context.Context, tenantID, channel, bindingID, messageID, traceID string) error {
	ctx, finish := s.start(ctx, tenantID, "fail_execution_claim")
	err := s.delegate.Fail(ctx, tenantID, channel, bindingID, messageID, traceID)
	finish(err)
	return err
}

func (s *ObservedExecutionDedupStore) Renew(ctx context.Context, tenantID, channel, bindingID, messageID, traceID string) error {
	ctx, finish := s.start(ctx, tenantID, "renew_execution_claim")
	err := s.delegate.Renew(ctx, tenantID, channel, bindingID, messageID, traceID)
	finish(err)
	return err
}

func (s *ObservedExecutionDedupStore) ListClaims(ctx context.Context, tenantID, appCode string, limit int) ([]Claim, error) {
	lister, ok := s.delegate.(ClaimLister)
	if !ok {
		return nil, fmt.Errorf("observed execution dedup delegate does not support claim listing")
	}
	ctx, finish := s.start(ctx, tenantID, "list_execution_claims")
	claims, err := lister.ListClaims(ctx, tenantID, appCode, limit)
	finish(err)
	return claims, err
}

// ObservedRetryTracker instruments durable Kafka retry accounting.
type ObservedRetryTracker struct {
	delegate RetryTracker
	observer metrics.StoreObserver
	backend  string
}

func NewObservedRetryTracker(delegate RetryTracker, observer metrics.StoreObserver, backend string) (*ObservedRetryTracker, error) {
	if delegate == nil || observer == nil || backend == "" {
		return nil, fmt.Errorf("observed retry tracker dependencies are required")
	}
	return &ObservedRetryTracker{delegate: delegate, observer: observer, backend: backend}, nil
}

func (s *ObservedRetryTracker) start(ctx context.Context, tenantID, operation string) (context.Context, func(error)) {
	return s.observer.StartStore(ctx, metrics.StoreAttributes{TenantID: tenantID, Backend: s.backend, Operation: operation})
}

func (s *ObservedRetryTracker) Increment(ctx context.Context, tenantID, sessionKey, eventID string) (int, error) {
	ctx, finish := s.start(ctx, tenantID, "increment_retry")
	attempt, err := s.delegate.Increment(ctx, tenantID, sessionKey, eventID)
	finish(err)
	return attempt, err
}

func (s *ObservedRetryTracker) Clear(ctx context.Context, tenantID, sessionKey, eventID string) error {
	ctx, finish := s.start(ctx, tenantID, "clear_retry")
	err := s.delegate.Clear(ctx, tenantID, sessionKey, eventID)
	finish(err)
	return err
}

func (s *ObservedRetryTracker) ListAttempts(ctx context.Context, tenantID, sessionKey, eventID string) (int, error) {
	lister, ok := s.delegate.(AttemptLister)
	if !ok {
		return 0, fmt.Errorf("observed retry tracker delegate does not support attempt listing")
	}
	ctx, finish := s.start(ctx, tenantID, "list_retry_attempts")
	attempt, err := lister.ListAttempts(ctx, tenantID, sessionKey, eventID)
	finish(err)
	return attempt, err
}

// ObservedIdempotencyStore instruments the Redis execution lease boundary.
type ObservedIdempotencyStore struct {
	delegate IdempotencyStore
	observer metrics.StoreObserver
	backend  string
}

func NewObservedIdempotencyStore(delegate IdempotencyStore, observer metrics.StoreObserver, backend string) (*ObservedIdempotencyStore, error) {
	if delegate == nil || observer == nil || backend == "" {
		return nil, fmt.Errorf("observed idempotency dependencies are required")
	}
	return &ObservedIdempotencyStore{delegate: delegate, observer: observer, backend: backend}, nil
}

func (s *ObservedIdempotencyStore) start(ctx context.Context, operation string) (context.Context, func(error)) {
	return s.observer.StartStore(ctx, metrics.StoreAttributes{Backend: s.backend, Operation: operation})
}

func (s *ObservedIdempotencyStore) Acquire(ctx context.Context, key string, processingTTL time.Duration) (AcquireResult, error) {
	ctx, finish := s.start(ctx, "acquire_idempotency_lease")
	result, err := s.delegate.Acquire(ctx, key, processingTTL)
	finish(err)
	return result, err
}

func (s *ObservedIdempotencyStore) Renew(ctx context.Context, lease Lease, processingTTL time.Duration) error {
	ctx, finish := s.start(ctx, "renew_idempotency_lease")
	err := s.delegate.Renew(ctx, lease, processingTTL)
	finish(err)
	return err
}

func (s *ObservedIdempotencyStore) Complete(ctx context.Context, lease Lease, completedTTL time.Duration) error {
	ctx, finish := s.start(ctx, "complete_idempotency_lease")
	err := s.delegate.Complete(ctx, lease, completedTTL)
	finish(err)
	return err
}

func (s *ObservedIdempotencyStore) Release(ctx context.Context, lease Lease) error {
	ctx, finish := s.start(ctx, "release_idempotency_lease")
	err := s.delegate.Release(ctx, lease)
	finish(err)
	return err
}

var _ StateStore = (*ObservedStateStore)(nil)
var _ OutboxDeliveryStore = (*ObservedStateStore)(nil)
var _ OutboxRetentionStore = (*ObservedStateStore)(nil)
var _ AuditRecorder = (*ObservedStateStore)(nil)
var _ AuditRetentionStore = (*ObservedStateStore)(nil)
var _ SessionLister = (*ObservedStateStore)(nil)
var _ ApplicationSessionLister = (*ObservedStateStore)(nil)
var _ SessionManager = (*ObservedStateStore)(nil)
var _ SessionOwnershipStore = (*ObservedStateStore)(nil)
var _ SessionMessageRouteStore = (*ObservedStateStore)(nil)
var _ IdleSessionArchiver = (*ObservedStateStore)(nil)
var _ SessionExecutionLeaser = (*ObservedStateStore)(nil)
var _ ExecutionDedupStore = (*ObservedExecutionDedupStore)(nil)
var _ ClaimLister = (*ObservedExecutionDedupStore)(nil)
var _ RetryTracker = (*ObservedRetryTracker)(nil)
var _ AttemptLister = (*ObservedRetryTracker)(nil)
var _ IdempotencyStore = (*ObservedIdempotencyStore)(nil)
