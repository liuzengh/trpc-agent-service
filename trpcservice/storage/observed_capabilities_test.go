package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
)

func TestObservedStateStoreForwardsDurableOptionalCapabilities(t *testing.T) {
	ctx := context.Background()
	delegate := NewMemoryStateStore()
	observer := &recordingStoreObserver{}
	store, err := NewObservedStateStore(delegate, observer, "memory")
	if err != nil {
		t.Fatalf("NewObservedStateStore() error = %v", err)
	}

	record := ExecutionRecord{
		TenantID: "tenant-a", AppCode: "support", SessionKey: "tenant-a/support/session/1",
		MessageID: "message-1", Channel: "web", BindingID: "web-console",
		ConversationID: "conversation-1", ConversationScope: "direct",
		ActorExternalUserID: "external-1", TriggerType: "direct",
		TraceID: "trace-1", Action: "agent.reply", Result: "queued",
		OutboxType: "channel_reply", OutboxPayload: []byte("{}"), OutboxRequestID: "request-1",
		SubjectID: "subject-1", OwnerPlatformUserID: "platform-1",
	}
	event, err := store.RecordExecution(ctx, record)
	if err != nil {
		t.Fatalf("RecordExecution() error = %v", err)
	}
	trace := ExecutionTraceRecord{
		TenantID: "tenant-a", AppCode: "support", Channel: "web", BindingID: "web-console",
		MessageID: "message-1", TraceID: "trace-1", Trace: AgentExecutionTrace{Status: "completed"},
	}
	if err := store.RecordExecutionTrace(ctx, trace); err != nil {
		t.Fatalf("RecordExecutionTrace() error = %v", err)
	}
	if got, err := store.GetExecutionTrace(ctx, "tenant-a", "web", "web-console", "message-1"); err != nil || got.TraceID != "trace-1" {
		t.Fatalf("GetExecutionTrace() = %#v, %v", got, err)
	}
	if got, err := store.ListExecutionTraces(ctx, "tenant-a", []ExecutionTraceRef{{Channel: "web", BindingID: "web-console", MessageID: "message-1"}}); err != nil || len(got) != 1 {
		t.Fatalf("ListExecutionTraces() = %#v, %v", got, err)
	}
	if _, err := store.GetSession(ctx, "tenant-a", record.SessionKey); err != nil {
		t.Fatalf("GetSession() error = %v", err)
	}
	if got, err := store.ListSessions(ctx, "tenant-a", 10); err != nil || len(got) == 0 {
		t.Fatalf("ListSessions() = %#v, %v", got, err)
	}
	if got, err := store.ListApplicationSessions(ctx, "tenant-a", "support"); err != nil || len(got) == 0 {
		t.Fatalf("ListApplicationSessions() = %#v, %v", got, err)
	}
	if got, err := store.ListAudit(ctx, "tenant-a", "trace-1"); err != nil || len(got) == 0 {
		t.Fatalf("ListAudit() = %#v, %v", got, err)
	}
	if err := store.RecordAudit(ctx, AuditEvent{TenantID: "tenant-a", TraceID: "manual-trace", Action: "manual"}); err != nil {
		t.Fatalf("RecordAudit() error = %v", err)
	}
	if _, err := store.PurgeAuditBefore(ctx, "tenant-a", time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("PurgeAuditBefore() error = %v", err)
	}

	claimed, err := store.ClaimPendingOutboxByType(ctx, "tenant-a", "dispatcher-1", time.Minute, 10, "channel_reply")
	if err != nil || len(claimed) != 1 || claimed[0].ID != event.ID {
		t.Fatalf("ClaimPendingOutboxByType() = %#v, %v", claimed, err)
	}
	if err := store.RenewOutboxDelivery(ctx, "tenant-a", event.ID, "dispatcher-1", time.Minute); err != nil {
		t.Fatalf("RenewOutboxDelivery() error = %v", err)
	}
	if err := store.CompleteOutboxDelivery(ctx, "tenant-a", event.ID, "dispatcher-1", "receipt-1"); err != nil {
		t.Fatalf("CompleteOutboxDelivery() error = %v", err)
	}
	if found, err := store.FindOutboxByRequestID(ctx, "tenant-a", "request-1"); err != nil || found.DeliveryReceipt != "receipt-1" {
		t.Fatalf("FindOutboxByRequestID() = %#v, %v", found, err)
	}
	if pending, err := store.ListPendingOutbox(ctx, "tenant-a", 10); err != nil || len(pending) != 0 {
		t.Fatalf("ListPendingOutbox() = %#v, %v", pending, err)
	}

	secondEvent, err := store.RecordExecution(ctx, ExecutionRecord{
		TenantID: "tenant-a", AppCode: "support", SessionKey: "tenant-a/support/session/1",
		MessageID: "message-2", Channel: "web", BindingID: "web-console", TraceID: "trace-2",
		Action: "agent.reply", Result: "queued", OutboxType: "channel_reply.web",
		OutboxPayload: []byte("{}"), OutboxRequestID: "request-2", SubjectID: "subject-1",
	})
	if err != nil {
		t.Fatalf("RecordExecution(second) error = %v", err)
	}
	if backlog, err := store.OutboxBacklog(ctx, "tenant-a", "channel_reply.web"); err != nil || backlog.Pending != 1 {
		t.Fatalf("OutboxBacklog() = %#v, %v", backlog, err)
	}
	claimed, err = store.ClaimPendingOutbox(ctx, "tenant-a", "dispatcher-2", time.Minute, 10)
	if err != nil || len(claimed) != 1 || claimed[0].ID != secondEvent.ID {
		t.Fatalf("ClaimPendingOutbox() = %#v, %v", claimed, err)
	}
	if batch, err := store.FindOutboxByRequestIDs(ctx, "tenant-a", []string{"request-1", "request-2"}); err != nil || len(batch) != 2 {
		t.Fatalf("FindOutboxByRequestIDs() = %#v, %v", batch, err)
	}
	if err := store.FailOutboxDelivery(ctx, "tenant-a", secondEvent.ID, "dispatcher-2", errors.New("retry later")); err != nil {
		t.Fatalf("FailOutboxDelivery() error = %v", err)
	}
	// The legacy mark-delivered capability is still used by non-leased callers.
	if err := store.MarkOutboxDelivered(ctx, "tenant-a", secondEvent.ID); err != nil {
		t.Fatalf("MarkOutboxDelivered() error = %v", err)
	}
	if _, err := store.PurgeDeliveredOutboxBefore(ctx, "tenant-a", time.Now().Add(time.Hour), 10); err != nil {
		t.Fatalf("PurgeDeliveredOutboxBefore() error = %v", err)
	}

	route := SessionRoute{
		TenantID: "tenant-a", AppCode: "support", Channel: "telegram", BindingID: "tg",
		ConversationID: "chat-1", ExternalUserID: "external-2", SubjectID: "subject-2", Scope: "direct",
	}
	sessionKey, err := store.ResolveSession(ctx, route, "tenant-a/support/session/2")
	if err != nil {
		t.Fatalf("ResolveSession() error = %v", err)
	}
	switched, err := store.SwitchSession(ctx, SessionSwitchRequest{
		Route: route, SessionKey: "tenant-a/support/session/3", RequestID: "switch-1",
		OutboxType: "channel_reply", OutboxPayload: []byte("{}"),
	})
	if err != nil || switched.SessionKey == "" {
		t.Fatalf("SwitchSession() = %#v, %v", switched, err)
	}
	if got, err := store.ListClaimableSessions(ctx, "tenant-a", "telegram", "tg", "external-2", 10); err != nil || len(got) == 0 {
		t.Fatalf("ListClaimableSessions() = %#v, %v", got, err)
	}
	if err := store.ClaimSession(ctx, "tenant-a", sessionKey, "platform-2"); err != nil {
		t.Fatalf("ClaimSession() error = %v", err)
	}
	if got, err := store.ListInboundMessageRoutes(ctx, "tenant-a", record.SessionKey); err != nil || len(got) == 0 {
		t.Fatalf("ListInboundMessageRoutes() = %#v, %v", got, err)
	}
	if got, err := store.ListInboundMessageRoutesByMessageIDs(ctx, "tenant-a", record.SessionKey, []string{"message-1"}); err != nil || len(got) != 1 {
		t.Fatalf("ListInboundMessageRoutesByMessageIDs() = %#v, %v", got, err)
	}
	if err := store.EndChannelIdentityRoutes(ctx, "tenant-a", "telegram", "tg", "external-2"); err != nil {
		t.Fatalf("EndChannelIdentityRoutes() error = %v", err)
	}
	if err := store.ArchiveSession(ctx, "tenant-a", sessionKey); err != nil {
		t.Fatalf("ArchiveSession() error = %v", err)
	}
	if _, err := store.ArchiveIdleSessions(ctx, time.Now().Add(time.Hour), 100); err != nil {
		t.Fatalf("ArchiveIdleSessions() error = %v", err)
	}

	lease, err := store.AcquireSessionExecutionLease(ctx, "tenant-a", record.SessionKey, "worker-1", time.Minute)
	if err != nil {
		t.Fatalf("AcquireSessionExecutionLease() error = %v", err)
	}
	lease, err = store.RenewSessionExecutionLease(ctx, lease, time.Minute)
	if err != nil {
		t.Fatalf("RenewSessionExecutionLease() error = %v", err)
	}
	if err := store.ReleaseSessionExecutionLease(ctx, lease); err != nil {
		t.Fatalf("ReleaseSessionExecutionLease() error = %v", err)
	}

	seen := make(map[string]bool)
	for _, attributes := range observer.attributes {
		if attributes.Backend != "memory" {
			t.Fatalf("observer backend = %q", attributes.Backend)
		}
		seen[attributes.Operation] = true
	}
	for _, operation := range []string{
		"record_execution", "record_execution_trace", "get_execution_trace", "list_execution_traces",
		"read_outbox_backlog", "claim_pending_outbox", "renew_outbox_delivery", "complete_outbox_delivery", "fail_outbox_delivery",
		"find_outbox_by_request", "find_outbox_by_requests", "mark_outbox_delivered", "purge_delivered_outbox",
		"record_audit", "purge_audit", "switch_session",
		"resolve_session", "archive_session", "archive_idle_sessions",
		"acquire_session_execution_lease", "renew_session_execution_lease", "release_session_execution_lease",
	} {
		if !seen[operation] {
			t.Fatalf("observer did not record %q: %#v", operation, observer.attributes)
		}
	}
}

func TestObservedStoreConstructorsAndUnsupportedCapabilitiesFailClosed(t *testing.T) {
	t.Parallel()
	observer := &recordingStoreObserver{}
	if _, err := NewObservedStateStore(nil, observer, "memory"); err == nil {
		t.Fatal("NewObservedStateStore accepted nil delegate")
	}
	if _, err := NewObservedStateStore(NewMemoryStateStore(), nil, "memory"); err == nil {
		t.Fatal("NewObservedStateStore accepted nil observer")
	}
	if _, err := NewObservedStateStore(NewMemoryStateStore(), observer, ""); err == nil {
		t.Fatal("NewObservedStateStore accepted empty backend")
	}

	minimal := &minimalObservedStateStore{}
	store, err := NewObservedStateStore(minimal, observer, "minimal")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SwitchSession(context.Background(), SessionSwitchRequest{}); err == nil {
		t.Fatal("SwitchSession succeeded without SessionCommandStore")
	}
	if err := store.RecordAudit(context.Background(), AuditEvent{}); err == nil {
		t.Fatal("RecordAudit succeeded without AuditRecorder")
	}
	if _, err := store.ClaimPendingOutbox(context.Background(), "tenant", "owner", time.Second, 1); err == nil {
		t.Fatal("ClaimPendingOutbox succeeded without OutboxDeliveryStore")
	}
	if _, err := store.ListSessions(context.Background(), "tenant", 1); err == nil {
		t.Fatal("ListSessions succeeded without SessionLister")
	}
}

type minimalObservedStateStore struct{}

func (*minimalObservedStateStore) ResolveSession(context.Context, SessionRoute, string) (string, error) {
	return "", errors.New("not implemented")
}
func (*minimalObservedStateStore) ArchiveSession(context.Context, string, string) error {
	return errors.New("not implemented")
}
func (*minimalObservedStateStore) AcquireSessionExecutionLease(context.Context, string, string, string, time.Duration) (SessionExecutionLease, error) {
	return SessionExecutionLease{}, errors.New("not implemented")
}
func (*minimalObservedStateStore) RenewSessionExecutionLease(context.Context, SessionExecutionLease, time.Duration) (SessionExecutionLease, error) {
	return SessionExecutionLease{}, errors.New("not implemented")
}
func (*minimalObservedStateStore) ReleaseSessionExecutionLease(context.Context, SessionExecutionLease) error {
	return errors.New("not implemented")
}
func (*minimalObservedStateStore) RecordExecution(context.Context, ExecutionRecord) (OutboxEvent, error) {
	return OutboxEvent{}, errors.New("not implemented")
}
func (*minimalObservedStateStore) RecordExecutionTrace(context.Context, ExecutionTraceRecord) error {
	return errors.New("not implemented")
}
func (*minimalObservedStateStore) GetExecutionTrace(context.Context, string, string, string, string) (ExecutionTraceRecord, error) {
	return ExecutionTraceRecord{}, errors.New("not implemented")
}
func (*minimalObservedStateStore) ListExecutionTraces(context.Context, string, []ExecutionTraceRef) ([]ExecutionTraceRecord, error) {
	return nil, errors.New("not implemented")
}
func (*minimalObservedStateStore) GetSession(context.Context, string, string) (Session, error) {
	return Session{}, errors.New("not implemented")
}
func (*minimalObservedStateStore) ListAudit(context.Context, string, string) ([]AuditEvent, error) {
	return nil, errors.New("not implemented")
}
func (*minimalObservedStateStore) ListPendingOutbox(context.Context, string, int) ([]OutboxEvent, error) {
	return nil, errors.New("not implemented")
}
func (*minimalObservedStateStore) MarkOutboxDelivered(context.Context, string, string) error {
	return errors.New("not implemented")
}

var _ StateStore = (*minimalObservedStateStore)(nil)
var _ metrics.StoreObserver = (*recordingStoreObserver)(nil)
