package governance

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPostgresApprovalStorePersistsLifecycleAndTenantScope(t *testing.T) {
	_, database, tenantID := postgresUsageGovernor(t)
	store, err := NewPostgresApprovalStore(database)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	record := ApprovalRecord{
		Token: "approval-1", TenantID: tenantID, AppCode: "support", ConfigVersion: 7,
		RequestID: "message-1", TraceID: "trace-1", Channel: "telegram", BindingID: "bot-a",
		ConversationID: "chat-1", ConversationScope: "direct", ExternalUserID: "external-user-1",
		RequesterUserID: "platform-user-1", ProgressMessageID: "progress-1", ToolName: "request_refund", ToolDescription: "提交退款申请",
		Status: ApprovalPending, CreatedAt: now, ExpiresAt: now.Add(10 * time.Minute),
	}
	ctx := context.Background()
	if err := store.Create(ctx, record); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	got, err := store.Get(ctx, tenantID, record.Token)
	if err != nil || got.Status != ApprovalPending || got.RequestID != record.RequestID || got.TraceID != record.TraceID || got.RequesterUserID != record.RequesterUserID || got.ProgressMessageID != record.ProgressMessageID {
		t.Fatalf("Get() = %#v, %v", got, err)
	}
	pending, err := store.ListPending(ctx, 10)
	if err != nil || len(pending) != 1 || pending[0].Token != record.Token {
		t.Fatalf("ListPending() = %#v, %v", pending, err)
	}
	if _, err := store.Get(ctx, "other-tenant", record.Token); !errors.Is(err, ErrApprovalRecordNotFound) {
		t.Fatalf("cross-tenant Get() error = %v, want ErrApprovalRecordNotFound", err)
	}
	if err := store.MarkNotified(ctx, tenantID, record.Token, "provider-message-1"); err != nil {
		t.Fatalf("MarkNotified() error = %v", err)
	}
	if err := store.Resolve(ctx, tenantID, record.Token, true, "external-user-1"); err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	got, err = store.Get(ctx, tenantID, record.Token)
	if err != nil || got.Status != ApprovalApproved || got.NotificationID != "provider-message-1" || got.ResolvedBy != "external-user-1" || got.NotifiedAt == nil || got.ResolvedAt == nil {
		t.Fatalf("resolved record = %#v, %v", got, err)
	}
	if err := store.Resolve(ctx, tenantID, record.Token, true, "external-user-1"); err != nil {
		t.Fatalf("idempotent Resolve() error = %v", err)
	}
	if err := store.Resolve(ctx, tenantID, record.Token, false, "external-user-1"); !errors.Is(err, ErrApprovalResolved) {
		t.Fatalf("conflicting Resolve() error = %v, want ErrApprovalResolved", err)
	}
}
