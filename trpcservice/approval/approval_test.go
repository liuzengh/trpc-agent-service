package approval_test

import (
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/approval"
)

func TestApprovalRequestStoresDigestOnly(t *testing.T) {
	args := []byte(`{"email":"person@example.com","password":"secret123"}`)
	request := approval.Request{
		TenantID:       "tenant-a",
		AppID:          "app-a",
		ConfigVersion:  "v1",
		RequestID:      "request-a",
		SessionID:      "session-a",
		ToolName:       "delete",
		ArgumentDigest: approval.DigestArguments(args),
		ExpiresAt:      time.Now().Add(time.Minute),
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("validate approval request: %v", err)
	}
	if strings.Contains(request.ArgumentDigest, "secret123") || strings.Contains(request.ArgumentDigest, "person@example.com") {
		t.Fatal("approval digest contains raw tool arguments")
	}
}

func TestApprovalRecordRequiresDecisionTimestamp(t *testing.T) {
	record := approval.Record{
		ApprovalID:     "approval-a",
		TenantID:       "tenant-a",
		AppID:          "app-a",
		ConfigVersion:  "v1",
		RequestID:      "request-a",
		SessionID:      "session-a",
		ToolName:       "delete",
		ArgumentDigest: approval.DigestArguments(nil),
		Status:         approval.StatusApproved,
		ExpiresAt:      time.Now().Add(time.Minute),
		CreatedAt:      time.Now(),
	}
	if err := record.Validate(); err == nil {
		t.Fatal("approved record without decided_at was accepted")
	}
}
