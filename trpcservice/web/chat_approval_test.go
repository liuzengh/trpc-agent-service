package web

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
)

type webApprovalResolverFunc func(context.Context, string, string, string, bool) (governance.PendingApproval, error)

func (f webApprovalResolverFunc) ResolveForRequester(ctx context.Context, tenantID, token, requesterUserID string, approved bool) (governance.PendingApproval, error) {
	return f(ctx, tenantID, token, requesterUserID, approved)
}

func TestConsoleResolvesOwnedWebApproval(t *testing.T) {
	var gotToken, gotRequester string
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.Approvals = webApprovalResolverFunc(func(_ context.Context, tenantID, token, requesterUserID string, approved bool) (governance.PendingApproval, error) {
			if tenantID != "example" || !approved {
				t.Fatalf("resolution identity = tenant %q approved %v", tenantID, approved)
			}
			gotToken, gotRequester = token, requesterUserID
			return governance.PendingApproval{TenantID: tenantID, RequestID: "request-1", ToolName: "request_refund", ToolDescription: "提交退款申请"}, nil
		})
	})
	recorder := handler.request(t, http.MethodPost, "/api/v1/chat/approval", `{"tenant_id":"example","action_id":"approval:approve:token-1"}`)
	if recorder.Code != http.StatusOK || gotToken != "token-1" || gotRequester != "console-admin" {
		t.Fatalf("resolve response=%d %s token=%q requester=%q", recorder.Code, recorder.Body.String(), gotToken, gotRequester)
	}
	if body := recorder.Body.String(); !strings.Contains(body, `"state":"approved"`) || !strings.Contains(body, "已确认") {
		t.Fatalf("resolved card = %s", body)
	}
}

func TestConsoleRejectsWebApprovalOwnedByAnotherUser(t *testing.T) {
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.Approvals = webApprovalResolverFunc(func(context.Context, string, string, string, bool) (governance.PendingApproval, error) {
			return governance.PendingApproval{}, governance.ErrApprovalRouteMismatch
		})
	})
	recorder := handler.request(t, http.MethodPost, "/api/v1/chat/approval", `{"tenant_id":"example","action_id":"approval:reject:token-1"}`)
	if recorder.Code != http.StatusForbidden || !strings.Contains(recorder.Body.String(), "another user") {
		t.Fatalf("route mismatch response = %d %s", recorder.Code, recorder.Body.String())
	}

	invalid := handler.request(t, http.MethodPost, "/api/v1/chat/approval", `{"tenant_id":"example","action_id":"not-an-approval"}`)
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid action response = %d %s", invalid.Code, invalid.Body.String())
	}

	unavailable := testConsoleHandler(t)
	missing := unavailable.request(t, http.MethodPost, "/api/v1/chat/approval", `{"tenant_id":"example","action_id":"approval:approve:token-1"}`)
	if missing.Code != http.StatusServiceUnavailable || !strings.Contains(missing.Body.String(), "unavailable") {
		t.Fatalf("unavailable response = %d %s", missing.Code, missing.Body.String())
	}
}
