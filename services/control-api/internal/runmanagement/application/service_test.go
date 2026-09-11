package application

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	approvalv1 "github.com/liuzengh/trpc-agent-service/api/runtime/approval/v1"
	managementv1 "github.com/liuzengh/trpc-agent-service/api/runtime/management/v1"
)

type accessFake struct{ allowed, owner bool }

func (a accessFake) IsActiveMember(context.Context, string, string) (bool, error) {
	return a.allowed, nil
}
func (a accessFake) IsActiveOwner(context.Context, string, string) (bool, error) { return a.owner, nil }

type runtimeFake struct {
	audit     managementv1.AuditPage
	approvals approvalv1.Page
	calls     int
}

func (r *runtimeFake) ListRuns(context.Context, string, int, int) (managementv1.RunPage, error) {
	r.calls++
	return managementv1.RunPage{Total: 1}, nil
}
func (r *runtimeFake) GetRun(context.Context, string, string) (managementv1.RunDetail, error) {
	r.calls++
	return managementv1.RunDetail{RunSummary: managementv1.RunSummary{RunID: "run"}}, nil
}
func (r *runtimeFake) ListAudit(context.Context, string, int, int) (managementv1.AuditPage, error) {
	r.calls++
	return r.audit, nil
}
func (r *runtimeFake) ListApprovals(context.Context, string, int, int) (approvalv1.Page, error) {
	r.calls++
	return r.approvals, nil
}
func (r *runtimeFake) DecideApproval(_ context.Context, tenant, operation, actor string, request approvalv1.DecisionRequest) (approvalv1.DecisionResponse, error) {
	r.calls++
	return approvalv1.DecisionResponse{Outcome: "DECIDED", Operation: approvalv1.Operation{TenantID: tenant, OperationID: operation, DecidedBy: actor, ArgumentsDigest: request.ExpectedArgumentsDigest, Status: approvalv1.StatusApproved}}, nil
}

type auditFake struct {
	page  managementv1.AuditPage
	calls int
}

func TestToolApprovalRequiresOwnerAndBindsDigest(t *testing.T) {
	runtime, audit := &runtimeFake{approvals: approvalv1.Page{Total: 1}}, &auditFake{}
	service, _ := New(accessFake{allowed: true}, runtime, audit)
	request := approvalv1.DecisionRequest{Action: "approve", ExpectedArgumentsDigest: "sha256:" + strings.Repeat("a", 64)}
	if _, err := service.DecideApproval(context.Background(), "tenant", "member", "tap_1", request); !errors.Is(err, ErrForbidden) || runtime.calls != 0 {
		t.Fatal(err, runtime.calls)
	}
	service, _ = New(accessFake{allowed: true, owner: true}, runtime, audit)
	response, err := service.DecideApproval(context.Background(), "tenant", "owner", "tap_1", request)
	if err != nil || response.Outcome != "DECIDED" || response.Operation.DecidedBy != "owner" || runtime.calls != 1 {
		t.Fatal(response, err, runtime.calls)
	}
	if _, err = service.DecideApproval(context.Background(), "tenant", "owner", "tap_1", approvalv1.DecisionRequest{Action: "approve"}); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
}

func (a *auditFake) List(context.Context, string, int, int) (managementv1.AuditPage, error) {
	a.calls++
	return a.page, nil
}

func TestTenantAuthorizationPrecedesRuntimeRead(t *testing.T) {
	runtime, audit := &runtimeFake{}, &auditFake{}
	service, _ := New(accessFake{}, runtime, audit)
	if _, err := service.ListRuns(context.Background(), "tenant", "user", 0, 25); !errors.Is(err, ErrForbidden) || runtime.calls != 0 {
		t.Fatal(err, runtime.calls)
	}
	service, _ = New(accessFake{allowed: true}, runtime, audit)
	if page, err := service.ListRuns(context.Background(), "tenant", "user", 0, 25); err != nil || page.Total != 1 || runtime.calls != 1 {
		t.Fatal(page, err, runtime.calls)
	}
}

func TestAuditMergesSourcesBeforeGlobalPagination(t *testing.T) {
	base := time.Date(2026, 9, 9, 1, 0, 0, 0, time.UTC)
	runtime := &runtimeFake{audit: managementv1.AuditPage{Total: 2, Events: []managementv1.AuditEvent{{EventID: "worker-new", OccurredAt: base.Add(3 * time.Minute)}, {EventID: "worker-old", OccurredAt: base}}}}
	audit := &auditFake{page: managementv1.AuditPage{Total: 2, Events: []managementv1.AuditEvent{{EventID: "control-new", OccurredAt: base.Add(2 * time.Minute)}, {EventID: "control-old", OccurredAt: base.Add(time.Minute)}}}}
	service, _ := New(accessFake{allowed: true}, runtime, audit)
	page, err := service.ListAudit(context.Background(), "tenant", "user", 1, 2)
	if err != nil || page.Total != 4 || len(page.Events) != 2 || page.Events[0].EventID != "control-new" || page.Events[1].EventID != "control-old" {
		t.Fatal(page, err)
	}
}

func TestAuditRejectsUnboundedDistributedOffset(t *testing.T) {
	runtime, audit := &runtimeFake{}, &auditFake{}
	service, _ := New(accessFake{allowed: true}, runtime, audit)
	if _, err := service.ListAudit(context.Background(), "tenant", "user", 100, 25); !errors.Is(err, ErrInvalidPage) || runtime.calls != 0 || audit.calls != 0 {
		t.Fatal(err, runtime.calls, audit.calls)
	}
}
