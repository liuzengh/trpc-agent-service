package application

import (
	"context"
	"errors"
	"sort"

	approvalv1 "github.com/liuzengh/trpc-agent-service/api/runtime/approval/v1"
	governancev1 "github.com/liuzengh/trpc-agent-service/api/runtime/governance/v1"
	managementv1 "github.com/liuzengh/trpc-agent-service/api/runtime/management/v1"
)

var (
	ErrForbidden   = errors.New("run management forbidden")
	ErrNotFound    = errors.New("run not found")
	ErrInvalidPage = errors.New("run management page invalid")
	ErrUnavailable = errors.New("run management unavailable")
	ErrConflict    = errors.New("tool approval conflict")
)

type TenantAccess interface {
	IsActiveMember(context.Context, string, string) (bool, error)
}
type OwnerAccess interface {
	IsActiveOwner(context.Context, string, string) (bool, error)
}
type RuntimeReader interface {
	ListRuns(context.Context, string, int, int) (managementv1.RunPage, error)
	GetRun(context.Context, string, string) (managementv1.RunDetail, error)
	ListAudit(context.Context, string, int, int) (managementv1.AuditPage, error)
}
type ApprovalRuntime interface {
	ListApprovals(context.Context, string, int, int) (approvalv1.Page, error)
	DecideApproval(context.Context, string, string, string, approvalv1.DecisionRequest) (approvalv1.DecisionResponse, error)
}
type ControlAuditReader interface {
	List(context.Context, string, int, int) (managementv1.AuditPage, error)
}

type Service struct {
	access  TenantAccess
	runtime RuntimeReader
	audit   ControlAuditReader
}

func New(access TenantAccess, runtime RuntimeReader, audit ControlAuditReader) (*Service, error) {
	if access == nil || runtime == nil || audit == nil {
		return nil, ErrUnavailable
	}
	return &Service{access: access, runtime: runtime, audit: audit}, nil
}

func (s *Service) ListApprovals(ctx context.Context, tenant, user string, offset, limit int) (approvalv1.Page, error) {
	if !validPage(offset, limit) {
		return approvalv1.Page{}, ErrInvalidPage
	}
	if err := s.authorize(ctx, tenant, user); err != nil {
		return approvalv1.Page{}, err
	}
	approvals, ok := s.runtime.(ApprovalRuntime)
	if !ok {
		return approvalv1.Page{}, ErrUnavailable
	}
	page, err := approvals.ListApprovals(ctx, tenant, offset, limit)
	if err != nil {
		return approvalv1.Page{}, ErrUnavailable
	}
	return page, nil
}

func (s *Service) DecideApproval(ctx context.Context, tenant, user, operation string, request approvalv1.DecisionRequest) (approvalv1.DecisionResponse, error) {
	if tenant == "" || user == "" || operation == "" || !approvalv1.ValidDigest(request.ExpectedArgumentsDigest) || (request.Action != "approve" && request.Action != "reject") || len(request.Reason) > 500 {
		return approvalv1.DecisionResponse{}, ErrConflict
	}
	owners, ok := s.access.(OwnerAccess)
	if !ok {
		return approvalv1.DecisionResponse{}, ErrForbidden
	}
	ok, err := owners.IsActiveOwner(ctx, tenant, user)
	if err != nil {
		return approvalv1.DecisionResponse{}, ErrUnavailable
	}
	if !ok {
		return approvalv1.DecisionResponse{}, ErrForbidden
	}
	approvals, ok := s.runtime.(ApprovalRuntime)
	if !ok {
		return approvalv1.DecisionResponse{}, ErrUnavailable
	}
	response, err := approvals.DecideApproval(ctx, tenant, operation, user, request)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return approvalv1.DecisionResponse{}, ErrNotFound
		}
		if errors.Is(err, ErrConflict) {
			return approvalv1.DecisionResponse{}, ErrConflict
		}
		return approvalv1.DecisionResponse{}, ErrUnavailable
	}
	return response, nil
}

func (s *Service) authorize(ctx context.Context, tenant, user string) error {
	if tenant == "" || user == "" {
		return ErrForbidden
	}
	ok, err := s.access.IsActiveMember(ctx, tenant, user)
	if err != nil {
		return ErrUnavailable
	}
	if !ok {
		return ErrForbidden
	}
	return nil
}

func validPage(offset, limit int) bool {
	return offset >= 0 && limit > 0 && limit <= managementv1.MaxPageSize
}

func (s *Service) ListRuns(ctx context.Context, tenant, user string, offset, limit int) (managementv1.RunPage, error) {
	if !validPage(offset, limit) {
		return managementv1.RunPage{}, ErrInvalidPage
	}
	if err := s.authorize(ctx, tenant, user); err != nil {
		return managementv1.RunPage{}, err
	}
	page, err := s.runtime.ListRuns(ctx, tenant, offset, limit)
	if err != nil {
		return managementv1.RunPage{}, ErrUnavailable
	}
	return page, nil
}

func (s *Service) GetRun(ctx context.Context, tenant, user, runID string) (managementv1.RunDetail, error) {
	if runID == "" {
		return managementv1.RunDetail{}, ErrNotFound
	}
	if err := s.authorize(ctx, tenant, user); err != nil {
		return managementv1.RunDetail{}, err
	}
	run, err := s.runtime.GetRun(ctx, tenant, runID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return managementv1.RunDetail{}, ErrNotFound
		}
		return managementv1.RunDetail{}, ErrUnavailable
	}
	return run, nil
}

func (s *Service) ListAudit(ctx context.Context, tenant, user string, offset, limit int) (managementv1.AuditPage, error) {
	if !validPage(offset, limit) {
		return managementv1.AuditPage{}, ErrInvalidPage
	}
	if err := s.authorize(ctx, tenant, user); err != nil {
		return managementv1.AuditPage{}, err
	}
	want := offset + limit
	if want > managementv1.MaxPageSize {
		return managementv1.AuditPage{}, ErrInvalidPage
	}
	control, err := s.audit.List(ctx, tenant, 0, want)
	if err != nil {
		return managementv1.AuditPage{}, ErrUnavailable
	}
	runtime, err := s.runtime.ListAudit(ctx, tenant, 0, want)
	if err != nil {
		return managementv1.AuditPage{}, ErrUnavailable
	}
	events := append(append([]managementv1.AuditEvent{}, control.Events...), runtime.Events...)
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].OccurredAt.Equal(events[j].OccurredAt) {
			return events[i].EventID > events[j].EventID
		}
		return events[i].OccurredAt.After(events[j].OccurredAt)
	})
	end := min(len(events), want)
	start := min(offset, end)
	return managementv1.AuditPage{Events: events[start:end], Offset: offset, Limit: limit, Total: control.Total + runtime.Total}, nil
}

func (s *Service) GetUsage(ctx context.Context, tenant, user string) (governancev1.UsageSummary, error) {
	if err := s.authorize(ctx, tenant, user); err != nil {
		return governancev1.UsageSummary{}, err
	}
	reader, ok := s.runtime.(interface {
		Usage(context.Context, string) (governancev1.UsageSummary, error)
	})
	if !ok {
		return governancev1.UsageSummary{}, ErrUnavailable
	}
	out, err := reader.Usage(ctx, tenant)
	if err != nil {
		return governancev1.UsageSummary{}, ErrUnavailable
	}
	if out.TenantID != tenant {
		return governancev1.UsageSummary{}, ErrUnavailable
	}
	return out, nil
}
