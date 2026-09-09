package tool

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	ApprovalPending  = "pending"
	ApprovalApproved = "approved"
	ApprovalRejected = "rejected"
	ApprovalRevoked  = "revoked"
	ApprovalConsumed = "consumed"
)

type ApprovalRequest struct {
	TenantID             string
	RequesterIdentity    string
	ToolName             string
	Capability           string
	ResourceScope        string
	ParameterFingerprint string
	PolicyVersion        int64
	ExpiresAt            time.Time
}

type Approval struct {
	ID                   string
	TenantID             string
	RequesterIdentity    string
	ApproverIdentity     string
	ToolName             string
	Capability           string
	ResourceScope        string
	ParameterFingerprint string
	PolicyVersion        int64
	Status               string
	CreatedAt            time.Time
	ExpiresAt            time.Time
	ApprovedAt           time.Time
	RejectedAt           time.Time
	RevokedAt            time.Time
	UsedCount            int
	MaxUses              int
	Version              int64
}

type ApprovalStore interface {
	Create(context.Context, tenant.TenantContext, ApprovalRequest) (Approval, error)
	Approve(context.Context, tenant.TenantContext, string, string, int64) (Approval, error)
	Reject(context.Context, tenant.TenantContext, string, string, int64) (Approval, error)
	Revoke(context.Context, tenant.TenantContext, string, string, int64) (Approval, error)
	Consume(context.Context, tenant.TenantContext, string, ApprovalRequest, int64) (Approval, error)
}

// ApprovalBoundary documents the durable contract without pretending that the
// current repository has an approval admin endpoint or PostgreSQL table.
type ApprovalBoundary struct {
	Store ApprovalStore
}

func (a Approval) ValidateAt(now time.Time) error {
	for _, value := range []string{a.ID, a.TenantID, a.RequesterIdentity, a.ToolName, a.ResourceScope, a.ParameterFingerprint, a.Status} {
		if value == "" || len(value) > 256 || strings.TrimSpace(value) != value {
			return errors.New("invalid approval")
		}
	}
	if a.PolicyVersion < 1 || a.Version < 1 || a.MaxUses < 1 || a.UsedCount < 0 || a.UsedCount > a.MaxUses || a.CreatedAt.IsZero() || a.ExpiresAt.IsZero() {
		return errors.New("invalid approval")
	}
	switch a.Status {
	case ApprovalPending, ApprovalApproved, ApprovalRejected, ApprovalRevoked, ApprovalConsumed:
	default:
		return errors.New("invalid approval status")
	}
	if !now.Before(a.ExpiresAt) && a.Status != ApprovalConsumed {
		return errors.New("approval is expired")
	}
	return nil
}

func (a ApprovalBoundary) Consume(ctx context.Context, tc tenant.TenantContext, id string, request ApprovalRequest, expectedVersion int64) (Approval, error) {
	if a.Store == nil {
		return Approval{}, safeError(CategoryApprovalRequired, "approval_deferred")
	}
	value, err := a.Store.Consume(ctx, tc, id, request, expectedVersion)
	if err == nil {
		return value, nil
	}
	if ctx != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return Approval{}, safeError(categoryForContext(contextErr), "approval_context")
		}
	}
	return Approval{}, safeError(CategoryPolicyUnavailable, "approval_unavailable")
}
