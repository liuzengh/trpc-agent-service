package tool

import (
	"errors"
)

func (r AuditRecord) validate() error {
	for _, value := range []string{r.TenantID, r.AgentAppID, r.SessionID, r.RequestID, r.MessageID, r.ToolName} {
		if !validServerID(value) {
			return errors.New("invalid governance audit record")
		}
	}
	if r.ToolVersion < 1 || r.PolicyVersion < 1 || r.InputBytes < 0 || r.OutputBytes < 0 || r.Latency < 0 || len(r.InputFingerprint) > 80 || len(r.OutputFingerprint) > 80 || !validAuditDecision(r.Decision) {
		return errors.New("invalid governance audit record")
	}
	return nil
}

func validAuditDecision(category DecisionCategory) bool {
	switch category {
	case CategoryAllow, CategoryDeny, CategoryApprovalRequired, CategoryInvalidInput, CategoryInvalidContext, CategoryPolicyUnavailable, CategoryExpiredPolicy, CategoryVersionMismatch, CategoryBudgetExceeded, CategoryRedacted, CategoryOversized, CategoryTimeout, CategoryCancelled, CategoryToolFailure, CategoryUnknown:
		return true
	default:
		return false
	}
}
