package web

import (
	"github.com/google/uuid"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/auth"
)

// Asset-change auditing.
//
// The audit trail used to cover agent runs only, so a member gaining write
// access to tenant assets would have been able to create, share and delete them
// without a trace. These helpers record every asset mutation on the existing
// audit_logs table: one row per change, carrying who did it, in which tenant,
// which asset, and whether the change was allowed or refused.
//
// The mapping onto the run-oriented schema is deliberate:
//   - Channel   = "console"      (the change came from the Admin API, not IM)
//   - AgentName = the asset kind  ("agent", "kb", "skill", "binding", "endpoint")
//   - ToolName  = the asset id    (which row was touched)
//   - SessionID = "asset:<kind>:<id>" (there is no session; the column is NOT NULL)
//   - Decision  = executed | deny
//   - ErrorType = the refusal reason, for a denied change
//
// AuditSourceAsset is the Channel value stamped on asset-change entries. It is
// what separates "an employee edited a tenant asset" from "an agent ran" in the
// audit view, since both live on the same table.
const (
	auditChannelConsole = "console"
	// AuditSourceAsset is exported for the audit API's source filter.
	AuditSourceAsset = auditChannelConsole
)

// assetAuditor is the narrow write side of the audit recorder that the asset
// APIs need. It matches audit.Recorder (and audit.MySQLRecorder), but stays an
// interface so a handler test can capture entries. May be nil — auditing is
// best-effort and never blocks the change itself.
type assetAuditor interface {
	Record(e audit.Entry)
}

// assetChangeKind names the audited asset families. Kept as constants so the
// audit view can filter on one vocabulary.
const (
	assetKindAgent    = "agent"
	assetKindKB       = "kb"
	assetKindSkill    = "skill"
	assetKindBinding  = "binding"
	assetKindEndpoint = "endpoint"
)

// recordAssetChange writes one audit row for an asset mutation. A refused change
// is recorded as well (decision=deny), so an attempted cross-tenant or
// non-authored write stays visible even though the handler answered 404.
//
// The actor comes from the request claims: an asset-change row without a
// user_id answers "what changed" but not "who changed it", which is exactly the
// question an audit trail exists to answer, so the caller always passes the
// authenticated member (empty only for an unauthenticated attempt, which the
// auth middleware already rejects).
func recordAssetChange(claims *auth.Claims, auditor assetAuditor, kind, id, tenantID, decision, reason string) {
	if auditor == nil {
		return
	}
	userID := ""
	if claims != nil {
		userID = claims.UserID
		if tenantID == "" {
			tenantID = claims.TenantID
		}
	}
	auditor.Record(audit.Entry{
		TenantID:  tenantID,
		Channel:   auditChannelConsole,
		UserID:    userID,
		SessionID: "asset:" + kind + ":" + id,
		AgentName: kind,
		ToolName:  id,
		Decision:  decision,
		ErrorType: reason,
		TraceID:   uuid.NewString(),
	})
}

// recordAssetAllowed records a change that went through, attributed to the
// authenticated caller.
func recordAssetAllowed(claims *auth.Claims, auditor assetAuditor, kind, id, tenantID string) {
	recordAssetChange(claims, auditor, kind, id, tenantID, audit.DecisionExecuted, "")
}

// recordAssetDenied records a refused change. tenantID is the tenant the
// resource would have belonged to (empty when it could not be resolved).
func recordAssetDenied(claims *auth.Claims, auditor assetAuditor, kind, id, tenantID, reason string) {
	recordAssetChange(claims, auditor, kind, id, tenantID, audit.DecisionDeny, reason)
}
