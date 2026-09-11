package executionv1

import (
	"errors"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
)

const BackendMigrationPath = "/internal/v1/backend-migrations:execute"

var ErrBackendMigrationBusy = errors.New("backend migration source is not quiescent")

// BackendMigrationRequest is an internal mTLS-only request. Password values
// are request-scoped and must be cleared after adapter initialization.
type BackendMigrationRequest struct {
	TenantID                 string                 `json:"tenant_id"`
	SourceDeploymentRevision string                 `json:"source_deployment_revision_id"`
	AgentID                  string                 `json:"agent_id"`
	Source                   BackendMigrationTarget `json:"source"`
	Target                   BackendMigrationTarget `json:"target"`
}

type BackendMigrationTarget struct {
	Backend  datav1.Snapshot `json:"backend"`
	Password string          `json:"password"`
}

type BackendMigrationResponse struct {
	MemoryScopesCopied int `json:"memory_scopes_copied"`
}
