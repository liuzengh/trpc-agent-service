package application

import "github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"

type CreateDeploymentCommand struct {
	TenantID       string
	ActorUserID    string
	IdempotencyKey string
	Name           string
	Description    string
}

type CreateDeploymentResult struct {
	Deployment domain.Deployment `json:"deployment"`
	Created    bool              `json:"-"`
}

type UpdateDeploymentCommand struct {
	TenantID                 string
	DeploymentID             string
	ActorUserID              string
	ExpectedMetadataRevision int64
	Name                     *string
	Description              *string
}

type ValidateDeploymentCommand struct {
	TenantID     string
	DeploymentID string
	ActorUserID  string
	Input        domain.DeploymentInput
}

type PublishDeploymentCommand struct {
	TenantID                     string
	DeploymentID                 string
	ActorUserID                  string
	IdempotencyKey               string
	ExpectedLatestRevisionNumber *int64
	Input                        domain.DeploymentInput
}

type PublishDeploymentResult struct {
	Published  domain.PublishedRevision `json:"published"`
	Validation domain.ValidationReport  `json:"validation"`
	Created    bool                     `json:"-"`
}

type MigrateAndPublishCommand struct {
	TenantID                     string
	DeploymentID                 string
	ActorUserID                  string
	IdempotencyKey               string
	SourceRevisionNumber         int64
	ExpectedLatestRevisionNumber *int64
	Input                        domain.DeploymentInput
}

type MigrateAndPublishResult struct {
	MemoryScopesCopied int                     `json:"memory_scopes_copied"`
	Publication        PublishDeploymentResult `json:"publication"`
}
