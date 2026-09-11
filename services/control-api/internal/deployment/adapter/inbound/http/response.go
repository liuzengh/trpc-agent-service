package httpadapter

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/santhosh-tekuri/jsonschema/v6"

	deploymentv1 "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
	identityapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
)

const manifestViewSchemaLocation = "https://jfsas.dev/schemas/deployment/v1/runtime-manifest-view.schema.json"

type deploymentResponse struct {
	ID                   string    `json:"id"`
	TenantID             string    `json:"tenant_id"`
	Name                 string    `json:"name"`
	Description          string    `json:"description"`
	MetadataRevision     int64     `json:"metadata_revision"`
	LatestRevisionNumber *int64    `json:"latest_revision_number"`
	CreatedBy            string    `json:"created_by"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`
}

func deploymentView(value domain.Deployment) deploymentResponse {
	return deploymentResponse{
		ID: value.ID, TenantID: value.TenantID, Name: value.Name,
		Description: value.Description, MetadataRevision: value.MetadataRevision,
		LatestRevisionNumber: value.LatestRevisionNumber, CreatedBy: value.CreatedBy,
		CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt,
	}
}

type createDeploymentResponse struct {
	Deployment deploymentResponse `json:"deployment"`
}

type deploymentPageResponse struct {
	Deployments []deploymentResponse `json:"deployments"`
	Total       int                  `json:"total"`
	Offset      int                  `json:"offset"`
	Limit       int                  `json:"limit"`
}

type agentInputResponse struct {
	AgentID       string `json:"agent_id"`
	VersionNumber int64  `json:"version_number"`
}

type profileInputResponse struct {
	ProfileID      string `json:"profile_id"`
	RevisionNumber int64  `json:"revision_number"`
}

type deploymentInputResponse struct {
	SchemaVersion string               `json:"schema_version"`
	Agent         agentInputResponse   `json:"agent"`
	Profile       profileInputResponse `json:"profile"`
}

func deploymentInputView(value domain.DeploymentInput) deploymentInputResponse {
	return deploymentInputResponse{
		SchemaVersion: value.SchemaVersion,
		Agent: agentInputResponse{
			AgentID: value.Agent.AgentID, VersionNumber: value.Agent.VersionNumber,
		},
		Profile: profileInputResponse{
			ProfileID: value.Profile.ProfileID, RevisionNumber: value.Profile.RevisionNumber,
		},
	}
}

type validationDiagnosticResponse struct {
	Code     string  `json:"code"`
	Severity string  `json:"severity"`
	Source   string  `json:"source"`
	Path     string  `json:"path"`
	Category *string `json:"category"`
	Name     *string `json:"name"`
	NodeID   *string `json:"node_id"`
	Message  string  `json:"message"`
}

type deploymentValidationReportResponse struct {
	Valid                  bool                           `json:"valid"`
	CompilerVersion        string                         `json:"compiler_version"`
	PlatformContractDigest string                         `json:"platform_contract_digest"`
	Diagnostics            []validationDiagnosticResponse `json:"diagnostics"`
}

func validationReportView(report domain.ValidationReport) deploymentValidationReportResponse {
	diagnostics := make([]validationDiagnosticResponse, 0, len(report.Diagnostics))
	for _, diagnostic := range report.Diagnostics {
		diagnostics = append(diagnostics, validationDiagnosticResponse{
			Code: diagnostic.Code, Severity: string(diagnostic.Severity),
			Source: string(diagnostic.Source), Path: diagnostic.Path,
			Category: diagnostic.Category, Name: diagnostic.Name, NodeID: diagnostic.NodeID,
			Message: diagnostic.Message,
		})
	}
	return deploymentValidationReportResponse{
		Valid: report.Valid, CompilerVersion: report.CompilerVersion,
		PlatformContractDigest: report.PlatformContractDigest,
		Diagnostics:            diagnostics,
	}
}

type deploymentRevisionSummaryResponse struct {
	ID                    string    `json:"id"`
	TenantID              string    `json:"tenant_id"`
	DeploymentID          string    `json:"deployment_id"`
	RevisionNumber        int64     `json:"revision_number"`
	SchemaVersion         string    `json:"schema_version"`
	AgentID               string    `json:"agent_id"`
	AgentVersionNumber    int64     `json:"agent_version_number"`
	ProfileID             string    `json:"profile_id"`
	ProfileRevisionNumber int64     `json:"profile_revision_number"`
	InputDigest           string    `json:"input_digest"`
	ManifestID            string    `json:"manifest_id"`
	ManifestDigest        string    `json:"manifest_digest"`
	PublishedBy           string    `json:"published_by"`
	PublishedAt           time.Time `json:"published_at"`
}

func revisionSummaryView(value domain.DeploymentRevisionSummary) deploymentRevisionSummaryResponse {
	return deploymentRevisionSummaryResponse{
		ID: value.ID, TenantID: value.TenantID, DeploymentID: value.DeploymentID,
		RevisionNumber: value.RevisionNumber, SchemaVersion: value.SchemaVersion,
		AgentID: value.AgentID, AgentVersionNumber: value.AgentVersionNumber,
		ProfileID: value.ProfileID, ProfileRevisionNumber: value.ProfileRevisionNumber,
		InputDigest: value.InputDigest, ManifestID: value.ManifestID,
		ManifestDigest: value.ManifestDigest, PublishedBy: value.PublishedBy,
		PublishedAt: value.PublishedAt,
	}
}

type deploymentRevisionResponse struct {
	ID                    string                  `json:"id"`
	TenantID              string                  `json:"tenant_id"`
	DeploymentID          string                  `json:"deployment_id"`
	RevisionNumber        int64                   `json:"revision_number"`
	SchemaVersion         string                  `json:"schema_version"`
	AgentID               string                  `json:"agent_id"`
	AgentVersionNumber    int64                   `json:"agent_version_number"`
	ProfileID             string                  `json:"profile_id"`
	ProfileRevisionNumber int64                   `json:"profile_revision_number"`
	Input                 deploymentInputResponse `json:"input"`
	InputDigest           string                  `json:"input_digest"`
	ManifestID            string                  `json:"manifest_id"`
	ManifestDigest        string                  `json:"manifest_digest"`
	ManifestView          json.RawMessage         `json:"manifest_view"`
	PublishedBy           string                  `json:"published_by"`
	PublishedAt           time.Time               `json:"published_at"`
}

func publishedRevisionView(value domain.PublishedRevision) (deploymentRevisionResponse, error) {
	manifestView, err := safeManifestView(value.ManifestView)
	if err != nil {
		return deploymentRevisionResponse{}, err
	}
	revision := value.Revision
	return deploymentRevisionResponse{
		ID: revision.ID, TenantID: revision.TenantID, DeploymentID: revision.DeploymentID,
		RevisionNumber: revision.RevisionNumber, SchemaVersion: revision.SchemaVersion,
		AgentID:               revision.Input.Agent.AgentID,
		AgentVersionNumber:    revision.Input.Agent.VersionNumber,
		ProfileID:             revision.Input.Profile.ProfileID,
		ProfileRevisionNumber: revision.Input.Profile.RevisionNumber,
		Input:                 deploymentInputView(revision.Input), InputDigest: revision.InputDigest,
		ManifestID: value.ManifestID, ManifestDigest: value.ManifestDigest,
		ManifestView: manifestView, PublishedBy: revision.PublishedBy,
		PublishedAt: revision.PublishedAt,
	}, nil
}

type deploymentRevisionPageResponse struct {
	Revisions []deploymentRevisionSummaryResponse `json:"revisions"`
	Total     int                                 `json:"total"`
	Offset    int                                 `json:"offset"`
	Limit     int                                 `json:"limit"`
}

type publishDeploymentRevisionResponse struct {
	Revision   deploymentRevisionResponse         `json:"revision"`
	Validation deploymentValidationReportResponse `json:"validation"`
}

type backendMigrationResponse struct {
	MemoryScopesCopied int                               `json:"memory_scopes_copied"`
	Publication        publishDeploymentRevisionResponse `json:"publication"`
}

type errorResponse struct {
	Error      errorBody                           `json:"error"`
	Validation *deploymentValidationReportResponse `json:"validation,omitempty"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeError(c *gin.Context, status int, code, message string) {
	c.JSON(status, errorResponse{Error: errorBody{Code: code, Message: message}})
}

func writeValidationError(c *gin.Context, report domain.ValidationReport) {
	view := validationReportView(report)
	c.JSON(http.StatusUnprocessableEntity, errorResponse{
		Error: errorBody{
			Code: "DEPLOYMENT_REVISION_INVALID", Message: "deployment revision validation failed",
		},
		Validation: &view,
	})
}

func writeRequestError(c *gin.Context, err error) {
	if errors.Is(err, errRequestTooLarge) {
		writeError(c, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "request body exceeds 32 KiB")
		return
	}
	writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "request is invalid")
}

func usableIdentity(c *gin.Context) (identityapp.IdentityContext, bool) {
	identity, ok := identityapp.IdentityFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "authentication required")
		return identityapp.IdentityContext{}, false
	}
	if identity.Restricted {
		writeError(c, http.StatusForbidden, "PASSWORD_CHANGE_REQUIRED", "password change is required")
		return identityapp.IdentityContext{}, false
	}
	return identity, true
}

func handleApplicationError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, application.ErrInvalidDeployment),
		errors.Is(err, application.ErrInvalidDeploymentInput):
		writeError(c, http.StatusBadRequest, "INVALID_DEPLOYMENT_REQUEST", "deployment request is invalid")
	case errors.Is(err, application.ErrDeploymentNotFound):
		writeError(c, http.StatusNotFound, "DEPLOYMENT_NOT_FOUND", "deployment was not found")
	case errors.Is(err, application.ErrDeploymentRevisionNotFound):
		writeError(c, http.StatusNotFound, "DEPLOYMENT_REVISION_NOT_FOUND", "deployment revision was not found")
	case errors.Is(err, application.ErrAgentVersionNotFound):
		writeError(c, http.StatusNotFound, "AGENT_VERSION_NOT_FOUND", "agent version was not found")
	case errors.Is(err, application.ErrProfileRevisionNotFound):
		writeError(c, http.StatusNotFound, "RUNTIME_PROFILE_REVISION_NOT_FOUND", "runtime profile revision was not found")
	case errors.Is(err, application.ErrTenantForbidden):
		writeError(c, http.StatusForbidden, "TENANT_FORBIDDEN", "tenant access is forbidden")
	case errors.Is(err, application.ErrMetadataRevisionConflict):
		writeError(c, http.StatusConflict, "DEPLOYMENT_METADATA_REVISION_CONFLICT", "metadata revision is stale")
	case errors.Is(err, application.ErrLatestRevisionConflict):
		writeError(c, http.StatusConflict, "DEPLOYMENT_LATEST_REVISION_CONFLICT", "latest revision is stale")
	case errors.Is(err, application.ErrIdempotencyConflict):
		writeError(c, http.StatusConflict, "IDEMPOTENCY_CONFLICT", "idempotency key was used for a different request")
	case errors.Is(err, application.ErrCredentialDependencyUnavailable):
		writeError(c, http.StatusServiceUnavailable, "DEPENDENCY_UNAVAILABLE", "required credential metadata is unavailable")
	case errors.Is(err, application.ErrBackendMigrationBusy):
		writeError(c, http.StatusConflict, "BACKEND_MIGRATION_BUSY", "source deployment still has active work")
	case errors.Is(err, application.ErrBackendMigrationInvalid):
		writeError(c, http.StatusBadRequest, "BACKEND_MIGRATION_INVALID", "backend migration request is invalid")
	case errors.Is(err, application.ErrBackendMigrationUnavailable):
		writeError(c, http.StatusServiceUnavailable, "BACKEND_MIGRATION_UNAVAILABLE", "backend migration worker is unavailable")
	case errors.Is(err, application.ErrBackendMigrationFailed):
		writeError(c, http.StatusConflict, "BACKEND_MIGRATION_FAILED", "backend migration could not be verified")
	default:
		writeError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "request could not be completed")
	}
}

var (
	manifestViewOnce     sync.Once
	compiledManifestView *jsonschema.Schema
	manifestViewError    error
)

func safeManifestView(raw json.RawMessage) (json.RawMessage, error) {
	manifestViewOnce.Do(func() {
		document, err := jsonschema.UnmarshalJSON(bytes.NewReader(deploymentv1.ManifestViewSchema))
		if err != nil {
			manifestViewError = err
			return
		}
		compiler := jsonschema.NewCompiler()
		if err := compiler.AddResource(manifestViewSchemaLocation, document); err != nil {
			manifestViewError = err
			return
		}
		compiledManifestView, manifestViewError = compiler.Compile(manifestViewSchemaLocation)
	})
	if manifestViewError != nil || compiledManifestView == nil || len(raw) == 0 {
		return nil, errors.New("invalid public manifest view")
	}
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil || compiledManifestView.Validate(instance) != nil || containsForbiddenManifestField(instance) {
		return nil, errors.New("invalid public manifest view")
	}
	return append(json.RawMessage(nil), raw...), nil
}

func containsForbiddenManifestField(value any) bool {
	forbidden := map[string]bool{
		"association_token":   true,
		"audience_digest":     true,
		"ciphertext":          true,
		"configured":          true,
		"credential":          true,
		"credential_id":       true,
		"credential_revision": true,
		"nonce":               true,
		"password":            true,
		"purpose":             true,
		"status":              true,
		"value":               true,
	}
	var visit func(any) bool
	visit = func(current any) bool {
		switch typed := current.(type) {
		case map[string]any:
			for name, child := range typed {
				if forbidden[name] || strings.HasSuffix(name, "_credential_id") || visit(child) {
					return true
				}
			}
		case []any:
			for _, child := range typed {
				if visit(child) {
					return true
				}
			}
		}
		return false
	}
	return visit(value)
}
