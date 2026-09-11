package httpadapter

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
)

func (h *Handler) validateDeploymentRevision(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	if !validRouteIDs(c) {
		writeRequestError(c, errInvalidRequestBody)
		return
	}
	input, err := decodeDeploymentInputRequest(c)
	if err != nil {
		writeRequestError(c, err)
		return
	}
	report, err := h.service.ValidateDeploymentRevision(c.Request.Context(), application.ValidateDeploymentCommand{
		TenantID: c.Param("tenant_id"), DeploymentID: c.Param("deployment_id"),
		ActorUserID: identity.UserID, Input: input,
	})
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	c.JSON(http.StatusOK, validationReportView(report))
}

func (h *Handler) publishDeploymentRevision(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	key, validKey := idempotencyKey(c)
	if !validRouteIDs(c) || !validKey {
		writeRequestError(c, errInvalidRequestBody)
		return
	}
	request, err := decodePublishDeploymentRequest(c)
	if err != nil {
		writeRequestError(c, err)
		return
	}
	result, err := h.service.PublishDeploymentRevision(c.Request.Context(), application.PublishDeploymentCommand{
		TenantID: c.Param("tenant_id"), DeploymentID: c.Param("deployment_id"),
		ActorUserID: identity.UserID, IdempotencyKey: key,
		ExpectedLatestRevisionNumber: request.ExpectedLatestRevisionNumber,
		Input:                        request.Input,
	})
	if errors.Is(err, application.ErrDeploymentRevisionInvalid) {
		writeValidationError(c, result.Validation)
		return
	}
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	revision, err := publishedRevisionView(result.Published)
	if err != nil {
		handleApplicationError(c, application.ErrPublicationIntegrity)
		return
	}
	status := http.StatusOK
	if result.Created {
		status = http.StatusCreated
	}
	c.JSON(status, publishDeploymentRevisionResponse{
		Revision: revision, Validation: validationReportView(result.Validation),
	})
}

func (h *Handler) migrateAndPublish(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	key, validKey := idempotencyKey(c)
	if !validRouteIDs(c) || !validKey {
		writeRequestError(c, errInvalidRequestBody)
		return
	}
	request, err := decodeMigrateAndPublishRequest(c)
	if err != nil {
		writeRequestError(c, err)
		return
	}
	result, err := h.service.MigrateAndPublish(c.Request.Context(), application.MigrateAndPublishCommand{
		TenantID: c.Param("tenant_id"), DeploymentID: c.Param("deployment_id"), ActorUserID: identity.UserID,
		IdempotencyKey: key, SourceRevisionNumber: request.SourceRevisionNumber,
		ExpectedLatestRevisionNumber: request.ExpectedLatestRevisionNumber, Input: request.Input,
	})
	if errors.Is(err, application.ErrDeploymentRevisionInvalid) {
		writeValidationError(c, result.Publication.Validation)
		return
	}
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	revision, err := publishedRevisionView(result.Publication.Published)
	if err != nil {
		handleApplicationError(c, application.ErrPublicationIntegrity)
		return
	}
	status := http.StatusOK
	if result.Publication.Created {
		status = http.StatusCreated
	}
	c.JSON(status, backendMigrationResponse{MemoryScopesCopied: result.MemoryScopesCopied, Publication: publishDeploymentRevisionResponse{Revision: revision, Validation: validationReportView(result.Publication.Validation)}})
}

func (h *Handler) listDeploymentRevisions(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	if !validRouteIDs(c) {
		writeRequestError(c, errInvalidRequestBody)
		return
	}
	page, err := parsePage(c)
	if err != nil {
		writeRequestError(c, err)
		return
	}
	result, err := h.service.ListDeploymentRevisions(
		c.Request.Context(), c.Param("tenant_id"), c.Param("deployment_id"),
		identity.UserID, page,
	)
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	items := make([]deploymentRevisionSummaryResponse, 0, len(result.Revisions))
	for _, revision := range result.Revisions {
		items = append(items, revisionSummaryView(revision))
	}
	c.JSON(http.StatusOK, deploymentRevisionPageResponse{
		Revisions: items, Total: result.Total, Offset: page.Offset, Limit: page.Limit,
	})
}

func (h *Handler) getDeploymentRevision(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	if !validRouteIDs(c) {
		writeRequestError(c, errInvalidRequestBody)
		return
	}
	number, err := parseRevisionNumber(c)
	if err != nil {
		writeRequestError(c, err)
		return
	}
	result, err := h.service.GetDeploymentRevision(
		c.Request.Context(), c.Param("tenant_id"), c.Param("deployment_id"),
		identity.UserID, number,
	)
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	view, err := publishedRevisionView(result)
	if err != nil {
		handleApplicationError(c, application.ErrPublicationIntegrity)
		return
	}
	c.JSON(http.StatusOK, view)
}
