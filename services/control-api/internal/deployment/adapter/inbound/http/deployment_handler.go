package httpadapter

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
)

func (h *Handler) createDeployment(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	key, validKey := idempotencyKey(c)
	if !validRouteIDs(c) || !validKey {
		writeRequestError(c, errInvalidRequestBody)
		return
	}
	request, err := decodeCreateDeploymentRequest(c)
	if err != nil {
		writeRequestError(c, err)
		return
	}
	result, err := h.service.CreateDeployment(c.Request.Context(), application.CreateDeploymentCommand{
		TenantID: c.Param("tenant_id"), ActorUserID: identity.UserID,
		IdempotencyKey: key,
		Name:           request.Name, Description: request.Description,
	})
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	status := http.StatusOK
	if result.Created {
		status = http.StatusCreated
	}
	c.JSON(status, createDeploymentResponse{Deployment: deploymentView(result.Deployment)})
}

func (h *Handler) listDeployments(c *gin.Context) {
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
	result, err := h.service.ListDeployments(
		c.Request.Context(), c.Param("tenant_id"), identity.UserID, page,
	)
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	items := make([]deploymentResponse, 0, len(result.Deployments))
	for _, deployment := range result.Deployments {
		items = append(items, deploymentView(deployment))
	}
	c.JSON(http.StatusOK, deploymentPageResponse{
		Deployments: items, Total: result.Total, Offset: page.Offset, Limit: page.Limit,
	})
}

func (h *Handler) getDeployment(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	if !validRouteIDs(c) {
		writeRequestError(c, errInvalidRequestBody)
		return
	}
	result, err := h.service.GetDeployment(
		c.Request.Context(), c.Param("tenant_id"), c.Param("deployment_id"), identity.UserID,
	)
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	c.JSON(http.StatusOK, deploymentView(result))
}

func (h *Handler) updateDeployment(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	if !validRouteIDs(c) {
		writeRequestError(c, errInvalidRequestBody)
		return
	}
	request, err := decodeUpdateDeploymentRequest(c)
	if err != nil {
		writeRequestError(c, err)
		return
	}
	result, err := h.service.UpdateDeploymentMetadata(c.Request.Context(), application.UpdateDeploymentCommand{
		TenantID: c.Param("tenant_id"), DeploymentID: c.Param("deployment_id"),
		ActorUserID: identity.UserID, ExpectedMetadataRevision: request.ExpectedMetadataRevision,
		Name: request.Name, Description: request.Description,
	})
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	c.JSON(http.StatusOK, deploymentView(result))
}
