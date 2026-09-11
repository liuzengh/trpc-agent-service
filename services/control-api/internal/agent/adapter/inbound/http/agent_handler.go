package httpadapter

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/application"
)

func (h *Handler) createAgent(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	var request createAgentRequest
	if err := decodeStrictJSON(c, &request, 32*1024); err != nil || strings.TrimSpace(request.Name) == "" {
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "name and description must be valid")
		return
	}
	result, err := h.service.CreateAgent(c.Request.Context(), application.CreateAgentCommand{
		TenantID: c.Param("tenant_id"), ActorUserID: identity.UserID,
		Name: request.Name, Description: request.Description,
	})
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	c.JSON(http.StatusCreated, gin.H{"agent": agentView(result.Agent), "draft": draftView(result.Draft)})
}

func (h *Handler) getAgent(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	agent, err := h.service.GetAgent(
		c.Request.Context(), c.Param("tenant_id"), c.Param("agent_id"), identity.UserID,
	)
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	c.JSON(http.StatusOK, agentView(agent))
}

func (h *Handler) listAgents(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	page, err := parsePage(c)
	if err != nil {
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "pagination is invalid")
		return
	}
	result, err := h.service.ListAgents(
		c.Request.Context(), c.Param("tenant_id"), identity.UserID, page,
	)
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	items := make([]agentResponse, 0, len(result.Agents))
	for _, agent := range result.Agents {
		items = append(items, agentView(agent))
	}
	c.JSON(http.StatusOK, gin.H{
		"agents": items, "total": result.Total,
		"offset": page.Offset, "limit": page.Limit,
	})
}

func (h *Handler) updateAgent(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	var request updateAgentRequest
	if err := decodeStrictJSON(c, &request, 32*1024); err != nil ||
		(request.Name == nil && request.Description == nil) {
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "name or description is required")
		return
	}
	agent, err := h.service.UpdateAgent(c.Request.Context(), application.UpdateAgentCommand{
		TenantID: c.Param("tenant_id"), AgentID: c.Param("agent_id"),
		ActorUserID: identity.UserID, Name: request.Name, Description: request.Description,
	})
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	c.JSON(http.StatusOK, agentView(agent))
}
