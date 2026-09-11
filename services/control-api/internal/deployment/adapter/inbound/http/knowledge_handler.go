package httpadapter

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/gowebpki/jcs"
	executionv1 "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
	"io"
	"net/http"
)

type knowledgeService interface {
	ImportKnowledge(context.Context, application.KnowledgeImportCommand) (application.KnowledgeResult, error)
}

func (h *Handler) importKnowledge(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	c.Header("Cache-Control", "no-store")
	if !validRouteIDs(c) || len(c.Request.URL.Query()) > 0 {
		writeRequestError(c, errInvalidRequestBody)
		return
	}
	revision, err := parseRevisionNumber(c)
	if err != nil {
		writeRequestError(c, err)
		return
	}
	raw, err := io.ReadAll(io.LimitReader(c.Request.Body, executionv1.MaxKnowledgeRequestBytes+1))
	if err != nil {
		writeRequestError(c, errInvalidRequestBody)
		return
	}
	defer clear(raw)
	if len(raw) > executionv1.MaxKnowledgeRequestBytes {
		writeKnowledgeError(c, application.ErrKnowledgeTooLarge)
		return
	}
	normalized, err := jcs.Transform(raw)
	if err != nil {
		writeRequestError(c, errInvalidRequestBody)
		return
	}
	defer clear(normalized)
	var fields map[string]json.RawMessage
	if json.Unmarshal(normalized, &fields) != nil || len(fields) != 2 || fields["name"] == nil || fields["text"] == nil {
		writeRequestError(c, errInvalidRequestBody)
		return
	}
	var body struct {
		Name string `json:"name"`
		Text string `json:"text"`
	}
	if json.Unmarshal(normalized, &body) != nil {
		writeRequestError(c, errInvalidRequestBody)
		return
	}
	service, ok := h.service.(knowledgeService)
	if !ok {
		writeKnowledgeError(c, application.ErrKnowledgeUnavailable)
		return
	}
	out, err := service.ImportKnowledge(c.Request.Context(), application.KnowledgeImportCommand{TenantID: c.Param("tenant_id"), DeploymentID: c.Param("deployment_id"), ActorUserID: identity.UserID, RevisionNumber: revision, Resource: c.Param("resource"), Name: body.Name, Text: body.Text})
	if err != nil {
		writeKnowledgeError(c, err)
		return
	}
	c.JSON(http.StatusOK, out)
}
func writeKnowledgeError(c *gin.Context, err error) {
	status := 503
	code := "KNOWLEDGE_UNAVAILABLE"
	switch {
	case errors.Is(err, application.ErrKnowledgeInvalid):
		status = 400
		code = "KNOWLEDGE_INVALID"
	case errors.Is(err, application.ErrKnowledgeForbidden), errors.Is(err, application.ErrTenantForbidden):
		status = 403
		code = "KNOWLEDGE_FORBIDDEN"
	case errors.Is(err, application.ErrKnowledgeTooLarge):
		status = 413
		code = "KNOWLEDGE_TOO_LARGE"
	case errors.Is(err, application.ErrDeploymentNotFound), errors.Is(err, application.ErrDeploymentRevisionNotFound):
		status = 404
		code = "DEPLOYMENT_REVISION_NOT_FOUND"
	}
	c.JSON(status, gin.H{"code": code})
}
