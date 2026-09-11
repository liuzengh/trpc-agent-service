package httpadapter

import (
	"context"
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
	"io"
	"mime"
	"net/http"
	"strconv"
)

type artifactService interface {
	AccessArtifact(context.Context, application.ArtifactCommand) (application.ArtifactResult, error)
}

func (h *Handler) artifact(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	if !validRouteIDs(c) {
		writeRequestError(c, errInvalidRequestBody)
		return
	}
	n, err := parseRevisionNumber(c)
	if err != nil {
		writeRequestError(c, err)
		return
	}
	c.Header("Cache-Control", "no-store")
	c.Header("X-Content-Type-Options", "nosniff")
	command := application.ArtifactCommand{TenantID: c.Param("tenant_id"), DeploymentID: c.Param("deployment_id"), ActorUserID: identity.UserID, RevisionNumber: n, Name: c.Param("filename"), RunID: c.Query("run_id"), Operation: "load"}
	q := c.Request.URL.Query()
	for k, values := range q {
		if (k != "run_id" && k != "version") || len(values) != 1 {
			writeRequestError(c, errInvalidRequestBody)
			return
		}
	}
	if v, exists := q["version"]; exists {
		num, e := strconv.Atoi(v[0])
		if e != nil || num < 0 {
			writeRequestError(c, errInvalidRequestBody)
			return
		}
		command.Version = &num
	}
	if c.Request.Method == http.MethodPut {
		command.Operation = "save"
		if command.Version != nil {
			writeRequestError(c, errInvalidRequestBody)
			return
		}
		command.MIMEType = c.GetHeader("Content-Type")
		if command.MIMEType == "" {
			command.MIMEType = "application/octet-stream"
		}
		data, e := io.ReadAll(io.LimitReader(c.Request.Body, application.MaxArtifactContentBytes+1))
		if e != nil {
			writeRequestError(c, errInvalidRequestBody)
			return
		}
		defer clear(data)
		if len(data) > application.MaxArtifactContentBytes {
			writeArtifactError(c, application.ErrArtifactTooLarge)
			return
		}
		command.Content = data
	}
	service, ok := h.service.(artifactService)
	if !ok {
		writeArtifactError(c, application.ErrArtifactUnavailable)
		return
	}
	result, err := service.AccessArtifact(c.Request.Context(), command)
	if err != nil {
		writeArtifactError(c, err)
		return
	}
	if command.Operation == "save" {
		result.Content = nil
		c.JSON(http.StatusOK, result)
		return
	}
	defer clear(result.Content)
	c.Header("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": result.Name}))
	c.Header("X-Artifact-Version", strconv.Itoa(result.Version))
	c.Data(http.StatusOK, result.MimeType, result.Content)
}
func writeArtifactError(c *gin.Context, err error) {
	status := 503
	code := "ARTIFACT_UNAVAILABLE"
	switch {
	case errors.Is(err, application.ErrArtifactInvalid):
		status = 400
		code = "ARTIFACT_INVALID"
	case errors.Is(err, application.ErrArtifactForbidden), errors.Is(err, application.ErrTenantForbidden):
		status = 403
		code = "ARTIFACT_FORBIDDEN"
	case errors.Is(err, application.ErrArtifactNotFound):
		status = 404
		code = "ARTIFACT_NOT_FOUND"
	case errors.Is(err, application.ErrArtifactTooLarge):
		status = 413
		code = "ARTIFACT_TOO_LARGE"
	case errors.Is(err, application.ErrDeploymentNotFound), errors.Is(err, application.ErrDeploymentRevisionNotFound):
		status = 404
		code = "DEPLOYMENT_REVISION_NOT_FOUND"
	}
	c.JSON(status, gin.H{"code": code})
}
