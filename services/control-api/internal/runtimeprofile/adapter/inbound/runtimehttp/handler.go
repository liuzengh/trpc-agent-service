package runtimehttp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

// Resolver is the sole application operation available to this internal adapter.
type Resolver interface {
	ResolveForAttempt(context.Context, application.ResolveAttemptCommand) (application.CredentialBatch, error)
}

type Handler struct{ resolver Resolver }

func NewHandler(resolver Resolver) *Handler { return &Handler{resolver: resolver} }

func (h *Handler) Register(routes gin.IRoutes) {
	routes.POST("/internal/v1/runtime-profiles/credentials/resolve", h.resolve)
	routes.POST(FinalArtifactResolvePath, h.resolveFinalArtifact)
}

func (h *Handler) resolve(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	workerID, ok := workerIdentity(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "WORKER_UNAUTHENTICATED", "worker authentication required")
		return
	}
	data, err := readBody(c)
	if err != nil {
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "credential resolution request is invalid")
		return
	}
	defer clear(data)
	input, err := application.DecodeResolveAttempt(data)
	if err != nil {
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "credential resolution request is invalid")
		return
	}
	if h.resolver == nil {
		writeError(c, http.StatusInternalServerError, "RESOLUTION_FAILED", "credential resolution failed")
		return
	}
	batch, err := h.resolver.ResolveForAttempt(c.Request.Context(), application.ResolveAttemptCommand{
		Authorization: application.ExecutionAuthorizationRequest{WorkloadIdentity: workerID, ExecutionToken: input.ExecutionToken, ManifestID: input.ManifestID, ManifestDigest: input.ManifestDigest},
		Uses:          input.Uses,
	})
	// Also clear a partially populated batch returned alongside an error. Only a
	// complete successful batch is ever projected into the internal wire response.
	defer batch.Clear()
	if err != nil {
		handleError(c, err)
		return
	}
	encoded, err := json.Marshal(batchResponse(batch))
	if err != nil {
		writeError(c, http.StatusInternalServerError, "RESOLUTION_FAILED", "credential resolution failed")
		return
	}
	defer clear(encoded)
	c.Data(http.StatusOK, "application/json; charset=utf-8", encoded)
}

func readBody(c *gin.Context) ([]byte, error) {
	mediaType, _, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return nil, domain.ErrCredentialInput
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, int64(domain.MaxDocumentBytes))
	data, err := io.ReadAll(c.Request.Body)
	if err != nil {
		clear(data)
		return nil, domain.ErrCredentialInput
	}
	return data, nil
}

func handleError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, application.ErrExecutionDependencyUnavailable):
		writeError(c, http.StatusServiceUnavailable, "EXECUTION_DEPENDENCY_UNAVAILABLE", "execution authorization dependency unavailable")
	case errors.Is(err, application.ErrExecutionUnauthorized):
		writeError(c, http.StatusForbidden, "EXECUTION_UNAUTHORIZED", "execution authorization failed")
	case errors.Is(err, domain.ErrCredentialInput):
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "credential resolution request is invalid")
	case errors.Is(err, domain.ErrCredentialUnavailable), errors.Is(err, domain.ErrCredentialAssociation), errors.Is(err, domain.ErrCredentialConflict), errors.Is(err, domain.ErrCredentialNotFound):
		writeError(c, http.StatusConflict, "CREDENTIAL_UNAVAILABLE", "credential resolution is unavailable")
	default:
		writeError(c, http.StatusInternalServerError, "RESOLUTION_FAILED", "credential resolution failed")
	}
}

func writeError(c *gin.Context, status int, code, message string) {
	c.JSON(status, gin.H{"error": gin.H{"code": code, "message": message}})
}
