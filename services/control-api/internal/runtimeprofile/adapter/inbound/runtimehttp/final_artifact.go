package runtimehttp

import (
	"context"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"net/http"
)

const FinalArtifactResolvePath = "/internal/v1/runtime-profiles/credentials/resolve-final-artifact"

type finalArtifactResolver interface {
	ResolveForFinalArtifact(context.Context, string, application.ResolveFinalArtifactInput) (application.CredentialBatch, error)
}

func (h *Handler) resolveFinalArtifact(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	worker, ok := workerIdentity(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "WORKER_UNAUTHENTICATED", "worker authentication required")
		return
	}
	b, e := readBody(c)
	if e != nil {
		handleError(c, e)
		return
	}
	defer clear(b)
	in, e := application.DecodeResolveFinalArtifact(b)
	if e != nil {
		handleError(c, e)
		return
	}
	resolver, ok := h.resolver.(finalArtifactResolver)
	if !ok {
		writeError(c, http.StatusServiceUnavailable, "EXECUTION_DEPENDENCY_UNAVAILABLE", "final artifact authorization unavailable")
		return
	}
	batch, e := resolver.ResolveForFinalArtifact(c.Request.Context(), worker, in)
	defer batch.Clear()
	if e != nil {
		handleError(c, e)
		return
	}
	encoded, e := json.Marshal(batchResponse(batch))
	if e != nil {
		writeError(c, 500, "RESOLUTION_FAILED", "credential resolution failed")
		return
	}
	defer clear(encoded)
	c.Data(200, "application/json; charset=utf-8", encoded)
}
