package httpadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	proof "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	"io"
	"net/http"
)

var ErrArtifactMissing = errors.New("artifact not found")
var ErrArtifactInvalid = errors.New("invalid artifact request")
var ErrArtifactCapacity = errors.New("artifact too large")

type ArtifactOperator interface {
	Artifact(context.Context, proof.ArtifactRequest) (proof.ArtifactResponse, error)
}

func (h *Handler) artifact(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r, h.control) {
		return
	}
	ctx, cancel, ok := h.bounded(w, r)
	if !ok {
		return
	}
	defer cancel()
	raw, ok := h.read(w, r, proof.MaxArtifactRequestBytes)
	if !ok {
		return
	}
	defer clear(raw)
	var req proof.ArtifactRequest
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&req) != nil || !errors.Is(d.Decode(new(any)), io.EOF) {
		respondError(w, 400, "ARTIFACT_INVALID")
		return
	}
	ctx, span := telemetrytrace.Resume(h.tracer, ctx, tracecontext.FromHeaders(r.Header), "worker.artifact.http")
	result, err := h.artifacts.Artifact(ctx, req)
	telemetrytrace.End(span, err)
	if err != nil {
		switch {
		case errors.Is(err, ErrArtifactMissing):
			respondError(w, 404, "ARTIFACT_NOT_FOUND")
		case errors.Is(err, ErrArtifactInvalid):
			respondError(w, 400, "ARTIFACT_INVALID")
		case errors.Is(err, ErrArtifactCapacity):
			respondError(w, 413, "ARTIFACT_TOO_LARGE")
		case errors.Is(err, ErrAttemptDenied):
			respondError(w, 403, "ARTIFACT_DENIED")
		default:
			respondError(w, 503, "ARTIFACT_UNAVAILABLE")
		}
		return
	}
	if ctx.Err() != nil {
		respondError(w, 503, "ARTIFACT_UNAVAILABLE")
		return
	}
	_ = json.NewEncoder(w).Encode(result)
}
