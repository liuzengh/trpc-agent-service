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
	"time"
)

type KnowledgeImporter interface {
	ImportKnowledge(context.Context, proof.KnowledgeRequest) (proof.KnowledgeResponse, error)
}

func (h *Handler) knowledge(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r, h.control) {
		return
	}
	// Synchronous import has its own HTTP transport budget, not the proof query
	// timeout and not a background job. Published backend limits still apply.
	select {
	case h.slots <- struct{}{}:
		defer func() { <-h.slots }()
	default:
		respondError(w, 503, "KNOWLEDGE_UNAVAILABLE")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Minute)
	defer cancel()
	raw, ok := h.read(w, r, proof.MaxKnowledgeRequestBytes)
	if !ok {
		return
	}
	defer clear(raw)
	var req proof.KnowledgeRequest
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&req) != nil || !errors.Is(d.Decode(new(any)), io.EOF) {
		respondError(w, 400, "KNOWLEDGE_INVALID")
		return
	}
	ctx, span := telemetrytrace.Resume(h.tracer, ctx, tracecontext.FromHeaders(r.Header), "worker.knowledge.import")
	result, err := h.knowledgeImporter.ImportKnowledge(ctx, req)
	telemetrytrace.End(span, err)
	if err != nil {
		switch {
		case errors.Is(err, ErrArtifactInvalid):
			respondError(w, 400, "KNOWLEDGE_INVALID")
		case errors.Is(err, ErrArtifactCapacity):
			respondError(w, 413, "KNOWLEDGE_TOO_LARGE")
		case errors.Is(err, ErrAttemptDenied):
			respondError(w, 403, "KNOWLEDGE_DENIED")
		default:
			respondError(w, 503, "KNOWLEDGE_UNAVAILABLE")
		}
		return
	}
	if ctx.Err() != nil {
		respondError(w, 503, "KNOWLEDGE_UNAVAILABLE")
		return
	}
	_ = json.NewEncoder(w).Encode(result)
}
