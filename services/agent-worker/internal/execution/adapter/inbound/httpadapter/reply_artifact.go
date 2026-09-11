package httpadapter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"mime"
	"net/http"
	"strconv"
	"strings"

	proof "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
)

type ReplyArtifactReader interface {
	ReplyArtifact(context.Context, proof.ReplyArtifactRequest) (proof.ReplyArtifactResponse, error)
}

func (h *Handler) replyArtifact(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r, h.gateway) {
		return
	}
	ctx, cancel, ok := h.bounded(w, r)
	if !ok {
		return
	}
	defer cancel()
	raw, ok := h.read(w, r, proof.MaxReplyArtifactRequestBytes)
	if !ok {
		return
	}
	defer clear(raw)
	req, err := proof.DecodeReplyArtifactRequest(raw)
	if err != nil {
		respondError(w, 400, "ARTIFACT_INVALID")
		return
	}
	result, err := h.replyArtifacts.ReplyArtifact(ctx, req)
	defer clear(result.Content)
	if err != nil || ctx.Err() != nil {
		switch {
		case ctx.Err() != nil:
			respondError(w, 503, "ARTIFACT_UNAVAILABLE")
		case errors.Is(err, ErrArtifactMissing), errors.Is(err, ErrNotYet):
			respondError(w, 404, "ARTIFACT_NOT_FOUND")
		case errors.Is(err, ErrArtifactInvalid):
			respondError(w, 400, "ARTIFACT_INVALID")
		case errors.Is(err, ErrArtifactCapacity):
			respondError(w, 413, "ARTIFACT_TOO_LARGE")
		case errors.Is(err, ErrAttemptDenied), errors.Is(err, ErrFinalMismatch):
			respondError(w, 403, "ARTIFACT_DENIED")
		default:
			respondError(w, 503, "ARTIFACT_UNAVAILABLE")
		}
		return
	}
	digest := sha256.Sum256(result.Content)
	_, _, mediaErr := mime.ParseMediaType(result.MimeType)
	if mediaErr != nil || !strings.Contains(result.MimeType, "/") || len(result.MimeType) > 256 || strings.ContainsAny(result.MimeType, "\r\n\x00") || result.SizeBytes != len(result.Content) || result.SizeBytes > proof.MaxArtifactBytes || result.SHA256 != hex.EncodeToString(digest[:]) {
		respondError(w, 503, "ARTIFACT_UNAVAILABLE")
		return
	}
	// All validation precedes success headers. No partial read is exposed as 200.
	w.Header().Set("Content-Type", result.MimeType)
	w.Header().Set("Content-Length", strconv.Itoa(result.SizeBytes))
	w.Header().Set("X-Content-SHA256", result.SHA256)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(result.Content)
}
