package httpadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	proof "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
)

const maxBackendMigrationRequestBytes = 64 << 10

func (h *Handler) backendMigration(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r, h.control) {
		return
	}
	select {
	case h.slots <- struct{}{}:
		defer func() { <-h.slots }()
	default:
		respondError(w, http.StatusServiceUnavailable, "BACKEND_MIGRATION_UNAVAILABLE")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Minute)
	defer cancel()
	raw, ok := h.read(w, r, maxBackendMigrationRequestBytes)
	if !ok {
		return
	}
	defer clear(raw)
	var request proof.BackendMigrationRequest
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil || !errors.Is(decoder.Decode(new(any)), io.EOF) {
		respondError(w, http.StatusBadRequest, "BACKEND_MIGRATION_INVALID")
		return
	}
	result, err := h.backendMigrations.Execute(ctx, request)
	request.Source.Password, request.Target.Password = "", ""
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			respondError(w, http.StatusServiceUnavailable, "BACKEND_MIGRATION_UNAVAILABLE")
		} else if errors.Is(err, proof.ErrBackendMigrationBusy) {
			respondError(w, http.StatusConflict, "BACKEND_MIGRATION_BUSY")
		} else {
			respondError(w, http.StatusConflict, "BACKEND_MIGRATION_FAILED")
		}
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}
