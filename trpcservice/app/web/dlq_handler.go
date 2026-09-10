package web

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/bus"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/dlqstore"
)

// DLQBus is the slice of the bus the replay path needs.
type DLQBus interface {
	PublishInbound(ctx context.Context, m *bus.Message) error
	// ClearIdem releases the idempotency claim of the replayed envelope so
	// the re-published message is processed instead of dropped as a
	// duplicate of the failed attempts.
	ClearIdem(ctx context.Context, msgKey string) error
}

// DLQAPI exposes the dead-letter queue to the admin front end: list the
// dead letters (newest first) and replay one by re-publishing its original
// envelope onto the inbound stream.
type DLQAPI struct {
	store *dlqstore.Store
	bus   DLQBus
}

// NewDLQAPI returns a DLQ API backed by the given store and bus.
func NewDLQAPI(store *dlqstore.Store, b DLQBus) *DLQAPI {
	return &DLQAPI{store: store, bus: b}
}

// Register mounts DLQ routes on the mux.
func (a *DLQAPI) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /dlq", a.list)
	mux.HandleFunc("POST /dlq/{id}/replay", a.replay)
}

func (a *DLQAPI) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := 100
	if raw := q.Get("limit"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			limit = n
		}
	}
	entries, err := a.store.List(r.Context(), q.Get("tenant_id"), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, entries)
}

func (a *DLQAPI) replay(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("invalid dead letter id"))
		return
	}
	entry, err := a.store.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, dlqstore.ErrNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if entry.ReplayedAt != nil {
		writeError(w, http.StatusConflict, errors.New("dead letter already replayed"))
		return
	}

	var m bus.Message
	if err := json.Unmarshal([]byte(entry.Payload), &m); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	// Release the failed attempts' idempotency claim, then re-publish the
	// original envelope: a consumer picks it up like any new message.
	if err := a.bus.ClearIdem(r.Context(), m.ID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if err := a.bus.PublishInbound(r.Context(), &m); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if err := a.store.MarkReplayed(r.Context(), id); err != nil {
		// The message is already re-published; a failed stamp only means the
		// row may be replayed again (the duplicate is deduped by idem key).
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"id": id, "message_id": m.ID, "status": "replayed"})
}
