package web

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

// SecretAPI manages the unified credential store. Values are write-only from
// the client's perspective: PUT accepts a plaintext value, but every read
// endpoint returns metadata only (key + updated_at) — plaintext never leaves
// the store over the API.
type SecretAPI struct {
	store secret.Store
}

// NewSecretAPI returns the credential-management API.
func NewSecretAPI(store secret.Store) *SecretAPI {
	return &SecretAPI{store: store}
}

// Register mounts secret routes.
func (a *SecretAPI) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /secrets", a.put)
	mux.HandleFunc("GET /secrets", a.list)
	mux.HandleFunc("DELETE /secrets/{key}", a.delete)
}

// secretPut is the POST /secrets body.
type secretPut struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (a *SecretAPI) put(w http.ResponseWriter, r *http.Request) {
	var in secretPut
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	in.Key = strings.TrimSpace(in.Key)
	if in.Key == "" {
		writeError(w, http.StatusBadRequest, errors.New("key is required"))
		return
	}
	if in.Value == "" {
		writeError(w, http.StatusBadRequest, errors.New("value is required"))
		return
	}
	if err := a.store.Put(r.Context(), in.Key, in.Value); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (a *SecretAPI) list(w http.ResponseWriter, r *http.Request) {
	items, err := a.store.List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (a *SecretAPI) delete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, errors.New("key is required"))
		return
	}
	if err := a.store.Delete(r.Context(), key); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
