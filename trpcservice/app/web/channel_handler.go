package web

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/channels"
)

// ChannelAPI manages IM channel bindings (wecom/feishu account -> tenant +
// agent). credential_ref and verification_token_ref are secret-store
// references, never plaintext credentials.
type ChannelAPI struct {
	store channels.BindingStore
	mgr   *channels.Manager // optional: reconciled on binding create/delete
}

// NewChannelAPI returns a channel-binding management API.
func NewChannelAPI(store channels.BindingStore) *ChannelAPI {
	return &ChannelAPI{store: store}
}

// SetManager wires a binding-driven connection manager so binding changes
// immediately reconcile the live IM adapters. May be nil (bindings are then
// only persisted, connections are reconciled on the next startup).
func (a *ChannelAPI) SetManager(m *channels.Manager) { a.mgr = m }

// Register mounts channel routes.
func (a *ChannelAPI) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /channels", a.list)
	mux.HandleFunc("POST /channels", a.create)
	mux.HandleFunc("DELETE /channels/{id}", a.delete)
}

func (a *ChannelAPI) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	items, err := a.store.List(r.Context(), q.Get("tenant_id"), q.Get("channel"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, items)
}

// channelInput is the POST /channels body (binding id assigned server-side).
type channelInput struct {
	TenantID             string `json:"tenant_id"`
	AgentID              string `json:"agent_id"`
	Channel              string `json:"channel"`
	AccountID            string `json:"account_id"`
	CredentialRef        string `json:"credential_ref,omitempty"`
	VerificationTokenRef string `json:"verification_token_ref,omitempty"`
}

func (a *ChannelAPI) create(w http.ResponseWriter, r *http.Request) {
	var in channelInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if in.TenantID == "" || in.AgentID == "" || in.AccountID == "" {
		writeError(w, http.StatusBadRequest, errors.New("tenant_id, agent_id and account_id are required"))
		return
	}
	in.Channel = strings.ToLower(strings.TrimSpace(in.Channel))
	if in.Channel != channels.ChannelWeCom && in.Channel != channels.ChannelFeishu {
		writeError(w, http.StatusBadRequest, errors.New("channel must be wecom or feishu"))
		return
	}
	err := a.store.Create(r.Context(), channels.ChannelBinding{
		BindingID:            uuid.NewString(),
		TenantID:             in.TenantID,
		AgentID:              in.AgentID,
		Channel:              in.Channel,
		AccountID:            in.AccountID,
		CredentialRef:        in.CredentialRef,
		VerificationTokenRef: in.VerificationTokenRef,
		CreatedAt:            time.Now().UTC(),
	})
	if err != nil {
		if errors.Is(err, channels.ErrBindingDuplicate) {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if a.mgr != nil {
		_ = a.mgr.Reload(r.Context())
	}
	w.WriteHeader(http.StatusCreated)
}

func (a *ChannelAPI) delete(w http.ResponseWriter, r *http.Request) {
	if err := a.store.Delete(r.Context(), r.PathValue("id")); err != nil {
		if errors.Is(err, channels.ErrBindingNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if a.mgr != nil {
		_ = a.mgr.Reload(r.Context())
	}
	w.WriteHeader(http.StatusNoContent)
}
