package web

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/asset"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/channels"
)

// channelAgentSource is the narrow slice of the agent manager ChannelAPI needs
// to keep a binding inside its tenant. May be nil (the check is then skipped).
type channelAgentSource interface {
	Get(ctx context.Context, id string) (*agent.Agent, error)
}

// ChannelAPI manages IM channel bindings (wecom/feishu account -> tenant +
// agent). credential_ref and verification_token_ref are secret-store
// references, never plaintext credentials.
type ChannelAPI struct {
	store   channels.BindingStore
	mgr     *channels.Manager // optional: reconciled on binding create/delete
	agents  channelAgentSource
	auditor assetAuditor // optional: asset changes are audited
}

// NewChannelAPI returns a channel-binding management API.
func NewChannelAPI(store channels.BindingStore) *ChannelAPI {
	return &ChannelAPI{store: store}
}

// SetAuditor wires asset-change auditing. May be nil.
func (a *ChannelAPI) SetAuditor(rec assetAuditor) { a.auditor = rec }

// SetAgentSource wires the agent manager so a binding can only target an agent
// of the binding's own tenant. May be nil (the check is then skipped).
func (a *ChannelAPI) SetAgentSource(src channelAgentSource) { a.agents = src }

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
	claims := GetClaims(r.Context())
	// A binding belongs to a tenant like any other asset: only the platform
	// owner may look across tenants.
	tenantID := ScopeTenant(claims, q.Get("tenant_id"))
	items, err := a.store.List(r.Context(), tenantID, q.Get("channel"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	// The store filters by tenant; whether a binding is shared is a row rule.
	// (A binding holds a credential reference, so an unshared one stays private
	// to its author and the tenant's managers rather than leaking the account.)
	visible := make([]channels.ChannelBinding, 0, len(items))
	for _, b := range items {
		if CanReadAsset(claims, b.CreatedBy, b.Visibility) {
			visible = append(visible, b)
		}
	}
	writeJSON(w, http.StatusOK, visible)
}

// channelInput is the POST /channels body (binding id assigned server-side).
type channelInput struct {
	TenantID             string `json:"tenant_id"`
	AgentID              string `json:"agent_id"`
	Channel              string `json:"channel"`
	AccountID            string `json:"account_id"`
	CredentialRef        string `json:"credential_ref,omitempty"`
	VerificationTokenRef string `json:"verification_token_ref,omitempty"`
	Visibility           string `json:"visibility,omitempty"`
}

func (a *ChannelAPI) create(w http.ResponseWriter, r *http.Request) {
	var in channelInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	claims := GetClaims(r.Context())
	// A binding is a tenant asset: only the platform owner may bind an account
	// into another tenant, and the author is the authenticated member.
	in.TenantID = ClaimTenant(claims, in.TenantID)
	if in.TenantID == "" || in.AgentID == "" || in.AccountID == "" {
		writeError(w, http.StatusBadRequest, errors.New("tenant_id, agent_id and account_id are required"))
		return
	}
	in.Channel = strings.ToLower(strings.TrimSpace(in.Channel))
	if in.Channel != channels.ChannelWeCom && in.Channel != channels.ChannelFeishu {
		writeError(w, http.StatusBadRequest, errors.New("channel must be wecom or feishu"))
		return
	}
	// The bound agent must live in the same tenant: otherwise inbound IM traffic
	// would be routed into another tenant's agent.
	if a.agents != nil {
		ag, err := a.agents.Get(r.Context(), in.AgentID)
		if err != nil {
			writeError(w, http.StatusBadRequest, errors.New("agent_id does not exist"))
			return
		}
		if ag.TenantID != in.TenantID {
			writeError(w, http.StatusBadRequest, errors.New("agent_id belongs to another tenant"))
			return
		}
	}
	createdBy := ""
	if claims != nil {
		createdBy = claims.UserID
	}
	bindingID := uuid.NewString()
	err := a.store.Create(r.Context(), channels.ChannelBinding{
		BindingID:            bindingID,
		TenantID:             in.TenantID,
		AgentID:              in.AgentID,
		Channel:              in.Channel,
		AccountID:            in.AccountID,
		CredentialRef:        in.CredentialRef,
		VerificationTokenRef: in.VerificationTokenRef,
		CreatedBy:            createdBy,
		Visibility:           asset.VisibilityOrDefault(in.Visibility),
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
	recordAssetAllowed(claims, a.auditor, assetKindBinding, bindingID, in.TenantID)
	if a.mgr != nil {
		_ = a.mgr.Reload(r.Context())
	}
	w.WriteHeader(http.StatusCreated)
}

func (a *ChannelAPI) delete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	b, err := a.store.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, channels.ErrBindingNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	claims := GetClaims(r.Context())
	if !TenantAccessible(claims, b.TenantID) {
		WriteCrossTenant(w)
		return
	}
	// A shared binding is tenant-readable but only its author (or a tenant
	// manager) may unbind it.
	if !CanManageAsset(claims, b.CreatedBy) {
		WriteAssetDenied(claims, w, a.auditor, assetKindBinding, id, b.TenantID, "shared but not authored")
		return
	}
	if err := a.store.Delete(r.Context(), id); err != nil {
		if errors.Is(err, channels.ErrBindingNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	recordAssetAllowed(claims, a.auditor, assetKindBinding, id, b.TenantID)
	if a.mgr != nil {
		_ = a.mgr.Reload(r.Context())
	}
	w.WriteHeader(http.StatusNoContent)
}
