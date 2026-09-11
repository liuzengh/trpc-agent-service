package admin

import (
	"errors"
	"net/http"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecommcp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

func (s *Service) WithChannelState(state wecommcp.Store) *Service { s.channelState = state; return s }

func (h *Handler) handleChannelState(w http.ResponseWriter, r *http.Request) {
	var input struct {
		TenantID               string    `json:"tenant_id"`
		BindingID              string    `json:"binding_id"`
		ChatHash               string    `json:"chat_hash"`
		ExpectedVersion        int64     `json:"expected_version"`
		ExpectedBindingVersion int64     `json:"expected_binding_version"`
		Action                 string    `json:"action"`
		From                   time.Time `json:"from"`
		AcknowledgeGap         bool      `json:"acknowledge_gap"`
		Limit                  int       `json:"limit"`
	}
	permission := PermissionRead
	if r.URL.Path == "/admin/channel-checkpoints/recover" {
		permission = PermissionOperate
	}
	if !decodeAdmin(w, r, &input) || !h.require(w, r, input.TenantID, permission) {
		return
	}
	if h.service.channelState == nil || input.BindingID == "" {
		h.writeResult(w, 0, nil, invalidf("channel state unavailable or binding missing"))
		return
	}
	b, err := h.service.repository.GetChannelBinding(r.Context(), input.TenantID, input.BindingID)
	if err != nil {
		h.writeResult(w, 0, nil, err)
		return
	}
	if b.ChannelType != wecommcp.ChannelType {
		h.writeResult(w, 0, nil, invalidf("channel does not use MCP checkpoints"))
		return
	}
	switch r.URL.Path {
	case "/admin/channel-rejections/list":
		items, err := h.service.channelState.ListRejections(r.Context(), input.TenantID, input.BindingID, input.Limit)
		h.writeResult(w, http.StatusOK, items, err)
	case "/admin/channel-checkpoints/list":
		items, err := h.service.channelState.ListCheckpoints(r.Context(), input.TenantID, input.BindingID)
		h.writeResult(w, http.StatusOK, items, err)
	case "/admin/channel-checkpoints/recover":
		if b.Status != controlplane.StatusDisabled || b.Version != input.ExpectedBindingVersion || input.ExpectedVersion <= 0 || input.ChatHash == "" || input.From.IsZero() {
			h.writeResult(w, 0, nil, invalidf("disable binding and supply current binding/checkpoint versions and explicit from time"))
			return
		}
		cp, err := h.service.channelState.RecoverCheckpoint(r.Context(), b, input.ChatHash, input.ExpectedVersion, input.Action, input.From, input.AcknowledgeGap)
		if err != nil {
			if errors.Is(err, wecommcp.ErrStateConflict) {
				err = controlplane.ErrConflict
			} else if errors.Is(err, wecommcp.ErrInvalidRecovery) {
				err = invalidf("checkpoint recovery rejected; check scope, time and gap acknowledgement")
			}
			h.writeResult(w, 0, nil, err)
			return
		}
		err = h.service.record(r.Context(), input.TenantID, "admin_channel_checkpoint_recovered", map[string]any{"binding_id": input.BindingID, "action": input.Action, "checkpoint_version": cp.Version, "acknowledge_gap": input.AcknowledgeGap})
		h.writeResult(w, http.StatusOK, cp, err)
	}
}
