package admin

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

func (s *Service) WithOutboundParts(j gateway.PartJournal) *Service { s.outboundParts = j; return s }
func (h *Handler) handleOutboundParts(w http.ResponseWriter, r *http.Request) {
	var in struct {
		TenantID      string `json:"tenant_id"`
		OutboundID    string `json:"outbound_id"`
		PartIndex     int    `json:"part_index"`
		ExpectedOwner string `json:"expected_owner"`
		Outcome       string `json:"outcome"`
		ProviderID    string `json:"provider_id"`
		Evidence      string `json:"evidence_ref"`
	}
	permission := PermissionRead
	if r.URL.Path == "/admin/outbound-parts/reconcile" {
		permission = PermissionOperate
	}
	if !decodeAdmin(w, r, &in) || !h.require(w, r, in.TenantID, permission) {
		return
	}
	j := h.service.outboundParts
	if j == nil || in.OutboundID == "" {
		h.writeResult(w, 0, nil, invalidf("outbound part store unavailable"))
		return
	}
	if permission == PermissionRead {
		parts, err := j.ListParts(r.Context(), in.TenantID, in.OutboundID)
		h.writeResult(w, http.StatusOK, parts, err)
		return
	}
	if (in.Outcome != "sent" && in.Outcome != "not_sent") || in.ExpectedOwner == "" || in.PartIndex < 0 || len(strings.TrimSpace(in.Evidence)) < 8 || len(in.Evidence) > 512 || len(in.ProviderID) > 255 {
		h.writeResult(w, 0, nil, invalidf("current owner, explicit outcome and evidence reference required"))
		return
	}
	digest := sha256.Sum256([]byte(in.Evidence))
	evidence := hex.EncodeToString(digest[:])
	if err := h.service.record(r.Context(), in.TenantID, "admin_outbound_reconciliation_requested", map[string]any{"outbound_id": in.OutboundID, "part_index": in.PartIndex, "evidence_hash": evidence}); err != nil {
		h.writeResult(w, 0, nil, err)
		return
	}
	err := j.ReconcilePart(r.Context(), gateway.OutboundPart{TenantID: in.TenantID, OutboundID: in.OutboundID, Index: in.PartIndex, Owner: in.ExpectedOwner}, in.Outcome, in.ProviderID, evidence, PrincipalName(r.Context()), audit.TraceID(r.Context()))
	h.writeResult(w, http.StatusOK, map[string]bool{"reconciled": err == nil}, err)
}
