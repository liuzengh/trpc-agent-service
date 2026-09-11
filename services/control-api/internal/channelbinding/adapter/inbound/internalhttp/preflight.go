package internalhttp

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"

	channelv1 "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

// PreflightRuntime is an independent diagnostic authorization boundary. In
// particular Resolve never calls the ordinary enabled-account credential path.
type PreflightRuntime interface {
	Claim(context.Context, application.WorkloadPrincipal, channelv1.PreflightClaimRequest) (*channelv1.PreflightGrant, error)
	Resolve(context.Context, application.WorkloadPrincipal, string, channelv1.PreflightResolveRequest) (channelv1.PreflightResolveResponse, error)
	Complete(context.Context, application.WorkloadPrincipal, string, channelv1.PreflightCompleteRequest) error
}

func preflightAllowed(w http.ResponseWriter, r *http.Request) bool {
	if !slices.Contains(principal(r).Consumers, "telegram_preflight") && !slices.Contains(principal(r).Consumers, "wecom_preflight") {
		failure(w, http.StatusForbidden, "CHANNEL_WORKLOAD_DENIED")
		return false
	}
	return true
}
func (h *Handler) preflightClaim(w http.ResponseWriter, r *http.Request) {
	if !preflightAllowed(w, r) {
		return
	}
	in, ok := decode[channelv1.PreflightClaimRequest](w, r, "preflight-claim.schema.json", 4*1024)
	if !ok {
		return
	}
	result, err := h.preflight.Claim(r.Context(), principal(r), in)
	if err != nil {
		preflightError(w, err)
		return
	}
	if result == nil {
		w.Header().Set("Retry-After", "2")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, http.StatusOK, result, 16*1024)
}
func (h *Handler) preflightResolve(w http.ResponseWriter, r *http.Request) {
	if !preflightAllowed(w, r) {
		return
	}
	id := r.PathValue("preflight_id")
	if !domain.ValidID(id) {
		failure(w, http.StatusBadRequest, domain.InputInvalid)
		return
	}
	in, ok := decode[channelv1.PreflightResolveRequest](w, r, "preflight-resolve.schema.json", 4*1024)
	if !ok {
		return
	}
	result, err := h.preflight.Resolve(r.Context(), principal(r), id, in)
	if err != nil {
		preflightError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result, 20*1024)
}
func (h *Handler) preflightComplete(w http.ResponseWriter, r *http.Request) {
	if !preflightAllowed(w, r) {
		return
	}
	// ServeMux wildcards occupy a complete path segment, so parse the frozen
	// :complete action explicitly rather than broadening the external contract.
	action := r.PathValue("preflight_action")
	id, ok := strings.CutSuffix(action, ":complete")
	if !ok {
		http.NotFound(w, r)
		return
	}
	if !domain.ValidID(id) {
		failure(w, http.StatusBadRequest, domain.InputInvalid)
		return
	}
	in, ok := decode[channelv1.PreflightCompleteRequest](w, r, "preflight-complete.schema.json", 16*1024)
	if !ok {
		return
	}
	if err := h.preflight.Complete(r.Context(), principal(r), id, in); err != nil {
		preflightError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func preflightError(w http.ResponseWriter, err error) {
	var d *domain.Error
	if errors.As(err, &d) {
		switch d.Code {
		case "CHANNEL_PREFLIGHT_NOT_FOUND":
			failure(w, http.StatusNotFound, d.Code)
			return
		case "CHANNEL_PREFLIGHT_PROVIDER_UNSUPPORTED":
			failure(w, http.StatusUnprocessableEntity, d.Code)
			return
		case "CHANNEL_PREFLIGHT_RATE_LIMITED":
			w.Header().Set("Retry-After", "2")
			failure(w, http.StatusTooManyRequests, d.Code)
			return
		}
	}
	handleError(w, err)
}
