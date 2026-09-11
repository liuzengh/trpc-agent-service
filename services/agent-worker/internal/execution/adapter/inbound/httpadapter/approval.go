package httpadapter

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	approvalv1 "github.com/liuzengh/trpc-agent-service/api/runtime/approval/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/toolapproval"
)

func approvalPage(r *http.Request) (int, int, bool) {
	for key, values := range r.URL.Query() {
		if (key != "offset" && key != "limit") || len(values) != 1 {
			return 0, 0, false
		}
	}
	offset, limit := 0, 25
	var err error
	if raw := r.URL.Query().Get("offset"); raw != "" {
		offset, err = strconv.Atoi(raw)
		if err != nil {
			return 0, 0, false
		}
	}
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil {
			return 0, 0, false
		}
	}
	return offset, limit, offset >= 0 && limit > 0 && limit <= approvalv1.MaxPageSize
}

func (h *Handler) listApprovals(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r, h.control) {
		return
	}
	offset, limit, ok := approvalPage(r)
	if !ok {
		respondError(w, http.StatusBadRequest, "INVALID_PAGE")
		return
	}
	page, err := h.approvals.List(r.Context(), r.PathValue("tenant_id"), offset, limit)
	if err != nil {
		approvalError(w, err)
		return
	}
	_ = json.NewEncoder(w).Encode(page)
}

func (h *Handler) decideApproval(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r, h.control) {
		return
	}
	if r.URL.RawQuery != "" || r.Header.Get("Content-Type") != "application/json" {
		respondError(w, http.StatusBadRequest, "INVALID_APPROVAL_DECISION")
		return
	}
	var request approvalv1.WorkerDecisionRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, 4097))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		respondError(w, http.StatusBadRequest, "INVALID_APPROVAL_DECISION")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		respondError(w, http.StatusBadRequest, "INVALID_APPROVAL_DECISION")
		return
	}
	response, err := h.approvals.Decide(r.Context(), r.PathValue("tenant_id"), r.PathValue("operation_id"), request.ActorID, request.Action, request.Reason, request.ExpectedArgumentsDigest)
	if err != nil {
		approvalError(w, err)
		return
	}
	_ = json.NewEncoder(w).Encode(response)
}

func approvalError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, toolapproval.ErrInvalid):
		respondError(w, http.StatusBadRequest, "INVALID_APPROVAL_DECISION")
	case errors.Is(err, toolapproval.ErrNotFound):
		respondError(w, http.StatusNotFound, "APPROVAL_NOT_FOUND")
	case errors.Is(err, toolapproval.ErrConflict):
		respondError(w, http.StatusConflict, "APPROVAL_CONFLICT")
	default:
		respondError(w, http.StatusServiceUnavailable, "APPROVAL_UNAVAILABLE")
	}
}
