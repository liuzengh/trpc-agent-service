package httpadapter

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	managementv1 "github.com/liuzengh/trpc-agent-service/api/runtime/management/v1"
	workerops "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/management"
)

func managementPage(r *http.Request) (int, int, bool) {
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
	return offset, limit, offset >= 0 && limit > 0 && limit <= managementv1.MaxPageSize
}

func (h *Handler) listRuns(w http.ResponseWriter, r *http.Request) {
	h.managementList(w, r, false)
}

func (h *Handler) listAudit(w http.ResponseWriter, r *http.Request) {
	h.managementList(w, r, true)
}

func (h *Handler) managementList(w http.ResponseWriter, r *http.Request, audit bool) {
	if !h.authorize(w, r, h.control) {
		return
	}
	tenant := strings.TrimSpace(r.PathValue("tenant_id"))
	offset, limit, ok := managementPage(r)
	if !ok || tenant == "" || len(tenant) > 256 {
		respondError(w, http.StatusBadRequest, "INVALID_MANAGEMENT_QUERY")
		return
	}
	ctx, cancel, ok := h.bounded(w, r)
	if !ok {
		return
	}
	defer cancel()
	var value any
	var err error
	if audit {
		value, err = h.management.Audit(ctx, tenant, offset, limit)
	} else {
		value, err = h.management.List(ctx, tenant, offset, limit)
	}
	if err != nil {
		respondError(w, http.StatusServiceUnavailable, "MANAGEMENT_UNAVAILABLE")
		return
	}
	_ = json.NewEncoder(w).Encode(value)
}

func (h *Handler) getRun(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r, h.control) {
		return
	}
	if r.URL.RawQuery != "" {
		respondError(w, http.StatusBadRequest, "INVALID_MANAGEMENT_QUERY")
		return
	}
	tenant, runID := strings.TrimSpace(r.PathValue("tenant_id")), strings.TrimSpace(r.PathValue("run_id"))
	if tenant == "" || runID == "" || len(tenant) > 256 || len(runID) > 256 {
		respondError(w, http.StatusBadRequest, "INVALID_MANAGEMENT_QUERY")
		return
	}
	ctx, cancel, ok := h.bounded(w, r)
	if !ok {
		return
	}
	defer cancel()
	value, err := h.management.Get(ctx, tenant, runID)
	if errors.Is(err, workerops.ErrNotFound) {
		respondError(w, http.StatusNotFound, "RUN_NOT_FOUND")
		return
	}
	if err != nil {
		respondError(w, http.StatusServiceUnavailable, "MANAGEMENT_UNAVAILABLE")
		return
	}
	_ = json.NewEncoder(w).Encode(value)
}

func (h *Handler) usageSummary(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r, h.control) {
		return
	}
	if r.URL.RawQuery != "" {
		respondError(w, http.StatusBadRequest, "INVALID_MANAGEMENT_QUERY")
		return
	}
	tenant := strings.TrimSpace(r.PathValue("tenant_id"))
	if tenant == "" || len(tenant) > 256 {
		respondError(w, http.StatusBadRequest, "INVALID_MANAGEMENT_QUERY")
		return
	}
	ctx, cancel, ok := h.bounded(w, r)
	if !ok {
		return
	}
	defer cancel()
	value, err := h.usageManagement.Usage(ctx, tenant)
	if err != nil {
		respondError(w, http.StatusServiceUnavailable, "MANAGEMENT_UNAVAILABLE")
		return
	}
	_ = json.NewEncoder(w).Encode(value)
}
