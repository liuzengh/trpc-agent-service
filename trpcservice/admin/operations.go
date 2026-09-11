package admin

import (
	"net/http"

	"github.com/liuzengh/trpc-agent-service/trpcservice/toolexec"
)

func (h *Handler) handleToolOperations(w http.ResponseWriter, r *http.Request) {
	var input struct {
		TenantID    string `json:"tenant_id"`
		OperationID string `json:"operation_id"`
		RequestID   string `json:"request_id"`
		Status      string `json:"status"`
		AfterID     string `json:"after_id"`
		Limit       int    `json:"limit"`
	}
	permission := PermissionRead
	if r.URL.Path == "/admin/tool-operations/reconcile" {
		permission = PermissionOperate
	}
	if !decodeAdmin(w, r, &input) || !h.require(w, r, input.TenantID, permission) {
		return
	}
	if h.service.operations == nil || h.service.toolJournal == nil {
		h.writeResult(w, 0, nil, invalidf("tool operation service is unavailable"))
		return
	}
	switch r.URL.Path {
	case "/admin/tool-executions/list":
		if input.RequestID == "" {
			h.writeResult(w, 0, nil, invalidf("request_id is required"))
			return
		}
		items, err := h.service.toolJournal.ListByRequest(r.Context(), input.TenantID, input.RequestID)
		h.writeResult(w, http.StatusOK, items, err)
	case "/admin/tool-operations/list":
		if input.Limit == 0 {
			input.Limit = 50
		}
		items, err := h.service.operations.List(r.Context(), input.TenantID, input.Status, input.AfterID, input.Limit)
		h.writeResult(w, http.StatusOK, items, err)
	case "/admin/tool-operations/get":
		op, err := h.service.operations.Get(r.Context(), input.TenantID, input.OperationID)
		h.writeResult(w, http.StatusOK, op, err)
	case "/admin/tool-operations/reconcile":
		// Only identifiers are accepted, never a caller-supplied status/result.
		if input.Status != "" || input.RequestID != "" || input.AfterID != "" || input.Limit != 0 {
			h.writeResult(w, 0, nil, invalidf("reconcile accepts tenant_id and operation_id only"))
			return
		}
		op, err := h.service.operations.Reconcile(r.Context(), input.TenantID, input.OperationID)
		if err == toolexec.ErrOperationUnknown || err == toolexec.ErrOperationRejected {
			err = nil
		}
		if err == nil {
			err = h.service.record(r.Context(), input.TenantID, "admin_tool_operation_reconciled", map[string]any{"operation_id": op.ID, "status": op.Status})
		}
		h.writeResult(w, http.StatusOK, op, err)
	}
}
