package admin

import (
	"github.com/liuzengh/trpc-agent-service/trpcservice/background"
	"net/http"
	"time"
)

func (h *Handler) handleJobsList(w http.ResponseWriter, r *http.Request) {
	var in struct {
		TenantID   string    `json:"tenant_id"`
		AppID      string    `json:"app_id"`
		RequestID  string    `json:"request_id"`
		BeforeTime time.Time `json:"before_time"`
		BeforeID   string    `json:"before_id"`
		Limit      int       `json:"limit"`
	}
	if !decodeAdmin(w, r, &in) || !h.require(w, r, in.TenantID, PermissionRead) {
		return
	}
	if in.Limit == 0 {
		in.Limit = 50
	}
	if in.Limit < 1 || in.Limit > 100 || len(in.BeforeID) > 128 || len(in.RequestID) > 128 {
		h.writeResult(w, 0, nil, invalidf("invalid job filter"))
		return
	}
	if in.AppID != "" {
		if _, err := h.service.repository.GetAgentApp(r.Context(), in.TenantID, in.AppID); err != nil {
			h.writeResult(w, 0, nil, err)
			return
		}
	}
	reader, ok := h.service.jobs.(background.Reader)
	if !ok {
		h.writeResult(w, 0, nil, invalidf("background job query unavailable"))
		return
	}
	items, err := reader.ListJobs(r.Context(), background.JobFilter{TenantID: in.TenantID, AppID: in.AppID, RequestID: in.RequestID, BeforeTime: in.BeforeTime, BeforeID: in.BeforeID, Limit: in.Limit})
	var next any
	if len(items) == in.Limit {
		last := items[len(items)-1]
		next = map[string]any{"before_time": last.CreatedAt, "before_id": last.ID}
	}
	h.writeResult(w, 200, map[string]any{"items": items, "next": next}, err)
}
