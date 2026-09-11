package admin

import (
	"context"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"net/http"
	"time"
)

// Pair the app pointer/policy change with its audit history in the shared SQL
// transaction. The development memory implementation is single-process only.
func (s *Service) releaseChange(ctx context.Context, tenant, appID string, expected int64, change func(context.Context, controlplane.AgentApp) (controlplane.AgentApp, error)) (controlplane.AgentApp, error) {
	s.draftMu.Lock()
	defer s.draftMu.Unlock()
	var app controlplane.AgentApp
	err := s.consoleStore.Transaction(ctx, func(ctx context.Context) error {
		if err := s.consoleStore.LockApp(ctx, tenant, appID); err != nil {
			return err
		}
		previous, err := s.repository.GetAgentApp(ctx, tenant, appID)
		if err != nil {
			return err
		}
		if previous.Version != expected {
			return controlplane.ErrConflict
		}
		app, err = change(ctx, previous)
		return err
	})
	return app, err
}
func (h *Handler) handleReleases(w http.ResponseWriter, r *http.Request) {
	var in struct {
		TenantID   string    `json:"tenant_id"`
		AppID      string    `json:"app_id"`
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
	if in.Limit < 1 || in.Limit > 100 || len(in.BeforeID) > 128 {
		h.writeResult(w, 0, nil, invalidf("invalid release cursor"))
		return
	}
	if _, err := h.service.repository.GetAgentApp(r.Context(), in.TenantID, in.AppID); err != nil {
		h.writeResult(w, 0, nil, err)
		return
	}
	items, err := h.service.QueryAudit(r.Context(), audit.Query{TenantID: in.TenantID, AppID: in.AppID, ReleaseOnly: true, BeforeTime: in.BeforeTime, BeforeID: in.BeforeID, Limit: in.Limit})
	var next any
	if len(items) == in.Limit {
		last := items[len(items)-1]
		next = map[string]any{"before_time": last.OccurredAt, "before_id": last.ID}
	}
	h.writeResult(w, 200, map[string]any{"items": items, "next": next}, err)
}
