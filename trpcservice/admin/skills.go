package admin

import (
	"context"
	"errors"
	"net/http"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	platformskill "github.com/liuzengh/trpc-agent-service/trpcservice/skill"
)

func (s *Service) WithSkillUploads(store *platformskill.Store) *Service {
	s.skillUploads = store
	return s
}

func (h *Handler) handleSkillManagement(w http.ResponseWriter, r *http.Request) {
	var in struct {
		TenantID string `json:"tenant_id"`
		After    string `json:"after"`
		Status   string `json:"status"`
		Expected int64  `json:"expected_revision"`
		platformskill.Upload
	}
	if !decodeAdmin(w, r, &in) || !h.require(w, r, in.TenantID, PermissionRead) {
		return
	}
	if !identifierPattern.MatchString(in.TenantID) || len(in.After) > 128 {
		h.writeResult(w, 0, nil, invalidf("invalid Skill identity"))
		return
	}
	if r.URL.Path == "/admin/skills/manage/list" {
		items, next, err := h.service.skillUploads.List(r.Context(), in.TenantID, in.After, false)
		h.skillResult(w, http.StatusOK, map[string]any{"items": items, "next": next, "deployment": h.service.skills.List(in.TenantID), "enabled": h.service.skillUploads != nil}, err)
		return
	}
	if r.URL.Path == "/admin/skills/inspect" {
		d, md, script, err := h.service.skillUploads.Inspect(r.Context(), in.TenantID, in.Name, in.Version)
		h.skillResult(w, http.StatusOK, map[string]any{"skill": d, "markdown": md, "script": script}, err)
		return
	}
	if !h.require(w, r, in.TenantID, PermissionWrite) {
		return
	}
	p := r.Context().Value(principalContextKey{}).(Principal)
	if r.URL.Path == "/admin/skills/review" && p.Role != RoleSuperAdmin {
		adminJSON(w, http.StatusForbidden, map[string]string{"error": "仅平台管理员可批准或撤销上传的 Skill"})
		return
	}
	var result platformskill.ManagedDescriptor
	err := h.service.consoleStore.Transaction(r.Context(), func(ctx context.Context) error {
		if _, err := h.service.repository.GetTenant(ctx, in.TenantID); err != nil {
			return err
		}
		var err error
		decision := "admin_skill_uploaded"
		if r.URL.Path == "/admin/skills/review" {
			result, err = h.service.skillUploads.Review(ctx, in.TenantID, in.Name, in.Version, in.Status, p.Name, in.Expected)
			decision = "admin_skill_" + in.Status
		} else {
			if h.service.skills.HasDeploymentVersion(in.Name, in.Version) {
				return controlplane.ErrConflict
			}
			result, err = h.service.skillUploads.Upload(ctx, in.TenantID, p.Name, in.Upload)
		}
		if err != nil {
			return err
		}
		return h.service.record(ctx, in.TenantID, decision, map[string]any{"name": result.Name, "version": result.Version, "checksum": result.Checksum, "revision": result.Revision})
	})
	h.skillResult(w, http.StatusOK, result, err)
}

func (h *Handler) skillResult(w http.ResponseWriter, status int, value any, err error) {
	if errors.Is(err, platformskill.ErrStoreUnavailable) {
		adminJSON(w, http.StatusServiceUnavailable, map[string]string{"error": platformskill.ErrStoreUnavailable.Error()})
		return
	}
	if errors.Is(err, platformskill.ErrUpload) || errors.Is(err, platformskill.ErrUploadLimit) {
		err = invalidf("%s", err.Error())
	}
	if errors.Is(err, controlplane.ErrConflict) {
		adminJSON(w, http.StatusConflict, map[string]string{"error": "Skill 版本已存在且内容不同，或审核状态已变化。请使用新版本号或刷新后重试。"})
		return
	}
	h.writeResult(w, status, value, err)
}
