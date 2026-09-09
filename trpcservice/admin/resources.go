package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

func (s *Service) modelResources(ctx context.Context, tenant, after string) ([]map[string]any, string, error) {
	repo, ok := s.repository.(controlplane.CatalogRepository)
	if !ok {
		return nil, "", invalidf("resource catalog unavailable")
	}
	page, err := repo.ListCatalog(ctx, controlplane.CatalogQuery{Kind: "apps", TenantIDs: []string{tenant}, After: after, Limit: 100})
	if err != nil {
		return nil, "", err
	}
	name := s.startupModelName
	if name == "" {
		name = "执行节点默认模型"
	}
	items := []map[string]any{}
	if after == "" {
		items = append(items, map[string]any{"resource_id": "model-startup", "name": name, "backend_type": "startup_env", "status": "configured", "description": "部署者配置；可用性需在工作台实际验证。", "model_config": map[string]string{"source": "startup_env"}})
	}
	seen := map[string]bool{}
	for _, raw := range page.Items {
		var app controlplane.AgentApp
		if json.Unmarshal(raw, &app) != nil || app.StableRevisionID == "" {
			continue
		}
		revision, err := s.repository.GetRevision(ctx, tenant, app.StableRevisionID)
		if err != nil {
			return nil, "", err
		}
		var cfg struct {
			Source   string `json:"source"`
			Provider string `json:"provider"`
			Name     string `json:"name"`
			Ref      string `json:"api_key_ref"`
			Env      string `json:"api_key_env"`
		}
		if json.Unmarshal(revision.ModelConfig, &cfg) != nil {
			continue
		}
		if source := strings.ToLower(strings.TrimSpace(cfg.Source)); source == "" || source == "startup_env" {
			continue
		}
		id := "model-" + digest(string(revision.ModelConfig))[:24]
		if seen[id] {
			continue
		}
		seen[id] = true
		ref := cfg.Ref
		if ref == "" && cfg.Env != "" {
			ref = "env://" + cfg.Env
		}
		status := "configured"
		description := "来自已发布版本；授权已检查，尚未探测模型服务。"
		if s.authorizeSecret(ctx, tenant, secret.Model, ref) != nil {
			status = "unavailable"
			description = "模型凭据授权已撤销或不可用，不能用于新执行。"
		}
		items = append(items, map[string]any{"resource_id": id, "name": cfg.Name, "backend_type": cfg.Provider, "status": status, "description": description, "source_app": app.Name, "model_config": json.RawMessage(revision.ModelConfig)})
	}
	return items, page.Next, nil
}

func (h *Handler) handleResources(w http.ResponseWriter, r *http.Request) {
	var in struct {
		TenantID string `json:"tenant_id"`
		Kind     string `json:"kind"`
		After    string `json:"after"`
		Purpose  string `json:"purpose"`
	}
	if !decodeAdmin(w, r, &in) || !h.require(w, r, in.TenantID, PermissionRead) {
		return
	}
	if len(in.After) > 256 {
		h.writeResult(w, 0, nil, invalidf("invalid cursor"))
		return
	}
	if in.Kind == "credentials" {
		catalog, ok := h.service.secrets.(secret.ReferenceCatalog)
		if !ok || !secret.ValidPurpose(in.Purpose) {
			h.writeResult(w, 0, nil, invalidf("authorized credential catalog unavailable or invalid purpose"))
			return
		}
		refs, err := catalog.References(r.Context(), in.TenantID, in.Purpose)
		items := []map[string]string{}
		next := ""
		for _, ref := range refs {
			if ref <= in.After {
				continue
			}
			if len(items) == 100 {
				next = items[len(items)-1]["reference"]
				break
			}
			items = append(items, map[string]string{"reference": ref, "purpose": in.Purpose})
		}
		h.writeResult(w, 200, map[string]any{"items": items, "next": next}, err)
		return
	}
	if in.Kind == "models" {
		items, next, err := h.service.modelResources(r.Context(), in.TenantID, in.After)
		h.writeResult(w, 200, safeConsoleValue(map[string]any{"items": items, "next": next}), err)
		return
	}
	if in.Kind != "knowledge" {
		h.writeResult(w, 0, nil, invalidf("unsupported resource kind"))
		return
	}
	repo, ok := h.service.repository.(controlplane.CatalogRepository)
	if !ok {
		h.writeResult(w, 0, nil, invalidf("resource catalog unavailable"))
		return
	}
	page, err := repo.ListCatalog(r.Context(), controlplane.CatalogQuery{Kind: "backends", TenantIDs: []string{in.TenantID}, After: in.After, Limit: 100})
	if err != nil {
		h.writeResult(w, 0, nil, err)
		return
	}
	items := []map[string]any{}
	for _, raw := range page.Items {
		var binding controlplane.BackendBinding
		if json.Unmarshal(raw, &binding) != nil || binding.ResourceType != "knowledge" {
			continue
		}
		var cfg map[string]any
		_ = json.Unmarshal(binding.Config, &cfg)
		name, _ := cfg["collection_name"].(string)
		if strings.TrimSpace(name) == "" {
			name = binding.ID
		}
		items = append(items, map[string]any{"resource_id": binding.ID, "name": name, "app_id": binding.AppID, "backend_type": binding.BackendType, "status": binding.MigrationState, "config": cfg, "secret_ref": binding.SecretRef, "description": "已注册的知识库后端；向量维度与 Embedding 需匹配。"})
	}
	h.writeResult(w, 200, safeConsoleValue(map[string]any{"items": items, "next": page.Next}), nil)
}
