package admin

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

func (h *Handler) handleCatalog(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Kind     string `json:"kind"`
		TenantID string `json:"tenant_id"`
		AppID    string `json:"app_id"`
		After    string `json:"after"`
		Limit    int    `json:"limit"`
	}
	if !decodeAdmin(w, r, &in) {
		return
	}
	if in.Limit == 0 {
		in.Limit = 50
	}
	if in.Limit < 1 || in.Limit > 100 || len(in.After) > 256 || len(in.AppID) > 128 {
		adminJSON(w, 400, map[string]string{"error": "invalid catalog pagination"})
		return
	}
	switch in.Kind {
	case "tenants", "apps", "revisions", "channels", "backends":
	default:
		adminJSON(w, 400, map[string]string{"error": "invalid catalog kind"})
		return
	}
	p, _ := r.Context().Value(principalContextKey{}).(Principal)
	q := controlplane.CatalogQuery{Kind: in.Kind, AppID: in.AppID, After: in.After, Limit: in.Limit}
	if in.TenantID != "" {
		if !h.require(w, r, in.TenantID, PermissionRead) {
			return
		}
		q.TenantIDs = []string{in.TenantID}
	} else if in.Kind == "tenants" {
		q.AllTenants = p.Role == RoleSuperAdmin
		for _, id := range p.TenantIDs {
			if id == "*" {
				q.AllTenants = true
			} else {
				q.TenantIDs = append(q.TenantIDs, id)
			}
		}
	} else {
		adminJSON(w, 400, map[string]string{"error": "tenant_id is required"})
		return
	}
	repo, ok := h.service.repository.(controlplane.CatalogRepository)
	if !ok {
		adminJSON(w, 503, map[string]string{"error": "catalog unavailable"})
		return
	}
	page, err := repo.ListCatalog(r.Context(), q)
	if err != nil {
		h.writeResult(w, 200, nil, err)
		return
	}
	for i, raw := range page.Items {
		var v any
		if json.Unmarshal(raw, &v) != nil {
			adminJSON(w, 500, map[string]string{"error": "invalid catalog result"})
			return
		}
		page.Items[i], _ = json.Marshal(redactCatalog(v))
	}
	adminJSON(w, 200, page)
}

func redactCatalog(value any) any {
	switch v := value.(type) {
	case map[string]any:
		for k, item := range v {
			lower := strings.ToLower(k)
			secret := false
			for _, word := range []string{"password", "secret", "api_key", "authorization", "credential", "dsn"} {
				secret = secret || strings.Contains(lower, word)
			}
			secret = secret || lower == "token" || strings.HasSuffix(lower, "_token")
			if secret && !strings.HasSuffix(lower, "_ref") && lower != "api_key_env" && lower != "secret_namespace" {
				v[k] = "[REDACTED]"
			} else {
				v[k] = redactCatalog(item)
			}
		}
		return v
	case []any:
		for i, item := range v {
			v[i] = redactCatalog(item)
		}
		return v
	case string:
		if u, err := url.Parse(v); err == nil && u.Scheme != "" && (u.User != nil || u.RawQuery != "") {
			return "[REDACTED URL]"
		}
		return platformlog.Redact(v)
	default:
		return value
	}
}
