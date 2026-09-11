package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/connections"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/credentials"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

func (s *Service) WithConnections(store *connections.Store) *Service { s.connections = store; return s }
func (h *Handler) handleConnections(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Tenant    string   `json:"tenant_id"`
		ID        string   `json:"connection_id"`
		App       string   `json:"app_id"`
		Kind      string   `json:"channel_type"`
		Token     string   `json:"token"`
		URL       string   `json:"url"`
		After     string   `json:"after"`
		Version   int64    `json:"expected_version"`
		Confirm   bool     `json:"confirm_replace"`
		Consent   bool     `json:"consent"`
		Group     string   `json:"group_id"`
		Groups    []string `json:"group_ids"`
		Binding   string   `json:"binding_id"`
		Enabled   bool     `json:"enabled"`
		User      string   `json:"user_id"`
		LocalOnly bool     `json:"local_only"`
	}
	if !decodeAdmin(w, r, &in) {
		return
	}
	action := strings.TrimPrefix(r.URL.Path, "/admin/connections/")
	permission := PermissionWrite
	if action == "list" || action == "get" {
		permission = PermissionRead
	}
	if !h.require(w, r, in.Tenant, permission) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 35*time.Second)
	defer cancel()
	p, _ := r.Context().Value(principalContextKey{}).(Principal)
	store := h.service.connections
	if action == "get" {
		c, e := store.Get(ctx, in.Tenant, in.ID)
		if e != nil {
			h.connectionError(w, e)
			return
		}
		adminJSON(w, 200, c)
		return
	}
	if action == "list" {
		if len(in.After) > 128 {
			h.connectionError(w, ErrInvalid)
			return
		}
		items, next, e := store.List(ctx, in.Tenant, in.After)
		if e != nil {
			h.connectionError(w, e)
			return
		}
		adminJSON(w, 200, map[string]any{"items": items, "next": next, "enabled": store != nil, "public_url": store.PublicURL(ctx)})
		return
	}
	if action == "legacy-toggle" {
		b, e := h.service.repository.GetChannelBinding(ctx, in.Tenant, in.Binding)
		if e != nil {
			h.connectionError(w, e)
			return
		}
		if strings.HasPrefix(b.SecretRef, "managed://") {
			h.connectionError(w, errors.New("请使用机器人连接页的暂停或连接按钮"))
			return
		}
		status := "disabled"
		if in.Enabled {
			status = "active"
		}
		value, e := h.service.UpdateChannelBinding(ctx, in.Tenant, b.ID, b.Config, status, in.Version)
		if e != nil {
			h.connectionError(w, e)
			return
		}
		adminJSON(w, 200, map[string]any{"binding_id": value.ID, "status": value.Status, "version": value.Version})
		return
	}
	if store == nil {
		h.connectionError(w, connections.ErrUnavailable)
		return
	}
	if p.Role != RoleSuperAdmin {
		adminJSON(w, 403, map[string]string{"error": "请联系平台管理员管理机器人连接"})
		return
	}
	if action != "groups" || in.Consent {
		if !h.loginLimit.allow(p.Name + "/connections/" + in.Tenant + "/" + action) {
			adminJSON(w, 429, map[string]string{"error": "操作太频繁，请稍后再试"})
			return
		}
	}
	if action == "public-address" {
		if e := store.SetPublicURL(ctx, in.Tenant, p.Name, in.URL); e != nil {
			h.connectionError(w, e)
			return
		}
		adminJSON(w, 200, map[string]string{"public_url": store.PublicURL(ctx)})
		return
	}
	if action == "prepare" {
		var c connections.Connection
		var e error
		switch in.Kind {
		case "telegram":
			c, e = store.PrepareTelegram(ctx, in.Tenant, in.App, in.ID, in.Token, p.Name)
		case "wecom_mcp":
			endpoint := strings.TrimSpace(in.URL)
			if strings.HasPrefix(endpoint, "{") {
				var cfg struct {
					Servers map[string]struct {
						URL string `json:"url"`
					} `json:"mcpServers"`
				}
				if json.Unmarshal([]byte(endpoint), &cfg) != nil || len(cfg.Servers) != 1 {
					h.connectionError(w, errors.New("JSON Config 中应包含一个消息 MCP 连接"))
					return
				}
				for _, server := range cfg.Servers {
					endpoint = server.URL
				}
			}
			c, e = store.PrepareWeCom(ctx, in.Tenant, in.App, in.ID, endpoint, p.Name)
		default:
			e = errors.New("请选择支持的机器人类型")
		}
		if e != nil {
			h.connectionError(w, e)
			return
		}
		adminJSON(w, 200, c)
		return
	}
	c, e := store.Get(ctx, in.Tenant, in.ID)
	if e != nil {
		h.connectionError(w, e)
		return
	}
	if c.Status == "removed" && action != "remove" {
		h.connectionError(w, controlplane.ErrNotFound)
		return
	}
	if c.Operation == "remove" && action != "remove" && action != "check" {
		h.connectionError(w, errors.New("移除操作尚未确认，请检查移除结果"))
		return
	}
	switch action {
	case "update-credential":
		value := in.Token
		if c.Kind == "wecom_mcp" {
			value, e = parseWeComURL(in.URL)
		}
		if e == nil {
			c, e = store.UpdateCredential(ctx, c, in.Version, value, p.Name)
		}
	case "rebind":
		c, e = store.Rebind(ctx, c, in.Version, in.App, p.Name)
	case "remove":
		if !in.Consent {
			e = errors.New("请先确认移除连接的影响")
		} else {
			c, e = store.Remove(ctx, c, in.Version, in.LocalOnly, p.Name)
		}
	case "revoke-member":
		if !in.Consent {
			e = errors.New("请先确认撤销授权")
		} else {
			c, e = store.RevokeWeCom(ctx, c, in.Version, in.Group, in.User, p.Name)
		}
	case "activate":
		if c.Kind == "telegram" {
			c, e = store.ActivateTelegram(ctx, c, in.Version, in.Confirm, p.Name)
		} else {
			c, e = store.ActivateWeCom(ctx, c, in.Version, p.Name)
		}
	case "pause":
		c, e = store.Pause(ctx, c, in.Version, p.Name)
	case "check":
		if c.Kind == "telegram" {
			c, e = store.CheckTelegram(ctx, c, p.Name)
		}
	case "retry":
		if !in.Consent {
			e = errors.New("请先确认重新设置回调")
		} else {
			c, e = store.RetryTelegram(ctx, c, in.Version, p.Name)
		}
	case "groups":
		var groups []connections.Group
		if c.Kind == "wecom_mcp" && in.Consent {
			groups, e = store.LoadWeComGroups(ctx, c, p.Name)
		} else {
			groups, e = store.Groups(ctx, c)
		}
		if e != nil {
			h.connectionError(w, e)
			return
		}
		adminJSON(w, 200, map[string]any{"items": groups, "enrollment": store.Enrollment(c)})
		return
	case "select-group", "check-message":
		var enrollment connections.Enrollment
		if action == "select-group" {
			c, enrollment, e = store.BeginEnrollment(ctx, c, in.Version, in.Group, p.Name)
		} else if in.Consent {
			c, enrollment, e = store.CheckEnrollment(ctx, c, in.Version, p.Name)
		} else {
			e = errors.New("请先确认允许检查所选群里的连接消息")
		}
		if e != nil {
			h.connectionError(w, e)
			return
		}
		adminJSON(w, 200, map[string]any{"connection": c, "enrollment": enrollment})
		return
	case "save-groups":
		c, e = store.SetTelegramGroups(ctx, c, in.Version, in.Groups, p.Name)
	default:
		e = ErrInvalid
	}
	if e != nil {
		h.connectionError(w, e)
		return
	}
	adminJSON(w, 200, c)
}

func parseWeComURL(value string) (string, error) {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, "{") {
		return value, nil
	}
	var cfg struct {
		Servers map[string]struct {
			URL string `json:"url"`
		} `json:"mcpServers"`
	}
	if json.Unmarshal([]byte(value), &cfg) != nil || len(cfg.Servers) != 1 {
		return "", errors.New("JSON Config 中应包含一个消息 MCP 连接")
	}
	for _, server := range cfg.Servers {
		return server.URL, nil
	}
	return "", ErrInvalid
}
func (h *Handler) connectionError(w http.ResponseWriter, e error) {
	status := 400
	switch {
	case errors.Is(e, connections.ErrUnavailable), errors.Is(e, credentials.ErrUnavailable):
		status = 503
	case errors.Is(e, connections.ErrConflict), errors.Is(e, connections.ErrBusy), errors.Is(e, connections.ErrConfirm), errors.Is(e, controlplane.ErrConflict):
		status = 409
	case errors.Is(e, controlplane.ErrNotFound):
		adminJSON(w, 404, map[string]string{"error": "没有找到这个连接，请刷新列表"})
		return
	case errors.Is(e, secret.ErrForbidden):
		adminJSON(w, 403, map[string]string{"error": "没有使用这个连接的权限，请联系管理员"})
		return
	case errors.Is(e, context.Canceled), errors.Is(e, context.DeadlineExceeded):
		adminJSON(w, 504, map[string]string{"error": "请求超时，请刷新连接状态后再操作"})
		return
	}
	adminJSON(w, status, map[string]string{"error": e.Error()})
}
