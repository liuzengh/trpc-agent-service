package connections

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecommcp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

// lockEditable must run inside the same transaction as the mutation. A paused
// binding row is the ingress barrier; existing work must finish or be reconciled.
func (s *Store) lockEditable(ctx context.Context, c Connection, version int64) (controlplane.ChannelBinding, error) {
	var b controlplane.ChannelBinding
	current, e := scan(s.sql(ctx).QueryRowContext(ctx, `SELECT `+columns+` FROM channel_connection WHERE tenant_id=$1 AND connection_id=$2 FOR UPDATE`, c.TenantID, c.ID))
	if e != nil {
		return b, e
	}
	if current.Version != version || c.Version != version || current.Status == "removed" {
		return b, ErrConflict
	}
	if current.BusyUntil != nil && current.BusyUntil.After(time.Now()) || current.Operation != "" {
		return b, ErrBusy
	}
	if current.Status == "connected" {
		return b, errors.New("请先暂停机器人，再修改或移除连接")
	}
	if current.Status == "unknown" || current.Status == "connecting" {
		return b, errors.New("上一次操作还未确认，请先检查连接状态")
	}
	if current.BindingID == "" {
		return b, nil
	}
	if _, e = s.sql(ctx).ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('channel-binding/' || $1,0))`, current.BindingID); e != nil {
		return b, safe(e)
	}
	var status string
	if e = s.sql(ctx).QueryRowContext(ctx, `SELECT status FROM channel_binding WHERE tenant_id=$1 AND channel_binding_id=$2 AND retired_at IS NULL FOR UPDATE`, c.TenantID, current.BindingID).Scan(&status); e != nil {
		return b, safe(e)
	}
	if status != "disabled" {
		return b, errors.New("请先暂停机器人")
	}
	var pending bool
	e = s.sql(ctx).QueryRowContext(ctx, `SELECT
 EXISTS(SELECT 1 FROM agent_run r JOIN conversation v ON v.conversation_id=r.conversation_id WHERE v.tenant_id=$1 AND v.channel_binding_id=$2 AND r.status NOT IN ('completed','dead','expired','cancelled'))
 OR EXISTS(SELECT 1 FROM outbound_message WHERE tenant_id=$1 AND channel_binding_id=$2 AND status IN ('pending','sending','unknown'))
 OR EXISTS(SELECT 1 FROM tool_approval WHERE tenant_id=$1 AND channel_binding_id=$2 AND expires_at>now() AND (status='pending' OR (status='approved' AND resumed_at IS NULL)))`, c.TenantID, current.BindingID).Scan(&pending)
	if e != nil {
		return b, safe(e)
	}
	if pending {
		return b, errors.New("还有未完成请求、待处理审批或未确认投递，请先到消息记录核对；本次未修改连接")
	}
	return s.repo.GetChannelBinding(ctx, c.TenantID, current.BindingID)
}

func (s *Store) retire(ctx context.Context, c Connection) error {
	if c.BindingID == "" {
		return nil
	}
	_, e := s.sql(ctx).ExecContext(ctx, `UPDATE channel_binding SET status='disabled',retired_at=COALESCE(retired_at,now()),version=version+1,updated_at=now() WHERE tenant_id=$1 AND channel_binding_id=$2 AND status='disabled'`, c.TenantID, c.BindingID)
	return safe(e)
}

// UpdateCredential validates without modifying the upstream service. A WeCom
// URL has no stable bot identity in discovery, so changing it requires enrollment
// again rather than copying authorizations to a potentially different robot.
func (s *Store) UpdateCredential(ctx context.Context, c Connection, version int64, value, actor string) (Connection, error) {
	value = strings.TrimSpace(value)
	if c.Version != version || c.Status == "removed" {
		return c, ErrConflict
	}
	if c.Status == "connected" {
		return c, errors.New("请先暂停机器人，再更新凭据")
	}
	name, remote := c.Name, c.RemoteHash
	var botID int64
	var username string
	switch c.Kind {
	case "telegram":
		bot, hook, e := s.telegram.Inspect(ctx, value)
		if e != nil {
			return c, e
		}
		if strconv.FormatInt(bot.ID, 10) != c.Account {
			return c, errors.New("这个 Token 属于另一个机器人，请使用“连接机器人”新增")
		}
		botID, username = bot.ID, bot.Username
		name = "@" + username
		remote = hash(hook.URL)
	case "wecom_mcp":
		if e := wecommcp.CheckConnection(ctx, value, s.mcpClient); e != nil {
			return c, e
		}
	default:
		return c, ErrConflict
	}
	e := database.InTransaction(ctx, s.db, func(ctx context.Context) error {
		b, e := s.lockEditable(ctx, c, version)
		if e != nil {
			return e
		}
		purposes := []string{secret.TelegramBot, secret.TelegramMedia}
		if c.Kind == "wecom_mcp" {
			purposes = []string{secret.WeComMCPRead, secret.WeComMCPSend}
		}
		ref, e := s.vault.Put(ctx, c.TenantID, purposes, value)
		if e != nil {
			return e
		}
		if c.Kind == "telegram" {
			var cfg map[string]any
			if json.Unmarshal(b.Config, &cfg) != nil {
				return ErrUnavailable
			}
			cfg["bot_token_ref"] = ref
			cfg["bot_user_id"] = botID
			cfg["bot_username"] = username
			if _, e = s.sql(ctx).ExecContext(ctx, `UPDATE channel_binding SET secret_ref=$3,config=$4,version=version+1,updated_at=now() WHERE tenant_id=$1 AND channel_binding_id=$2 AND status='disabled' AND retired_at IS NULL`, c.TenantID, b.ID, ref, encode(cfg)); e != nil {
				return safe(e)
			}
			if _, e = s.sql(ctx).ExecContext(ctx, `UPDATE channel_connection SET credential_ref=$3,display_name=$4,remote_hash=$5 WHERE tenant_id=$1 AND connection_id=$2`, c.TenantID, c.ID, ref, name, remote); e != nil {
				return safe(e)
			}
			if e = s.outcome(ctx, c, "paused", ""); e != nil {
				return e
			}
		} else {
			if e = s.retire(ctx, c); e != nil {
				return e
			}
			if _, e = s.sql(ctx).ExecContext(ctx, `UPDATE channel_connection SET credential_ref=$3,account_key=$4,binding_id=NULL,settings='{}',display_name='企业微信机器人' WHERE tenant_id=$1 AND connection_id=$2`, c.TenantID, c.ID, ref, hash(value)); e != nil {
				return ErrConflict
			}
			if _, e = s.sql(ctx).ExecContext(ctx, `DELETE FROM channel_connection_group WHERE tenant_id=$1 AND connection_id=$2`, c.TenantID, c.ID); e != nil {
				return safe(e)
			}
			if e = s.outcome(ctx, c, "draft", "地址已更新，请重新选择群并确认成员权限"); e != nil {
				return e
			}
		}
		return s.record(ctx, c, actor, "channel_credential_updated")
	})
	if e != nil {
		return c, e
	}
	return s.Get(ctx, c.TenantID, c.ID)
}

// Rebind creates a new route/session scope; the previous route remains as an
// immutable retired identity for messages, approvals and audit references.
func (s *Store) Rebind(ctx context.Context, c Connection, version int64, app, actor string) (Connection, error) {
	if c.Version != version || c.AppID == app {
		return c, ErrConflict
	}
	if e := s.app(ctx, c.TenantID, app); e != nil {
		return c, e
	}
	public := ""
	if c.Kind == "telegram" {
		var e error
		public, e = validPublicURL(s.PublicURL(ctx))
		if e != nil {
			return c, e
		}
	}
	e := database.InTransaction(ctx, s.db, func(ctx context.Context) error {
		old, e := s.lockEditable(ctx, c, version)
		if e != nil {
			return e
		}
		if e = s.retire(ctx, c); e != nil {
			return e
		}
		next := c
		next.AppID = app
		id, url, state, remote := "", c.URL, "draft", c.RemoteHash
		if old.ID != "" {
			cfg := old.Config
			if c.Kind == "wecom_mcp" {
				parsed, e := wecommcp.ParseBinding(old)
				if e != nil {
					return e
				}
				// A new Agent is a new conversation. Do not backfill the old
				// Agent's history or transfer pending approvals into this scope.
				now := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339)
				parsed.StartAt = now
				for chat, g := range parsed.GroupGrants {
					g.StartAt = now
					for i := range g.Members {
						g.Members[i].Since = now
					}
					parsed.GroupGrants[chat] = g
				}
				cfg = encode(parsed)
			}
			b, e := s.newBinding(ctx, next, cfg, "disabled")
			if e != nil {
				return safe(e)
			}
			id = b.ID
			state = "paused"
			if c.Kind == "telegram" {
				url = public + "/callbacks/telegram/" + b.CallbackKey
				remote = hash(c.URL)
				state = "needs_confirmation"
			}
		}
		if _, e = s.sql(ctx).ExecContext(ctx, `UPDATE channel_connection SET app_id=$3,binding_id=NULLIF($4,''),callback_url=$5,remote_hash=$6,settings='{}' WHERE tenant_id=$1 AND connection_id=$2`, c.TenantID, c.ID, app, id, url, remote); e != nil {
			return safe(e)
		}
		if e = s.outcome(ctx, c, state, "Agent 已更换，点击连接后开始新的会话"); e != nil {
			return e
		}
		next.BindingID = id
		return s.record(ctx, next, actor, "channel_agent_rebound")
	})
	if e != nil {
		return c, e
	}
	return s.Get(ctx, c.TenantID, c.ID)
}

func (s *Store) finishRemoval(ctx context.Context, c Connection, actor, note string) (Connection, error) {
	e := database.InTransaction(ctx, s.db, func(ctx context.Context) error {
		if e := s.outcome(ctx, c, "removed", note); e != nil {
			return e
		}
		if e := s.retire(ctx, c); e != nil {
			return e
		}
		return s.record(ctx, c, actor, "channel_connection_removed")
	})
	if e != nil {
		return c, e
	}
	return s.Get(ctx, c.TenantID, c.ID)
}

// Remove is a tombstone, not a history/credential purge. localOnly is an
// explicitly confirmed escape for an already revoked token; it never pretends
// the provider webhook was removed. Otherwise deleteWebhook is scoped to ours.
func (s *Store) Remove(ctx context.Context, c Connection, version int64, localOnly bool, actor string) (Connection, error) {
	if c.Status == "removed" {
		return c, nil
	}
	if c.Version != version {
		return c, ErrConflict
	}
	if c.BusyUntil != nil && c.BusyUntil.After(time.Now()) {
		return c, ErrBusy
	}
	e := database.InTransaction(ctx, s.db, func(ctx context.Context) error {
		if c.Operation != "remove" {
			if _, e := s.lockEditable(ctx, c, version); e != nil {
				return e
			}
		}
		_, e := s.reserve(ctx, c, version, actor, "channel_connection_removal_requested")
		return e
	})
	if e != nil {
		return c, e
	}
	c, e = s.Get(ctx, c.TenantID, c.ID)
	if e != nil {
		return c, e
	}
	if c.Kind != "telegram" || localOnly {
		note := ""
		if c.Kind == "telegram" {
			note = "仅移除了本服务连接；请在 Telegram 或新的接收服务中处理原回调"
		}
		return s.finishRemoval(ctx, c, actor, note)
	}
	token, e := s.vault.Resolve(ctx, c.TenantID, secret.TelegramBot, c.Credential)
	if e == nil {
		var hURL string
		h, e2 := s.telegram.Webhook(ctx, token)
		e = e2
		hURL = h.URL
		if e == nil && hURL != c.URL {
			return s.finishRemoval(ctx, c, actor, "")
		}
		if e == nil {
			e = s.telegram.Unregister(ctx, token)
		}
	}
	if e != nil {
		s.failed(c, "移除结果未确认，请检查状态；Token 已失效时可选择仅移除本地连接")
		return c, e
	}
	return s.CheckRemoval(ctx, c, actor)
}

// CheckRemoval only reads the remote assignment. It cannot reactivate a route
// or repeat an external delete when a previous response was lost.
func (s *Store) CheckRemoval(ctx context.Context, c Connection, actor string) (Connection, error) {
	if c.Operation != "remove" {
		return c, ErrConflict
	}
	token, e := s.vault.Resolve(ctx, c.TenantID, secret.TelegramBot, c.Credential)
	if e != nil {
		return c, e
	}
	h, e := s.telegram.Webhook(ctx, token)
	if e != nil || h.URL == c.URL {
		s.failed(c, "尚未确认回调已移除，请检查或明确重试移除")
		return c, errors.New("尚未确认移除结果，没有自动重复操作")
	}
	return s.finishRemoval(ctx, c, actor, "")
}
