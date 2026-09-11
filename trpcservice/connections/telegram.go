package connections

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/telegram"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecommcp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

func (s *Store) PrepareTelegram(ctx context.Context, t, app, id, token, actor string) (Connection, error) {
	if e := s.app(ctx, t, app); e != nil {
		return Connection{}, e
	}
	if !identifier.MatchString(id) {
		return Connection{}, ErrConflict
	}
	if previous, e := s.Get(ctx, t, id); e == nil {
		if previous.AppID == app && previous.Kind == "telegram" && previous.Status != "removed" {
			return previous, nil
		}
		return Connection{}, ErrConflict
	} else if !errors.Is(e, controlplane.ErrNotFound) {
		return Connection{}, e
	}
	public, e := validPublicURL(s.PublicURL(ctx))
	if e != nil {
		return Connection{}, errors.New("请先设置服务器的公网地址，再连接 Telegram")
	}
	bot, hook, e := s.telegram.Inspect(ctx, strings.TrimSpace(token))
	if e != nil {
		return Connection{}, e
	}
	var used bool
	if s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM channel_binding WHERE channel_type='telegram' AND retired_at IS NULL AND config->>'bot_user_id'=$1) OR EXISTS(SELECT 1 FROM channel_connection WHERE channel_type='telegram' AND status<>'removed' AND account_key=$1)`, strconv.FormatInt(bot.ID, 10)).Scan(&used) != nil {
		return Connection{}, ErrUnavailable
	}
	if used {
		return Connection{}, errors.New("这个机器人已在本服务中配置，请从连接列表管理，或换一个机器人")
	}
	c := Connection{ID: id, TenantID: t, AppID: app, Kind: "telegram", Account: strconv.FormatInt(bot.ID, 10), Name: "@" + bot.Username, Status: "draft", RemoteHash: hash(hook.URL)}
	if hook.URL != "" {
		c.Status = "needs_confirmation"
	}
	e = database.InTransaction(ctx, s.db, func(ctx context.Context) error {
		var e error
		c.Credential, e = s.vault.Put(ctx, t, []string{secret.TelegramBot, secret.TelegramMedia}, strings.TrimSpace(token))
		if e != nil {
			return e
		}
		c.Webhook, e = s.vault.Put(ctx, t, []string{secret.TelegramWebhook}, randomText())
		if e != nil {
			return e
		}
		config := encode(map[string]any{"bot_token_ref": c.Credential, "webhook_secret_ref": c.Webhook, "bot_user_id": bot.ID, "bot_username": bot.Username, "ignore_bot_messages": true, "require_mention": true, "groups_enabled": false, "allowed_chat_ids": []int64{}})
		b, e := s.newBinding(ctx, c, config, "disabled")
		if e != nil {
			return safe(e)
		}
		c.BindingID = b.ID
		c.URL = public + "/callbacks/telegram/" + b.CallbackKey
		if _, e = s.sql(ctx).ExecContext(ctx, `INSERT INTO channel_connection(tenant_id,connection_id,app_id,channel_type,account_key,display_name,binding_id,credential_ref,webhook_ref,callback_url,remote_hash,status) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, t, id, app, c.Kind, c.Account, c.Name, c.BindingID, c.Credential, c.Webhook, c.URL, c.RemoteHash, c.Status); e != nil {
			return ErrConflict
		}
		return s.record(ctx, c, actor, "channel_connection_prepared")
	})
	if e != nil {
		return Connection{}, e
	}
	return s.Get(ctx, t, id)
}

func (s *Store) ActivateTelegram(ctx context.Context, c Connection, version int64, confirm bool, actor string) (Connection, error) {
	if c.Kind != "telegram" || c.Status == "removed" || c.Operation == "remove" {
		return c, ErrConflict
	}
	if e := s.app(ctx, c.TenantID, c.AppID); e != nil {
		return c, e
	}
	if c.Status == "connected" {
		return c, nil
	}
	if c.Version != version {
		return c, ErrConflict
	}
	if c.Status == "unknown" || c.Status == "connecting" {
		return c, errors.New("请先检查上次连接结果，避免重复修改回调")
	}
	token, e := s.vault.Resolve(ctx, c.TenantID, secret.TelegramBot, c.Credential)
	if e != nil {
		return c, e
	}
	hook, e := s.telegram.Webhook(ctx, token)
	if e != nil {
		return c, e
	}
	if hook.URL != c.URL && hash(hook.URL) != c.RemoteHash {
		return s.captureRemote(ctx, c, hook)
	}
	if hook.URL != "" && hook.URL != c.URL && !confirm {
		return c, ErrConfirm
	}
	c, e = s.reserve(ctx, c, version, actor, "channel_connection_activation_requested")
	if e != nil {
		return c, e
	}
	key, e := s.vault.Resolve(ctx, c.TenantID, secret.TelegramWebhook, c.Webhook)
	if e != nil {
		s.failed(c, "未能读取校验密钥，请检查连接状态")
		return c, e
	}
	if e = s.telegram.Register(ctx, token, c.URL, key); e != nil {
		s.failed(c, "未能确认回调设置，请点击检查连接")
		return c, e
	}
	hook, e = s.telegram.Webhook(ctx, token)
	if e != nil || hook.URL != c.URL {
		s.failed(c, "回调设置尚未确认，请点击检查连接")
		return c, errors.New("请检查连接状态，暂时不要再次提交")
	}
	if e = s.finish(ctx, c, actor); e != nil {
		s.failed(c, "Telegram 已接受设置，正在等待本地确认")
		return c, e
	}
	return s.Get(ctx, c.TenantID, c.ID)
}
func (s *Store) finish(ctx context.Context, c Connection, actor string) error {
	return database.InTransaction(ctx, s.db, func(ctx context.Context) error {
		// Reserve the connection version before changing the binding. Both changes
		// roll back if another operation has already paused/replaced this attempt.
		if e := s.outcome(ctx, c, "connected", ""); e != nil {
			return e
		}
		b, e := s.repo.GetChannelBinding(ctx, c.TenantID, c.BindingID)
		if e != nil {
			return e
		}
		if c.Kind == "wecom_mcp" {
			cfg, e := wecommcp.ParseBinding(b)
			if e != nil {
				return e
			}
			groups := slices.Clone(cfg.AllowedChatIDs)
			slices.Sort(groups)
			for _, chat := range groups {
				if len(cfg.GroupGrants[chat].Members) > 0 {
					if e := s.checkWeComRoute(ctx, c, chat, cfg.MentionPrefix); e != nil {
						return e
					}
				}
			}
		}
		if b.Status != "active" {
			if _, e = s.repo.UpdateChannelBinding(ctx, b.TenantID, b.ID, b.Config, "active", b.Version); e != nil {
				return e
			}
		}
		return s.record(ctx, c, actor, "channel_connection_activated")
	})
}
func (s *Store) captureRemote(ctx context.Context, c Connection, h telegram.WebhookInfo) (Connection, error) {
	status := "draft"
	if h.URL != "" && h.URL != c.URL {
		status = "needs_confirmation"
	}
	result, e := s.sql(ctx).ExecContext(ctx, `UPDATE channel_connection SET remote_hash=$4,status=$5,version=version+1,updated_at=now(),message='' WHERE tenant_id=$1 AND connection_id=$2 AND version=$3 AND (busy_until IS NULL OR busy_until<now())`, c.TenantID, c.ID, c.Version, hash(h.URL), status)
	if e != nil {
		return c, safe(e)
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return c, ErrConflict
	}
	return s.Get(ctx, c.TenantID, c.ID)
}
func (s *Store) CheckTelegram(ctx context.Context, c Connection, actor string) (Connection, error) {
	if c.BusyUntil != nil && c.BusyUntil.After(time.Now()) {
		return c, ErrBusy
	}
	if c.Operation == "remove" {
		return s.CheckRemoval(ctx, c, actor)
	}
	token, e := s.vault.Resolve(ctx, c.TenantID, secret.TelegramBot, c.Credential)
	if e != nil {
		return c, e
	}
	hook, e := s.telegram.Webhook(ctx, token)
	if e != nil {
		return c, e
	}
	if hook.URL == c.URL {
		if c.Status == "unknown" || c.Status == "connecting" {
			if e = s.finish(ctx, c, actor); e != nil {
				return c, e
			}
		}
		return s.Get(ctx, c.TenantID, c.ID)
	}
	// An uncertain request may still be in flight upstream. Do not clear that
	// uncertainty merely because a read currently shows the previous URL.
	if c.Status == "unknown" || c.Status == "connecting" {
		if c.Status == "connecting" {
			if e := s.outcome(ctx, c, "unknown", "上次连接结果未确认，请检查或手动重试"); e != nil {
				return c, e
			}
		}
		return c, errors.New("尚未确认上一次回调设置，请稍后再检查；没有自动重新注册")
	}
	return s.captureRemote(ctx, c, hook)
}

// RetryTelegram is an explicit, audited retry of the same idempotent webhook
// assignment. A status check never calls setWebhook on its own.
func (s *Store) RetryTelegram(ctx context.Context, c Connection, version int64, actor string) (Connection, error) {
	if c.Kind != "telegram" || c.Version != version || c.Operation == "remove" || c.Status == "removed" {
		return c, ErrConflict
	}
	if c.BusyUntil != nil && c.BusyUntil.After(time.Now()) {
		return c, ErrBusy
	}
	token, e := s.vault.Resolve(ctx, c.TenantID, secret.TelegramBot, c.Credential)
	if e != nil {
		return c, e
	}
	hook, e := s.telegram.Webhook(ctx, token)
	if e != nil {
		return c, e
	}
	if e = s.record(ctx, c, actor, "channel_connection_retry_authorized"); e != nil {
		return c, e
	}
	c, e = s.captureRemote(ctx, c, hook)
	if e != nil {
		return c, e
	}
	return s.ActivateTelegram(ctx, c, c.Version, true, actor)
}
func (s *Store) Pause(ctx context.Context, c Connection, version int64, actor string) (Connection, error) {
	if c.Version != version || c.Status == "removed" || c.Operation == "remove" {
		return c, ErrConflict
	}
	if c.BusyUntil != nil && c.BusyUntil.After(time.Now()) {
		return c, ErrBusy
	}
	e := database.InTransaction(ctx, s.db, func(ctx context.Context) error {
		if e := s.outcome(ctx, c, "paused", ""); e != nil {
			return e
		}
		if c.BindingID != "" {
			b, e := s.repo.GetChannelBinding(ctx, c.TenantID, c.BindingID)
			if e != nil {
				return e
			}
			if _, e = s.repo.UpdateChannelBinding(ctx, b.TenantID, b.ID, b.Config, "disabled", b.Version); e != nil {
				return e
			}
		}
		return s.record(ctx, c, actor, "channel_connection_paused")
	})
	if e != nil {
		return c, e
	}
	return s.Get(ctx, c.TenantID, c.ID)
}

type Group struct {
	ID       string                 `json:"id"`
	Name     string                 `json:"name"`
	Selected bool                   `json:"selected"`
	Members  []wecommcp.MemberGrant `json:"members,omitempty"`
}

func (s *Store) Groups(ctx context.Context, c Connection) ([]Group, error) {
	result := []Group{}
	rows, e := s.db.QueryContext(ctx, `SELECT chat_id,display_name FROM channel_connection_group WHERE tenant_id=$1 AND connection_id=$2 ORDER BY display_name,chat_id LIMIT 200`, c.TenantID, c.ID)
	if e != nil {
		return nil, safe(e)
	}
	defer func() { _ = rows.Close() }()
	selected := map[string]bool{}
	if c.BindingID != "" {
		b, e := s.repo.GetChannelBinding(ctx, c.TenantID, c.BindingID)
		if e != nil {
			return nil, e
		}
		var config struct {
			Chats []int64 `json:"allowed_chat_ids"`
		}
		if c.Kind == "telegram" {
			_ = json.Unmarshal(b.Config, &config)
			for _, id := range config.Chats {
				selected[strconv.FormatInt(id, 10)] = true
			}
		}
	}
	for rows.Next() {
		var g Group
		if rows.Scan(&g.ID, &g.Name) != nil {
			return nil, ErrUnavailable
		}
		g.Selected = selected[g.ID]
		result = append(result, g)
	}
	if e := rows.Err(); e != nil {
		return nil, safe(e)
	}
	return s.decorateWeComGroups(ctx, c, result)
}
func (s *Store) SetTelegramGroups(ctx context.Context, c Connection, version int64, ids []string, actor string) (Connection, error) {
	if c.BusyUntil != nil && c.BusyUntil.After(time.Now()) {
		return c, ErrBusy
	}
	if c.Kind != "telegram" || len(ids) > 20 || c.Version != version {
		return c, ErrConflict
	}
	available, e := s.Groups(ctx, c)
	if e != nil {
		return c, e
	}
	known := map[string]bool{}
	for _, g := range available {
		known[g.ID] = true
	}
	values := []int64{}
	for _, id := range ids {
		if !known[id] {
			return c, errors.New("群列表已变化，请刷新后重新选择")
		}
		delete(known, id)
		n, e := strconv.ParseInt(id, 10, 64)
		if e != nil || n == 0 {
			return c, ErrConflict
		}
		values = append(values, n)
	}
	e = database.InTransaction(ctx, s.db, func(ctx context.Context) error {
		if e := s.outcome(ctx, c, c.Status, ""); e != nil {
			return e
		}
		b, e := s.repo.GetChannelBinding(ctx, c.TenantID, c.BindingID)
		if e != nil {
			return e
		}
		var cfg map[string]any
		if json.Unmarshal(b.Config, &cfg) != nil {
			return ErrUnavailable
		}
		cfg["groups_enabled"] = len(values) > 0
		cfg["allowed_chat_ids"] = values
		if _, e = s.repo.UpdateChannelBinding(ctx, b.TenantID, b.ID, encode(cfg), b.Status, b.Version); e != nil {
			return e
		}
		return s.record(ctx, c, actor, "channel_connection_groups_updated")
	})
	if e != nil {
		return c, e
	}
	return s.Get(ctx, c.TenantID, c.ID)
}
func cleanName(value string) string {
	value = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, value)
	r := []rune(value)
	if len(r) > 100 {
		r = r[:100]
	}
	return string(r)
}
func (s *Store) ObserveTelegram(ctx context.Context, b controlplane.ChannelBinding, id int64, title string) error {
	if s == nil || !strings.HasPrefix(b.SecretRef, "managed://") {
		return nil
	}
	_, e := s.db.ExecContext(ctx, `UPDATE channel_connection SET last_received_at=now() WHERE tenant_id=$1 AND binding_id=$2 AND channel_type='telegram'`, b.TenantID, b.ID)
	if e != nil {
		return safe(e)
	}
	if id == 0 {
		return nil
	}
	title = cleanName(title)
	if title == "" {
		title = "未命名群"
	}
	_, e = s.db.ExecContext(ctx, `INSERT INTO channel_connection_group(tenant_id,connection_id,chat_id,display_name) SELECT tenant_id,connection_id,$3,$4 FROM channel_connection c WHERE tenant_id=$1 AND binding_id=$2 AND (SELECT count(*) FROM channel_connection_group g WHERE g.tenant_id=c.tenant_id AND g.connection_id=c.connection_id)<200 ON CONFLICT(tenant_id,connection_id,chat_id) DO UPDATE SET display_name=EXCLUDED.display_name,seen_at=now()`, b.TenantID, b.ID, strconv.FormatInt(id, 10), title)
	return safe(e)
}
