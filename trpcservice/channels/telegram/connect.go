package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"time"
)

type BotInfo struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
	Name     string `json:"first_name"`
	IsBot    bool   `json:"is_bot"`
}
type WebhookInfo struct {
	URL     string `json:"url"`
	Pending int    `json:"pending_update_count"`
}
type Connector struct{ client *http.Client }

func NewConnector(client *http.Client) *Connector {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	copy := *client
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Connector{&copy}
}

var tokenPattern = regexp.MustCompile(`^[0-9]{1,20}:[A-Za-z0-9_-]{20,}$`)
var usernamePattern = regexp.MustCompile(`^[A-Za-z0-9_]{5,32}$`)

func (c *Connector) call(ctx context.Context, token, method string, body any, output any) error {
	if !tokenPattern.MatchString(token) {
		return errors.New("请完整复制 BotFather 给出的 Token")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	raw, e := json.Marshal(body)
	if e != nil {
		return errors.New("无法准备 Telegram 请求")
	}
	r, e := http.NewRequestWithContext(ctx, "POST", defaultAPIBase+"/bot"+token+"/"+method, bytes.NewReader(raw))
	if e != nil {
		return errors.New("无法准备 Telegram 请求")
	}
	r.Header.Set("Content-Type", "application/json")
	resp, e := c.client.Do(r)
	if e != nil {
		return errors.New("未能连接 Telegram，请稍后重试")
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == 401 || resp.StatusCode == 404 {
		return errors.New("机器人 Token 无效，请检查是否复制完整或已被撤销")
	}
	var result struct {
		OK     bool            `json:"ok"`
		Result json.RawMessage `json:"result"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(&result) != nil || resp.StatusCode != 200 || !result.OK {
		return errors.New("未能完成 Telegram 请求，请检查连接设置或稍后重试")
	}
	if output != nil && json.Unmarshal(result.Result, output) != nil {
		return errors.New("收到的 Telegram 数据不完整")
	}
	return nil
}
func (c *Connector) Inspect(ctx context.Context, token string) (BotInfo, WebhookInfo, error) {
	var bot BotInfo
	var hook WebhookInfo
	if e := c.call(ctx, token, "getMe", struct{}{}, &bot); e != nil {
		return bot, hook, e
	}
	if !bot.IsBot || bot.ID <= 0 || !usernamePattern.MatchString(bot.Username) {
		return bot, hook, errors.New("这个 Token 没有对应到有效的机器人")
	}
	e := c.call(ctx, token, "getWebhookInfo", struct{}{}, &hook)
	return bot, hook, e
}
func (c *Connector) Webhook(ctx context.Context, token string) (WebhookInfo, error) {
	var h WebhookInfo
	e := c.call(ctx, token, "getWebhookInfo", struct{}{}, &h)
	return h, e
}
func (c *Connector) Register(ctx context.Context, token, url, secret string) error {
	var accepted bool
	if e := c.call(ctx, token, "setWebhook", map[string]any{"url": url, "secret_token": secret, "allowed_updates": []string{"message", "edited_message", "my_chat_member"}, "drop_pending_updates": false}, &accepted); e != nil {
		return e
	}
	if !accepted {
		return errors.New("未收到 Telegram 的回调设置确认")
	}
	return nil
}

// Unregister preserves pending updates. Callers verify ownership of the current
// webhook immediately before this explicit operation; Telegram offers no CAS.
func (c *Connector) Unregister(ctx context.Context, token string) error {
	var accepted bool
	if e := c.call(ctx, token, "deleteWebhook", map[string]any{"drop_pending_updates": false}, &accepted); e != nil {
		return e
	}
	if !accepted {
		return errors.New("未收到 Telegram 的移除确认，请检查连接状态")
	}
	return nil
}
