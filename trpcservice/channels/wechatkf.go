package channels

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// WeChat KF protocol constants (official docs 94670/94677/94679):
//   - text replies are capped at 2048 bytes and truncated server-side, so
//     longer answers are split on rune boundaries like WeCom;
//   - one sync_msg pull returns at most 1000 messages and has_more must be
//     honoured (msg_list may be empty while has_more is 1);
//   - the event token is valid for 10 minutes and lifts sync_msg rate limits;
//   - replies are only delivered inside the 48h / 5-message window opened by
//     the customer's latest message — a platform-side constraint we cannot
//     enforce locally, so window expiry surfaces as a send_msg errcode.
const (
	kfMaxTextBytes  = 2048
	kfSyncLimit     = 1000
	kfSyncMaxRounds = 10
)

// WeChatKf is the channel adapter for WeChat customer service (微信客服).
//
// Unlike WeCom, the callback only announces "there is news" (encrypted event
// kf_msg_or_event carrying Token + OpenKfId); actual messages are pulled with
// kf/sync_msg using a persisted cursor (proposal doc 3.3 pull model). The
// callback crypto is the shared official scheme: msg_signature =
// SHA1(sort(token, timestamp, nonce, encrypt)), AES-256-CBC, receiveid =
// corp_id — so it reuses the WeCom helpers.
//
// Outbound: streaming chunks are aggregated per conversation and flushed on
// Done through kf/send_msg, split at kfMaxTextBytes. The open_kfid to answer
// on is remembered per session from the callback that established it.
type WeChatKf struct {
	lookup  func(tenantID string) (*tenant.WeChatKfBinding, bool)
	apiBase string
	client  *http.Client
	bootAt  int64 // unix time; guards against replaying 3 days of history on a fresh cursor

	mu      sync.Mutex
	tokens  map[string]*wecomToken      // key: corp_id (own cache: the kf secret differs from the WeCom app secret)
	cursors map[string]string           // key: tenant_id + ":" + open_kfid -> next_cursor
	kfIDs   map[string]string           // key: SessionID() -> open_kfid to reply on
	bufs    map[string]*strings.Builder // key: SessionID(), pending aggregated reply
	state   coordination.StateStore
}

// WithStateStore persists sync cursors outside the process. Use the shared
// Redis coordinator in production so callback ownership can move or restart
// without replaying the customer-service backlog.
func (k *WeChatKf) WithStateStore(state coordination.StateStore) *WeChatKf {
	k.state = state
	return k
}

// NewWeChatKf builds the WeChat KF adapter. lookup resolves the per-tenant
// binding; tenants without one reject callbacks with a clear error.
func NewWeChatKf(lookup func(tenantID string) (*tenant.WeChatKfBinding, bool)) *WeChatKf {
	return &WeChatKf{
		lookup:  lookup,
		apiBase: wecomDefaultAPIBase,
		client:  &http.Client{Timeout: 10 * time.Second},
		bootAt:  time.Now().Unix(),
		tokens:  make(map[string]*wecomToken),
		cursors: make(map[string]string),
		kfIDs:   make(map[string]string),
		bufs:    make(map[string]*strings.Builder),
	}
}

// Type implements Adapter.
func (k *WeChatKf) Type() Type { return TypeWeChatKF }

// kfEvent is the decrypted callback payload: a notification that open_kfid
// has new messages waiting for sync_msg, not the messages themselves.
type kfEvent struct {
	XMLName    xml.Name `xml:"xml"`
	ToUserName string   `xml:"ToUserName"`
	CreateTime int64    `xml:"CreateTime"`
	MsgType    string   `xml:"MsgType"`
	Event      string   `xml:"Event"`
	Token      string   `xml:"Token"`
	OpenKfId   string   `xml:"OpenKfId"`
}

// Callback implements Adapter: verifies the shared-scheme signature, answers
// the GET probe, ACKs POST events with "success", then pulls the announced
// messages with sync_msg and returns them as one ordered batch.
func (k *WeChatKf) Callback(rw http.ResponseWriter, r *http.Request) ([]*InboundMessage, error) {
	tenantID := strings.Trim(strings.TrimPrefix(r.URL.Path, "/callback/"+string(TypeWeChatKF)+"/"), "/")
	b, ok := k.lookup(tenantID)
	if !ok {
		return nil, fmt.Errorf("tenant %q has no wechat_kf binding", tenantID)
	}
	q := r.URL.Query()
	sig, ts, nonce := q.Get("msg_signature"), q.Get("timestamp"), q.Get("nonce")

	if r.Method == http.MethodGet { // URL 验证：解密 echostr 并原样回显明文
		echo := q.Get("echostr")
		if weComSign(b.Token, ts, nonce, echo) != sig {
			return nil, errors.New("wechat_kf probe signature mismatch")
		}
		plain, err := k.decrypt(b, echo)
		if err != nil {
			return nil, err
		}
		_, _ = io.WriteString(rw, plain)
		return nil, nil
	}
	if r.Method != http.MethodPost {
		return nil, errors.New("wechat_kf callback requires GET or POST")
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	var env wecomEnvelope // same encrypted XML envelope as WeCom
	if err := xml.Unmarshal(body, &env); err != nil || env.Encrypt == "" {
		return nil, fmt.Errorf("parse callback xml: %v", err)
	}
	if weComSign(b.Token, ts, nonce, env.Encrypt) != sig {
		return nil, errors.New("wechat_kf signature mismatch")
	}
	plain, err := k.decrypt(b, env.Encrypt)
	if err != nil {
		return nil, err
	}
	var ev kfEvent
	if err := xml.Unmarshal([]byte(plain), &ev); err != nil {
		return nil, fmt.Errorf("parse decrypted event: %w", err)
	}
	_, _ = io.WriteString(rw, "success") // ACK 先落，拉取失败靠下次事件补齐
	if ev.MsgType != "event" || ev.Event != "kf_msg_or_event" || ev.OpenKfId == "" {
		return nil, nil // 非客服消息事件 v1 只 ACK 不处理
	}
	return k.syncMessages(r.Context(), tenantID, b, &ev)
}

// kfSyncRequest / kfSyncResponse mirror the kf/sync_msg wire format.
type kfSyncRequest struct {
	Cursor   string `json:"cursor,omitempty"`
	Token    string `json:"token,omitempty"`
	Limit    int    `json:"limit"`
	OpenKfID string `json:"open_kfid"`
}

type kfSyncResponse struct {
	ErrCode    int         `json:"errcode"`
	ErrMsg     string      `json:"errmsg"`
	NextCursor string      `json:"next_cursor"`
	HasMore    int         `json:"has_more"`
	MsgList    []kfSyncMsg `json:"msg_list"`
}

type kfSyncMsg struct {
	Msgid          string `json:"msgid"`
	OpenKfID       string `json:"open_kfid"`
	ExternalUserID string `json:"external_userid"`
	SendTime       int64  `json:"send_time"`
	Origin         uint32 `json:"origin"` // 3-客户 4-系统事件 5-接待人员
	MsgType        string `json:"msgtype"`
	Text           struct {
		Content string `json:"content"`
	} `json:"text"`
}

// syncMessages pulls every pending message for the event's open_kfid,
// following next_cursor while has_more is set. Only customer text messages
// (msgtype=text, origin=3) become inbound; events and servicer replies are
// skipped. The cursor is saved after each successful round, so a later
// failure never loses messages — the next event resumes from it.
func (k *WeChatKf) syncMessages(ctx context.Context, tenantID string, b *tenant.WeChatKfBinding, ev *kfEvent) ([]*InboundMessage, error) {
	token, err := k.accessToken(ctx, b)
	if err != nil {
		return nil, err
	}
	key := tenantID + ":" + ev.OpenKfId
	k.mu.Lock()
	cursor := k.cursors[key]
	k.mu.Unlock()
	if cursor == "" && k.state != nil {
		stored, err := k.state.Get(ctx, "wechat-kf-cursor:"+key)
		if err != nil {
			return nil, fmt.Errorf("wechat_kf load cursor: %w", err)
		}
		cursor = stored
	}
	// With no cursor the server replays up to 3 days of history; on a fresh
	// boot we only accept messages newer than the process start.
	fresh := cursor == ""

	var out []*InboundMessage
	for round := 0; round < kfSyncMaxRounds; round++ {
		req := kfSyncRequest{Cursor: cursor, Token: ev.Token, Limit: kfSyncLimit, OpenKfID: ev.OpenKfId}
		body, err := json.Marshal(req)
		if err != nil {
			return nil, err
		}
		u := k.apiBase + "/cgi-bin/kf/sync_msg?access_token=" + url.QueryEscape(token)
		var resp kfSyncResponse
		err = apiDoJSON(ctx, k.client, http.MethodPost, u, body, &resp)
		if err == nil && resp.ErrCode != 0 {
			err = &wecomAPIError{ErrCode: resp.ErrCode, ErrMsg: resp.ErrMsg}
		}
		if err != nil {
			if round == 0 {
				return nil, fmt.Errorf("wechat_kf sync_msg: %w", err)
			}
			return out, nil // partial batch; cursor already saved, next event resumes
		}
		if resp.NextCursor != "" {
			cursor = resp.NextCursor
			if k.state != nil {
				if err := k.state.Set(ctx, "wechat-kf-cursor:"+key, cursor); err != nil {
					return out, fmt.Errorf("wechat_kf save cursor: %w", err)
				}
			}
			k.mu.Lock()
			k.cursors[key] = cursor
			k.mu.Unlock()
		}
		for _, m := range resp.MsgList {
			if m.MsgType != "text" || m.Origin != 3 || m.Text.Content == "" {
				continue
			}
			if fresh && m.SendTime < k.bootAt {
				continue // skip pre-boot history on the first pull
			}
			in := &InboundMessage{
				TenantID: tenantID,
				Channel:  TypeWeChatKF,
				UserID:   m.ExternalUserID,
				MsgID:    m.Msgid,
				Text:     m.Text.Content,
			}
			k.mu.Lock()
			k.kfIDs[in.SessionID()] = ev.OpenKfId
			k.mu.Unlock()
			out = append(out, in)
		}
		if resp.HasMore == 0 {
			break
		}
	}
	return out, nil
}

// Send implements Adapter: aggregates streaming chunks per conversation and
// flushes the reply as (possibly split) kf/send_msg texts when Done arrives.
func (k *WeChatKf) Send(ctx context.Context, msg *OutboundMessage) error {
	b, ok := k.lookup(msg.Target.TenantID)
	if !ok {
		return fmt.Errorf("tenant %q has no wechat_kf binding", msg.Target.TenantID)
	}

	key := msg.Target.SessionID()
	k.mu.Lock()
	if msg.Text != "" {
		buf, ok := k.bufs[key]
		if !ok {
			buf = &strings.Builder{}
			k.bufs[key] = buf
		}
		buf.WriteString(msg.Text)
	}
	var full string
	if msg.Done {
		if buf, ok := k.bufs[key]; ok {
			full = buf.String()
			delete(k.bufs, key)
		}
	}
	kfID := k.kfIDs[key]
	k.mu.Unlock()
	if full == "" {
		return nil
	}
	if kfID == "" {
		// kf conversations are always customer-initiated, so a callback has
		// normally recorded the open_kfid before any reply is due.
		return fmt.Errorf("wechat_kf: no open_kfid recorded for session %q", key)
	}

	if err := k.flushOutbox(ctx, b, key, msg.Target.UserID, kfID); err != nil {
		return err
	}
	if err := k.sendFull(ctx, b, msg.Target.UserID, kfID, full); err != nil {
		k.saveOutbox(ctx, key, full)
		return err
	}
	return nil
}

func (k *WeChatKf) sendFull(ctx context.Context, b *tenant.WeChatKfBinding, toUser, kfID, full string) error {
	token, err := k.accessToken(ctx, b)
	if err != nil {
		return err
	}
	for _, part := range splitUTF8(full, kfMaxTextBytes) {
		if err := k.sendText(ctx, b, token, toUser, kfID, part); err != nil {
			return err
		}
	}
	return nil
}

func (k *WeChatKf) flushOutbox(ctx context.Context, b *tenant.WeChatKfBinding, key, toUser, kfID string) error {
	if k.state == nil {
		return nil
	}
	pending, err := k.state.Get(ctx, "wechat-kf-outbox:"+key)
	if err != nil || pending == "" {
		return err
	}
	if err := k.sendFull(ctx, b, toUser, kfID, pending); err != nil {
		return err
	}
	return k.state.Delete(ctx, "wechat-kf-outbox:"+key)
}

func (k *WeChatKf) saveOutbox(ctx context.Context, key, full string) {
	if k.state != nil {
		_ = k.state.Set(ctx, "wechat-kf-outbox:"+key, full)
	}
}

func (k *WeChatKf) decrypt(b *tenant.WeChatKfBinding, b64 string) (string, error) {
	aesKey, err := weComAESKey(b.EncodingAESKey)
	if err != nil {
		return "", err
	}
	return weComDecrypt(aesKey, b64, b.CorpID)
}

// accessToken caches the corp token per adapter instance; the WeChat KF
// secret differs from the WeCom app secret, so this cache is deliberately
// separate from the WeCom adapter's.
func (k *WeChatKf) accessToken(ctx context.Context, b *tenant.WeChatKfBinding) (string, error) {
	k.mu.Lock()
	if t, ok := k.tokens[b.CorpID]; ok && time.Now().Before(t.expiresAt) {
		k.mu.Unlock()
		return t.token, nil
	}
	k.mu.Unlock()

	token, ttl, err := fetchAccessToken(ctx, k.client, k.apiBase, b.CorpID, b.Secret)
	if err != nil {
		return "", err
	}
	k.mu.Lock()
	k.tokens[b.CorpID] = &wecomToken{token: token, expiresAt: time.Now().Add(ttl)}
	k.mu.Unlock()
	return token, nil
}

// sendText posts one kf text message, retrying once with a fresh token when
// the cached one is rejected (40014/42001).
func (k *WeChatKf) sendText(ctx context.Context, b *tenant.WeChatKfBinding, token, toUser, kfID, content string) error {
	err := k.postMessage(ctx, token, toUser, kfID, content)
	var apiErr *wecomAPIError
	if errors.As(err, &apiErr) && apiErr.tokenInvalid() {
		k.mu.Lock()
		delete(k.tokens, b.CorpID)
		k.mu.Unlock()
		fresh, ferr := k.accessToken(ctx, b)
		if ferr != nil {
			return ferr
		}
		return k.postMessage(ctx, fresh, toUser, kfID, content)
	}
	return err
}

func (k *WeChatKf) postMessage(ctx context.Context, token, toUser, kfID, content string) error {
	payload := struct {
		ToUser   string `json:"touser"`
		OpenKfID string `json:"open_kfid"`
		MsgType  string `json:"msgtype"`
		Text     struct {
			Content string `json:"content"`
		} `json:"text"`
	}{ToUser: toUser, OpenKfID: kfID, MsgType: "text"}
	payload.Text.Content = content
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	u := k.apiBase + "/cgi-bin/kf/send_msg?access_token=" + url.QueryEscape(token)
	var out struct {
		ErrCode int    `json:"errcode"`
		ErrMsg  string `json:"errmsg"`
		MsgID   string `json:"msgid"`
	}
	if err := apiDoJSON(ctx, k.client, http.MethodPost, u, body, &out); err != nil {
		return err
	}
	if out.ErrCode != 0 {
		return &wecomAPIError{ErrCode: out.ErrCode, ErrMsg: out.ErrMsg}
	}
	return nil
}

// cursorOf exposes the persisted sync cursor of one tenant/open_kfid pair
// for tests and diagnostics.
func (k *WeChatKf) cursorOf(tenantID, openKfID string) string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.cursors[tenantID+":"+openKfID]
}

// kfIDOf exposes the open_kfid recorded for one session for tests.
func (k *WeChatKf) kfIDOf(sessionID string) string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.kfIDs[sessionID]
}
