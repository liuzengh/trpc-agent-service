// Package wecom implements the WeCom channel adapter.
//
// Inbound: the IM platform posts AES-encrypted XML callbacks; the adapter
// verifies the signature and decrypts via wxbizmsgcrypt, normalizes the
// message and hands it to the Handler, which
// acks within the 5-second window (sync ack + async consume).
//
// Outbound: replies go through the app message/send API (direct chats) or
// appchat/send (group chats), with the access_token cached in process and
// refreshed on expiry. Texts longer than the platform limit are split into
// sequential segments.
package wecom

import (
	"bytes"
	"container/list"
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

	"github.com/sbzhu/weworkapi_golang/wxbizmsgcrypt"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

// ChannelName is the channel identifier, stamped on every message this
// channel serves and keyed by in the channel_binding rows.
const ChannelName = "wecom"

// CallbackPath is the webhook path mounted on the platform mux; it must match
// the channel_binding.webhook_path row for tenant routing. Exported so a
// caller-supplied webhook_path can be validated against the routes the
// platform serves: a path no handler is mounted on answers 404 forever while
// the binding row itself looks healthy.
const CallbackPath = "/wecom/callback"

// defaultAPIBase is the WeCom API endpoint; overridable for tests.
const defaultAPIBase = "https://qyapi.weixin.qq.com"

// maxTextBytes is the platform limit for one text message;
// longer replies are split into sequential segments.
const maxTextBytes = 2048

// template_card field limits, in bytes. Over-long fields are rejected by the
// platform with an errcode, which the sender would retry forever, so
// cardPayload enforces the caps before sending.
const (
	maxCardTitleBytes = 128
	maxCardDescBytes  = 512
	maxCardURLBytes   = 1024
)

// tokenExpiryMargin refreshes the access token ahead of its stated TTL.
const tokenExpiryMargin = 5 * time.Minute

// maxCryptCacheEntries bounds the value-keyed crypt cache against unbounded
// growth under repeated key rotation.
const maxCryptCacheEntries = 32

// maxTokenCacheEntries bounds the identity-keyed access_token cache.
const maxTokenCacheEntries = 32

// Config holds the WeCom channel configuration. Secret material is carried
// as references and resolved through the SecretResolver, never logged.
type Config struct {
	CorpID    string // corp ID
	AgentID   int    // agent ID
	TokenRef  string // callback token secret ref
	AESKeyRef string // EncodingAESKey secret ref
	SecretRef string // corpsecret secret ref (for access_token)
	APIBase   string // default https://qyapi.weixin.qq.com
	// Media stores fetched media_id content; nil degrades
	// media messages to a placeholder text.
	Media channels.MediaStore
	// Bindings resolves the per-binding outbound identity (corp/agent/
	// secret) at Send time; nil keeps every reply on the env-global identity
	// above (the env-configured single-binding default).
	Bindings channels.BindingProvider
}

// Channel is the WeCom implementation of channels.Channel.
type Channel struct {
	cfg    Config
	secret config.SecretResolver
	client *http.Client
	// media is the artifact store for inbound media_id fetches; nil degrades
	// media messages to a placeholder text.
	media channels.MediaStore
	// bindings resolves the outbound identity of a binding-scoped reply; nil
	// keeps the env-global identity (the env-configured single-binding
	// default).
	bindings channels.BindingProvider

	// crypts caches one WXBizMsgCrypt per credential set
	// (corp|tokenRef|aesKeyRef): multi-tenant callbacks arrive with
	// per-binding refs and each verification needs the
	// matching crypt. Ref resolution rides the process-level cached
	// resolver, so rotation propagates within the cache TTL.
	cryptMu sync.Mutex
	crypts  map[string]*wxbizmsgcrypt.WXBizMsgCrypt

	// tokens caches one access_token per (corpID, secretRef) identity:
	// bindings of different corps — or different self-built apps within one
	// corp — each authenticate with their own secret, and a single global
	// entry would send every reply under the env identity. Keyed by REF, not
	// the resolved secret: the send hot path stays off the secret resolver,
	// and a rotation behind a ref is picked up when the cached token expires
	// or the platform rejects it (40014/42001 invalidates just that entry).
	tokenMu    sync.Mutex
	tokens     map[string]tokenEntry
	tokenOrder *list.List // front = most recently used identity
	// tokenFlights collapses concurrent misses for one identity into a
	// single gettoken call, taken off the cache lock: the HTTP call takes
	// seconds and a channel-wide mutex held across it would stall every
	// identity's sends.
	tokenFlights singleflight.Group
}

// tokenEntry is one cached access_token, its refresh deadline, and its
// position in the LRU order.
type tokenEntry struct {
	token  string
	expiry time.Time
	elem   *list.Element
}

// New creates the channel: the env callback token and AES key are resolved
// and validated at startup (fail fast on misconfiguration). They remain the
// single-binding default for the env-configured /wecom/callback path;
// per-binding credentials flow through CallbackHandler.
func New(cfg Config, resolver config.SecretResolver) (*Channel, error) {
	if cfg.CorpID == "" || cfg.AgentID == 0 {
		return nil, fmt.Errorf("wecom: CorpID and AgentID are required")
	}
	if cfg.APIBase == "" {
		cfg.APIBase = defaultAPIBase
	}
	ctx := context.Background()
	if _, err := resolver.Resolve(ctx, cfg.TokenRef); err != nil {
		return nil, fmt.Errorf("wecom: resolve token: %w", err)
	}
	if _, err := resolver.Resolve(ctx, cfg.AESKeyRef); err != nil {
		return nil, fmt.Errorf("wecom: resolve aes key: %w", err)
	}
	return &Channel{
		cfg:        cfg,
		secret:     resolver,
		client:     &http.Client{Timeout: 10 * time.Second},
		media:      cfg.Media,
		bindings:   cfg.Bindings,
		crypts:     map[string]*wxbizmsgcrypt.WXBizMsgCrypt{},
		tokens:     map[string]tokenEntry{},
		tokenOrder: list.New(),
	}, nil
}

// bindingConfig is the optional per-binding outbound identity in
// channel_binding.config: a binding whose replies must go out under its own
// corp / self-built app carries them here. Every field falls back to the
// env-global Config when absent, so an empty (or corp_id-only) config keeps
// the env-global single-identity behavior. corp_id doubles as the inbound
// crypt receiver id a callback verifies against.
type bindingConfig struct {
	CorpID    string `json:"corp_id"`
	AgentID   int    `json:"agent_id"`
	SecretRef string `json:"secret_ref"`
}

// ValidateBindingConfig is the admin API's write gate for wecom binding
// config. Every field is optional, but unknown fields are refused: the
// config jsonb lands verbatim in audit details, so e.g. a "secret" key would
// smuggle plaintext credentials into the audit trail while being silently
// ignored by the channel.
func ValidateBindingConfig(raw json.RawMessage) error {
	if len(raw) == 0 {
		return nil
	}
	var cfg bindingConfig
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return fmt.Errorf("wecom binding config only accepts corp_id, agent_id and secret_ref: %w", err)
	}
	return nil
}

// outboundID is the IM identity one reply goes out under.
type outboundID struct {
	corpID    string
	secretRef string
	agentID   int
}

// outboundIDFor resolves the identity for msg: the binding's own config when
// the message is binding-scoped, field by field falling back to the
// env-global identity (the env-configured default, and bindings that carry no
// per-binding outbound config). An unresolvable binding or config fails the
// send — replying under the global identity instead could deliver one
// tenant's message as another corp's bot.
func (c *Channel) outboundIDFor(ctx context.Context, msg channels.OutboundMessage) (outboundID, error) {
	id := outboundID{corpID: c.cfg.CorpID, secretRef: c.cfg.SecretRef, agentID: c.cfg.AgentID}
	if msg.BindingID == "" || c.bindings == nil {
		return id, nil
	}
	b, err := c.bindings.BindingByID(ctx, msg.BindingID)
	if err != nil {
		return outboundID{}, fmt.Errorf("wecom: resolve binding %s for outbound identity: %w", msg.BindingID, err)
	}
	var cfg bindingConfig
	if len(b.Config) > 0 {
		if err := json.Unmarshal(b.Config, &cfg); err != nil {
			return outboundID{}, fmt.Errorf("wecom: binding %s config: %w", msg.BindingID, err)
		}
	}
	if cfg.CorpID != "" {
		id.corpID = cfg.CorpID
	}
	if cfg.SecretRef != "" {
		id.secretRef = cfg.SecretRef
	}
	if cfg.AgentID != 0 {
		id.agentID = cfg.AgentID
	}
	return id, nil
}

// cryptFor resolves the credential set and returns its crypt. The cache is
// keyed by the RESOLVED values (not the refs): rotation behind a ref
// propagates within the secret resolver's cache TTL instead of living in a
// ref-keyed cache until process restart. Empty refs fall back to the
// env-configured single-binding default — only the env path may rely on that
// fallback; the dispatcher refuses binding-scoped callbacks with empty refs.
func (c *Channel) cryptFor(corpID, tokenRef, aesKeyRef string) (*wxbizmsgcrypt.WXBizMsgCrypt, error) {
	if corpID == "" {
		corpID = c.cfg.CorpID
	}
	if tokenRef == "" {
		tokenRef = c.cfg.TokenRef
	}
	if aesKeyRef == "" {
		aesKeyRef = c.cfg.AESKeyRef
	}
	ctx := context.Background()
	token, err := c.secret.Resolve(ctx, tokenRef)
	if err != nil {
		return nil, fmt.Errorf("wecom: resolve token %q: %w", tokenRef, err)
	}
	aesKey, err := c.secret.Resolve(ctx, aesKeyRef)
	if err != nil {
		return nil, fmt.Errorf("wecom: resolve aes key %q: %w", aesKeyRef, err)
	}
	key := corpID + "|" + token + "|" + aesKey
	c.cryptMu.Lock()
	defer c.cryptMu.Unlock()
	if crypt, ok := c.crypts[key]; ok {
		return crypt, nil
	}
	if len(c.crypts) >= maxCryptCacheEntries {
		// Rotations accumulate value-keyed entries; a wholesale reset bounds
		// the map without correctness impact (the next call rebuilds).
		c.crypts = map[string]*wxbizmsgcrypt.WXBizMsgCrypt{}
	}
	crypt := wxbizmsgcrypt.NewWXBizMsgCrypt(token, aesKey, corpID, wxbizmsgcrypt.XmlType)
	c.crypts[key] = crypt
	return crypt, nil
}

// Name implements channels.Channel.
func (c *Channel) Name() string { return ChannelName }

// RegisterRoutes implements channels.Channel: GET verifies the callback URL,
// POST receives encrypted messages. The path is the env-configured
// single-binding default; tenant bindings are served through
// CallbackHandler at /callback/{channel}/{binding_id}.
func (c *Channel) RegisterRoutes(mux *http.ServeMux, h channels.Handler) {
	handler, err := c.CallbackHandler(h, channels.BindingCredentials{
		CorpID:    c.cfg.CorpID,
		TokenRef:  c.cfg.TokenRef, // the env single-binding default, explicitly
		AESKeyRef: c.cfg.AESKeyRef,
	})
	if err != nil {
		// New already validated the env refs, so this is defensive: mount
		// nothing and let the path 404 instead of half-serving.
		plog.Errorf("wecom callback mount failed: %v", err)
		return
	}
	mux.HandleFunc(CallbackPath, handler)
}

// CallbackHandler implements channels.BindingAware. A binding-scoped callback
// must verify under the binding's OWN credentials: empty refs are an error
// here (the dispatcher answers 503), because falling back to the env-global
// keys would let whoever holds them forge this tenant's callbacks. The env
// path passes its refs explicitly via RegisterRoutes.
func (c *Channel) CallbackHandler(h channels.Handler, creds channels.BindingCredentials) (http.HandlerFunc, error) {
	if creds.TokenRef == "" || creds.AESKeyRef == "" {
		return nil, fmt.Errorf("wecom: binding %s lacks callback credentials", creds.BindingID)
	}
	crypt, err := c.cryptFor(creds.CorpID, creds.TokenRef, creds.AESKeyRef)
	if err != nil {
		return nil, err
	}
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			c.verifyURL(w, r, crypt)
		case http.MethodPost:
			c.receive(w, r, crypt, h)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}, nil
}

// verifyURL answers the platform's URL-registration challenge.
func (c *Channel) verifyURL(w http.ResponseWriter, r *http.Request, crypt *wxbizmsgcrypt.WXBizMsgCrypt) {
	q := r.URL.Query()
	echo, cerr := crypt.VerifyURL(q.Get("msg_signature"), q.Get("timestamp"), q.Get("nonce"), q.Get("echostr"))
	if cerr != nil {
		plog.Warnf("wecom url verification failed: %s", cerr.ErrMsg)
		http.Error(w, "verification failed", http.StatusForbidden)
		return
	}
	//nolint:gosec // G705: echo is the platform-challenge plaintext, verified and decrypted above
	_, _ = w.Write(echo)
}

// receive handles one encrypted message callback.
func (c *Channel) receive(w http.ResponseWriter, r *http.Request, crypt *wxbizmsgcrypt.WXBizMsgCrypt, h channels.Handler) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	q := r.URL.Query()
	plain, cerr := crypt.DecryptMsg(q.Get("msg_signature"), q.Get("timestamp"), q.Get("nonce"), body)
	if cerr != nil {
		// Two failures, two answers. An unverified callback (bad signature,
		// envelope that is not XML) is indistinguishable from internet junk: ack
		// it, redelivery cannot make it readable. A verified one we still cannot
		// read is the platform's and broken on our side — wrong or misshapen AES
		// key, ciphertext truncated in transit, receiver_id naming another corp —
		// so acking it would drop the user's message behind one warn line. 5xx
		// makes the platform redeliver, and fixing the credential recovers it.
		if cerr.ErrCode == wxbizmsgcrypt.ValidateSignatureError || cerr.ErrCode == wxbizmsgcrypt.ParseXmlError {
			plog.Warnf("wecom drop unverified callback: %s", cerr.ErrMsg)
			writeSuccess(w)
			return
		}
		plog.Errorf("wecom cannot read an authenticated callback (code %d): %s", cerr.ErrCode, cerr.ErrMsg)
		http.Error(w, "decrypt failed", http.StatusInternalServerError)
		return
	}

	var cm callbackMessage
	//nolint:gosec // G709: plain is the signature-verified, decrypted platform callback
	if err := xml.Unmarshal(plain, &cm); err != nil {
		plog.Warnf("wecom parse callback xml: %v", err)
		writeSuccess(w)
		return
	}
	// The signature never expires, so freshness is the only replay bound that
	// outlives the inbound dedup TTL. A stale callback is acked, not 5xx'd:
	// the platform would redeliver a capture we will always refuse.
	if channels.StaleCallback(cm.CreateTime, time.Now()) {
		plog.Warnf("wecom drop stale callback (create_time %d, msg %s)", cm.CreateTime, cm.MsgID)
		writeSuccess(w)
		return
	}
	// Only text, media, recall events and placeholder-able types enter the
	// pipeline; other events (enter_agent, ...) are acked and skipped.
	// Messages without a MsgId cannot be deduplicated — skip them too.
	switch {
	case cm.MsgType == "text" && cm.MsgID != "":
		// normal text path below
	case cm.MsgType == "event" && cm.Event == "revoke" && cm.MsgID != "":
		// Message recalled: the MsgId is the RECALLED message's
		// id, so the recall gets its own dedup namespace; the guardrail audits
		// it and marks the session state instead of running the model.
		msg := c.baseMessage(r, &cm)
		msg.Type = channels.TypeRecall
		msg.MsgID = "recall:" + cm.MsgID
		c.hand(ctxOf(r), w, h, msg, cm.MsgID)
		return
	case mediaTypes[cm.MsgType] != "" && cm.MsgID != "":
		c.receiveMedia(r, w, h, &cm)
		return
	case placeholderTypes[cm.MsgType] != "" && cm.MsgID != "":
		// No retrievable content (video/location/link): a placeholder text
		// lets the agent answer "not supported yet" instead of the message
		// vanishing silently.
		msg := c.baseMessage(r, &cm)
		msg.Type = channels.TypeText
		msg.Text = "[" + placeholderTypes[cm.MsgType] + "]"
		c.hand(ctxOf(r), w, h, msg, cm.MsgID)
		return
	default:
		writeSuccess(w)
		return
	}

	msg := channels.InboundMessage{
		Channel:     c.Name(),
		MsgID:       cm.MsgID,
		SessionKey:  channels.SessionKey(c.Name(), cm.FromUserName, cm.ChatID),
		UserID:      cm.FromUserName,
		ChatID:      cm.ChatID,
		Text:        cm.Content,
		Type:        channels.TypeText,
		WebhookPath: r.URL.Path,
		ReceivedAt:  time.Now(),
	}
	c.hand(r.Context(), w, h, msg, cm.MsgID)
}

// baseMessage builds the shared normalized message skeleton.
func (c *Channel) baseMessage(r *http.Request, cm *callbackMessage) channels.InboundMessage {
	return channels.InboundMessage{
		Channel:     c.Name(),
		MsgID:       cm.MsgID,
		SessionKey:  channels.SessionKey(c.Name(), cm.FromUserName, cm.ChatID),
		UserID:      cm.FromUserName,
		ChatID:      cm.ChatID,
		WebhookPath: r.URL.Path,
		ReceivedAt:  time.Now(),
	}
}

func ctxOf(r *http.Request) context.Context { return r.Context() }

// hand runs the handler and maps the outcome onto the callback response:
// duplicates and success ack 200; failures 5xx so the platform redelivers.
func (c *Channel) hand(ctx context.Context, w http.ResponseWriter, h channels.Handler, msg channels.InboundMessage, msgID string) {
	if _, err := h.Handle(ctx, msg); err != nil {
		if errors.Is(err, channels.ErrDuplicate) {
			// ErrDuplicate is a success outcome, not a failure: answer 200 so
			// the platform stops redelivering.
			plog.Warnf("wecom duplicate message %s dropped", msgID)
			writeSuccess(w)
			return
		}
		// 5xx makes the platform redeliver; the gateway rolls the dedup key
		// back first so that retry is not swallowed.
		plog.Errorf("wecom handle msg %s: %v", msgID, err)
		http.Error(w, "handle error", http.StatusInternalServerError)
		return
	}
	writeSuccess(w)
}

// receiveMedia fetches the media_id into artifact storage and normalizes a
// media message; when the fetch or the store is unavailable, a placeholder
// text goes through so the agent can still acknowledge the message.
func (c *Channel) receiveMedia(r *http.Request, w http.ResponseWriter, h channels.Handler, cm *callbackMessage) {
	msg := c.baseMessage(r, cm)
	msg.Type = channels.TypeMedia
	msg.Text = "[" + mediaTypes[cm.MsgType] + "]"

	ref, err := c.fetchMedia(r.Context(), cm)
	if err != nil {
		plog.Warnf("wecom media fetch %s: %v", cm.MsgID, err)
		msg.Text += "（素材拉取失败）"
	} else {
		msg.MediaRef = ref
		msg.Text += " " + ref
	}
	c.hand(r.Context(), w, h, msg, cm.MsgID)
}

// fetchMedia downloads media via the platform media/get API and stores it.
// Inbound media still fetches under the env-global identity: the callback path
// carries the binding's token/AES refs but not its outbound secret_ref, so a
// multi-corp deployment's media fetch stays on the global corp until that is
// threaded through BindingCredentials.
func (c *Channel) fetchMedia(ctx context.Context, cm *callbackMessage) (string, error) {
	if c.media == nil {
		return "", errors.New("media store not configured")
	}
	token, err := c.getAccessToken(ctx, c.cfg.CorpID, c.cfg.SecretRef)
	if err != nil {
		return "", err
	}
	data, mime, errcode, err := c.getMedia(ctx, token, cm.MediaID)
	if err != nil {
		return "", err
	}
	if errcode == 40014 || errcode == 42001 { // token expired/invalid: refresh once and retry
		c.invalidateToken(c.cfg.CorpID, c.cfg.SecretRef)
		if token, err = c.getAccessToken(ctx, c.cfg.CorpID, c.cfg.SecretRef); err != nil {
			return "", err
		}
		if data, mime, errcode, err = c.getMedia(ctx, token, cm.MediaID); err != nil {
			return "", err
		}
	}
	if errcode != 0 {
		return "", fmt.Errorf("wecom media get: errcode %d", errcode)
	}
	filename := cm.MsgID + "." + mediaFileExt(cm.MsgType, mime)
	return c.media.SaveMedia(ctx, c.Name(), cm.MsgID, filename, mime, data)
}

// getMedia downloads one media_id. The platform reports failures as HTTP 200
// with a JSON error body, so the content type — never the status code —
// decides what the body is: application/json decodes into an errcode (which
// must NOT be stored as media bytes), anything else is the media itself.
func (c *Channel) getMedia(ctx context.Context, token, mediaID string) (data []byte, mime string, errcode int, err error) {
	apiURL := c.cfg.APIBase + "/cgi-bin/media/get?access_token=" + url.QueryEscape(token) +
		"&media_id=" + url.QueryEscape(mediaID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, "", 0, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, "", 0, fmt.Errorf("wecom media get: %w", channels.ScrubError(err))
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, "", 0, fmt.Errorf("wecom media get: status %d", resp.StatusCode)
	}
	mime = resp.Header.Get("Content-Type")
	if strings.Contains(mime, "application/json") {
		var result struct {
			ErrCode int    `json:"errcode"`
			ErrMsg  string `json:"errmsg"`
		}
		if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result); err != nil {
			return nil, "", 0, fmt.Errorf("wecom media get decode: %w", err)
		}
		if result.ErrCode == 0 {
			return nil, "", 0, fmt.Errorf("wecom media get: unexpected json body %q", result.ErrMsg)
		}
		return nil, "", result.ErrCode, nil
	}
	data, err = io.ReadAll(http.MaxBytesReader(nil, resp.Body, 20<<20))
	if err != nil {
		return nil, "", 0, err
	}
	return data, mime, 0, nil
}

// mediaFileExt picks a file extension from the message type / content type.
func mediaFileExt(msgType, mime string) string {
	if msgType == "voice" {
		return "amr"
	}
	if msgType == "file" {
		return "bin"
	}
	switch {
	case strings.Contains(mime, "png"):
		return "png"
	case strings.Contains(mime, "gif"):
		return "gif"
	default:
		return "jpg"
	}
}

// callbackMessage is the decrypted inner XML of a WeCom callback.
type callbackMessage struct {
	XMLName      xml.Name `xml:"xml"`
	FromUserName string   `xml:"FromUserName"`
	CreateTime   int64    `xml:"CreateTime"`
	MsgType      string   `xml:"MsgType"`
	Content      string   `xml:"Content"`
	MsgID        string   `xml:"MsgId"`
	MediaID      string   `xml:"MediaId"` // image/voice/file carry a media_id, not content
	PicUrl       string   `xml:"PicUrl"`
	AgentID      int      `xml:"AgentID"`
	ChatID       string   `xml:"ChatId"` // group chat ID; empty for direct chats
	Event        string   `xml:"Event"`
}

// mediaTypes are the callback msgtypes carrying a media_id instead of text.
var mediaTypes = map[string]string{
	"image": "图片",
	"voice": "语音",
	"file":  "文件",
}

// placeholderTypes are the callback msgtypes with no retrievable content;
// they enter the pipeline as a bracketed placeholder text so the agent can
// tell the user the type is unsupported instead of the message vanishing.
var placeholderTypes = map[string]string{
	"video":    "视频",
	"location": "位置",
	"link":     "链接",
}

func writeSuccess(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte("success"))
}

// Send implements channels.Channel: direct chats go to message/send, group
// chats to appchat/send. Long texts are split into sequential segments within
// this one call, so concurrent senders cannot interleave segments of the same
// reply. An empty (whitespace-only) text is skipped without any API call.
// A card reply to a direct chat goes out as ONE template_card message —
// appchat/send has no template_card, so group chats keep the text segments.
// message/send accepts no caller idempotency key, so duplicate suppression
// relies on the sender's sent: marker window plus the platform's
// touser+content duplicate check (see postMessage) — the latter also absorbs
// the prefix segments of a retried mid-split failure.
func (c *Channel) Send(ctx context.Context, msg channels.OutboundMessage) error {
	// An empty reply carries nothing the platform accepts; sending it would
	// surface as an errcode, so skip it quietly. A direct-chat card is the
	// exception: the template_card renders without any text, so it is sent
	// even when the fallback Text is empty. channels.Card still requires
	// producers to always fill Text; this exemption only keeps the sender
	// robust against contract-violating input. A group-chat card with an
	// empty Text is NOT exempt — appchat/send would fall back to the empty
	// text path and get rejected.
	if strings.TrimSpace(msg.Text) == "" && (msg.Card == nil || msg.ChatID != "") {
		plog.Debugf("wecom send: empty reply for msg %s skipped", msg.MsgID)
		return nil
	}
	// One identity lookup for the whole reply: every segment of one message
	// goes out under the same binding identity.
	id, err := c.outboundIDFor(ctx, msg)
	if err != nil {
		return err
	}
	// A card is an atomic unit: splitting it into 2048-byte text segments
	// would destroy the rendering, so it skips the segmentation loop entirely.
	if msg.Card != nil && msg.ChatID == "" {
		return c.sendSegment(ctx, id, msg, msg.Text)
	}
	segments := splitText(msg.Text, maxTextBytes)
	for i, seg := range segments {
		if err := c.sendSegment(ctx, id, msg, seg); err != nil {
			if i > 0 {
				return fmt.Errorf("send segment %d/%d (partial delivery): %w", i+1, len(segments), err)
			}
			return err
		}
	}
	return nil
}

func (c *Channel) sendSegment(ctx context.Context, id outboundID, msg channels.OutboundMessage, text string) error {
	token, err := c.getAccessToken(ctx, id.corpID, id.secretRef)
	if err != nil {
		return err
	}
	errcode, err := c.postMessage(ctx, token, id.agentID, msg, text)
	if err != nil {
		return err
	}
	if errcode == 40014 || errcode == 42001 { // token expired/invalid: refresh once and retry
		c.invalidateToken(id.corpID, id.secretRef)
		if token, err = c.getAccessToken(ctx, id.corpID, id.secretRef); err != nil {
			return err
		}
		errcode, err = c.postMessage(ctx, token, id.agentID, msg, text)
		if err != nil {
			return err
		}
	}
	if errcode != 0 {
		return fmt.Errorf("wecom send: errcode %d", errcode)
	}
	return nil
}

// postMessage calls the send API and returns the platform errcode. Markdown
// replies use the markdown msgtype (WeCom renders it; channels without
// markdown downgrade upstream via channels.RenderPlain). A Card on a direct
// chat renders as a template_card (text_notice); appchat/send has no
// template_card, so group chats always take the text path. agentID is the
// resolved outbound identity's app, not necessarily the env-global one.
func (c *Channel) postMessage(ctx context.Context, token string, agentID int, msg channels.OutboundMessage, text string) (int, error) {
	msgType, contentKey := "text", "text"
	if msg.TextType == "markdown" {
		msgType, contentKey = "markdown", "markdown"
	}
	var apiURL string
	var payload map[string]any
	switch {
	case msg.ChatID != "":
		apiURL = c.cfg.APIBase + "/cgi-bin/appchat/send?access_token=" + url.QueryEscape(token)
		payload = map[string]any{
			"chatid":   msg.ChatID,
			"msgtype":  msgType,
			contentKey: map[string]string{"content": text},
		}
	case msg.Card != nil:
		apiURL = c.cfg.APIBase + "/cgi-bin/message/send?access_token=" + url.QueryEscape(token)
		payload = map[string]any{
			"touser":        msg.UserID,
			"msgtype":       "template_card",
			"agentid":       agentID,
			"template_card": cardPayload(msg.Card),
			// Same duplicate-suppression rationale as the text path below.
			"enable_duplicate_check":   1,
			"duplicate_check_interval": 1800,
		}
	default:
		apiURL = c.cfg.APIBase + "/cgi-bin/message/send?access_token=" + url.QueryEscape(token)
		payload = map[string]any{
			"touser":   msg.UserID,
			"msgtype":  msgType,
			"agentid":  agentID,
			contentKey: map[string]string{"content": text},
			// message/send accepts no caller idempotency key, but the
			// platform's duplicate check dedups by touser+content within the
			// interval: when a mid-split failure makes the sender retry the
			// whole reply, the already-delivered prefix segments are absorbed
			// instead of reaching the user twice.
			"enable_duplicate_check":   1,
			"duplicate_check_interval": 1800,
		}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("wecom send request: %w", channels.ScrubError(err))
	}
	defer func() { _ = resp.Body.Close() }()
	var result struct {
		ErrCode int    `json:"errcode"`
		ErrMsg  string `json:"errmsg"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("wecom send decode: %w", err)
	}
	if result.ErrCode != 0 {
		zap.L().Warn("wecom send rejected",
			zap.Int("errcode", result.ErrCode), zap.String("errmsg", result.ErrMsg))
	}
	return result.ErrCode, nil
}

// cardPayload builds the template_card (text_notice) body of a message/send
// call. A URL turns the whole card into a jump link (card_action type 1);
// without one the card is a plain notice. Title and desc are truncated to
// their platform byte caps on rune boundaries; an over-long URL is dropped
// rather than truncated — a cut URL almost never resolves, while a missing
// one merely degrades the card to a plain notice, which the platform accepts.
func cardPayload(card *channels.Card) map[string]any {
	payload := map[string]any{
		"card_type": "text_notice",
		"main_title": map[string]string{
			"title": truncateCardField(card.Title, maxCardTitleBytes),
			"desc":  truncateCardField(card.Desc, maxCardDescBytes),
		},
	}
	if card.URL != "" && len(card.URL) <= maxCardURLBytes {
		payload["card_action"] = map[string]any{"type": 1, "url": card.URL}
	}
	return payload
}

// truncateCardField caps s at n bytes without splitting inside a UTF-8
// sequence, reusing the first segment of splitText.
func truncateCardField(s string, n int) string {
	return splitText(s, n)[0]
}

// getAccessToken returns the cached token for one (corpID, secretRef)
// identity, refreshing it when expired. The cache is keyed by the secret REF,
// not its resolved value: the send hot path stays off the secret resolver, and
// a rotation behind a ref is picked up when the cached token expires or the
// platform rejects it (sendSegment invalidates just that entry on
// 40014/42001).
func (c *Channel) getAccessToken(ctx context.Context, corpID, secretRef string) (string, error) {
	key := corpID + "|" + secretRef
	c.tokenMu.Lock()
	if e, ok := c.tokens[key]; ok && time.Now().Before(e.expiry) {
		c.tokenOrder.MoveToFront(e.elem)
		c.tokenMu.Unlock()
		return e.token, nil
	}
	c.tokenMu.Unlock()
	// The refresh runs off the cache lock (see tokenFlights): concurrent
	// misses for one identity share a single gettoken call.
	token, err, _ := c.tokenFlights.Do(key, func() (any, error) {
		return c.refreshAccessToken(ctx, corpID, secretRef)
	})
	if err != nil {
		return "", err
	}
	return token.(string), nil
}

// refreshAccessToken resolves the secret, calls gettoken, and stores the
// result. It runs without the cache lock; singleflight serializes callers
// for the same identity.
func (c *Channel) refreshAccessToken(ctx context.Context, corpID, secretRef string) (any, error) {
	key := corpID + "|" + secretRef
	secret, err := c.secret.Resolve(ctx, secretRef)
	if err != nil {
		return nil, fmt.Errorf("wecom: resolve corpsecret: %w", err)
	}
	apiURL := c.cfg.APIBase + "/cgi-bin/gettoken?corpid=" + url.QueryEscape(corpID) +
		"&corpsecret=" + url.QueryEscape(secret)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("wecom gettoken: %w", channels.ScrubError(err))
	}
	defer func() { _ = resp.Body.Close() }()
	var result struct {
		ErrCode     int    `json:"errcode"`
		ErrMsg      string `json:"errmsg"`
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("wecom gettoken decode: %w", err)
	}
	if result.ErrCode != 0 || result.AccessToken == "" {
		return nil, fmt.Errorf("wecom gettoken: errcode %d", result.ErrCode)
	}
	ttl := time.Duration(result.ExpiresIn)*time.Second - tokenExpiryMargin
	if ttl <= 0 {
		ttl = time.Hour
	}
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	// Refreshing a cached identity reuses its LRU node: pushing a new one
	// would orphan the old node (list growth without a matching map entry), and
	// a later eviction popping the orphan would delete the LIVE entry. Only a
	// new identity runs the eviction loop — refreshing a cached key while the
	// cache is full must not evict an innocent identity.
	if e, ok := c.tokens[key]; ok && e.elem != nil {
		c.tokenOrder.MoveToFront(e.elem)
		c.tokens[key] = tokenEntry{token: result.AccessToken, expiry: time.Now().Add(ttl), elem: e.elem}
		return result.AccessToken, nil
	}
	// Evict the least recently used identity when full: a wholesale reset
	// would re-authenticate every live binding at once.
	for len(c.tokens) >= maxTokenCacheEntries {
		back := c.tokenOrder.Back()
		if back == nil {
			break
		}
		delete(c.tokens, back.Value.(string))
		c.tokenOrder.Remove(back)
	}
	elem := c.tokenOrder.PushFront(key)
	c.tokens[key] = tokenEntry{token: result.AccessToken, expiry: time.Now().Add(ttl), elem: elem}
	return result.AccessToken, nil
}

func (c *Channel) invalidateToken(corpID, secretRef string) {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	key := corpID + "|" + secretRef
	if e, ok := c.tokens[key]; ok && e.elem != nil {
		c.tokenOrder.Remove(e.elem)
	}
	delete(c.tokens, key)
}

// splitText breaks s into segments of at most n bytes, on rune boundaries.
func splitText(s string, n int) []string {
	return channels.SplitText(s, n)
}
