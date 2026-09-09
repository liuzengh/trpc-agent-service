// Package wecom implements encrypted Enterprise WeChat *self-built app*
// callbacks (自建应用 webhook：AES 加解密 + /callback).
//
// This is NOT the required WeCom integration and is NOT an acceptance item.
// The original reading mixed it up with 智能机器人. Keep this adapter for
// protocol completeness only; do not demo, operate, or follow up on a real
// WeCom app callback. The in-scope WeCom channel is package wecombot.
package wecom

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Violet2314/trpc-agent-service/trpcservice/channels"
	"github.com/Violet2314/trpc-agent-service/trpcservice/gateway"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

const (
	channelType       = "wecom"
	defaultAPIBaseURL = "https://qyapi.weixin.qq.com"
)

// Adapter handles encrypted WeCom callbacks and active application replies.
type Adapter struct {
	cache   tenant.ConfigCache
	client  *http.Client
	baseURL string
	now     func() time.Time

	tokenMu sync.Mutex
	tokens  map[string]tokenEntry
}

// New constructs a WeCom adapter.
func New(cache tenant.ConfigCache, client *http.Client, baseURL string) (*Adapter, error) {
	if cache == nil {
		return nil, errors.New("WeCom config cache is required")
	}
	if client == nil {
		client = &http.Client{Timeout: 6 * time.Second}
	}
	if strings.TrimSpace(baseURL) == "" {
		baseURL = defaultAPIBaseURL
	}
	return &Adapter{
		cache:   cache,
		client:  client,
		baseURL: strings.TrimRight(baseURL, "/"),
		now:     time.Now,
		tokens:  make(map[string]tokenEntry),
	}, nil
}

// Type implements channels.Adapter.
func (*Adapter) Type() string {
	return channelType
}

// Run registers handshake and callback endpoints.
func (a *Adapter) Run(_ context.Context, mux *http.ServeMux, sink channels.Sink) error {
	if mux == nil || sink == nil {
		return errors.New("WeCom mux and Gateway sink are required")
	}
	mux.HandleFunc("GET /channels/wecom/{routeKey}/callback", a.handleHandshake)
	mux.HandleFunc("POST /channels/wecom/{routeKey}/callback", func(w http.ResponseWriter, r *http.Request) {
		a.handleMessage(w, r, sink)
	})
	return nil
}

// NewReplier implements channels.Adapter.
func (a *Adapter) NewReplier(snapshot tenant.Snapshot) channels.Replier {
	return &replier{adapter: a, snapshot: snapshot}
}

func (a *Adapter) handleHandshake(w http.ResponseWriter, r *http.Request) {
	snapshot, ok := a.resolveSnapshot(w, r)
	if !ok {
		return
	}
	token, encodingKey, corpID, ok := callbackSecrets(w, snapshot)
	if !ok {
		return
	}
	query := r.URL.Query()
	encrypted := query.Get("echostr")
	if !verifyMessageSignature(
		token,
		query.Get("timestamp"),
		query.Get("nonce"),
		encrypted,
		query.Get("msg_signature"),
	) {
		http.Error(w, "invalid callback signature", http.StatusUnauthorized)
		return
	}
	plaintext, err := decryptMessage(encrypted, corpID, encodingKey)
	if err != nil {
		http.Error(w, "decrypt callback challenge", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(plaintext)
}

func (a *Adapter) handleMessage(w http.ResponseWriter, r *http.Request, sink channels.Sink) {
	snapshot, ok := a.resolveSnapshot(w, r)
	if !ok {
		return
	}
	token, encodingKey, corpID, ok := callbackSecrets(w, snapshot)
	if !ok {
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read callback body", http.StatusBadRequest)
		return
	}
	var encryptedEnvelope struct {
		Encrypt string `xml:"Encrypt"`
	}
	if err := xml.Unmarshal(body, &encryptedEnvelope); err != nil || encryptedEnvelope.Encrypt == "" {
		http.Error(w, "invalid encrypted callback", http.StatusBadRequest)
		return
	}
	query := r.URL.Query()
	if !verifyMessageSignature(
		token,
		query.Get("timestamp"),
		query.Get("nonce"),
		encryptedEnvelope.Encrypt,
		query.Get("msg_signature"),
	) {
		http.Error(w, "invalid callback signature", http.StatusUnauthorized)
		return
	}
	plaintext, err := decryptMessage(encryptedEnvelope.Encrypt, corpID, encodingKey)
	if err != nil {
		http.Error(w, "decrypt callback", http.StatusBadRequest)
		return
	}
	var message inboundMessage
	if err := xml.Unmarshal(plaintext, &message); err != nil {
		http.Error(w, "invalid callback message", http.StatusBadRequest)
		return
	}
	if message.MsgType != "text" {
		writeSuccess(w)
		return
	}
	result, err := sink(r.Context(), gateway.InboundMessage{
		Channel:        channelType,
		RouteKey:       r.PathValue("routeKey"),
		MsgID:          message.MsgID,
		ChatType:       "p2p",
		SenderID:       message.FromUserName,
		AddressedToBot: true,
		Text:           message.Content,
		Raw:            replyTarget{UserID: message.FromUserName},
		TraceID:        message.MsgID,
	})
	if err != nil {
		http.Error(w, "Gateway unavailable", http.StatusServiceUnavailable)
		return
	}
	if result.Outcome == gateway.OutcomeAgentOffline {
		http.Error(w, "Agent offline", http.StatusServiceUnavailable)
		return
	}
	writeSuccess(w)
}

func (a *Adapter) resolveSnapshot(w http.ResponseWriter, r *http.Request) (tenant.Snapshot, bool) {
	snapshot, err := a.cache.ResolveBinding(
		r.Context(), channelType, r.PathValue("routeKey"),
	)
	if err != nil {
		if errors.Is(err, tenant.ErrNotFound) {
			http.Error(w, "binding not found", http.StatusNotFound)
		} else {
			http.Error(w, "binding unavailable", http.StatusServiceUnavailable)
		}
		return tenant.Snapshot{}, false
	}
	return snapshot, true
}

func callbackSecrets(
	w http.ResponseWriter,
	snapshot tenant.Snapshot,
) (token string, encodingKey string, corpID string, ok bool) {
	var err error
	token, err = resolveSecretConfig(snapshot.Binding.Config, "token", true)
	if err == nil {
		encodingKey, err = resolveSecretConfig(snapshot.Binding.Config, "encoding_aes_key", true)
	}
	corpID = snapshot.Binding.Config["corp_id"]
	if err != nil || corpID == "" {
		http.Error(w, "invalid binding callback configuration", http.StatusInternalServerError)
		return "", "", "", false
	}
	return token, encodingKey, corpID, true
}

func resolveSecretConfig(config map[string]string, key string, required bool) (string, error) {
	reference := config[key]
	if reference == "" {
		if required {
			return "", errors.New("required secret reference is missing")
		}
		return "", nil
	}
	return tenant.ResolveSecret(reference)
}

func writeSuccess(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("success"))
}

type inboundMessage struct {
	XMLName      xml.Name `xml:"xml"`
	ToUserName   string   `xml:"ToUserName"`
	FromUserName string   `xml:"FromUserName"`
	CreateTime   int64    `xml:"CreateTime"`
	MsgType      string   `xml:"MsgType"`
	Content      string   `xml:"Content"`
	MsgID        string   `xml:"MsgId"`
	AgentID      int64    `xml:"AgentID"`
}

type replyTarget struct {
	UserID string
}
