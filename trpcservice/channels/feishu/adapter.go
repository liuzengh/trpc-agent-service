// Package feishu implements Feishu event-subscription push callbacks.
package feishu

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Violet2314/trpc-agent-service/trpcservice/channels"
	"github.com/Violet2314/trpc-agent-service/trpcservice/gateway"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

const (
	channelType       = "feishu"
	defaultAPIBaseURL = "https://open.feishu.cn"
)

// Adapter verifies callbacks, resolves Feishu events, and builds repliers.
type Adapter struct {
	cache   tenant.ConfigCache
	client  *http.Client
	baseURL string
	now     func() time.Time

	tokenMu sync.Mutex
	tokens  map[string]tokenEntry
}

// New constructs a Feishu adapter.
func New(cache tenant.ConfigCache, client *http.Client, baseURL string) (*Adapter, error) {
	if cache == nil {
		return nil, errors.New("Feishu config cache is required")
	}
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
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

// Run registers the event callback endpoint.
func (a *Adapter) Run(_ context.Context, mux *http.ServeMux, sink channels.Sink) error {
	if mux == nil || sink == nil {
		return errors.New("Feishu mux and Gateway sink are required")
	}
	mux.HandleFunc("POST /channels/feishu/{routeKey}/events", func(w http.ResponseWriter, r *http.Request) {
		a.handleEvent(w, r, sink)
	})
	return nil
}

// NewReplier implements channels.Adapter.
func (a *Adapter) NewReplier(snapshot tenant.Snapshot) channels.Replier {
	return &replier{adapter: a, snapshot: snapshot}
}

func (a *Adapter) handleEvent(w http.ResponseWriter, r *http.Request, sink channels.Sink) {
	routeKey := r.PathValue("routeKey")
	snapshot, err := a.cache.ResolveBinding(r.Context(), channelType, routeKey)
	if err != nil {
		if errors.Is(err, tenant.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "binding not found"})
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "binding unavailable"})
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "read callback body"})
		return
	}
	verificationToken, err := resolveSecretConfig(snapshot.Binding.Config, "verification_token", true)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "invalid binding secret"})
		return
	}
	encryptKey, err := resolveSecretConfig(snapshot.Binding.Config, "encrypt_key", false)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "invalid binding secret"})
		return
	}

	payload := body
	signedEncryptedCallback := false
	if encryptKey != "" {
		timestamp := r.Header.Get("X-Lark-Request-Timestamp")
		nonce := r.Header.Get("X-Lark-Request-Nonce")
		signature := r.Header.Get("X-Lark-Signature")
		hasAnySignatureHeader := timestamp != "" || nonce != "" || signature != ""
		if hasAnySignatureHeader {
			if timestamp == "" || nonce == "" || signature == "" {
				log.Printf("feishu callback rejected: incomplete signature headers")
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid callback signature"})
				return
			}
			if err := verifyTimestamp(timestamp, a.now()); err != nil {
				log.Printf("feishu callback rejected: timestamp validation failed: %v", err)
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid callback signature"})
				return
			}
			if !verifySignature(timestamp, nonce, encryptKey, body, signature) {
				log.Printf("feishu callback rejected: signature mismatch")
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid callback signature"})
				return
			}
			signedEncryptedCallback = true
		}
		var encrypted struct {
			Encrypt string `json:"encrypt"`
		}
		if err := json.Unmarshal(body, &encrypted); err != nil || encrypted.Encrypt == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid encrypted callback"})
			return
		}
		payload, err = decryptCallback(encrypted.Encrypt, encryptKey)
		if err != nil {
			log.Printf("feishu callback rejected: decryption failed: %v", err)
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "decrypt callback"})
			return
		}
	}

	var envelope eventEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid callback payload"})
		return
	}
	token := envelope.Header.Token
	if token == "" {
		token = envelope.Token
	}
	if subtle.ConstantTimeCompare([]byte(token), []byte(verificationToken)) != 1 {
		log.Printf("feishu callback rejected: verification token mismatch")
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "invalid verification token"})
		return
	}
	if envelope.Type == "url_verification" || envelope.Challenge != "" {
		writeJSON(w, http.StatusOK, map[string]string{"challenge": envelope.Challenge})
		return
	}
	if encryptKey != "" && !signedEncryptedCallback {
		log.Printf("feishu callback rejected: encrypted event missing signature headers")
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid callback signature"})
		return
	}
	if envelope.Header.EventType != "im.message.receive_v1" {
		writeJSON(w, http.StatusOK, map[string]any{})
		return
	}
	expectedAppID := snapshot.Binding.Config["app_id"]
	if expectedAppID == "" {
		expectedAppID = routeKey
	}
	if envelope.Header.AppID != "" && envelope.Header.AppID != expectedAppID {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "callback app_id mismatch"})
		return
	}
	if envelope.Event.Sender.SenderType == "bot" {
		writeJSON(w, http.StatusOK, map[string]any{})
		return
	}
	message := envelope.Event.Message
	if message.MessageType != "text" {
		writeJSON(w, http.StatusOK, map[string]any{})
		return
	}
	var content struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal([]byte(message.Content), &content); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid text content"})
		return
	}
	addressed := message.ChatType == "p2p"
	if message.ChatType == "group" {
		botOpenID := snapshot.Binding.Config["bot_open_id"]
		for _, mention := range message.Mentions {
			if botOpenID != "" && mention.ID.OpenID == botOpenID {
				addressed = true
				content.Text = strings.TrimSpace(strings.ReplaceAll(content.Text, mention.Key, ""))
			}
		}
	}
	messageID := envelope.Header.EventID
	if messageID == "" {
		messageID = message.MessageID
	}
	result, err := sink(r.Context(), gateway.InboundMessage{
		Channel:        channelType,
		RouteKey:       routeKey,
		MsgID:          messageID,
		ChatType:       message.ChatType,
		SenderID:       envelope.Event.Sender.SenderID.OpenID,
		GroupID:        message.ChatID,
		AddressedToBot: addressed,
		Text:           content.Text,
		Raw:            replyTarget{ChatID: message.ChatID},
		TraceID:        envelope.Header.EventID,
	})
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "Gateway unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

type eventEnvelope struct {
	Schema    string `json:"schema"`
	Type      string `json:"type"`
	Challenge string `json:"challenge"`
	Token     string `json:"token"`
	Header    struct {
		EventID   string `json:"event_id"`
		EventType string `json:"event_type"`
		Token     string `json:"token"`
		AppID     string `json:"app_id"`
	} `json:"header"`
	Event struct {
		Sender struct {
			SenderID struct {
				OpenID string `json:"open_id"`
			} `json:"sender_id"`
			SenderType string `json:"sender_type"`
		} `json:"sender"`
		Message struct {
			MessageID   string `json:"message_id"`
			ChatID      string `json:"chat_id"`
			ChatType    string `json:"chat_type"`
			MessageType string `json:"message_type"`
			Content     string `json:"content"`
			Mentions    []struct {
				Key string `json:"key"`
				ID  struct {
					OpenID string `json:"open_id"`
				} `json:"id"`
			} `json:"mentions"`
		} `json:"message"`
	} `json:"event"`
}

type replyTarget struct {
	ChatID string
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

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
