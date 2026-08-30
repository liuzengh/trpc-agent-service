package lark

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

const (
	maxWebhookBodyBytes = 1 << 20
	maxWebhookTextBytes = 60 << 10
	defaultClockSkew    = 5 * time.Minute
)

var (
	ErrInvalidWebhookConfig    = errors.New("lark: invalid webhook config")
	ErrInvalidWebhookSignature = errors.New("lark: invalid webhook signature")
	ErrInvalidWebhookPayload   = errors.New("lark: invalid webhook payload")
	ErrInvalidWebhookChallenge = errors.New("lark: invalid webhook challenge")
	ErrUnsupportedWebhookEvent = errors.New("lark: unsupported webhook event")
	ErrWebhookClockSkew        = errors.New("lark: webhook timestamp outside allowed window")
)

// WebhookConfig contains server-owned Lark callback verification material.
// VerificationToken and EncryptKey are never read from a callback body.
type WebhookConfig struct {
	Binding           Binding
	VerificationToken string
	EncryptKey        string
	MaxClockSkew      time.Duration
	Now               func() time.Time
}

// WebhookAdapter verifies and projects Lark event subscription callbacks.
// It does not resolve tenants, enqueue jobs, call providers, or mutate state.
type WebhookAdapter struct {
	binding           Binding
	verificationToken string
	encryptKey        string
	maxClockSkew      time.Duration
	now               func() time.Time
}

func NewWebhookAdapter(config WebhookConfig) (*WebhookAdapter, error) {
	if err := config.Binding.Validate(); err != nil || !validWebhookValue(config.VerificationToken) || !validWebhookValue(config.EncryptKey) {
		return nil, ErrInvalidWebhookConfig
	}
	if config.MaxClockSkew == 0 {
		config.MaxClockSkew = defaultClockSkew
	}
	if config.MaxClockSkew <= 0 || config.MaxClockSkew > time.Hour {
		return nil, ErrInvalidWebhookConfig
	}
	if config.Now == nil {
		config.Now = func() time.Time { return time.Now().UTC() }
	}
	return &WebhookAdapter{
		binding:           config.Binding,
		verificationToken: config.VerificationToken,
		encryptKey:        config.EncryptKey,
		maxClockSkew:      config.MaxClockSkew,
		now:               config.Now,
	}, nil
}

func (a *WebhookAdapter) Binding() Binding {
	if a == nil {
		return Binding{}
	}
	return a.binding
}

func (a *WebhookAdapter) Verify(request *http.Request, body []byte) error {
	if a == nil || !a.binding.Enabled || request == nil || len(body) == 0 || len(body) > maxWebhookBodyBytes {
		return ErrInvalidWebhookSignature
	}
	plain, err := a.decodeBody(body)
	if err != nil {
		return err
	}
	var envelope struct {
		Type      string `json:"type"`
		Token     string `json:"token"`
		Challenge string `json:"challenge"`
	}
	if err := decodeWebhookJSON(plain, &envelope); err != nil {
		return ErrInvalidWebhookPayload
	}
	if envelope.Type == "url_verification" {
		if !constantTimeString(a.verificationToken, envelope.Token) || envelope.Challenge == "" {
			return ErrInvalidWebhookChallenge
		}
		return nil
	}
	if a.encryptKey == "" {
		return ErrInvalidWebhookSignature
	}
	timestamp := request.Header.Get("X-Lark-Request-Timestamp")
	nonce := request.Header.Get("X-Lark-Request-Nonce")
	signature := request.Header.Get("X-Lark-Signature")
	if timestamp == "" || nonce == "" || signature == "" || !validWebhookValue(nonce) {
		return ErrInvalidWebhookSignature
	}
	seconds, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return ErrInvalidWebhookSignature
	}
	when := time.Unix(seconds, 0)
	if delta := a.now().Sub(when); delta < -a.maxClockSkew || delta > a.maxClockSkew {
		return ErrWebhookClockSkew
	}
	digest := sha256.Sum256([]byte(timestamp + nonce + a.encryptKey + string(body)))
	expected := hex.EncodeToString(digest[:])
	if len(signature) != len(expected) || subtle.ConstantTimeCompare([]byte(strings.ToLower(signature)), []byte(expected)) != 1 {
		return ErrInvalidWebhookSignature
	}
	return nil
}

// Challenge returns the official URL verification response when body is a
// valid url_verification callback. The boolean is false for event callbacks.
func (a *WebhookAdapter) Challenge(body []byte) ([]byte, bool, error) {
	if a == nil || len(body) == 0 || len(body) > maxWebhookBodyBytes {
		return nil, false, ErrInvalidWebhookChallenge
	}
	plain, err := a.decodeBody(body)
	if err != nil {
		return nil, false, err
	}
	var envelope struct {
		Type      string `json:"type"`
		Token     string `json:"token"`
		Challenge string `json:"challenge"`
	}
	if err := decodeWebhookJSON(plain, &envelope); err != nil {
		return nil, false, ErrInvalidWebhookPayload
	}
	if envelope.Type != "url_verification" {
		return nil, false, nil
	}
	if !constantTimeString(a.verificationToken, envelope.Token) || envelope.Challenge == "" || !validWebhookValue(envelope.Challenge) {
		return nil, true, ErrInvalidWebhookChallenge
	}
	response, err := json.Marshal(struct {
		Challenge string `json:"challenge"`
	}{Challenge: envelope.Challenge})
	return response, true, err
}

func (a *WebhookAdapter) Parse(body []byte) (channels.Incoming, error) {
	if a == nil || len(body) == 0 || len(body) > maxWebhookBodyBytes {
		return channels.Incoming{}, ErrInvalidWebhookPayload
	}
	plain, err := a.decodeBody(body)
	if err != nil {
		return channels.Incoming{}, err
	}
	var callback larkCallback
	if err := decodeWebhookJSON(plain, &callback); err != nil {
		return channels.Incoming{}, ErrInvalidWebhookPayload
	}
	if callback.Type == "url_verification" {
		return channels.Incoming{}, ErrUnsupportedWebhookEvent
	}
	message := callback.Event.Message
	eventID := callback.Header.EventID
	if eventID == "" {
		eventID = callback.Event.EventID
	}
	if eventID == "" {
		eventID = message.MessageID
	}
	if eventID == "" || message.MessageID == "" || message.ChatID == "" || message.Content == "" {
		return channels.Incoming{}, ErrUnsupportedWebhookEvent
	}
	if callback.Header.AppID != "" && callback.Header.AppID != a.binding.AppID {
		return channels.Incoming{}, ErrInvalidWebhookPayload
	}
	if message.MessageType != "" && message.MessageType != "text" {
		return channels.Incoming{}, ErrUnsupportedWebhookEvent
	}
	var content struct {
		Text string `json:"text"`
	}
	if err := decodeWebhookJSON([]byte(message.Content), &content); err != nil || !validWebhookText(content.Text) {
		return channels.Incoming{}, ErrUnsupportedWebhookEvent
	}
	userID := callback.Event.Sender.SenderID.OpenID
	if userID == "" {
		userID = callback.Event.Sender.SenderID.UserID
	}
	if userID == "" {
		userID = callback.Event.Sender.SenderID.UnionID
	}
	if userID == "" {
		userID = callback.Event.OpenID
	}
	return channels.Incoming{ID: eventID, Channel: Channel, UserID: userID, ChatID: message.ChatID, Text: content.Text}, nil
}

type larkCallback struct {
	Type   string `json:"type"`
	Header struct {
		EventID   string `json:"event_id"`
		EventType string `json:"event_type"`
		AppID     string `json:"app_id"`
	} `json:"header"`
	Event struct {
		EventID string `json:"event_id"`
		OpenID  string `json:"open_id"`
		Sender  struct {
			SenderID struct {
				OpenID  string `json:"open_id"`
				UserID  string `json:"user_id"`
				UnionID string `json:"union_id"`
			} `json:"sender_id"`
		} `json:"sender"`
		Message struct {
			MessageID   string `json:"message_id"`
			ChatID      string `json:"chat_id"`
			MessageType string `json:"message_type"`
			Content     string `json:"content"`
		} `json:"message"`
	} `json:"event"`
}

func (a *WebhookAdapter) decodeBody(body []byte) ([]byte, error) {
	var envelope struct {
		Encrypt string `json:"encrypt"`
	}
	if err := decodeWebhookJSON(body, &envelope); err != nil {
		return nil, ErrInvalidWebhookPayload
	}
	if envelope.Encrypt == "" {
		return body, nil
	}
	if a.encryptKey == "" {
		return nil, ErrInvalidWebhookSignature
	}
	ciphertext, err := base64.StdEncoding.DecodeString(envelope.Encrypt)
	if err != nil {
		ciphertext, err = base64.RawStdEncoding.DecodeString(envelope.Encrypt)
	}
	if err != nil || len(ciphertext) < aes.BlockSize {
		return nil, ErrInvalidWebhookPayload
	}
	key := sha256.Sum256([]byte(a.encryptKey))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, ErrInvalidWebhookPayload
	}
	iv := ciphertext[:block.BlockSize()]
	ciphertext = ciphertext[block.BlockSize():]
	if len(ciphertext) == 0 || len(ciphertext)%block.BlockSize() != 0 {
		return nil, ErrInvalidWebhookPayload
	}
	plain := make([]byte, len(ciphertext))
	cipher.NewCBCDecrypter(block, iv).CryptBlocks(plain, ciphertext)
	return trimWebhookPadding(plain, block.BlockSize())
}

func trimWebhookPadding(value []byte, blockSize int) ([]byte, error) {
	if len(value) == 0 {
		return nil, ErrInvalidWebhookPayload
	}
	padding := int(value[len(value)-1])
	if padding < 1 || padding > blockSize || padding > len(value) {
		return nil, ErrInvalidWebhookPayload
	}
	for _, b := range value[len(value)-padding:] {
		if int(b) != padding {
			return nil, ErrInvalidWebhookPayload
		}
	}
	return value[:len(value)-padding], nil
}

func decodeWebhookJSON(body []byte, dst any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("trailing webhook data")
	}
	return nil
}

func validWebhookValue(value string) bool {
	return value != "" && utf8.ValidString(value) && len(value) <= 512 && !strings.ContainsAny(value, "\r\n\x00")
}

func validWebhookText(value string) bool {
	return value != "" && utf8.ValidString(value) && len(value) <= maxWebhookTextBytes
}

func constantTimeString(left, right string) bool {
	if len(left) != len(right) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}
