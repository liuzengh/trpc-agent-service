// Package webui implements the local browser IM protocol used to exercise the
// same durable Channel pipeline as external providers.
package webui

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

const (
	headerTimestamp = "X-WebUI-Timestamp"
	headerNonce     = "X-WebUI-Nonce"
	headerSignature = "X-WebUI-Signature"
	routeKeyQuery   = "route_key"
	maxTextBytes    = 32 << 10
)

// VerificationMaterial is stored under the existing channel_verify scope.
// The account ID prevents one browser credential from being replayed against
// another WebUI binding even if a route is accidentally misconfigured.
type VerificationMaterial struct {
	Token             string `json:"token"`
	ExternalAccountID string `json:"external_account_id"`
}

type inboundEnvelope struct {
	SchemaVersion     uint16    `json:"schema_version"`
	ExternalAccountID string    `json:"external_account_id"`
	ExternalMessageID string    `json:"external_message_id"`
	ExternalUserID    string    `json:"external_user_id"`
	ExternalChatID    string    `json:"external_chat_id"`
	ConversationType  string    `json:"conversation_type"`
	MessageType       string    `json:"message_type"`
	Text              string    `json:"text"`
	OccurredAt        time.Time `json:"occurred_at"`
}

type Verifier struct {
	Now     func() time.Time
	MaxSkew time.Duration
}

func RouteKeyDigest(routeKey string) string {
	sum := sha256.Sum256([]byte("webui-route-v1\x00" + routeKey))
	return hex.EncodeToString(sum[:])
}

// SignCallback is the small client-side protocol surface used by the bundled
// browser and acceptance clients. It never performs routing or tenant lookup.
func SignCallback(token, timestamp, nonce string, body []byte) string {
	return signatureFor(token, timestamp, nonce, body)
}

func (v Verifier) Verify(_ context.Context, request channel.CallbackRequest, secret []byte) (channel.VerifiedProtocolPayload, error) {
	material, err := parseMaterial(secret)
	if err != nil || len(request.Body) == 0 || len(request.Body) > 1<<20 {
		return channel.VerifiedProtocolPayload{}, runtime.ErrVersionMismatch
	}
	timestamp := header(request.Headers, headerTimestamp)
	nonce := header(request.Headers, headerNonce)
	signature := header(request.Headers, headerSignature)
	now := time.Now().UTC()
	if v.Now != nil {
		now = v.Now().UTC()
	}
	maxSkew := v.MaxSkew
	if maxSkew <= 0 {
		maxSkew = 5 * time.Minute
	}
	if err := verifySignature(material.Token, timestamp, nonce, signature, request.Body, now, maxSkew); err != nil {
		return channel.VerifiedProtocolPayload{}, err
	}
	value, err := decodeInbound(request.Body)
	if err != nil || value.ExternalAccountID != material.ExternalAccountID {
		return channel.VerifiedProtocolPayload{}, runtime.ErrVersionMismatch
	}
	identity := digest(material.ExternalAccountID, value.ExternalUserID, value.ExternalMessageID)
	return channel.VerifiedProtocolPayload{Body: append([]byte(nil), request.Body...),
		Headers: map[string]string{"content-type": "application/json"}, ProtocolIdentityDigest: identity}, nil
}

func verifySignature(token, timestamp, nonce, signature string, body []byte, now time.Time, maxSkew time.Duration) error {
	seconds, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || nonce == "" || len(nonce) > 256 || len(signature) != sha256.Size*2 {
		return runtime.ErrInvalidEnvelope
	}
	requestTime := time.Unix(seconds, 0).UTC()
	if requestTime.Before(now.Add(-maxSkew)) || requestTime.After(now.Add(maxSkew)) {
		return runtime.ErrInvalidEnvelope
	}
	expected := signatureFor(token, timestamp, nonce, body)
	if subtle.ConstantTimeCompare([]byte(strings.ToLower(signature)), []byte(expected)) != 1 {
		return runtime.ErrVersionMismatch
	}
	return nil
}

func parseMaterial(secret []byte) (VerificationMaterial, error) {
	var value VerificationMaterial
	decoder := json.NewDecoder(bytes.NewReader(secret))
	decoder.DisallowUnknownFields()
	decodeErr := decoder.Decode(&value)
	var trailing any
	trailingErr := decoder.Decode(&trailing)
	if decodeErr != nil || trailingErr != io.EOF || strings.TrimSpace(value.Token) != value.Token || len(value.Token) < 16 ||
		strings.TrimSpace(value.ExternalAccountID) != value.ExternalAccountID || value.ExternalAccountID == "" {
		return VerificationMaterial{}, runtime.ErrVersionMismatch
	}
	return value, nil
}

func decodeInbound(body []byte) (inboundEnvelope, error) {
	var value inboundEnvelope
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	decodeErr := decoder.Decode(&value)
	var trailing any
	trailingErr := decoder.Decode(&trailing)
	if decodeErr != nil || trailingErr != io.EOF || value.SchemaVersion != 1 || value.ExternalAccountID == "" ||
		value.ExternalMessageID == "" || value.ExternalUserID == "" || value.ExternalChatID == "" ||
		(value.ConversationType != "p2p" && value.ConversationType != "group") || value.MessageType != "text" ||
		strings.TrimSpace(value.Text) == "" || !utf8.ValidString(value.Text) || len([]byte(value.Text)) > maxTextBytes || value.OccurredAt.IsZero() {
		return inboundEnvelope{}, runtime.ErrInvalidEnvelope
	}
	return value, nil
}

func decodeMessage(callback channel.VerifiedCallback) (channel.ProviderEvent, error) {
	value, err := decodeInbound(callback.Body)
	if err != nil {
		return channel.ProviderEvent{}, err
	}
	return channel.ProviderEvent{SchemaVersion: 1, Channel: "webui", ExternalAccountID: value.ExternalAccountID,
		ExternalMessageID: value.ExternalMessageID, ConversationType: value.ConversationType,
		ExternalUserID: value.ExternalUserID, ExternalChatID: value.ExternalChatID,
		MessageType: value.MessageType, Text: value.Text, OccurredAt: value.OccurredAt}, nil
}

func signatureFor(token, timestamp, nonce string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(token))
	_, _ = mac.Write([]byte(timestamp))
	_, _ = mac.Write([]byte("\n"))
	_, _ = mac.Write([]byte(nonce))
	_, _ = mac.Write([]byte("\n"))
	_, _ = mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func digest(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		_, _ = h.Write([]byte(part))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func header(values map[string]string, name string) string {
	for key, value := range values {
		if strings.EqualFold(key, name) {
			return value
		}
	}
	return ""
}
