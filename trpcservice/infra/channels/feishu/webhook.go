// Feishu's HTTP event-subscription mode ("事件订阅 → 请求网址").
//
// In this mode Feishu POSTs every event to an endpoint we expose instead of
// pushing it over the long connection. The transport is different but the
// payload semantics are not, so the normalizer's whole job is to turn the HTTP
// callback into the exact byte shape the adapter already decodes (see
// Adapter.Start): a message event passes through untouched, a card-action event
// is re-wrapped into the cardActionEnvelope the long connection produces.
package feishu

import (
	"encoding/json"
	"errors"
	"fmt"

	eventsdk "github.com/larksuite/oapi-sdk-go/v3/event"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/channels"
)

// webhookCallback is the outermost body of an event-subscription callback. The
// event itself is kept raw: a message event is forwarded verbatim, while a card
// action needs its own shape.
type webhookCallback struct {
	Header    Header          `json:"header"`
	Event     json.RawMessage `json:"event"`
	Type      string          `json:"type"`      // "url_verification" for the handshake
	Challenge string          `json:"challenge"` // handshake echo
	Token     string          `json:"token"`     // verification token (plaintext mode)
	Encrypt   string          `json:"encrypt"`   // AES-encrypted body (encrypted mode)
}

// cardActionEvent is the card.action.trigger body as it arrives over HTTP. It
// carries the same fields as the long connection's callback, only not wrapped in
// the SDK's CardActionTriggerEvent.
type cardActionEvent struct {
	Operator *struct {
		OpenID string `json:"open_id"`
		UserID string `json:"user_id"`
	} `json:"operator"`
	Action *struct {
		Value map[string]interface{} `json:"value"`
	} `json:"action"`
	Context *struct {
		OpenMessageID string `json:"open_message_id"`
	} `json:"context"`
	Token string `json:"token"`
}

// VerifyWebhookSignature checks the signature Feishu sends with an HTTP
// event-subscription callback:
//
//	sha256(timestamp + nonce + encrypt_key + body)
//
// Note this is a different formula from VerifySignature (the card-callback
// signature): the body is part of the hash. The computation is delegated to the
// official SDK so a change in the scheme is picked up with the dependency.
func VerifyWebhookSignature(encryptKey, timestamp, nonce, signature string, body []byte) bool {
	if encryptKey == "" || timestamp == "" || nonce == "" || signature == "" {
		return false
	}
	return eventsdk.Signature(timestamp, nonce, encryptKey, string(body)) == signature
}

// DecryptWebhookBody decrypts an encrypted callback body ("encrypt" field) with
// the app's Encrypt Key. A body that is not encrypted is returned unchanged.
func DecryptWebhookBody(encryptKey string, body []byte) ([]byte, error) {
	var envelope struct {
		Encrypt string `json:"encrypt"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("feishu: callback body is not JSON: %w", err)
	}
	if envelope.Encrypt == "" {
		return body, nil
	}
	if encryptKey == "" {
		return nil, errors.New("feishu: callback is encrypted but no encrypt key is configured")
	}
	plain, err := eventsdk.EventDecrypt(envelope.Encrypt, encryptKey)
	if err != nil {
		return nil, fmt.Errorf("feishu: decrypt callback: %w", err)
	}
	return plain, nil
}

// NormalizeWebhookCallback turns an authenticated callback body into the event
// the adapter consumes, plus the handshake challenge when the callback is a URL
// verification.
func NormalizeWebhookCallback(body []byte) (payload []byte, challenge string, err error) {
	var cb webhookCallback
	if err := json.Unmarshal(body, &cb); err != nil {
		return nil, "", fmt.Errorf("feishu: callback body is not JSON: %w", err)
	}
	if cb.Type == "url_verification" {
		if cb.Challenge == "" {
			return nil, "", errors.New("feishu: url_verification callback carries no challenge")
		}
		return nil, cb.Challenge, nil
	}
	switch cb.Header.EventType {
	case "card.action.trigger":
		return normalizeCardAction(cb)
	default:
		// A message event is already in the adapter's wire shape; forward as-is.
		return body, "", nil
	}
}

// normalizeCardAction re-wraps an HTTP card-action event into the envelope the
// adapter decodes, so a button click takes the identical path to a typed reply.
func normalizeCardAction(cb webhookCallback) ([]byte, string, error) {
	var ev cardActionEvent
	if len(cb.Event) > 0 {
		if err := json.Unmarshal(cb.Event, &ev); err != nil {
			return nil, "", fmt.Errorf("feishu: card action body: %w", err)
		}
	}
	act := cardActionPayload{EventID: cb.Header.EventID, Value: map[string]string{}}
	if ev.Operator != nil {
		act.OpenID = ev.Operator.OpenID
		act.UserID = ev.Operator.UserID
	}
	if ev.Context != nil {
		act.MessageID = ev.Context.OpenMessageID
	}
	if ev.Action != nil {
		for k, v := range ev.Action.Value {
			if s, ok := v.(string); ok {
				act.Value[k] = s
			}
		}
	}
	if act.EventID == "" {
		act.EventID = ev.Token
	}
	raw, err := json.Marshal(cardActionEnvelope{CardAction: &act})
	if err != nil {
		return nil, "", fmt.Errorf("feishu: encode card action: %w", err)
	}
	return raw, "", nil
}

// WebhookVerifier returns the channels.WebhookVerifier for Feishu: it verifies
// the callback signature with the binding's Encrypt Key, decrypts the body when
// needed, and normalizes it. Every failure path is a refusal — there is no
// "accept when unverified" branch.
func WebhookVerifier() channels.WebhookVerifier {
	return func(secret string, cb channels.WebhookCallback) (channels.WebhookEvent, error) {
		if !VerifyWebhookSignature(secret, cb.Timestamp, cb.Nonce, cb.Signature, cb.Body) {
			return channels.WebhookEvent{}, errors.New("feishu: signature mismatch")
		}
		plain, err := DecryptWebhookBody(secret, cb.Body)
		if err != nil {
			return channels.WebhookEvent{}, err
		}
		payload, challenge, err := NormalizeWebhookCallback(plain)
		if err != nil {
			return channels.WebhookEvent{}, err
		}
		return channels.WebhookEvent{Payload: payload, Challenge: challenge}, nil
	}
}

// NewWebhookAdapter builds the webhook-mode Feishu adapter for a binding: it
// reuses the app credentials for sending and attachment download, and receives
// events through the Conn's Feed.
func NewWebhookAdapter(tenantID, appID, appSecret string) channels.WebhookAdapter {
	conn := NewWebhookConn(appID, appSecret)
	return New(tenantID, conn.BotOpenID(), conn)
}
