package feishu

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/channels"
)

// signCallback produces the headers Feishu sends with a callback.
func signCallback(encryptKey, timestamp, nonce string, body []byte) channels.WebhookCallback {
	sum := sha256.Sum256([]byte(timestamp + nonce + encryptKey + string(body)))
	return channels.WebhookCallback{
		Body:      body,
		Timestamp: timestamp,
		Nonce:     nonce,
		Signature: hex.EncodeToString(sum[:]),
	}
}

func TestVerifyWebhookSignature(t *testing.T) {
	body := []byte(`{"header":{"event_id":"e1"}}`)
	cb := signCallback("key-1", "1700000000", "nonce-1", body)
	if !VerifyWebhookSignature("key-1", cb.Timestamp, cb.Nonce, cb.Signature, cb.Body) {
		t.Fatal("a correctly signed callback must verify")
	}
	// The body is part of the hash: a tampered body invalidates the signature.
	tampered := append([]byte(nil), body...)
	tampered[0] = 'X'
	if VerifyWebhookSignature("key-1", cb.Timestamp, cb.Nonce, cb.Signature, tampered) {
		t.Error("a tampered body must not verify")
	}
	if VerifyWebhookSignature("other-key", cb.Timestamp, cb.Nonce, cb.Signature, cb.Body) {
		t.Error("a wrong encrypt key must not verify")
	}
	// Missing inputs are refusals, never "skip verification".
	if VerifyWebhookSignature("", cb.Timestamp, cb.Nonce, cb.Signature, cb.Body) ||
		VerifyWebhookSignature("key-1", "", cb.Nonce, cb.Signature, cb.Body) ||
		VerifyWebhookSignature("key-1", cb.Timestamp, "", cb.Signature, cb.Body) ||
		VerifyWebhookSignature("key-1", cb.Timestamp, cb.Nonce, "", cb.Body) {
		t.Error("an incomplete signature must not verify")
	}
}

func TestDecryptWebhookBodyRoundTrip(t *testing.T) {
	plain := `{"header":{"event_id":"ev-dec"}}`
	encrypted, err := larkcore.EncryptedEventMsg(t.Context(), plain, "enc-key")
	if err != nil {
		t.Fatalf("encrypt fixture: %v", err)
	}
	// Feishu wraps the ciphertext in an {"encrypt": "..."} envelope.
	envelope := []byte(`{"encrypt":"` + encrypted + `"}`)
	got, err := DecryptWebhookBody("enc-key", envelope)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if string(got) != plain {
		t.Errorf("decrypted = %s, want %s", got, plain)
	}

	// A plaintext body passes through untouched.
	if got, err := DecryptWebhookBody("enc-key", []byte(plain)); err != nil || string(got) != plain {
		t.Errorf("plaintext body = %s (err %v), want it unchanged", got, err)
	}
	// Encrypted with no key configured is an error, not a silent pass.
	if _, err := DecryptWebhookBody("", envelope); err == nil {
		t.Error("an encrypted body without a key must fail")
	}
}

func TestNormalizeWebhookCallbackMessagePassthrough(t *testing.T) {
	body := []byte(`{"schema":"2.0","header":{"event_id":"ev-1","event_type":"im.message.receive_v1"},"event":{"message":{"message_id":"om_1"}}}`)
	payload, challenge, err := NormalizeWebhookCallback(body)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if challenge != "" {
		t.Errorf("challenge = %q, want none", challenge)
	}
	// Forwarded verbatim so the adapter's existing normalization is reused.
	if string(payload) != string(body) {
		t.Errorf("payload = %s, want the body unchanged", payload)
	}
}

func TestNormalizeWebhookCallbackHandshake(t *testing.T) {
	body := []byte(`{"type":"url_verification","challenge":"chal-abc","token":"vt"}`)
	payload, challenge, err := NormalizeWebhookCallback(body)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if payload != nil {
		t.Errorf("a handshake must not produce an event payload: %s", payload)
	}
	if challenge != "chal-abc" {
		t.Errorf("challenge = %q, want chal-abc", challenge)
	}
	if _, _, err := NormalizeWebhookCallback([]byte(`{"type":"url_verification"}`)); err == nil {
		t.Error("a handshake without a challenge must fail")
	}
}

// TestNormalizeWebhookCallbackCardAction covers the HTTP shape of a card button
// click: it must become the same envelope the long connection produces, so the
// approval decision takes the identical path.
func TestNormalizeWebhookCallbackCardAction(t *testing.T) {
	body := []byte(`{
		"schema":"2.0",
		"header":{"event_id":"ev-click","event_type":"card.action.trigger"},
		"event":{
			"operator":{"open_id":"ou_1"},
			"action":{"value":{"session_id":"s-1","decision":"approve","chat_id":"oc_1"}},
			"context":{"open_message_id":"om_1"},
			"token":"tok"
		}
	}`)
	payload, _, err := NormalizeWebhookCallback(body)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	var env cardActionEnvelope
	if err := json.Unmarshal(payload, &env); err != nil {
		t.Fatalf("payload is not a card-action envelope: %s", payload)
	}
	if env.CardAction == nil {
		t.Fatal("card action missing")
	}
	if env.CardAction.EventID != "ev-click" || env.CardAction.OpenID != "ou_1" {
		t.Errorf("identity lost: %+v", env.CardAction)
	}
	if env.CardAction.Value["decision"] != "approve" || env.CardAction.Value["session_id"] != "s-1" {
		t.Errorf("button payload lost: %+v", env.CardAction.Value)
	}
	// The adapter must turn it into the approval reply the worker recognizes.
	in := CardActionToInbound("t1", &env)
	if in == nil || in.Content != "批准" || in.SessionID != "s-1" {
		t.Fatalf("card action did not normalize to an approval reply: %+v", in)
	}
}

// TestWebhookVerifierFailsClosed is the security-critical case: every failure
// mode must be a refusal.
func TestWebhookVerifierFailsClosed(t *testing.T) {
	verify := WebhookVerifier()
	body := []byte(`{"header":{"event_id":"e1","event_type":"im.message.receive_v1"}}`)

	if _, err := verify("key-1", channels.WebhookCallback{Body: body, Timestamp: "t", Nonce: "n", Signature: "bad"}); err == nil {
		t.Error("a bad signature must be rejected")
	}
	if _, err := verify("key-1", channels.WebhookCallback{Body: body}); err == nil {
		t.Error("missing signature headers must be rejected")
	}
	if _, err := verify("", signCallback("key-1", "t", "n", body)); err == nil {
		t.Error("an empty secret must be rejected")
	}

	cb := signCallback("key-1", "t", "n", body)
	event, err := verify("key-1", cb)
	if err != nil {
		t.Fatalf("a valid callback must verify: %v", err)
	}
	if !strings.Contains(string(event.Payload), "im.message.receive_v1") {
		t.Errorf("payload = %s", event.Payload)
	}
}

// TestWebhookVerifierAcceptsEncryptedCallback proves the encrypted mode works
// end to end: sign the encrypted body, decrypt, normalize.
func TestWebhookVerifierAcceptsEncryptedCallback(t *testing.T) {
	plain := `{"schema":"2.0","header":{"event_id":"ev-enc","event_type":"im.message.receive_v1"},"event":{"message":{"message_id":"om_1"}}}`
	encrypted, err := larkcore.EncryptedEventMsg(t.Context(), plain, "enc-key")
	if err != nil {
		t.Fatalf("encrypt fixture: %v", err)
	}
	cb := signCallback("enc-key", "t", "n", []byte(`{"encrypt":"`+encrypted+`"}`))
	event, err := WebhookVerifier()("enc-key", cb)
	if err != nil {
		t.Fatalf("encrypted callback: %v", err)
	}
	if !strings.Contains(string(event.Payload), "ev-enc") {
		t.Errorf("payload = %s, want the decrypted event", event.Payload)
	}
}

// TestWebhookConnFeedAndRecv: the webhook transport must surface pushed events
// through the same Recv the long connection uses, so the adapter is unchanged.
func TestWebhookConnFeedAndRecv(t *testing.T) {
	conn := NewWebhookConn("cli_app", "secret")
	defer func() { _ = conn.Close() }()
	if !conn.Feed([]byte(`{"header":{"event_id":"e1"}}`)) {
		t.Fatal("Feed refused a payload with an empty buffer")
	}
	raw, err := conn.Recv(t.Context())
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if !strings.Contains(string(raw), "e1") {
		t.Errorf("Recv returned %s", raw)
	}

	// Close unblocks Recv rather than hanging the adapter's read loop.
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := conn.Recv(t.Context()); err == nil {
		t.Error("Recv after Close must fail so the adapter stops instead of blocking")
	}
	if conn.Feed([]byte("{}")) {
		t.Error("Feed after Close must be refused")
	}
}
