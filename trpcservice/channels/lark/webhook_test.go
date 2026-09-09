package lark

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func webhookTestAdapter(t *testing.T) (*WebhookAdapter, time.Time) {
	t.Helper()
	now := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	adapter, err := NewWebhookAdapter(WebhookConfig{
		Binding:           Binding{TenantID: "tenant-lark", BindingID: "binding-lark", Channel: Channel, AppID: "cli_lark", SecretRef: "env://LARK_SECRET", ReceiverIDType: "open_id", Enabled: true},
		VerificationToken: "verify-token",
		EncryptKey:        "encrypt-key",
		Now:               func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	return adapter, now
}

func TestWebhookChallengeRequiresServerToken(t *testing.T) {
	adapter, _ := webhookTestAdapter(t)
	body := []byte(`{"type":"url_verification","token":"verify-token","challenge":"challenge-value"}`)
	request := httptest.NewRequest("POST", "/webhook/lark/cli_lark", strings.NewReader(string(body)))
	if err := adapter.Verify(request, body); err != nil {
		t.Fatalf("Verify challenge: %v", err)
	}
	response, challenge, err := adapter.Challenge(body)
	if err != nil || !challenge || string(response) != `{"challenge":"challenge-value"}` {
		t.Fatalf("Challenge() = %s, %v, %v", response, challenge, err)
	}
	bad := []byte(`{"type":"url_verification","token":"request-token","challenge":"challenge-value"}`)
	if _, _, err := adapter.Challenge(bad); err == nil {
		t.Fatal("expected invalid server-owned verification token")
	}
}

func TestWebhookVerifiesAndParsesEvent(t *testing.T) {
	adapter, now := webhookTestAdapter(t)
	body := []byte(`{"schema":"2.0","header":{"event_id":"evt-1","event_type":"im.message.receive_v1","app_id":"cli_lark"},"event":{"sender":{"sender_id":{"open_id":"ou-user"}},"message":{"message_id":"om-1","chat_id":"oc-chat","message_type":"text","content":"{\"text\":\"hello\"}"}}}`)
	timestamp := now.Unix()
	nonce := "nonce-1"
	digest := sha256.Sum256([]byte(hexTimestamp(timestamp) + nonce + adapter.encryptKey + string(body)))
	request := httptest.NewRequest("POST", "/webhook/lark/cli_lark", strings.NewReader(string(body)))
	request.Header.Set("X-Lark-Request-Timestamp", hexTimestamp(timestamp))
	request.Header.Set("X-Lark-Request-Nonce", nonce)
	request.Header.Set("X-Lark-Signature", hex.EncodeToString(digest[:]))
	if err := adapter.Verify(request, body); err != nil {
		t.Fatalf("Verify event: %v", err)
	}
	incoming, err := adapter.Parse(body)
	if err != nil {
		t.Fatalf("Parse event: %v", err)
	}
	if incoming.ID != "evt-1" || incoming.UserID != "ou-user" || incoming.ChatID != "oc-chat" || incoming.Text != "hello" {
		t.Fatalf("unexpected incoming: %+v", incoming)
	}
}

func TestWebhookRejectsStaleSignature(t *testing.T) {
	adapter, now := webhookTestAdapter(t)
	body := []byte(`{"header":{"event_id":"evt-1"},"event":{"message":{"message_id":"om-1","chat_id":"oc-chat","content":"{\"text\":\"hello\"}"}}}`)
	timestamp := now.Add(-6 * time.Minute).Unix()
	nonce := "nonce-1"
	digest := sha256.Sum256([]byte(hexTimestamp(timestamp) + nonce + adapter.encryptKey + string(body)))
	request := httptest.NewRequest("POST", "/webhook/lark/cli_lark", strings.NewReader(string(body)))
	request.Header.Set("X-Lark-Request-Timestamp", hexTimestamp(timestamp))
	request.Header.Set("X-Lark-Request-Nonce", nonce)
	request.Header.Set("X-Lark-Signature", hex.EncodeToString(digest[:]))
	if err := adapter.Verify(request, body); err != ErrWebhookClockSkew {
		t.Fatalf("Verify stale event error = %v", err)
	}
}

func TestWebhookDecryptsEncryptedEvent(t *testing.T) {
	adapter, now := webhookTestAdapter(t)
	plain := []byte(`{"header":{"event_id":"evt-encrypted","event_type":"im.message.receive_v1","app_id":"cli_lark"},"event":{"sender":{"sender_id":{"open_id":"ou-user"}},"message":{"message_id":"om-1","chat_id":"oc-chat","message_type":"text","content":"{\"text\":\"encrypted\"}"}}}`)
	encrypted := encryptWebhook(t, adapter.encryptKey, plain)
	body := []byte(`{"encrypt":"` + encrypted + `"}`)
	timestamp := now.Unix()
	nonce := "nonce-encrypted"
	digest := sha256.Sum256([]byte(hexTimestamp(timestamp) + nonce + adapter.encryptKey + string(body)))
	request := httptest.NewRequest("POST", "/webhook/lark/cli_lark", strings.NewReader(string(body)))
	request.Header.Set("X-Lark-Request-Timestamp", hexTimestamp(timestamp))
	request.Header.Set("X-Lark-Request-Nonce", nonce)
	request.Header.Set("X-Lark-Signature", hex.EncodeToString(digest[:]))
	if err := adapter.Verify(request, body); err != nil {
		t.Fatalf("Verify encrypted event: %v", err)
	}
	incoming, err := adapter.Parse(body)
	if err != nil || incoming.Text != "encrypted" {
		t.Fatalf("Parse encrypted event = %+v, %v", incoming, err)
	}
}

func hexTimestamp(timestamp int64) string {
	return fmtInt64(timestamp)
}

func fmtInt64(value int64) string {
	if value < 0 {
		return "-" + fmtInt64(-value)
	}
	if value < 10 {
		return string(rune('0' + value))
	}
	return fmtInt64(value/10) + string(rune('0'+value%10))
}

func encryptWebhook(t *testing.T, encryptKey string, plain []byte) string {
	t.Helper()
	key := sha256.Sum256([]byte(encryptKey))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		t.Fatal(err)
	}
	padding := block.BlockSize() - len(plain)%block.BlockSize()
	plain = append(append([]byte(nil), plain...), bytesRepeat(byte(padding), padding)...)
	iv := make([]byte, block.BlockSize())
	for index := range iv {
		iv[index] = byte(index + 1)
	}
	ciphertext := make([]byte, len(plain))
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(ciphertext, plain)
	return base64.StdEncoding.EncodeToString(append(iv, ciphertext...))
}

func bytesRepeat(value byte, count int) []byte {
	out := make([]byte, count)
	for index := range out {
		out[index] = value
	}
	return out
}
