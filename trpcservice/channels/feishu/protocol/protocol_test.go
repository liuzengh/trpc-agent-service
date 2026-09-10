package protocol_test

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu/protocol"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestVerifierChecksSignatureClockTokenAndAppThenDecrypts(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	material := protocol.VerificationMaterial{EncryptKey: "encrypt-key", VerificationToken: "verify-token", AppID: "cli_app", BotOpenID: "ou_bot"}
	secret, _ := json.Marshal(material)
	plaintext := messagePayload("p2p", `{"text":"hello"}`, nil)
	body := encryptedBody(t, plaintext, material.EncryptKey)
	timestamp, nonce := fmt.Sprint(now.Unix()), "nonce"
	request := channel.CallbackRequest{Body: body, ReceivedAt: now, Headers: map[string]string{
		"X-Lark-Request-Timestamp": timestamp, "X-Lark-Request-Nonce": nonce,
		"X-Lark-Signature": larkevent.Signature(timestamp, nonce, material.EncryptKey, string(body)),
	}}
	verifier := protocol.Verifier{Now: func() time.Time { return now }}
	verified, err := verifier.Verify(context.Background(), request, secret)
	if err != nil || string(verified.Body) != string(plaintext) || verified.ProtocolIdentityDigest == "" || verified.Headers["x-feishu-bot-open-id"] != "ou_bot" {
		t.Fatalf("verified=%#v err=%v", verified, err)
	}
	for name, mutate := range map[string]func(*channel.CallbackRequest){
		"bad signature": func(in *channel.CallbackRequest) { in.Headers["X-Lark-Signature"] = "forged" },
		"stale": func(in *channel.CallbackRequest) {
			in.Headers["X-Lark-Request-Timestamp"] = fmt.Sprint(now.Add(-6 * time.Minute).Unix())
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := request
			candidate.Headers = clone(request.Headers)
			mutate(&candidate)
			if _, err := verifier.Verify(context.Background(), candidate, secret); err == nil {
				t.Fatal("forged callback accepted")
			}
		})
	}
}

func TestVerifierChecksPlaintextAndEncryptedChallenges(t *testing.T) {
	material := protocol.VerificationMaterial{EncryptKey: "encrypt-key", VerificationToken: "verify-token", AppID: "cli_app", BotOpenID: "ou_bot"}
	secret, _ := json.Marshal(material)
	plaintext := []byte(`{"type":"url_verification","token":"verify-token","challenge":"challenge-value"}`)
	v2 := []byte(`{"schema":"2.0","header":{"event_id":"event","event_type":"contact.user.created_v3","app_id":"cli_app","tenant_key":"tenant","create_time":"1800000000000","token":"verify-token"},"event":{"object":{"open_id":"ou_user"}},"challenge":"challenge-value","type":"url_verification"}`)
	verifier := protocol.Verifier{}

	for name, body := range map[string][]byte{
		"plaintext": plaintext,
		"v2":        v2,
		"encrypted": encryptedBody(t, plaintext, material.EncryptKey),
	} {
		t.Run(name, func(t *testing.T) {
			request := channel.CallbackRequest{Body: body}
			if !protocol.IsChallengeRequest(request) {
				t.Fatal("challenge not recognized")
			}
			verified, err := verifier.VerifyChallenge(context.Background(), request, secret)
			if err != nil || string(verified.Body) != `{"challenge":"challenge-value"}` ||
				verified.Headers["content-type"] != "application/json" || verified.ProtocolIdentityDigest == "" {
				t.Fatalf("verified=%#v err=%v", verified, err)
			}
		})
	}

	for name, body := range map[string][]byte{
		"bad token":  []byte(`{"type":"url_verification","token":"forged","challenge":"challenge-value"}`),
		"wrong type": []byte(`{"type":"callback","token":"verify-token","challenge":"challenge-value"}`),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := verifier.VerifyChallenge(context.Background(), channel.CallbackRequest{Body: body}, secret); err == nil {
				t.Fatal("invalid challenge accepted")
			}
		})
	}
}

func TestDecodeMessageMentionMatrix(t *testing.T) {
	mention := func(key, openID, name string) map[string]any {
		return map[string]any{"key": key, "id": map[string]any{"open_id": openID}, "name": name, "tenant_key": "tenant-key"}
	}
	tests := []struct {
		name, chatType, text, want string
		mentions                   []map[string]any
		ignored, wantErr           bool
	}{
		{name: "p2p", chatType: "p2p", text: "hello", want: "hello"},
		{name: "bot only", chatType: "group", text: "@_user_1", mentions: []map[string]any{mention("@_user_1", "ou_bot", "Bot")}, ignored: true},
		{name: "other only", chatType: "group", text: "@_user_1 hi", mentions: []map[string]any{mention("@_user_1", "ou_alice", "Alice")}, ignored: true},
		{name: "all only", chatType: "group", text: "@_all hi", mentions: []map[string]any{mention("@_all", "", "All")}, ignored: true},
		{name: "bot and other", chatType: "group", text: "@_user_1 @_user_2 hello", mentions: []map[string]any{mention("@_user_1", "ou_bot", "Bot"), mention("@_user_2", "ou_alice", "Alice")}, want: "@Alice hello"},
		{name: "bot and all", chatType: "group", text: "@_user_1 @_all hello", mentions: []map[string]any{mention("@_user_1", "ou_bot", "Bot"), mention("@_all", "", "All")}, want: "@所有人 hello"},
		{name: "duplicate self", chatType: "group", text: "@_user_1 hello", mentions: []map[string]any{mention("@_user_1", "ou_bot", "Bot"), mention("@_user_1", "ou_bot", "Bot")}, wantErr: true},
		{name: "spoofed display name", chatType: "group", text: "@_user_1 hello", mentions: []map[string]any{mention("@_user_1", "ou_other", "Bot")}, ignored: true},
		{name: "unresolved placeholder", chatType: "group", text: "@_user_9 hello", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			content, _ := json.Marshal(map[string]string{"text": test.text})
			result, err := protocol.DecodeMessage(channel.VerifiedCallback{Body: messagePayload(test.chatType, string(content), test.mentions), Headers: map[string]string{"x-feishu-bot-open-id": "ou_bot"}})
			if test.wantErr {
				if !errors.Is(err, runtime.ErrInvalidEnvelope) {
					t.Fatalf("error=%v", err)
				}
				return
			}
			if err != nil || result.Ignored != test.ignored || result.Event.Text != test.want {
				t.Fatalf("result=%#v err=%v", result, err)
			}
		})
	}
}

func TestDecodeMessageWithoutBotOpenIDAllowsP2PAndIgnoresGroups(t *testing.T) {
	p2p, err := protocol.DecodeMessage(channel.VerifiedCallback{Body: messagePayload("p2p", `{"text":"hello"}`, nil)})
	if err != nil || p2p.Ignored || p2p.Event.Text != "hello" {
		t.Fatalf("p2p=%#v err=%v", p2p, err)
	}
	group, err := protocol.DecodeMessage(channel.VerifiedCallback{Body: messagePayload("group", `{"text":"hello"}`, nil)})
	if err != nil || !group.Ignored {
		t.Fatalf("group=%#v err=%v", group, err)
	}
}

func TestDecodeMessageMapsP2PMediaAndRejectsGroupMedia(t *testing.T) {
	for _, test := range []struct{ kind, content, id string }{
		{kind: "image", content: `{"image_key":"img_key"}`, id: "img_key"},
		{kind: "file", content: `{"file_key":"file_key"}`, id: "file_key"},
	} {
		t.Run(test.kind, func(t *testing.T) {
			result, err := protocol.DecodeMessage(channel.VerifiedCallback{Body: providerMessagePayload("p2p", test.kind, test.content, nil),
				Headers: map[string]string{"x-feishu-bot-open-id": "ou_bot"}})
			if err != nil || result.Ignored || result.Event.MessageType != test.kind || result.Event.Text != "" ||
				len(result.Event.MediaRefs) != 1 || result.Event.MediaRefs[0].ID != test.id || result.Event.MediaRefs[0].MessageID != "om_message" || result.Event.MediaRefs[0].Kind != test.kind {
				t.Fatalf("result=%#v err=%v", result, err)
			}
			if _, err := protocol.DecodeMessage(channel.VerifiedCallback{Body: providerMessagePayload("group", test.kind, test.content, nil),
				Headers: map[string]string{"x-feishu-bot-open-id": "ou_bot"}}); !errors.Is(err, runtime.ErrInvalidEnvelope) {
				t.Fatalf("group media err=%v", err)
			}
		})
	}
}

func messagePayload(chatType, content string, mentions []map[string]any) []byte {
	return providerMessagePayload(chatType, "text", content, mentions)
}

func providerMessagePayload(chatType, messageType, content string, mentions []map[string]any) []byte {
	payload := map[string]any{
		"schema": "2.0", "header": map[string]any{"event_id": "evt_1", "event_type": protocol.EventTypeMessageReceive, "app_id": "cli_app", "tenant_key": "tenant-key", "token": "verify-token", "create_time": "1800000000000"},
		"event": map[string]any{
			"sender":  map[string]any{"sender_id": map[string]any{"open_id": "ou_user"}, "sender_type": "user", "tenant_key": "tenant-key"},
			"message": map[string]any{"message_id": "om_message", "chat_id": "oc_chat", "chat_type": chatType, "message_type": messageType, "content": content, "mentions": mentions, "create_time": "1800000000000"},
		},
	}
	encoded, _ := json.Marshal(payload)
	return encoded
}

func encryptedBody(t *testing.T, plaintext []byte, secret string) []byte {
	t.Helper()
	key := sha256.Sum256([]byte(secret))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		t.Fatal(err)
	}
	padding := aes.BlockSize - len(plaintext)%aes.BlockSize
	padded := append(append([]byte(nil), plaintext...), make([]byte, padding)...)
	iv := []byte("0123456789abcdef")
	ciphertext := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(ciphertext, padded)
	value := base64.StdEncoding.EncodeToString(append(append([]byte(nil), iv...), ciphertext...))
	body, _ := json.Marshal(map[string]string{"encrypt": value})
	return body
}

func clone(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
