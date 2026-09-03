package wecom

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

func TestCallbackDecryptsTextMessage(t *testing.T) {
	keyText := testEncodingKey()
	secrets := secret.StaticStore{
		"secret://callback-token": "callback-token",
		"secret://aes-key":        keyText,
		"secret://app-secret":     "app-secret",
	}
	adapter, err := New(secrets, nil)
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	binding := testBinding(t, "https://example.invalid")
	plaintext := []byte(`<xml><ToUserName>corp-test</ToUserName><FromUserName>alice</FromUserName><CreateTime>1700000000</CreateTime><MsgType>text</MsgType><Content>hello</Content><MsgId>10001</MsgId><AgentID>1000002</AgentID></xml>`)
	encrypted := encryptForTest(t, keyText, "corp-test", plaintext)
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	nonce := "nonce"
	signature := signatureForTest("callback-token", timestamp, nonce, encrypted)
	body, _ := xml.Marshal(struct {
		XMLName xml.Name `xml:"xml"`
		Encrypt string   `xml:"Encrypt"`
	}{Encrypt: encrypted})
	request := httptest.NewRequest(
		http.MethodPost,
		"/callbacks/wecom/key?timestamp="+timestamp+"&nonce="+nonce+"&msg_signature="+signature,
		bytes.NewReader(body),
	)
	result, err := adapter.Callback(context.Background(), binding, request)
	if err != nil {
		t.Fatalf("callback: %v", err)
	}
	if result.StatusCode != http.StatusOK || string(result.Body) != "success" || len(result.Messages) != 1 {
		t.Fatalf("callback result = %+v", result)
	}
	message := result.Messages[0]
	if message.ExternalMessageID != "10001" || message.ExternalUserID != "alice" ||
		message.Text != "hello" || message.ChatType != "direct" {
		t.Fatalf("decoded message = %+v", message)
	}
}

func TestCallbackVerifiesURLChallenge(t *testing.T) {
	keyText := testEncodingKey()
	adapter, _ := New(secret.StaticStore{
		"secret://callback-token": "callback-token",
		"secret://aes-key":        keyText,
		"secret://app-secret":     "app-secret",
	}, nil)
	challenge := encryptForTest(t, keyText, "corp-test", []byte("challenge"))
	timestamp, nonce := strconv.FormatInt(time.Now().Unix(), 10), "nonce"
	request := httptest.NewRequest(http.MethodGet, "/?"+url.Values{
		"timestamp":     {timestamp},
		"nonce":         {nonce},
		"echostr":       {challenge},
		"msg_signature": {signatureForTest("callback-token", timestamp, nonce, challenge)},
	}.Encode(), nil)
	result, err := adapter.Callback(context.Background(), testBinding(t, ""), request)
	if err != nil || string(result.Body) != "challenge" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestSendGetsTokenAndSendsMessage(t *testing.T) {
	var tokenCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cgi-bin/gettoken":
			tokenCalls++
			if r.URL.Query().Get("corpid") != "corp-test" || r.URL.Query().Get("corpsecret") != "app-secret" {
				t.Errorf("unexpected token query: %s", r.URL.RawQuery)
			}
			_, _ = io.WriteString(w, `{"errcode":0,"access_token":"access-token","expires_in":7200}`)
		case "/cgi-bin/message/send":
			if r.URL.Query().Get("access_token") != "access-token" {
				t.Errorf("unexpected access token")
			}
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode send body: %v", err)
			}
			if body["touser"] != "alice" {
				t.Errorf("unexpected target: %v", body["touser"])
			}
			_, _ = io.WriteString(w, `{"errcode":0,"errmsg":"ok","msgid":"provider-1"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	adapter, _ := New(secret.StaticStore{
		"secret://callback-token": "callback-token",
		"secret://aes-key":        testEncodingKey(),
		"secret://app-secret":     "app-secret",
	}, server.Client())
	binding := testBinding(t, server.URL)
	for i := 0; i < 2; i++ {
		receipt, err := adapter.Send(context.Background(), binding, channels.OutboundMessage{
			OutboundID:  "outbound",
			RequestID:   "request",
			Text:        "hello",
			ReplyTarget: "alice",
		})
		if err != nil || receipt.ProviderMessageID != "provider-1" {
			t.Fatalf("receipt=%+v err=%v", receipt, err)
		}
	}
	if tokenCalls != 1 {
		t.Fatalf("token calls = %d, want 1", tokenCalls)
	}
}

func testBinding(t *testing.T, apiBase string) controlplane.ChannelBinding {
	t.Helper()
	configJSON, err := json.Marshal(bindingConfig{
		CorpID:            "corp-test",
		AgentID:           1000002,
		CallbackTokenRef:  "secret://callback-token",
		EncodingAESKeyRef: "secret://aes-key",
		AppSecretRef:      "secret://app-secret",
		APIBaseURL:        apiBase,
	})
	if err != nil {
		t.Fatalf("marshal binding: %v", err)
	}
	return controlplane.ChannelBinding{
		ID:          "wecom-binding",
		TenantID:    "tenant",
		AppID:       "app",
		ChannelType: "wecom",
		Config:      configJSON,
		Status:      controlplane.StatusActive,
		Version:     1,
	}
}

func testEncodingKey() string {
	key := bytes.Repeat([]byte{7}, 32)
	return strings.TrimRight(base64.StdEncoding.EncodeToString(key), "=")
}

func encryptForTest(t *testing.T, keyText string, corpID string, message []byte) string {
	t.Helper()
	key, _ := base64.StdEncoding.DecodeString(keyText + "=")
	plaintext := append(bytes.Repeat([]byte{1}, 16), make([]byte, 4)...)
	binary.BigEndian.PutUint32(plaintext[16:20], uint32(len(message)))
	plaintext = append(plaintext, message...)
	plaintext = append(plaintext, []byte(corpID)...)
	padding := 32 - len(plaintext)%32
	plaintext = append(plaintext, bytes.Repeat([]byte{byte(padding)}, padding)...)
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	ciphertext := make([]byte, len(plaintext))
	cipher.NewCBCEncrypter(block, key[:aes.BlockSize]).CryptBlocks(ciphertext, plaintext)
	return base64.StdEncoding.EncodeToString(ciphertext)
}

func signatureForTest(token, timestamp, nonce, encrypted string) string {
	values := []string{token, timestamp, nonce, encrypted}
	sort.Strings(values)
	digest := sha1.Sum([]byte(strings.Join(values, "")))
	return hex.EncodeToString(digest[:])
}

func TestProviderRateLimitIsRetryable(t *testing.T) {
	err := providerError("send", 45009, "rate limited")
	var deliveryErr *channels.DeliveryError
	if !errors.As(err, &deliveryErr) || !deliveryErr.Retryable || deliveryErr.RetryAfter != time.Minute {
		t.Fatalf("delivery error = %+v", deliveryErr)
	}
}
