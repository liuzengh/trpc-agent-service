package feishu_test

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway/openclaw"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
)

const (
	testAppID   = "cli_test_app"
	testToken   = "verification-token-fixture"
	testEncrypt = "encrypt-key-fixture"
)

func testBinding(tenantID string) feishu.Binding {
	return feishu.Binding{
		TenantID: tenantID, AppID: "assistant", BindingID: "feishu-a",
		FeishuAppID: testAppID, VerificationToken: testToken,
		ConfigVersion: 3,
	}
}

func staticProvider(bindings ...feishu.Binding) feishu.BindingProvider {
	return func(string) []feishu.Binding { return bindings }
}

type acceptorStub struct {
	mu       sync.Mutex
	messages []gateway.InboundMessage
	err      error
}

func (stub *acceptorStub) AcceptInbound(_ context.Context, message gateway.InboundMessage) (gateway.AcceptedMessage, error) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.messages = append(stub.messages, message)
	if stub.err != nil {
		return gateway.AcceptedMessage{}, stub.err
	}
	return gateway.AcceptedMessage{RequestID: message.ExternalMessageID, SessionID: message.SessionID, TraceID: message.TraceID}, nil
}

func textEvent(appID, eventID, openID, chatType, chatID, text string, mentions ...string) string {
	mentionJSON := ""
	if len(mentions) > 0 {
		mentionJSON = `,"mentions":[`
		for i, key := range mentions {
			if i > 0 {
				mentionJSON += ","
			}
			mentionJSON += fmt.Sprintf(`{"key":%q,"name":"bot"}`, key)
		}
		mentionJSON += `]`
	}
	content, _ := json.Marshal(map[string]string{"text": text})
	return fmt.Sprintf(`{
  "schema": "2.0",
  "header": {"event_id": %q, "event_type": "im.message.receive_v1", "token": %q, "app_id": %q, "tenant_key": "tk"},
  "event": {
    "sender": {"sender_id": {"open_id": %q, "union_id": "un-1"}, "sender_type": "user"},
    "message": {"message_id": "om_1", "chat_id": %q, "chat_type": %q, "message_type": "text", "content": %s%s}
  }
}`, eventID, testToken, appID, openID, chatID, chatType, strconv(content), mentionJSON)
}

func mediaEvent(appID, eventID, openID, messageType, content string) string {
	return fmt.Sprintf(`{
  "schema": "2.0",
  "header": {"event_id": %q, "event_type": "im.message.receive_v1", "token": %q, "app_id": %q, "tenant_key": "tk"},
  "event": {
    "sender": {"sender_id": {"open_id": %q}, "sender_type": "user"},
    "message": {"message_id": "om_2", "chat_id": "oc_dm", "chat_type": "p2p", "message_type": %q, "content": %s}
  }
}`, eventID, testToken, appID, openID, messageType, strconv([]byte(content)))
}

func strconv(value []byte) string {
	encoded, _ := json.Marshal(string(value))
	return string(encoded)
}

func encryptBody(t *testing.T, key string, plain []byte) []byte {
	t.Helper()
	sum := sha256.Sum256([]byte(key))
	block, err := aes.NewCipher(sum[:])
	if err != nil {
		t.Fatal(err)
	}
	padding := aes.BlockSize - len(plain)%aes.BlockSize
	padded := append([]byte(nil), plain...)
	for i := 0; i < padding; i++ {
		padded = append(padded, byte(padding))
	}
	iv := make([]byte, aes.BlockSize)
	for i := range iv {
		iv[i] = byte(i + 7)
	}
	encrypted := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(encrypted, padded)
	body, _ := json.Marshal(map[string]string{"encrypt": base64.StdEncoding.EncodeToString(append(iv, encrypted...))})
	return body
}

func post(adapter http.Handler, bindingID string, body []byte) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/channels/feishu/"+bindingID, strings.NewReader(string(body)))
	response := httptest.NewRecorder()
	adapter.ServeHTTP(response, request)
	return response
}

func postSigned(adapter http.Handler, bindingID string, body []byte, encryptKey string) *httptest.ResponseRecorder {
	const timestamp = "1788080000"
	const nonce = "feishu-callback-nonce"
	digest := sha256.Sum256([]byte(timestamp + nonce + encryptKey + string(body)))
	request := httptest.NewRequest(http.MethodPost, "/channels/feishu/"+bindingID, strings.NewReader(string(body)))
	request.Header.Set("X-Lark-Request-Timestamp", timestamp)
	request.Header.Set("X-Lark-Request-Nonce", nonce)
	request.Header.Set("X-Lark-Signature", hex.EncodeToString(digest[:]))
	response := httptest.NewRecorder()
	adapter.ServeHTTP(response, request)
	return response
}

func TestURLVerificationReturnsChallenge(t *testing.T) {
	acceptor := &acceptorStub{}
	adapter, err := feishu.NewDynamicHandler(acceptor, staticProvider(testBinding("tenant-a")))
	if err != nil {
		t.Fatal(err)
	}
	body := fmt.Sprintf(`{"challenge":"ch-1","token":%q,"type":"url_verification"}`, testToken)
	response := post(adapter, "feishu-a", []byte(body))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"challenge":"ch-1"`) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestURLVerificationRejectsBadToken(t *testing.T) {
	adapter, _ := feishu.NewDynamicHandler(&acceptorStub{}, staticProvider(testBinding("tenant-a")))
	response := post(adapter, "feishu-a", []byte(`{"challenge":"ch-1","token":"wrong","type":"url_verification"}`))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d", response.Code)
	}
}

func TestEncryptedURLVerificationDoesNotRequireEventSignature(t *testing.T) {
	binding := testBinding("tenant-a")
	binding.EncryptKey = testEncrypt
	adapter, _ := feishu.NewDynamicHandler(&acceptorStub{}, staticProvider(binding))
	body := encryptBody(t, testEncrypt, []byte(fmt.Sprintf(`{"challenge":"ch-encrypted","token":%q,"type":"url_verification"}`, testToken)))
	response := post(adapter, "feishu-a", body)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"challenge":"ch-encrypted"`) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}

	wrong := encryptBody(t, testEncrypt, []byte(`{"challenge":"ch-encrypted","token":"wrong","type":"url_verification"}`))
	if response = post(adapter, "feishu-a", wrong); response.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestEncryptedEventDecryptsAndEntersGateway(t *testing.T) {
	binding := testBinding("tenant-a")
	binding.EncryptKey = testEncrypt
	acceptor := &acceptorStub{}
	adapter, _ := feishu.NewDynamicHandler(acceptor, staticProvider(binding))
	event := textEvent(testAppID, "evt-enc-1", "ou_alice", "p2p", "oc_dm", "hello agent", "@_user_1")
	body := encryptBody(t, testEncrypt, []byte(event))
	response := postSigned(adapter, "feishu-a", body, testEncrypt)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if len(acceptor.messages) != 1 {
		t.Fatalf("messages=%d", len(acceptor.messages))
	}
	if acceptor.messages[0].Text != "hello agent" {
		t.Fatalf("mention normalization=%q", acceptor.messages[0].Text)
	}
}

func TestEncryptedCallbackRejectsWrongKeyAndPlaintext(t *testing.T) {
	binding := testBinding("tenant-a")
	binding.EncryptKey = testEncrypt
	adapter, _ := feishu.NewDynamicHandler(&acceptorStub{}, staticProvider(binding))
	// Encrypted with a different key must be rejected.
	body := encryptBody(t, "another-key", []byte(textEvent(testAppID, "evt-x", "ou_a", "p2p", "oc", "hi")))
	response := postSigned(adapter, "feishu-a", body, testEncrypt)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("wrong key status=%d", response.Code)
	}
	// Plaintext must never be accepted when encryption is configured.
	response = post(adapter, "feishu-a", []byte(textEvent(testAppID, "evt-x", "ou_a", "p2p", "oc", "hi")))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("plaintext status=%d", response.Code)
	}
}

func TestEncryptedCallbackRequiresValidSignature(t *testing.T) {
	binding := testBinding("tenant-a")
	binding.EncryptKey = testEncrypt
	adapter, _ := feishu.NewDynamicHandler(&acceptorStub{}, staticProvider(binding))
	body := encryptBody(t, testEncrypt, []byte(textEvent(testAppID, "evt-sig", "ou_a", "p2p", "oc", "hi")))

	if response := post(adapter, "feishu-a", body); response.Code != http.StatusUnauthorized {
		t.Fatalf("missing signature status=%d", response.Code)
	}
	if response := postSigned(adapter, "feishu-a", body, "wrong-signature-key"); response.Code != http.StatusUnauthorized {
		t.Fatalf("invalid signature status=%d", response.Code)
	}
}

func TestEncryptedCallbackNarrowsTenantByEncryptKey(t *testing.T) {
	// These bindings deliberately share binding_id, app_id, and Verification
	// Token. Only tenant-b owns the key that authenticates and decrypts the
	// callback, so database row order must never route it to tenant-a.
	bindingA := testBinding("tenant-a")
	bindingA.EncryptKey = "tenant-a-encrypt-key"
	bindingB := testBinding("tenant-b")
	bindingB.EncryptKey = "tenant-b-encrypt-key"
	acceptor := &acceptorStub{}
	adapter, _ := feishu.NewDynamicHandler(acceptor, staticProvider(bindingA, bindingB))
	event := textEvent(testAppID, "evt-key-scope", "ou_same", "p2p", "oc_dm", "hello")
	body := encryptBody(t, bindingB.EncryptKey, []byte(event))

	response := postSigned(adapter, "feishu-a", body, bindingB.EncryptKey)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if len(acceptor.messages) != 1 || acceptor.messages[0].TenantID != "tenant-b" {
		t.Fatalf("messages=%+v", acceptor.messages)
	}
}

func TestPlaintextCallbackRejectsAmbiguousTenant(t *testing.T) {
	// Without an Encrypt Key, identical app_id and Verification Token values
	// cannot prove which tenant owns the callback. Fail closed instead of
	// choosing the first database row.
	bindingA := testBinding("tenant-a")
	bindingB := testBinding("tenant-b")
	acceptor := &acceptorStub{}
	adapter, _ := feishu.NewDynamicHandler(acceptor, staticProvider(bindingA, bindingB))

	response := post(adapter, "feishu-a", []byte(textEvent(testAppID, "evt-ambiguous", "ou_same", "p2p", "oc_dm", "hello")))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if len(acceptor.messages) != 0 {
		t.Fatalf("messages=%+v", acceptor.messages)
	}
}

func TestRejectsAppIDMismatch(t *testing.T) {
	adapter, _ := feishu.NewDynamicHandler(&acceptorStub{}, staticProvider(testBinding("tenant-a")))
	response := post(adapter, "feishu-a", []byte(textEvent("cli_other_app", "evt-1", "ou_a", "p2p", "oc", "hi")))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d", response.Code)
	}
}

func TestUnknownBindingReturns404(t *testing.T) {
	adapter, _ := feishu.NewDynamicHandler(&acceptorStub{}, staticProvider())
	for _, path := range []string{"/channels/feishu/missing", "/channels/feishu/", "/other"} {
		request := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}"))
		response := httptest.NewRecorder()
		adapter.ServeHTTP(response, request)
		if response.Code != http.StatusNotFound {
			t.Fatalf("path=%s status=%d", path, response.Code)
		}
	}
}

func TestDisabledBindingReceivesNoMessages(t *testing.T) {
	// A disabled tenant/app/binding resolves zero candidates, which is a 404
	// and never reaches the acceptor.
	adapter, _ := feishu.NewDynamicHandler(&acceptorStub{}, staticProvider())
	response := post(adapter, "feishu-a", []byte(textEvent(testAppID, "evt-1", "ou_a", "p2p", "oc", "hi")))
	if response.Code != http.StatusNotFound {
		t.Fatalf("status=%d", response.Code)
	}
}

func TestDuplicateEventIDClaimsInboxOnce(t *testing.T) {
	submitter := &captureSubmitter{}
	core := &openclaw.Handler{Inbox: idempotency.NewMemoryStore(), Submitter: submitter, ClaimOwner: "feishu-gateway", ClaimTTL: time.Minute}
	adapter, err := feishu.NewDynamicHandler(core, staticProvider(testBinding("tenant-a")))
	if err != nil {
		t.Fatal(err)
	}
	event := textEvent(testAppID, "evt-dup", "ou_alice", "p2p", "oc_dm", "hello")
	for i := 0; i < 2; i++ {
		response := post(adapter, "feishu-a", []byte(event))
		if response.Code != http.StatusOK {
			t.Fatalf("attempt %d status=%d body=%s", i, response.Code, response.Body.String())
		}
	}
	if len(submitter.requests) != 1 {
		t.Fatalf("submitted=%d, want 1", len(submitter.requests))
	}
	request := submitter.requests[0]
	if request.TenantID != "tenant-a" || request.AppID != "assistant" || request.BindingID != "feishu-a" || request.ConfigVersion != 3 {
		t.Fatalf("scope=%+v", request)
	}
	if request.ExternalMessageID != "evt-dup" || request.UserID != "feishu/feishu-a/ou_alice" || request.SessionID != "dm/feishu-a/ou_alice" {
		t.Fatalf("identity=%+v", request)
	}
}

type captureSubmitter struct {
	mu       sync.Mutex
	requests []gateway.RunRequest
}

func (submitter *captureSubmitter) Submit(request gateway.RunRequest) error {
	submitter.mu.Lock()
	submitter.requests = append(submitter.requests, request)
	submitter.mu.Unlock()
	return nil
}

func TestTenantsSharingAppIDAndBindingIDStayIsolated(t *testing.T) {
	// Two tenants use the same Feishu app_id and the same binding_id. The
	// Verification Token disambiguates them; a callback carrying tenant-b's
	// token must land in tenant-b only.
	bindingA := testBinding("tenant-a")
	bindingB := testBinding("tenant-b")
	bindingB.VerificationToken = "tenant-b-token"
	acceptor := &acceptorStub{}
	adapter, _ := feishu.NewDynamicHandler(acceptor, staticProvider(bindingA, bindingB))

	event := textEvent(testAppID, "evt-iso", "ou_same_user", "p2p", "oc_dm", "hello")
	response := post(adapter, "feishu-a", []byte(strings.Replace(event, testToken, "tenant-b-token", 1)))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if len(acceptor.messages) != 1 || acceptor.messages[0].TenantID != "tenant-b" {
		t.Fatalf("messages=%+v", acceptor.messages)
	}
	// And tenant-a's token scopes the same external user to tenant-a.
	response = post(adapter, "feishu-a", []byte(event))
	if response.Code != http.StatusOK || len(acceptor.messages) != 2 || acceptor.messages[1].TenantID != "tenant-a" {
		t.Fatalf("status=%d messages=%+v", response.Code, acceptor.messages)
	}
}

func TestInboxFailureReturnsRetryable503(t *testing.T) {
	acceptor := &acceptorStub{err: context.DeadlineExceeded}
	adapter, _ := feishu.NewDynamicHandler(acceptor, staticProvider(testBinding("tenant-a")))
	response := post(adapter, "feishu-a", []byte(textEvent(testAppID, "evt-503", "ou_a", "p2p", "oc", "hi")))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", response.Code)
	}
}

func TestUnsupportedEventsAreSafelyAcked(t *testing.T) {
	adapter, _ := feishu.NewDynamicHandler(&acceptorStub{}, staticProvider(testBinding("tenant-a")))
	// Non-message event types must ACK to avoid infinite Feishu retries.
	other := strings.Replace(textEvent(testAppID, "evt-ev", "ou_a", "p2p", "oc", "hi"), "im.message.receive_v1", "im.chat.member.user.added_v1", 1)
	if response := post(adapter, "feishu-a", []byte(other)); response.Code != http.StatusOK {
		t.Fatalf("event ack status=%d", response.Code)
	}
	// Unsupported message types (e.g. sticker) also ACK.
	sticker := mediaEvent(testAppID, "evt-sticker", "ou_a", "sticker", `{"file_key":"fk"}`)
	if response := post(adapter, "feishu-a", []byte(sticker)); response.Code != http.StatusOK {
		t.Fatalf("sticker ack status=%d", response.Code)
	}
}

func TestMalformedJSONReturns400(t *testing.T) {
	adapter, _ := feishu.NewDynamicHandler(&acceptorStub{}, staticProvider(testBinding("tenant-a")))
	if response := post(adapter, "feishu-a", []byte("{broken")); response.Code != http.StatusBadRequest {
		t.Fatalf("status=%d", response.Code)
	}
}

func TestImageAndFileBecomeSafeMetadata(t *testing.T) {
	acceptor := &acceptorStub{}
	adapter, _ := feishu.NewDynamicHandler(acceptor, staticProvider(testBinding("tenant-a")))
	image := mediaEvent(testAppID, "evt-img", "ou_a", "image", `{"image_key":"img-secret-key"}`)
	if response := post(adapter, "feishu-a", []byte(image)); response.Code != http.StatusOK {
		t.Fatalf("image status=%d", response.Code)
	}
	file := mediaEvent(testAppID, "evt-file", "ou_a", "file", `{"file_key":"fk-secret","file_name":"report.pdf"}`)
	if response := post(adapter, "feishu-a", []byte(file)); response.Code != http.StatusOK {
		t.Fatalf("file status=%d", response.Code)
	}
	if len(acceptor.messages) != 2 {
		t.Fatalf("messages=%d", len(acceptor.messages))
	}
	for _, message := range acceptor.messages {
		if strings.Contains(message.Text, "img-secret-key") || strings.Contains(message.Text, "fk-secret") {
			t.Fatalf("provider key leaked into agent input: %q", message.Text)
		}
	}
	if !strings.Contains(acceptor.messages[1].Text, "report.pdf") {
		t.Fatalf("file name missing: %q", acceptor.messages[1].Text)
	}
}

type controlledDownloader func(context.Context, gateway.MediaReference) (channels.MediaDownload, error)

func (download controlledDownloader) Download(ctx context.Context, ref gateway.MediaReference) (channels.MediaDownload, error) {
	return download(ctx, ref)
}

func TestControlledFileDownloadExtractsTextAndDropsProviderKey(t *testing.T) {
	submitter := &captureSubmitter{}
	core := &openclaw.Handler{Inbox: idempotency.NewMemoryStore(), Submitter: submitter, ClaimOwner: "gateway", ClaimTTL: time.Minute}
	adapter, err := feishu.NewDynamicHandlerWithMedia(core, staticProvider(testBinding("tenant-a")), func(feishu.Binding) channels.MediaDownloader {
		return controlledDownloader(func(_ context.Context, ref gateway.MediaReference) (channels.MediaDownload, error) {
			if ref.Key != "private-file-key" || ref.MessageID != "om_2" {
				t.Fatalf("reference=%+v", ref)
			}
			return channels.MediaDownload{Body: io.NopCloser(strings.NewReader("document text")), ContentType: "text/plain", Name: "notes.txt", Size: 13}, nil
		})
	}, channels.MediaPolicy{TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	event := mediaEvent(testAppID, "evt-file-controlled", "ou_a", "file", `{"file_key":"private-file-key","file_name":"notes.txt"}`)
	response := post(adapter, "feishu-a", []byte(event))
	if response.Code != http.StatusOK || len(submitter.requests) != 1 {
		t.Fatalf("status=%d requests=%d body=%s", response.Code, len(submitter.requests), response.Body.String())
	}
	request := submitter.requests[0]
	serialized, _ := json.Marshal(request)
	if len(request.Attachments) != 1 || request.Attachments[0].ExtractedText != "document text" || strings.Contains(string(serialized), "private-file-key") {
		t.Fatalf("unsafe durable request: %s", serialized)
	}
}

func TestGroupChatSessionIsStableAndFeishuWeComIdentitiesDiffer(t *testing.T) {
	acceptor := &acceptorStub{}
	adapter, _ := feishu.NewDynamicHandler(acceptor, staticProvider(testBinding("tenant-a")))
	for i, eventID := range []string{"evt-g1", "evt-g2"} {
		event := textEvent(testAppID, eventID, fmt.Sprintf("ou_user_%d", i), "group", "oc_group_1", "hi group")
		if response := post(adapter, "feishu-a", []byte(event)); response.Code != http.StatusOK {
			t.Fatalf("status=%d", response.Code)
		}
	}
	if len(acceptor.messages) != 2 {
		t.Fatalf("messages=%d", len(acceptor.messages))
	}
	first, second := acceptor.messages[0], acceptor.messages[1]
	// The group session is bound to binding + chat, not to the sender.
	if first.SessionID != "group/feishu-a/oc_group_1" || second.SessionID != first.SessionID {
		t.Fatalf("group sessions=%q / %q", first.SessionID, second.SessionID)
	}
	if first.ConversationID != "oc_group_1" {
		t.Fatalf("conversation=%q", first.ConversationID)
	}
	// The same external ID on WeCom and Feishu can never collide.
	wecomUser := "wecom/feishu-a/ou_user_0"
	if first.UserID == wecomUser || !strings.HasPrefix(first.UserID, "feishu/") {
		t.Fatalf("user=%q", first.UserID)
	}
}

func TestCallbackAckIsFast(t *testing.T) {
	acceptor := &acceptorStub{}
	adapter, _ := feishu.NewDynamicHandler(acceptor, staticProvider(testBinding("tenant-a")))
	started := time.Now()
	response := post(adapter, "feishu-a", []byte(textEvent(testAppID, "evt-fast", "ou_a", "p2p", "oc", "hi")))
	if response.Code != http.StatusOK || time.Since(started) > time.Second {
		t.Fatalf("status=%d elapsed=%s", response.Code, time.Since(started))
	}
}

func TestSecretsNeverAppearInResponses(t *testing.T) {
	const canaryToken = "vt-canary-3f9a1c"
	const canaryEncrypt = "ek-canary-8b2d4e"
	binding := testBinding("tenant-a")
	binding.VerificationToken = canaryToken
	binding.EncryptKey = canaryEncrypt
	adapter, _ := feishu.NewDynamicHandler(&acceptorStub{}, staticProvider(binding))
	responses := []*httptest.ResponseRecorder{
		// Wrong verification token.
		post(adapter, "feishu-a", []byte(`{"challenge":"c","token":"wrong","type":"url_verification"}`)),
		// Undecryptable payload.
		post(adapter, "feishu-a", []byte(`{"encrypt":"not-base64!!"}`)),
		// Plaintext while encryption is configured.
		post(adapter, "feishu-a", []byte(textEvent(testAppID, "evt-c", "ou_a", "p2p", "oc", "hi"))),
		// Malformed JSON.
		post(adapter, "feishu-a", []byte("{broken")),
	}
	for i, response := range responses {
		body := response.Body.String()
		if strings.Contains(body, canaryToken) || strings.Contains(body, canaryEncrypt) {
			t.Fatalf("response %d leaks canary: %s", i, body)
		}
	}
}
