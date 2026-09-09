package acceptance_test

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha1" // #nosec G505 -- WeCom callback protocol requires SHA-1.
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway/openclaw"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
	"github.com/liuzengh/trpc-agent-service/trpcservice/policy"
	serviceruntime "github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessioncoord"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

const (
	contractMessageID = "contract-message"
	contractUserID    = "contract-user"
	contractText      = "synthetic contract input"
	wecomBindingID    = "contract-wecom"
	feishuBindingID   = "contract-feishu"
	wecomTenantID     = "contract-tenant-wecom"
	feishuTenantID    = "contract-tenant-feishu"
	contractAppID     = "assistant"
	wecomCorpID       = "ww-contract-corp"
	wecomAgentID      = "1000002"
	wecomToken        = "synthetic-wecom-token"
	feishuAppID       = "cli_contract_app"
	feishuToken       = "synthetic-feishu-token"
	feishuEncryptKey  = "synthetic-feishu-encrypt-key"
)

var contractAESKey = []byte("0123456789abcdef0123456789abcdef")

type processSubmitter struct {
	processor *worker.Processor
	mu        sync.Mutex
	requests  []gateway.RunRequest
}

func (submitter *processSubmitter) Submit(request gateway.RunRequest) error {
	submitter.mu.Lock()
	submitter.requests = append(submitter.requests, request)
	submitter.mu.Unlock()
	return submitter.processor.Process(context.Background(), request)
}

func (submitter *processSubmitter) count() int {
	submitter.mu.Lock()
	defer submitter.mu.Unlock()
	return len(submitter.requests)
}

type staticToken string

func (token staticToken) Token(context.Context) (string, error) { return string(token), nil }
func (staticToken) Invalidate(string)                           {}

type providerCapture struct {
	mu     sync.Mutex
	wecom  int
	feishu int
}

func (capture *providerCapture) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.Body != nil {
		defer request.Body.Close()
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	status := http.StatusOK
	body := `{"code":0,"data":{"message_id":"synthetic-reply"}}`
	switch {
	case request.URL.Path == "/cgi-bin/message/send" && request.URL.Query().Get("access_token") == "synthetic-provider-token":
		capture.wecom++
		body = `{"errcode":0}`
	case request.URL.Path == "/open-apis/im/v1/messages" && request.Header.Get("Authorization") == "Bearer synthetic-provider-token":
		capture.feishu++
	default:
		status = http.StatusBadRequest
		body = `{"code":400}`
	}
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func (capture *providerCapture) counts() (int, int) {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	return capture.wecom, capture.feishu
}

// TestDualIMContractE2E is a credential-free, deterministic replacement for
// the non-reproducible part of real-platform acceptance. It deliberately uses
// the same provider message/user IDs in two tenants and proves the complete
// callback -> Inbox -> Worker -> tRPC-Agent -> session projections -> Outbox
// -> provider API contract without logging callback or reply bodies.
func TestDualIMContractE2E(t *testing.T) {
	file := contractConfig()
	if err := file.Validate(); err != nil {
		t.Fatal(err)
	}
	inbox := idempotency.NewMemoryStore()
	writes := sessioncoord.NewMemoryWriteStore()
	coordinator, err := sessioncoord.NewCoordinator(writes)
	if err != nil {
		t.Fatal(err)
	}
	runtimes, err := serviceruntime.NewManager(worker.TestRuntimeFactory(writes))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtimes.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	processor := &worker.Processor{
		WorkerID: "contract-worker", Inbox: inbox, Coordinator: coordinator,
		Writes: writes, Runtimes: runtimes, Snapshots: gateway.FileSnapshotResolver{File: file},
		Policy: &policy.Engine{Identity: policy.AuthenticatedIdentityAuthorizer{}}, LeaseTTL: time.Second,
	}
	submitter := &processSubmitter{processor: processor}
	core := &openclaw.Handler{Inbox: inbox, Submitter: submitter, ClaimOwner: "contract-gateway", ClaimTTL: time.Second}

	postEncryptedWeCom(t, core)
	postEncryptedFeishu(t, core)
	if got := submitter.count(); got != 2 {
		t.Fatalf("runner submissions=%d, want one per isolated channel", got)
	}

	wecomOutbox := assertDurableResult(t, writes, wecomTenantID, wecomBindingID, tenant.ChannelTypeWeCom)
	feishuOutbox := assertDurableResult(t, writes, feishuTenantID, feishuBindingID, tenant.ChannelTypeFeishu)
	if wecomOutbox.TraceID == "" || feishuOutbox.TraceID == "" || wecomOutbox.TraceID == feishuOutbox.TraceID {
		t.Fatal("trace correlation is missing or crossed channel boundaries")
	}

	capture := &providerCapture{}
	client := &http.Client{Transport: capture}
	providerToken := staticToken("synthetic-provider-token")
	wecomSender := &wecom.Sender{AgentID: 1000002, Tokens: providerToken, BaseURL: "https://provider.invalid", Client: client}
	feishuSender := &feishu.Sender{Tokens: providerToken, BaseURL: "https://provider.invalid", Client: client}
	if err := wecomSender.SendText(context.Background(), wecomOutbox); err != nil {
		t.Fatal(err)
	}
	if err := feishuSender.SendText(context.Background(), feishuOutbox); err != nil {
		t.Fatal(err)
	}
	if wecomCalls, feishuCalls := capture.counts(); wecomCalls != 1 || feishuCalls != 1 {
		t.Fatalf("provider calls wecom=%d feishu=%d", wecomCalls, feishuCalls)
	}
}

func contractConfig() *config.File {
	backend := tenant.BackendConfig{Type: tenant.BackendInMemory}
	storage := tenant.StorageProfile{Session: backend, Memory: backend, Summary: backend, Artifact: backend, Knowledge: backend, Audit: backend}
	secret := func(key string) tenant.SecretRef {
		return tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: key}
	}
	makeTenant := func(tenantID string, channel tenant.ChannelBinding) tenant.Tenant {
		return tenant.Tenant{
			ID: tenantID, Name: tenantID, Enabled: true, ConfigVersion: 1,
			Audit: tenant.AuditPolicy{Enabled: true, RetentionDays: 1},
			Apps: []tenant.AgentApp{{
				ID: contractAppID, Name: "Contract Agent", Enabled: true,
				Config: tenant.AppConfig{Instruction: "Return a deterministic contract reply."},
				Model:  tenant.ModelProfile{Provider: "mock", Name: "offline-mock", MaxTokens: 64},
				Tools:  tenant.ToolPolicy{Allow: []string{"echo"}}, Channels: []tenant.ChannelBinding{channel}, Storage: storage,
			}},
		}
	}
	return &config.File{SchemaVersion: config.CurrentSchemaVersion, Tenants: []tenant.Tenant{
		makeTenant(wecomTenantID, tenant.ChannelBinding{ID: wecomBindingID, Type: tenant.ChannelTypeWeCom, ProviderAccountID: wecomCorpID, ProviderAppID: wecomAgentID, Token: secret("SYNTHETIC_WECOM_TOKEN"), Secret: secret("SYNTHETIC_WECOM_SECRET"), EncryptionKey: secret("SYNTHETIC_WECOM_AES"), Enabled: true}),
		makeTenant(feishuTenantID, tenant.ChannelBinding{ID: feishuBindingID, Type: tenant.ChannelTypeFeishu, ProviderAccountID: feishuAppID, Token: secret("SYNTHETIC_FEISHU_TOKEN"), Secret: secret("SYNTHETIC_FEISHU_SECRET"), EncryptionKey: secret("SYNTHETIC_FEISHU_ENCRYPT"), Enabled: true}),
	}}
}

func postEncryptedWeCom(t *testing.T, core gateway.InboundAcceptor) {
	t.Helper()
	aesKey := strings.TrimSuffix(base64.StdEncoding.EncodeToString(contractAESKey), "=")
	crypt, err := wecom.NewCrypt(wecomToken, aesKey, wecomCorpID)
	if err != nil {
		t.Fatal(err)
	}
	binding := wecom.Binding{TenantID: wecomTenantID, AppID: contractAppID, BindingID: wecomBindingID, CorpID: wecomCorpID, AgentID: wecomAgentID, ConfigVersion: 1, Crypt: crypt}
	adapter, err := wecom.NewHandler(core, binding)
	if err != nil {
		t.Fatal(err)
	}
	plain := fmt.Sprintf(`<xml><ToUserName>%s</ToUserName><FromUserName>%s</FromUserName><CreateTime>1788080000</CreateTime><MsgType>text</MsgType><Content>%s</Content><MsgId>%s</MsgId><AgentID>%s</AgentID></xml>`, wecomCorpID, contractUserID, contractText, contractMessageID, wecomAgentID)
	encrypted := encryptWeCom(t, plain, wecomCorpID)
	body := `<xml><Encrypt><![CDATA[` + encrypted + `]]></Encrypt></xml>`
	values := url.Values{"timestamp": {"1788080000"}, "nonce": {"contract-nonce"}}
	values.Set("msg_signature", wecomSignature(wecomToken, values.Get("timestamp"), values.Get("nonce"), encrypted))
	endpoint := "/channels/wecom/" + wecomBindingID + "?" + values.Encode()
	for attempt := 0; attempt < 2; attempt++ {
		response := httptest.NewRecorder()
		adapter.ServeHTTP(response, httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(body)))
		if response.Code != http.StatusOK {
			t.Fatalf("wecom callback attempt %d status=%d", attempt, response.Code)
		}
	}
}

func postEncryptedFeishu(t *testing.T, core gateway.InboundAcceptor) {
	t.Helper()
	binding := feishu.Binding{TenantID: feishuTenantID, AppID: contractAppID, BindingID: feishuBindingID, FeishuAppID: feishuAppID, VerificationToken: feishuToken, EncryptKey: feishuEncryptKey, ConfigVersion: 1}
	adapter, err := feishu.NewDynamicHandler(core, func(string) []feishu.Binding { return []feishu.Binding{binding} })
	if err != nil {
		t.Fatal(err)
	}
	content, _ := json.Marshal(map[string]string{"text": contractText})
	plain := []byte(fmt.Sprintf(`{"schema":"2.0","header":{"event_id":%q,"event_type":"im.message.receive_v1","token":%q,"app_id":%q},"event":{"sender":{"sender_id":{"open_id":%q},"sender_type":"user"},"message":{"message_id":%q,"chat_id":"synthetic-dm","chat_type":"p2p","message_type":"text","content":%q}}}`, contractMessageID, feishuToken, feishuAppID, contractUserID, contractMessageID, string(content)))
	body := encryptFeishu(t, feishuEncryptKey, plain)
	const timestamp, nonce = "1788080000", "contract-nonce"
	digest := sha256.Sum256([]byte(timestamp + nonce + feishuEncryptKey + string(body)))
	for attempt := 0; attempt < 2; attempt++ {
		request := httptest.NewRequest(http.MethodPost, "/channels/feishu/"+feishuBindingID, bytes.NewReader(body))
		request.Header.Set("X-Lark-Request-Timestamp", timestamp)
		request.Header.Set("X-Lark-Request-Nonce", nonce)
		request.Header.Set("X-Lark-Signature", hex.EncodeToString(digest[:]))
		response := httptest.NewRecorder()
		adapter.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("feishu callback attempt %d status=%d", attempt, response.Code)
		}
	}
}

func assertDurableResult(t *testing.T, writes *sessioncoord.MemoryWriteStore, tenantID, bindingID string, channel tenant.ChannelType) gateway.OutboundMessage {
	t.Helper()
	userID, err := tenant.CanonicalUserID(channel, bindingID, contractUserID)
	if err != nil {
		t.Fatal(err)
	}
	sessionID, err := tenant.DirectSessionID(bindingID, contractUserID)
	if err != nil {
		t.Fatal(err)
	}
	key := gateway.SessionKey{TenantID: tenantID, AppID: contractAppID, UserID: userID, SessionID: sessionID}
	head, events, summary, memories := writes.Snapshot(key)
	if head.LastEventSeq != 1 || len(events) != 1 || summary == nil || len(memories) != 1 {
		t.Fatalf("durable projections tenant=%s head=%d events=%d summary=%t memories=%d", tenantID, head.LastEventSeq, len(events), summary != nil, len(memories))
	}
	dedupeKey := "reply:" + tenantID + "/" + bindingID + "/" + contractMessageID
	outbound, ok := writes.Outbox(tenantID, dedupeKey)
	if !ok || outbound.TenantID != tenantID || outbound.BindingID != bindingID || outbound.UserID != userID || outbound.SessionID != sessionID {
		t.Fatalf("isolated outbox not found for tenant=%s binding=%s", tenantID, bindingID)
	}
	return outbound
}

func wecomSignature(token, timestamp, nonce, encrypted string) string {
	values := []string{token, timestamp, nonce, encrypted}
	sort.Strings(values)
	sum := sha1.Sum([]byte(strings.Join(values, ""))) // #nosec G401 -- protocol requirement.
	return hex.EncodeToString(sum[:])
}

func encryptWeCom(t *testing.T, message, receiveID string) string {
	t.Helper()
	plain := make([]byte, 20+len(message)+len(receiveID))
	copy(plain[:16], []byte("0123456789abcdef"))
	binary.BigEndian.PutUint32(plain[16:20], uint32(len(message)))
	copy(plain[20:], message)
	copy(plain[20+len(message):], receiveID)
	padding := 32 - len(plain)%32
	plain = append(plain, bytes.Repeat([]byte{byte(padding)}, padding)...)
	block, err := aes.NewCipher(contractAESKey)
	if err != nil {
		t.Fatal(err)
	}
	encrypted := make([]byte, len(plain))
	cipher.NewCBCEncrypter(block, contractAESKey[:aes.BlockSize]).CryptBlocks(encrypted, plain)
	return base64.StdEncoding.EncodeToString(encrypted)
}

func encryptFeishu(t *testing.T, key string, plain []byte) []byte {
	t.Helper()
	sum := sha256.Sum256([]byte(key))
	block, err := aes.NewCipher(sum[:])
	if err != nil {
		t.Fatal(err)
	}
	padding := aes.BlockSize - len(plain)%aes.BlockSize
	padded := append([]byte(nil), plain...)
	padded = append(padded, bytes.Repeat([]byte{byte(padding)}, padding)...)
	iv := []byte("0123456789abcdef")
	encrypted := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(encrypted, padded)
	body, err := json.Marshal(map[string]string{"encrypt": base64.StdEncoding.EncodeToString(append(iv, encrypted...))})
	if err != nil {
		t.Fatal(err)
	}
	return body
}
