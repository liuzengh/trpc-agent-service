package integration

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	feishuprotocol "github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu/protocol"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/httpcallback"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress"
	ingressmemory "github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom"
	wecomprotocol "github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom/protocol"
	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
	preprocessmemory "github.com/liuzengh/trpc-agent-service/trpcservice/preprocess/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	secretmemory "github.com/liuzengh/trpc-agent-service/trpcservice/secrets/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	messagingmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/inmemory"
)

const imDuplicateCallbacks = 1000

type imIMFixture struct {
	handler          http.Handler
	callback         func() *http.Request
	tamperedCallback func() *http.Request
	challenge        func() *http.Request
	challengeBody    string
	callbackACK      string
	tenantID         string
	requestID        string
	intake           *preprocessmemory.Store
	payloads         *messagingmemory.Store
}

func TestIMFeishuAndWeComDurableHTTPIngressContract(t *testing.T) {
	for _, test := range []struct {
		name    string
		fixture func(*testing.T) imIMFixture
	}{
		{name: "feishu", fixture: newIMFeishuFixture},
		{name: "wecom", fixture: newIMWeComFixture},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := test.fixture(t)
			assertIMChallengeDoesNotCreateInbox(t, fixture)
			assertIMTamperedCallbackFailsClosed(t, fixture)
			assertIMConcurrentDuplicatesConverge(t, fixture)
		})
	}
}

func assertIMChallengeDoesNotCreateInbox(t *testing.T, fixture imIMFixture) {
	t.Helper()
	response := httptest.NewRecorder()
	fixture.handler.ServeHTTP(response, fixture.challenge())
	if response.Code != http.StatusOK || response.Body.String() != fixture.challengeBody {
		t.Fatalf("challenge code=%d body=%q", response.Code, response.Body.String())
	}
	jobs, err := fixture.intake.ClaimJobs(context.Background(), preprocess.ClaimOptions{
		Owner: "challenge-check", Now: time.Now().UTC(), TTL: time.Minute, Limit: 2,
	})
	if err != nil || len(jobs) != 0 {
		t.Fatalf("challenge created preprocess jobs=%d err=%v", len(jobs), err)
	}
}

func assertIMTamperedCallbackFailsClosed(t *testing.T, fixture imIMFixture) {
	t.Helper()
	response := httptest.NewRecorder()
	fixture.handler.ServeHTTP(response, fixture.tamperedCallback())
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("tampered callback code=%d body=%q", response.Code, response.Body.String())
	}
	jobs, err := fixture.intake.ClaimJobs(context.Background(), preprocess.ClaimOptions{
		Owner: "tamper-check", Now: time.Now().UTC(), TTL: time.Minute, Limit: 2,
	})
	if err != nil || len(jobs) != 0 {
		t.Fatalf("tampered callback created preprocess jobs=%d err=%v", len(jobs), err)
	}
}

func assertIMConcurrentDuplicatesConverge(t *testing.T, fixture imIMFixture) {
	t.Helper()
	start := make(chan struct{})
	failures := make(chan string, imDuplicateCallbacks)
	var wait sync.WaitGroup
	for index := 0; index < imDuplicateCallbacks; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			response := httptest.NewRecorder()
			fixture.handler.ServeHTTP(response, fixture.callback())
			if response.Code != http.StatusOK || response.Body.String() != fixture.callbackACK {
				failures <- fmt.Sprintf("code=%d body=%q", response.Code, response.Body.String())
			}
		}()
	}
	close(start)
	wait.Wait()
	close(failures)
	if failure, ok := <-failures; ok {
		t.Fatal(failure)
	}
	jobs, err := fixture.intake.ClaimJobs(context.Background(), preprocess.ClaimOptions{
		Owner: "acceptance", Now: time.Now().UTC(), TTL: time.Minute, Limit: 2,
	})
	if err != nil || len(jobs) != 1 || jobs[0].RequestID != fixture.requestID {
		t.Fatalf("jobs=%#v err=%v", jobs, err)
	}
	stored, err := fixture.payloads.GetPayload(context.Background(), fixture.tenantID, fixture.requestID)
	if err != nil || stored.RequestID != fixture.requestID || len(stored.Content) == 0 {
		t.Fatalf("payload=%#v err=%v", stored, err)
	}
}

func newIMFeishuFixture(t *testing.T) imIMFixture {
	t.Helper()
	now := time.Unix(1_800_000_000, 0).UTC()
	const tenantID, routeKey = "tenant-feishu", "opaque-feishu-route"
	material := feishuprotocol.VerificationMaterial{EncryptKey: "encrypt-key", VerificationToken: "verify-token", AppID: "cli_app", BotOpenID: "ou_bot"}
	verificationSecret, _ := json.Marshal(material)
	adapter := &feishu.Adapter{Protocol: feishuprotocol.Verifier{Now: func() time.Time { return now }}}
	endpoint, intake, payloads := newIMEndpoint(t, adapter, tenantID, routeKey, material.AppID, feishuprotocol.RouteKeyDigest(routeKey), verificationSecret, now)
	plaintext := imFeishuPayload()
	body := imEncryptFeishu(t, plaintext, material.EncryptKey)
	timestamp, nonce := fmt.Sprint(now.Unix()), "nonce"
	signature := larkevent.Signature(timestamp, nonce, material.EncryptKey, string(body))
	newCallback := func(value string) *http.Request {
		request := httptest.NewRequest(http.MethodPost, "/callback?route_key="+url.QueryEscape(routeKey), bytes.NewReader(body))
		request.Header.Set("X-Lark-Request-Timestamp", timestamp)
		request.Header.Set("X-Lark-Request-Nonce", nonce)
		request.Header.Set("X-Lark-Signature", value)
		return request
	}
	challengeBody := []byte(`{"type":"url_verification","token":"verify-token","challenge":"feishu-challenge"}`)
	key := messaging.InboxKey{TenantID: tenantID, Channel: "feishu", ExternalAccountID: material.AppID, ExternalMessageID: "om_message"}
	requestID, _ := messaging.StableInboxIdentity(key)
	return imIMFixture{handler: endpoint, callback: func() *http.Request { return newCallback(signature) },
		tamperedCallback: func() *http.Request { return newCallback("forged") }, challenge: func() *http.Request {
			return httptest.NewRequest(http.MethodPost, "/callback?route_key="+url.QueryEscape(routeKey), bytes.NewReader(challengeBody))
		}, challengeBody: `{"challenge":"feishu-challenge"}`, callbackACK: `{"code":0}`, tenantID: tenantID,
		requestID: requestID, intake: intake, payloads: payloads}
}

func newIMWeComFixture(t *testing.T) imIMFixture {
	t.Helper()
	now := time.Unix(1_800_000_000, 0).UTC()
	const tenantID, routeKey = "tenant-wecom", "opaque-wecom-route"
	material := wecomprotocol.VerificationMaterial{Token: "callback-token", EncodingAESKey: base64.RawStdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef")), ReceiveID: "ww_corp", AgentID: 218}
	verificationSecret, _ := json.Marshal(material)
	adapter := &wecom.Adapter{Protocol: wecomprotocol.Verifier{Now: func() time.Time { return now }}}
	endpoint, intake, payloads := newIMEndpoint(t, adapter, tenantID, routeKey, material.ReceiveID, wecomprotocol.RouteKeyDigest(routeKey), verificationSecret, now)
	plaintext := imWeComPayload(material)
	encrypted := imEncryptWeCom(t, plaintext, material.EncodingAESKey, material.ReceiveID)
	timestamp, nonce := fmt.Sprint(now.Unix()), "nonce"
	newCallback := func(signature string) *http.Request {
		values := url.Values{"route_key": {routeKey}, "timestamp": {timestamp}, "nonce": {nonce}, "msg_signature": {signature}}
		return httptest.NewRequest(http.MethodPost, "/callback?"+values.Encode(), bytes.NewReader(imWeComEnvelope(material, encrypted)))
	}
	echo := imEncryptWeCom(t, []byte("wecom-challenge"), material.EncodingAESKey, material.ReceiveID)
	challengeValues := url.Values{"route_key": {routeKey}, "timestamp": {timestamp}, "nonce": {nonce}, "echostr": {echo},
		"msg_signature": {wecomprotocol.Signature(material.Token, timestamp, nonce, echo)}}
	key := messaging.InboxKey{TenantID: tenantID, Channel: "wecom", ExternalAccountID: material.ReceiveID, ExternalMessageID: "message-1"}
	requestID, _ := messaging.StableInboxIdentity(key)
	return imIMFixture{handler: endpoint, callback: func() *http.Request {
		return newCallback(wecomprotocol.Signature(material.Token, timestamp, nonce, encrypted))
	}, tamperedCallback: func() *http.Request { return newCallback("forged") }, challenge: func() *http.Request {
		return httptest.NewRequest(http.MethodGet, "/callback?"+challengeValues.Encode(), nil)
	}, challengeBody: "wecom-challenge", callbackACK: "success", tenantID: tenantID, requestID: requestID, intake: intake, payloads: payloads}
}

func newIMEndpoint(t *testing.T, adapter channel.HTTPAdapter, tenantID, routeKey, externalAccountID, routeDigest string, verificationSecret []byte, now time.Time) (*httpcallback.Endpoint, *preprocessmemory.Store, *messagingmemory.Store) {
	t.Helper()
	routes := ingressmemory.New()
	route := ingress.BindingRoute{OpaqueBindingID: "opaque-" + adapter.ID(), Channel: adapter.ID(), RouteKeyDigest: routeDigest,
		TenantID: tenantID, AgentAppID: "app", ChannelBindingID: "binding-" + adapter.ID(), ExternalAccountID: externalAccountID,
		TenantVersion: 1, BindingVersion: 1, SecretRef: secrets.SecretRef{Ref: "secret://verify/" + adapter.ID(), Version: 1},
		IdentitySecretRef: secrets.SecretRef{Ref: "secret://identity/" + adapter.ID(), Version: 1},
		SessionSecretRef:  secrets.SecretRef{Ref: "secret://session/" + adapter.ID(), Version: 1}, Enabled: true}
	if routeKey == "" {
		t.Fatal("empty route key")
	}
	if err := routes.PutBindingRoute(context.Background(), route); err != nil {
		t.Fatal(err)
	}
	secretProvider := secretmemory.New()
	secretProvider.Put(secrets.Scope{TenantID: tenantID, Subject: route.ChannelBindingID, Purpose: secrets.PurposeChannelVerify,
		ResourceID: route.ChannelBindingID, ResourceVersion: 1}, route.SecretRef, verificationSecret)
	secretProvider.Put(secrets.Scope{TenantID: tenantID, Subject: tenantID, Purpose: secrets.PurposeTenantIdentity,
		ResourceID: tenantID, ResourceVersion: 1}, route.IdentitySecretRef, []byte("identity-key-"+adapter.ID()))
	secretProvider.Put(secrets.Scope{TenantID: tenantID, Subject: tenantID, Purpose: secrets.PurposeTenantSession,
		ResourceID: tenantID, ResourceVersion: 1}, route.SessionSecretRef, []byte("session-key-"+adapter.ID()))
	resolver := ingress.Resolver{Store: routes, Secrets: secretProvider, TTL: time.Minute, Now: func() time.Time { return now }}
	intake := preprocessmemory.New()
	payloads := messagingmemory.New()
	pipeline := ingress.Pipeline{Verification: ingress.Service{Adapter: adapter, Bindings: resolver}, Identity: identity.Mapper{Secrets: secretProvider},
		Intake: intake, Payloads: payloads, KeyVersion: 1}
	endpoint, err := httpcallback.NewEndpoint(adapter, pipeline, ingress.ChallengeService{Adapter: adapter, Bindings: resolver})
	if err != nil {
		t.Fatal(err)
	}
	endpoint.Now = func() time.Time { return now }
	return endpoint, intake, payloads
}

func imFeishuPayload() []byte {
	payload := map[string]any{"schema": "2.0", "header": map[string]any{"event_id": "evt_1", "event_type": feishuprotocol.EventTypeMessageReceive,
		"app_id": "cli_app", "tenant_key": "tenant-key", "token": "verify-token", "create_time": "1800000000000"},
		"event": map[string]any{"sender": map[string]any{"sender_id": map[string]any{"open_id": "ou_user"}, "sender_type": "user", "tenant_key": "tenant-key"},
			"message": map[string]any{"message_id": "om_message", "chat_id": "oc_chat", "chat_type": "p2p", "message_type": "text", "content": `{"text":"hello"}`, "create_time": "1800000000000"}}}
	encoded, _ := json.Marshal(payload)
	return encoded
}

func imEncryptFeishu(t *testing.T, plaintext []byte, secret string) []byte {
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

func imWeComPayload(material wecomprotocol.VerificationMaterial) []byte {
	type cdata struct {
		Value string `xml:",cdata"`
	}
	payload := struct {
		XMLName      xml.Name `xml:"xml"`
		ToUserName   cdata    `xml:"ToUserName"`
		FromUserName cdata    `xml:"FromUserName"`
		CreateTime   int64    `xml:"CreateTime"`
		MsgType      cdata    `xml:"MsgType"`
		Content      cdata    `xml:"Content"`
		MsgID        string   `xml:"MsgId"`
		AgentID      int64    `xml:"AgentID"`
	}{ToUserName: cdata{material.ReceiveID}, FromUserName: cdata{"zhangsan"}, CreateTime: 1_800_000_000,
		MsgType: cdata{"text"}, Content: cdata{"hello"}, MsgID: "message-1", AgentID: material.AgentID}
	encoded, _ := xml.Marshal(payload)
	return encoded
}

func imWeComEnvelope(material wecomprotocol.VerificationMaterial, encrypted string) []byte {
	return []byte(fmt.Sprintf("<xml><ToUserName><![CDATA[%s]]></ToUserName><Encrypt><![CDATA[%s]]></Encrypt><AgentID>%d</AgentID></xml>", material.ReceiveID, encrypted, material.AgentID))
}

func imEncryptWeCom(t *testing.T, message []byte, encodingAESKey, receiveID string) string {
	t.Helper()
	key, err := base64.RawStdEncoding.DecodeString(encodingAESKey)
	if err != nil {
		t.Fatal(err)
	}
	plaintext := append([]byte("0123456789abcdef"), make([]byte, 4)...)
	binary.BigEndian.PutUint32(plaintext[16:20], uint32(len(message)))
	plaintext = append(plaintext, message...)
	plaintext = append(plaintext, receiveID...)
	padding := 32 - len(plaintext)%32
	plaintext = append(plaintext, bytes.Repeat([]byte{byte(padding)}, padding)...)
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	ciphertext := make([]byte, len(plaintext))
	cipher.NewCBCEncrypter(block, key[:aes.BlockSize]).CryptBlocks(ciphertext, plaintext)
	return base64.StdEncoding.EncodeToString(ciphertext)
}
