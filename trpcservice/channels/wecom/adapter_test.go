package wecom_test

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/contracttest"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress"
	ingressmemory "github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom/protocol"
	preprocessmemory "github.com/liuzengh/trpc-agent-service/trpcservice/preprocess/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	secretmemory "github.com/liuzengh/trpc-agent-service/trpcservice/secrets/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	messagingmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/inmemory"
)

func TestPublicRouteUsesOpaqueRouteKeyAndSignedAttempt(t *testing.T) {
	adapter := &wecom.Adapter{}
	hint, err := adapter.PublicRoute(context.Background(), channel.CallbackRequest{Body: []byte("body"), Query: map[string]string{
		"route_key": "opaque-route", "timestamp": "1", "nonce": "nonce", "msg_signature": "signature",
	}})
	if err != nil || hint.Channel != "wecom" || hint.RouteKeyDigest != protocol.RouteKeyDigest("opaque-route") || hint.IngressAttemptID == "" {
		t.Fatalf("hint=%#v err=%v", hint, err)
	}
	if hint.RouteKeyDigest == "opaque-route" {
		t.Fatal("public route key was not digested")
	}
}

func TestEncryptedCallbackRunsThroughCommonDurablePipeline(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	material := testMaterial()
	verificationSecret, _ := json.Marshal(material)
	routeKey := "opaque-route"
	route := ingress.BindingRoute{OpaqueBindingID: "opaque-binding", Channel: "wecom", RouteKeyDigest: protocol.RouteKeyDigest(routeKey),
		TenantID: "tenant", AgentAppID: "app", ChannelBindingID: "binding", ExternalAccountID: material.ReceiveID,
		TenantVersion: 1, BindingVersion: 1, SecretRef: secrets.SecretRef{Ref: "secret://verify", Version: 1},
		IdentitySecretRef: secrets.SecretRef{Ref: "secret://identity", Version: 1}, SessionSecretRef: secrets.SecretRef{Ref: "secret://session", Version: 1}, Enabled: true}
	routes := ingressmemory.New()
	if err := routes.PutBindingRoute(context.Background(), route); err != nil {
		t.Fatal(err)
	}
	secretProvider := secretmemory.New()
	secretProvider.Put(secrets.Scope{TenantID: "tenant", Subject: "binding", Purpose: secrets.PurposeChannelVerify, ResourceID: "binding", ResourceVersion: 1}, route.SecretRef, verificationSecret)
	secretProvider.Put(secrets.Scope{TenantID: "tenant", Subject: "tenant", Purpose: secrets.PurposeTenantIdentity, ResourceID: "tenant", ResourceVersion: 1}, route.IdentitySecretRef, []byte("identity-key"))
	secretProvider.Put(secrets.Scope{TenantID: "tenant", Subject: "tenant", Purpose: secrets.PurposeTenantSession, ResourceID: "tenant", ResourceVersion: 1}, route.SessionSecretRef, []byte("session-key"))
	resolver := ingress.Resolver{Store: routes, Secrets: secretProvider, TTL: time.Minute, Now: func() time.Time { return now }}
	adapter := &wecom.Adapter{Protocol: protocol.Verifier{Now: func() time.Time { return now }}}
	payloads := messagingmemory.New()
	pipeline := ingress.Pipeline{Verification: ingress.Service{Adapter: adapter, Bindings: resolver}, Identity: identity.Mapper{Secrets: secretProvider},
		Intake: preprocessmemory.New(), Payloads: payloads, KeyVersion: 1}
	plaintext := wecomTextPayload(material, "message-1", "hello")
	encrypted := encryptMessage(t, plaintext, material.EncodingAESKey, material.ReceiveID)
	timestamp, nonce := fmt.Sprint(now.Unix()), "nonce"
	accepted, err := pipeline.Accept(context.Background(), channel.CallbackRequest{Body: encryptedEnvelope(material, encrypted), ReceivedAt: now, Query: map[string]string{
		"route_key": routeKey, "timestamp": timestamp, "nonce": nonce,
		"msg_signature": protocol.Signature(material.Token, timestamp, nonce, encrypted),
	}})
	if err != nil || len(accepted) != 1 {
		t.Fatalf("accepted=%#v err=%v", accepted, err)
	}
	stored, err := payloads.GetPayload(context.Background(), "tenant", accepted[0].RequestID)
	if err != nil || string(stored.Content) != `{"external_message_id":"message-1","external_user_id":"zhangsan","external_chat_id":"","channel_binding_id":"binding","external_account_id":"ww_corp","config_version":1,"text":"hello"}` {
		t.Fatalf("payload=%s err=%v", stored.Content, err)
	}
}

func TestDeliverUsesFrozenUserTargetAndSenderClassification(t *testing.T) {
	sender := &recordingSender{messageID: "wecom-reply"}
	adapter := &wecom.Adapter{Sender: sender}
	content := []byte("reply")
	sum := sha256.Sum256(content)
	request := channel.DeliveryRequest{Event: channel.ReplyEvent{TenantID: "tenant", ChannelBindingID: "binding", ConfigVersion: 1}, ClientRequestID: "stable-key",
		Target:  channel.DeliveryTarget{Channel: "wecom", ExternalAccountID: "ww_corp", ExternalUserID: "zhangsan"},
		Content: content, ContentDigest: hex.EncodeToString(sum[:])}
	result, err := adapter.Deliver(context.Background(), request)
	if err != nil || !result.Delivered || result.ProviderMessageID != "wecom-reply" || sender.externalUserID != "zhangsan" || sender.clientRequestID != "stable-key" {
		t.Fatalf("result=%#v sender=%#v err=%v", result, sender, err)
	}
	sender.err = channel.PermanentDeliveryError{Err: errors.New("invalid user"), Class: "provider_rejected"}
	if _, err := adapter.Deliver(context.Background(), request); err == nil {
		t.Fatal("classified sender error was swallowed")
	}
	request.ContentType = "application/vnd.trpc.card+json"
	if _, err := adapter.Deliver(context.Background(), request); err == nil {
		t.Fatal("unsupported structured content was sent as text")
	} else {
		var permanent channel.PermanentDeliveryError
		if !errors.As(err, &permanent) || permanent.Class != "content_type_unsupported" {
			t.Fatalf("error=%v", err)
		}
	}
}

func TestAdapterDeliversImageOnlyThroughImageSender(t *testing.T) {
	sender := &imageRecordingSender{recordingSender: recordingSender{messageID: "wecom-image"}}
	adapter := &wecom.Adapter{Sender: sender}
	content := []byte("image-bytes")
	sum := sha256.Sum256(content)
	request := channel.DeliveryRequest{Event: channel.ReplyEvent{TenantID: "tenant", ChannelBindingID: "binding", ConfigVersion: 1}, ClientRequestID: "stable-image",
		Target: channel.DeliveryTarget{Channel: "wecom", ExternalAccountID: "ww_corp", ExternalUserID: "zhangsan"}, Content: content, ContentDigest: hex.EncodeToString(sum[:]), ContentType: "image/png"}
	if _, err := adapter.Deliver(context.Background(), request); err != nil || sender.contentType != "image/png" || sender.clientRequestID != "stable-image" || !bytes.Equal(sender.content, content) {
		t.Fatalf("sender=%#v err=%v", sender, err)
	}
	if !adapter.Capabilities().Image {
		t.Fatalf("capabilities=%#v", adapter.Capabilities())
	}
}

func TestAdapterDeliveryContract(t *testing.T) {
	contracttest.RunDelivery(t, func(testing.TB) contracttest.DeliveryHarness {
		sender := &recordingSender{messageID: "wecom-reply"}
		content := []byte("contract reply")
		sum := sha256.Sum256(content)
		result := messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request",
			ContentDigest: hex.EncodeToString(sum[:]), Content: content, KeyVersion: 1}
		target := channel.DeliveryTarget{Channel: "wecom", ExternalAccountID: "ww_corp", ExternalUserID: "zhangsan"}
		event := channel.ReplyEvent{SchemaVersion: 1, TenantID: "tenant", RequestID: "request", ChannelBindingID: "binding",
			DeliveryKey: "reply-wecom", ConfigVersion: 1, ContentRef: result.ResultRef, Target: target, Final: true}
		return contracttest.DeliveryHarness{Adapter: &wecom.Adapter{Sender: sender}, Event: event, Result: result, Observe: func() contracttest.DeliveryObservation {
			return contracttest.DeliveryObservation{Calls: sender.calls, ClientRequestID: sender.clientRequestID, Content: []byte(sender.text),
				Target: channel.DeliveryTarget{Channel: "wecom", ExternalAccountID: sender.destination.ExternalAccountID, ExternalUserID: sender.externalUserID}}
		}}
	})
}

func TestOfficialSenderClassifiesResponsesAndRefreshesInvalidTokenOnce(t *testing.T) {
	for _, test := range []struct {
		name      string
		status    int
		responses []string
		checkErr  func(error) bool
		refreshes int
	}{
		{name: "success", status: 200, responses: []string{`{"errcode":0,"errmsg":"ok","msgid":"message-reply"}`}},
		{name: "invalid token refresh", status: 200, responses: []string{`{"errcode":40014,"errmsg":"invalid token"}`, `{"errcode":0,"errmsg":"ok","msgid":"message-reply"}`}, refreshes: 1},
		{name: "rate limit", status: 429, responses: []string{`{"errcode":45009,"errmsg":"rate limited"}`}, checkErr: func(err error) bool {
			var target channel.RetryableDeliveryError
			return errors.As(err, &target) && target.RetryAfter == 2*time.Second
		}},
		{name: "server", status: 503, responses: []string{`{"errcode":-1,"errmsg":"busy"}`}, checkErr: func(err error) bool {
			var target channel.RetryableDeliveryError
			return errors.As(err, &target)
		}},
		{name: "invalid user", status: 200, responses: []string{`{"errcode":60111,"errmsg":"user not found"}`}, checkErr: func(err error) bool {
			var target channel.PermanentDeliveryError
			return errors.As(err, &target)
		}},
		{name: "partial recipient rejection", status: 200, responses: []string{`{"errcode":0,"errmsg":"ok","msgid":"message-reply","invaliduser":"zhangsan"}`}, checkErr: func(err error) bool {
			var target channel.PermanentDeliveryError
			return errors.As(err, &target) && target.Class == "invalid_recipient"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			var sentBody []byte
			client := httpClientFunc(func(request *http.Request) (*http.Response, error) {
				sentBody, _ = io.ReadAll(request.Body)
				index := calls
				if index >= len(test.responses) {
					index = len(test.responses) - 1
				}
				calls++
				headers := make(http.Header)
				headers.Set("Retry-After", "2")
				return &http.Response{StatusCode: test.status, Header: headers, Body: io.NopCloser(strings.NewReader(test.responses[index])), Request: request}, nil
			})
			tokens := &recordingTokens{}
			sender := wecom.OfficialSender{Tokens: tokens, Client: client, BaseURL: "https://unit.test"}
			messageID, err := sender.SendText(context.Background(), channel.ReplyDestination{TenantID: "tenant", ChannelBindingID: "binding", ExternalAccountID: "ww_corp", ConfigVersion: 1}, "zhangsan", "hello", "stable-key")
			if test.checkErr != nil {
				if !test.checkErr(err) {
					t.Fatalf("message=%q err=%T %v", messageID, err, err)
				}
				return
			}
			var sent struct {
				ToUser                 string `json:"touser"`
				MessageType            string `json:"msgtype"`
				AgentID                int64  `json:"agentid"`
				EnableDuplicateCheck   int    `json:"enable_duplicate_check"`
				DuplicateCheckInterval int    `json:"duplicate_check_interval"`
			}
			_ = json.Unmarshal(sentBody, &sent)
			if err != nil || messageID != "message-reply" || tokens.refreshes != test.refreshes || sent.ToUser != "zhangsan" ||
				sent.MessageType != "text" || sent.AgentID != 218 || sent.EnableDuplicateCheck != 1 || sent.DuplicateCheckInterval != 1800 {
				t.Fatalf("message=%q sent=%#v refreshes=%d err=%v", messageID, sent, tokens.refreshes, err)
			}
		})
	}
}

func TestOfficialSenderTreatsTransportFailureAsAmbiguous(t *testing.T) {
	sender := wecom.OfficialSender{Tokens: &recordingTokens{}, Client: httpClientFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("connection reset")
	})}
	_, err := sender.SendText(context.Background(), channel.ReplyDestination{TenantID: "tenant", ChannelBindingID: "binding", ExternalAccountID: "ww_corp", ConfigVersion: 1}, "zhangsan", "hello", "stable-key")
	var ambiguous channel.AmbiguousDeliveryError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("error=%T %v", err, err)
	}
}

func TestOfficialSenderUploadsImageBeforeSending(t *testing.T) {
	var sent map[string]any
	client := httpClientFunc(func(request *http.Request) (*http.Response, error) {
		response := `{"errcode":0,"errmsg":"ok"}`
		switch request.URL.Path {
		case "/cgi-bin/media/upload":
			if err := request.ParseMultipartForm(1 << 20); err != nil {
				return nil, err
			}
			response = `{"errcode":0,"errmsg":"ok","media_id":"media-image"}`
		case "/cgi-bin/message/send":
			_ = json.NewDecoder(request.Body).Decode(&sent)
			response = `{"errcode":0,"errmsg":"ok","msgid":"image-reply"}`
		}
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(response)), Request: request}, nil
	})
	sender := wecom.OfficialSender{Tokens: &recordingTokens{}, Client: client, BaseURL: "https://unit.test"}
	messageID, err := sender.SendImage(context.Background(), channel.ReplyDestination{TenantID: "tenant", ChannelBindingID: "binding", ExternalAccountID: "ww_corp", ConfigVersion: 1}, "zhangsan", []byte("image"), "image/png", "stable-image")
	if err != nil || messageID != "image-reply" || sent["msgtype"] != "image" {
		t.Fatalf("message=%q sent=%#v err=%v", messageID, sent, err)
	}
	image, ok := sent["image"].(map[string]any)
	if !ok || image["media_id"] != "media-image" {
		t.Fatalf("image payload=%#v", sent)
	}
}

type recordingSender struct {
	destination                                channel.ReplyDestination
	text                                       string
	externalUserID, clientRequestID, messageID string
	calls                                      int
	err                                        error
}

type imageRecordingSender struct {
	recordingSender
	content     []byte
	contentType string
}

func (s *imageRecordingSender) SendImage(_ context.Context, destination channel.ReplyDestination, externalUserID string, image []byte, contentType, clientRequestID string) (string, error) {
	s.destination, s.externalUserID, s.clientRequestID, s.contentType, s.content = destination, externalUserID, clientRequestID, contentType, append([]byte(nil), image...)
	return s.messageID, s.err
}

func (s *recordingSender) SendText(_ context.Context, destination channel.ReplyDestination, externalUserID, text, clientRequestID string) (string, error) {
	s.destination, s.externalUserID, s.text, s.clientRequestID = destination, externalUserID, text, clientRequestID
	s.calls++
	return s.messageID, s.err
}

type recordingTokens struct{ refreshes int }

func (t *recordingTokens) ResolveWeComAccessToken(_ context.Context, _ channel.ReplyDestination, forceRefresh bool) (string, int64, error) {
	if forceRefresh {
		t.refreshes++
		return "refreshed-token", 218, nil
	}
	return "cached-token", 218, nil
}

type httpClientFunc func(*http.Request) (*http.Response, error)

func (f httpClientFunc) Do(request *http.Request) (*http.Response, error) { return f(request) }

func testMaterial() protocol.VerificationMaterial {
	return protocol.VerificationMaterial{Token: "callback-token", EncodingAESKey: base64.RawStdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef")), ReceiveID: "ww_corp", AgentID: 218}
}

func encryptedEnvelope(material protocol.VerificationMaterial, encrypted string) []byte {
	return []byte(fmt.Sprintf("<xml><ToUserName><![CDATA[%s]]></ToUserName><Encrypt><![CDATA[%s]]></Encrypt><AgentID>%d</AgentID></xml>", material.ReceiveID, encrypted, material.AgentID))
}

func wecomTextPayload(material protocol.VerificationMaterial, messageID, content string) []byte {
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
		MsgType: cdata{"text"}, Content: cdata{content}, MsgID: messageID, AgentID: material.AgentID}
	encoded, _ := xml.Marshal(payload)
	return encoded
}

func encryptMessage(t *testing.T, message []byte, encodingAESKey, receiveID string) string {
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
