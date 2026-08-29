package lark

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

const (
	realLarkGateEnv       = "LARK_B1_REAL"
	realLarkAppIDEnv      = "LARK_B1_APP_ID"
	realLarkSecretEnv     = "LARK_B1_APP_SECRET"
	realLarkReceiverEnv   = "LARK_B1_RECEIVER_ID"
	realLarkSecretRef     = "env://LARK_B1_APP_SECRET"
	realLarkReceiverRef   = "env://LARK_B1_RECEIVER_ID"
	realLarkTenantID      = "tenant-lark-b1"
	realLarkBindingID     = "binding-lark-b1"
	realLarkTestText      = "P0-09G-B1 Lark sender test"
	realLarkProviderToken = ""
)

// TestRealLarkSender is deliberately opt-in. It sends at most one initial
// message and one controlled replay with the same durable identity.
func TestRealLarkSender(t *testing.T) {
	if os.Getenv(realLarkGateEnv) != "1" {
		t.Skip("LARK_B1_REAL is not 1; real Lark gate is disabled")
	}
	t.Log("real Lark gate: enabled")

	appID, appIDPresent := os.LookupEnv(realLarkAppIDEnv)
	secret, secretPresent := os.LookupEnv(realLarkSecretEnv)
	receiverID, receiverPresent := os.LookupEnv(realLarkReceiverEnv)
	if !appIDPresent || appID == "" {
		t.Skip("blocked; LARK_B1_APP_ID is not available through an approved local secret/config source")
	}
	if !secretPresent || secret == "" || !receiverPresent || receiverID == "" {
		t.Skip("real Lark config: incomplete")
	}
	binding := Binding{
		TenantID:       realLarkTenantID,
		BindingID:      realLarkBindingID,
		Channel:        Channel,
		AppID:          appID,
		SecretRef:      realLarkSecretRef,
		ReceiverIDType: ReceiverIDTypeChatID,
		Enabled:        true,
	}
	if err := binding.Validate(); err != nil || !validRealLarkSecret(secret) || !validRealLarkReceiver(receiverID) {
		t.Fatal("real Lark config: local format validation failed")
	}
	t.Log("real Lark config: present")

	transport := &realLarkRecordingTransport{base: http.DefaultTransport, appID: appID, receiverID: receiverID}
	client := &http.Client{Transport: transport, Timeout: DefaultRequestTimeout}
	resolver, err := NewHTTPAccessTokenResolver(TokenResolverConfig{
		Secrets: SecretResolverFunc(resolveRealLarkSecret),
		Client:  client,
	})
	if err != nil {
		t.Fatal("real Lark token resolver configuration failed")
	}

	tokenContext, cancel := context.WithTimeout(context.Background(), DefaultRequestTimeout)
	token, tokenErr := resolver.Resolve(tokenContext, binding)
	cancel()
	if tokenErr != nil || token == "" {
		t.Fatalf("real Lark tenant access token failed; token-present=%t", token != "")
	}
	t.Log("Lark tenant access token: PASS")

	sender, err := NewSender(SenderConfig{
		Bindings: []Binding{binding},
		Tokens:   resolver,
		Client:   client,
		BaseURL:  DefaultBaseURL,
		Timeout:  DefaultRequestTimeout,
	})
	if err != nil {
		t.Fatal("real Lark sender configuration failed")
	}
	message := realLarkOutboxMessage(t, receiverID)

	first := sender.Send(context.Background(), message)
	if first.Class != channels.OutcomeDelivered {
		t.Fatalf("real Lark message send failed; class=%s code=%q", first.Class, first.Code)
	}
	snapshot := transport.snapshot()
	if snapshot.messageCount != 1 || !snapshot.messageContractsValid || len(snapshot.messageUUIDs) != 1 || snapshot.messageUUIDs[0] == "" || !snapshot.tokenContractsValid {
		t.Fatal("real Lark message request contract validation failed")
	}
	if snapshot.messageStatuses[0] < 200 || snapshot.messageStatuses[0] >= 300 {
		t.Fatal("real Lark message response was not 2xx")
	}
	t.Log("Lark real message send: PASS")
	t.Log("Sender outcome: Delivered")
	t.Log("status_class=2xx outcome=delivered")

	second := sender.Send(context.Background(), message)
	snapshot = transport.snapshot()
	if snapshot.messageCount != 2 || len(snapshot.messageUUIDs) != 2 || snapshot.messageUUIDs[0] != snapshot.messageUUIDs[1] || !snapshot.messageContractsValid {
		t.Fatal("real Lark stable UUID replay contract validation failed")
	}
	logRealLarkReplay(t, second, snapshot.messageStatuses[1])
}

func resolveRealLarkSecret(ctx context.Context, ref string) (string, error) {
	if ctx == nil || ctx.Err() != nil || ref != realLarkSecretRef {
		return "", ErrCredentialResolution
	}
	secret, ok := os.LookupEnv(realLarkSecretEnv)
	if !ok || secret == "" {
		return "", ErrCredentialResolution
	}
	return secret, nil
}

func validRealLarkSecret(value string) bool {
	return value != "" && len(value) <= 4096 && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\r\n")
}

func validRealLarkReceiver(value string) bool {
	return channels.ValidateDestination(Channel, channels.DestinationTypeChat, value) == nil
}

func realLarkOutboxMessage(t *testing.T, receiverID string) storage.OutboxMessage {
	t.Helper()
	payload := channels.ReplyOutboxPayload{
		SchemaVersion:        channels.ReplyOutboxSchemaVersion,
		Kind:                 channels.ReplyOutboxKind,
		TenantID:             realLarkTenantID,
		SessionID:            "session-lark-b1",
		JobID:                "job-lark-b1",
		ExecutionID:          "execution-lark-b1",
		RequestID:            "request-lark-b1",
		MessageID:            "message-lark-b1",
		TraceID:              "trace-lark-b1",
		BindingID:            realLarkBindingID,
		Channel:              Channel,
		DestinationType:      channels.DestinationTypeChat,
		DestinationID:        receiverID,
		ReplyText:            realLarkTestText,
		SenderRoutingVersion: channels.SenderRoutingVersion,
	}
	encoded, err := channels.EncodeReplyOutboxPayload(payload)
	if err != nil {
		t.Fatal("real Lark test payload is invalid")
	}
	return storage.OutboxMessage{
		TenantID:    payload.TenantID,
		ID:          "reply-" + payload.ExecutionID,
		Kind:        channels.ReplyOutboxKind,
		AggregateID: payload.ExecutionID,
		DedupKey:    payload.TenantID + "|" + payload.ExecutionID + "|" + channels.ReplyOutboxKind,
		Payload:     encoded,
	}
}

func logRealLarkReplay(t *testing.T, outcome channels.SenderOutcome, status int) {
	t.Helper()
	switch {
	case outcome.Class == channels.OutcomeDelivered:
		t.Log("Lark stable UUID replay: provider accepted again; provider-side dedup not demonstrated")
	case outcome.Class == channels.OutcomePermanentFailure && status == http.StatusConflict:
		t.Log("Lark stable UUID replay: provider returned conflict status; provider-side dedup not demonstrated")
	default:
		t.Log("Lark stable UUID replay: provider behavior unknown")
	}
	t.Log("provider-side dedup: not demonstrated; exactly-once: not supported")
}

type realLarkTransportSnapshot struct {
	tokenContractsValid   bool
	messageContractsValid bool
	messageCount          int
	messageUUIDs          []string
	messageStatuses       []int
}

type realLarkRecordingTransport struct {
	base     http.RoundTripper
	appID    string
	receiver string

	mu                    sync.Mutex
	tokenContractsValid   bool
	messageContractsValid bool
	messageCount          int
	messageUUIDs          []string
	messageStatuses       []int
}

func (t *realLarkRecordingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request == nil || request.URL == nil || request.URL.Host != "open.feishu.cn" || request.URL.Scheme != "https" {
		return nil, errors.New("real Lark request target validation failed")
	}
	if request.URL.Path == tenantAccessTokenPath {
		valid := request.Method == http.MethodPost && request.URL.RawQuery == "" && request.Header.Get("Authorization") == ""
		body, err := readAndRestoreRequestBody(request)
		if err != nil {
			valid = false
		}
		var tokenRequest struct {
			AppID     string `json:"app_id"`
			AppSecret string `json:"app_secret"`
		}
		if json.Unmarshal(body, &tokenRequest) != nil || tokenRequest.AppID != t.appID || !validRealLarkSecret(tokenRequest.AppSecret) {
			valid = false
		}
		t.mu.Lock()
		t.tokenContractsValid = t.tokenContractsValid && valid
		if t.tokenContractsValid == false && t.messageCount == 0 && len(t.messageUUIDs) == 0 {
			// Keep the initial false value meaningful until the first valid token request.
		}
		if len(t.messageUUIDs) == 0 && t.tokenContractsValid == false {
			t.tokenContractsValid = valid
		}
		t.mu.Unlock()
	} else if request.URL.Path == messageCreatePath {
		valid, uuid := validateRealLarkMessageRequest(request, t.receiver)
		t.mu.Lock()
		t.messageContractsValid = t.messageContractsValid && valid
		if t.messageCount == 0 {
			t.messageContractsValid = valid
		}
		t.messageCount++
		t.messageUUIDs = append(t.messageUUIDs, uuid)
		base := t.base
		t.mu.Unlock()
		response, err := base.RoundTrip(request)
		if response != nil {
			t.mu.Lock()
			t.messageStatuses = append(t.messageStatuses, response.StatusCode)
			t.mu.Unlock()
		}
		return response, err
	}
	return nil, errors.New("real Lark endpoint contract validation failed")
}

func (t *realLarkRecordingTransport) snapshot() realLarkTransportSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	return realLarkTransportSnapshot{
		tokenContractsValid:   t.tokenContractsValid,
		messageContractsValid: t.messageContractsValid,
		messageCount:          t.messageCount,
		messageUUIDs:          append([]string(nil), t.messageUUIDs...),
		messageStatuses:       append([]int(nil), t.messageStatuses...),
	}
}

func readAndRestoreRequestBody(request *http.Request) ([]byte, error) {
	if request.Body == nil {
		return nil, errors.New("request body is missing")
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, 8193))
	_ = request.Body.Close()
	request.Body = io.NopCloser(bytes.NewReader(body))
	if err != nil || len(body) > 8192 {
		return nil, errors.New("request body is invalid")
	}
	return body, nil
}

func validateRealLarkMessageRequest(request *http.Request, receiverID string) (bool, string) {
	valid := request.Method == http.MethodPost && request.URL.Query().Get("receive_id_type") == ReceiverIDTypeChatID && len(request.URL.Query()) == 1
	valid = valid && strings.HasPrefix(request.Header.Get("Authorization"), "Bearer ") && request.Header.Get("Content-Type") == "application/json"
	body, err := readAndRestoreRequestBody(request)
	if err != nil {
		return false, ""
	}
	var message struct {
		ReceiveID string `json:"receive_id"`
		MsgType   string `json:"msg_type"`
		Content   string `json:"content"`
		UUID      string `json:"uuid"`
	}
	if json.Unmarshal(body, &message) != nil || message.ReceiveID != receiverID || message.MsgType != "text" || message.UUID == "" {
		return false, ""
	}
	var content struct {
		Text string `json:"text"`
	}
	if json.Unmarshal([]byte(message.Content), &content) != nil || content.Text != realLarkTestText {
		return false, message.UUID
	}
	return true, message.UUID
}
