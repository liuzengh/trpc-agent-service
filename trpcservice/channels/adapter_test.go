package channels

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/delivery"
	"github.com/cyl6/trpc-agent-service/trpcservice/domain"
)

type leakingErrorDoer struct{}

func (leakingErrorDoer) Do(req *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("network failure for %s", req.URL.String())
}

type captureDoer struct {
	payload map[string]any
	reply   string
}

func (d *captureDoer) Do(req *http.Request) (*http.Response, error) {
	if err := json.NewDecoder(req.Body).Decode(&d.payload); err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(d.reply)),
		Header:     make(http.Header),
	}, nil
}

type fixedResponseDoer struct {
	status   int
	body     string
	header   http.Header
	err      error
	calls    int
	payloads []map[string]any
}

func (d *fixedResponseDoer) Do(req *http.Request) (*http.Response, error) {
	d.calls++
	if req.Body != nil {
		payload := map[string]any{}
		if err := json.NewDecoder(req.Body).Decode(&payload); err == nil {
			d.payloads = append(d.payloads, payload)
		}
	}
	if d.err != nil {
		return nil, d.err
	}
	status := d.status
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(d.body)),
		Header:     d.header.Clone(),
	}, nil
}

func TestTelegramVerifyAndParse(t *testing.T) {
	t.Setenv("TG_SECRET", "hook-secret")
	binding := config.ChannelConfig{Type: "telegram", BindingID: "main", SigningSecretEnv: "TG_SECRET"}
	body := []byte(`{"update_id":42,"message":{"message_id":3,"date":1700000000,"text":"hello","from":{"id":7},"chat":{"id":9,"type":"private"}}}`)
	req := httptest.NewRequest("POST", "/", bytes.NewReader(body))
	req.Header.Set(telegramSecretHeader, os.Getenv("TG_SECRET"))
	adapter := NewTelegram(nil)
	if err := adapter.Verify(req, body, binding); err != nil {
		t.Fatal(err)
	}
	parsed, err := adapter.Parse(body, binding)
	if err != nil {
		t.Fatal(err)
	}
	msg := parsed.Messages[0]
	if msg.Scope != domain.ScopeDirect || msg.ExternalMessageID != "42" || msg.ExternalUserID != "7" {
		t.Fatalf("unexpected normalized message: %+v", msg)
	}
}

func TestSlackVerifyParseAndRejectReplay(t *testing.T) {
	t.Setenv("SLACK_SECRET", "signing-secret")
	now := time.Unix(1_700_000_000, 0)
	body := []byte(`{"type":"event_callback","team_id":"T1","api_app_id":"A1","event_id":"Ev1","event_time":1700000000,"event":{"type":"message","user":"U1","text":"hello","channel":"C1","channel_type":"channel","ts":"1.2"}}`)
	ts := strconv.FormatInt(now.Unix(), 10)
	mac := hmac.New(sha256.New, []byte("signing-secret"))
	_, _ = mac.Write([]byte("v0:" + ts + ":" + string(body)))
	signature := "v0=" + hex.EncodeToString(mac.Sum(nil))
	req := httptest.NewRequest("POST", "/", bytes.NewReader(body))
	req.Header.Set("X-Slack-Request-Timestamp", ts)
	req.Header.Set("X-Slack-Signature", signature)
	binding := config.ChannelConfig{Type: "slack", BindingID: "main", SigningSecretEnv: "SLACK_SECRET", WorkspaceID: "T1", ApplicationID: "A1"}
	adapter := NewSlack(nil)
	adapter.now = func() time.Time { return now }
	if err := adapter.Verify(req, body, binding); err != nil {
		t.Fatal(err)
	}
	parsed, err := adapter.Parse(body, binding)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Messages[0].Scope != domain.ScopeGroup || parsed.Messages[0].ExternalMessageID != "Ev1" || parsed.Messages[0].ThreadID != "1.2" {
		t.Fatalf("unexpected normalized message: %+v", parsed.Messages[0])
	}
	mismatched := binding
	mismatched.WorkspaceID = "T2"
	if _, err := adapter.Parse(body, mismatched); err != ErrBindingMismatch {
		t.Fatalf("expected Slack workspace mismatch rejection, got %v", err)
	}
	adapter.now = func() time.Time { return now.Add(6 * time.Minute) }
	if err := adapter.Verify(req, body, binding); err != ErrInvalidSignature {
		t.Fatalf("expected stale request rejection, got %v", err)
	}
}

func TestTelegramTopicParticipatesInNormalizedThread(t *testing.T) {
	body := []byte(`{"update_id":43,"message":{"message_id":4,"message_thread_id":99,"date":1700000000,"text":"topic","from":{"id":7},"chat":{"id":-9,"type":"supergroup"}}}`)
	parsed, err := NewTelegram(nil).Parse(body, config.ChannelConfig{BindingID: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if got := parsed.Messages[0]; got.Scope != domain.ScopeGroup || got.ThreadID != "99" {
		t.Fatalf("telegram topic was not normalized: %+v", got)
	}
}

func TestChunksUsesRuneLength(t *testing.T) {
	got := chunks("你好世界", 3)
	if len(got) != 2 || got[0] != "你好世" || got[1] != "界" {
		t.Fatalf("unexpected chunks: %#v", got)
	}
}

func TestTelegramDeliveryErrorNeverLeaksBotToken(t *testing.T) {
	t.Setenv("TG_TOKEN", "fixture-bot-token")
	binding := config.ChannelConfig{TokenEnv: "TG_TOKEN", MaxMessageLength: 4096}
	result := NewTelegram(leakingErrorDoer{}).Deliver(context.Background(), binding, delivery.Request{
		OperationKey: "op-1", Message: domain.OutboundMessage{Target: "1", Text: "hello"},
	})
	if result.Outcome != delivery.Unknown || result.Err == nil {
		t.Fatalf("delivery result = %+v, want unknown", result)
	}
	if strings.Contains(result.Err.Error(), os.Getenv("TG_TOKEN")) {
		t.Fatalf("bot token leaked in error: %v", result.Err)
	}
}

func TestSlackAndTelegramDeliverPreserveThread(t *testing.T) {
	t.Setenv("SLACK_TOKEN", "slack-test-token")
	slackHTTP := &captureDoer{reply: `{"ok":true}`}
	result := NewSlack(slackHTTP).Deliver(context.Background(), config.ChannelConfig{
		TokenEnv: "SLACK_TOKEN", APIBaseURL: "https://slack.invalid/api", MaxMessageLength: 100,
	}, delivery.Request{OperationKey: "op-slack", Message: domain.OutboundMessage{Target: "C1", ThreadID: "123.45", Text: "reply"}})
	if result.Outcome != delivery.Confirmed || slackHTTP.payload["thread_ts"] != "123.45" {
		t.Fatalf("Slack thread payload = %#v, result=%+v", slackHTTP.payload, result)
	}
	if _, exists := slackHTTP.payload["client_msg_id"]; exists {
		t.Fatalf("Slack payload must not claim unsupported provider idempotency: %#v", slackHTTP.payload)
	}

	t.Setenv("TG_TOKEN", "telegram-test-token")
	telegramHTTP := &captureDoer{reply: `{"ok":true}`}
	result = NewTelegram(telegramHTTP).Deliver(context.Background(), config.ChannelConfig{
		TokenEnv: "TG_TOKEN", APIBaseURL: "https://telegram.invalid", MaxMessageLength: 100,
	}, delivery.Request{OperationKey: "op-telegram", Message: domain.OutboundMessage{Target: "-1", ThreadID: "99", Text: "reply"}})
	if result.Outcome != delivery.Confirmed || telegramHTTP.payload["message_thread_id"] != float64(99) {
		t.Fatalf("Telegram topic payload = %#v, result=%+v", telegramHTTP.payload, result)
	}
}

func TestRunePlanIsDeterministicAndDeliverSendsOnePart(t *testing.T) {
	t.Setenv("SLACK_TOKEN", "slack-test-token")
	adapter := NewSlack(nil)
	message := domain.OutboundMessage{Target: "C1", Text: "你好世界"}
	parts, err := adapter.Plan(config.ChannelConfig{MaxMessageLength: 3}, message)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 2 || parts[0].Index != 0 || parts[0].Total != 2 || parts[0].Message.Text != "你好世" || parts[1].Message.Text != "界" {
		t.Fatalf("unexpected plan: %+v", parts)
	}
	doer := &fixedResponseDoer{body: `{"ok":true,"ts":"171.0001"}`}
	adapter = NewSlack(doer)
	result := adapter.Deliver(context.Background(), config.ChannelConfig{
		TokenEnv: "SLACK_TOKEN", APIBaseURL: "https://slack.invalid/api",
	}, delivery.Request{OperationKey: "stable-op", AttemptNo: 1, Message: parts[0].Message})
	if result.Outcome != delivery.Confirmed || doer.calls != 1 || len(doer.payloads) != 1 {
		t.Fatalf("result=%+v calls=%d payloads=%#v", result, doer.calls, doer.payloads)
	}
	if doer.payloads[0]["text"] != "你好世" {
		t.Fatalf("unexpected single-part payload: %#v", doer.payloads[0])
	}
}

func TestTelegramStructuredDeliveryOutcomes(t *testing.T) {
	t.Setenv("TG_TOKEN", "telegram-test-token")
	binding := config.ChannelConfig{TokenEnv: "TG_TOKEN", APIBaseURL: "https://telegram.invalid"}
	request := delivery.Request{OperationKey: "telegram-op", Message: domain.OutboundMessage{Target: "1", Text: "hello"}}
	tests := []struct {
		name       string
		doer       *fixedResponseDoer
		outcome    delivery.Outcome
		code       string
		retryAfter time.Duration
		messageID  string
		requestID  string
	}{
		{
			name: "confirmed", doer: &fixedResponseDoer{
				body:   `{"ok":true,"result":{"message_id":77}}`,
				header: http.Header{"X-Request-Id": []string{"tg-request-1"}},
			}, outcome: delivery.Confirmed, messageID: "77", requestID: "tg-request-1",
		},
		{
			name: "rate limited", doer: &fixedResponseDoer{
				status: http.StatusTooManyRequests,
				body:   `{"ok":false,"error_code":429,"parameters":{"retry_after":7}}`,
				header: http.Header{"Retry-After": []string{"3"}},
			}, outcome: delivery.RetryableNotSent, code: "429", retryAfter: 7 * time.Second,
		},
		{
			name: "permanent rejection", doer: &fixedResponseDoer{
				status: http.StatusBadRequest, body: `{"ok":false,"error_code":400}`,
			}, outcome: delivery.PermanentRejected, code: "400",
		},
		{
			name: "ambiguous server failure", doer: &fixedResponseDoer{
				status: http.StatusServiceUnavailable, body: `{"ok":false}`,
			}, outcome: delivery.Unknown, code: "503",
		},
		{
			name: "malformed success", doer: &fixedResponseDoer{body: `{not-json`},
			outcome: delivery.Unknown,
		},
		{
			name: "transport unknown", doer: &fixedResponseDoer{err: errors.New("connection reset after write")},
			outcome: delivery.Unknown,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := NewTelegram(test.doer).Deliver(context.Background(), binding, request)
			if result.Outcome != test.outcome || result.ProviderCode != test.code || result.RetryAfter != test.retryAfter ||
				result.ProviderMessageID != test.messageID || result.ProviderRequestID != test.requestID {
				t.Fatalf("result = %+v", result)
			}
			if test.doer.err == nil && len(result.ResponseHash) != 64 {
				t.Fatalf("response hash = %q", result.ResponseHash)
			}
		})
	}
}

func TestSlackStructuredDeliveryOutcomes(t *testing.T) {
	t.Setenv("SLACK_TOKEN", "slack-test-token")
	binding := config.ChannelConfig{TokenEnv: "SLACK_TOKEN", APIBaseURL: "https://slack.invalid/api"}
	request := delivery.Request{OperationKey: "slack-op", Message: domain.OutboundMessage{Target: "C1", Text: "hello"}}
	tests := []struct {
		name       string
		doer       *fixedResponseDoer
		outcome    delivery.Outcome
		code       string
		retryAfter time.Duration
		messageID  string
		requestID  string
	}{
		{
			name: "confirmed", doer: &fixedResponseDoer{
				body:   `{"ok":true,"channel":"C1","ts":"171.0002"}`,
				header: http.Header{"X-Slack-Req-Id": []string{"slack-request-1"}},
			}, outcome: delivery.Confirmed, messageID: "171.0002", requestID: "slack-request-1",
		},
		{
			name: "http rate limited", doer: &fixedResponseDoer{
				status: http.StatusTooManyRequests, body: `{"ok":false,"error":"ratelimited"}`,
				header: http.Header{"Retry-After": []string{"9"}},
			}, outcome: delivery.RetryableNotSent, code: "ratelimited", retryAfter: 9 * time.Second,
		},
		{
			name: "provider transient", doer: &fixedResponseDoer{body: `{"ok":false,"error":"internal_error"}`},
			outcome: delivery.RetryableNotSent, code: "internal_error",
		},
		{
			name: "permanent rejection", doer: &fixedResponseDoer{body: `{"ok":false,"error":"channel_not_found"}`},
			outcome: delivery.PermanentRejected, code: "channel_not_found",
		},
		{
			name: "malformed success", doer: &fixedResponseDoer{body: `nope`}, outcome: delivery.Unknown,
		},
		{
			name: "transport unknown", doer: &fixedResponseDoer{err: errors.New("timeout")}, outcome: delivery.Unknown,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			adapter := NewSlack(test.doer)
			adapter.now = func() time.Time { return time.Unix(1_700_000_000, 0) }
			result := adapter.Deliver(context.Background(), binding, request)
			if result.Outcome != test.outcome || result.ProviderCode != test.code || result.RetryAfter != test.retryAfter ||
				result.ProviderMessageID != test.messageID || result.ProviderRequestID != test.requestID {
				t.Fatalf("result = %+v", result)
			}
		})
	}
}

func TestParseRetryAfterHTTPDate(t *testing.T) {
	now := time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC)
	if got := parseRetryAfter(now.Add(45*time.Second).Format(http.TimeFormat), now); got != 45*time.Second {
		t.Fatalf("Retry-After date parsed as %v", got)
	}
}
