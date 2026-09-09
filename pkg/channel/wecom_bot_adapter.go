package channel

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

const (
	WeComBotBridgeTokenHeader = "X-WeCom-Bot-Bridge-Token"
	// The platform sends one final stream update, bounded to 20 KiB.
	MaxWeComBotReplyBytes = 20480
)

// WeComBotAdapter translates an operator-owned WebSocket connector's trusted
// envelope. Provider Bot Secret and WebSocket ownership stay in that connector.
type WeComBotAdapter struct {
	client   *http.Client
	endpoint string
}

// ValidateWeComBotBridgeURL validates an operator-configured origin. HTTP is
// supported for an isolated local network; deployments crossing trust domains
// must terminate TLS at the connector. No tenant field selects this address.
func ValidateWeComBotBridgeURL(value string) error {
	u, err := url.Parse(value)
	if err != nil || value == "" || strings.TrimSpace(value) != value || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawFragment != "" || (u.Path != "" && u.Path != "/") || u.RawPath != "" {
		return fmt.Errorf("WeCom bot bridge must be an HTTP(S) origin without credentials, path, query or fragment")
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return fmt.Errorf("WeCom bot bridge port is invalid")
		}
	}
	if strings.ContainsAny(u.Host, "\\\x00\r\n") || strings.HasSuffix(u.Host, ":") {
		return fmt.Errorf("WeCom bot bridge host is invalid")
	}
	return nil
}

// NewWeComBotAdapter accepts an empty origin for Gateway's ingress-only use.
// Delivery validates a non-empty origin at startup and uses the fixed /reply
// path. Redirects never carry bridge credentials to another destination.
func NewWeComBotAdapter(bridgeBaseURL string) *WeComBotAdapter {
	a := &WeComBotAdapter{client: &http.Client{
		Timeout:       20 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
	if ValidateWeComBotBridgeURL(bridgeBaseURL) == nil {
		a.endpoint = strings.TrimRight(bridgeBaseURL, "/") + "/reply"
	}
	return a
}

func (a *WeComBotAdapter) VerifySignature(req *http.Request, binding *tenant.ChannelBinding) error {
	if req == nil || binding == nil || !tenant.IsValidWeComBotBridgeToken(binding.Token) {
		return permanentCredentialError("WeComBot")
	}
	values := req.Header.Values(WeComBotBridgeTokenHeader)
	if len(values) != 1 || !tenant.IsValidWeComBotBridgeToken(values[0]) {
		return permanentCredentialError("WeComBot")
	}
	want, got := sha256.Sum256([]byte(binding.Token)), sha256.Sum256([]byte(values[0]))
	if subtle.ConstantTimeCompare(want[:], got[:]) != 1 {
		return permanentCredentialError("WeComBot")
	}
	return nil
}

func (a *WeComBotAdapter) ParseInbound(req *http.Request, binding *tenant.ChannelBinding) (*InboundMessage, error) {
	if binding == nil || tenant.ValidateWeComBotBinding(*binding) != nil {
		return nil, permanentCredentialError("WeComBot")
	}
	data, err := readAdapterInboundBody(req, false)
	if err != nil {
		return nil, err
	}
	var envelope struct {
		ReceivedAt time.Time `json:"received_at"`
		Frame      struct {
			Cmd     string `json:"cmd"`
			Headers struct {
				RequestID string `json:"req_id"`
			} `json:"headers"`
			Body struct {
				MessageID string `json:"msgid"`
				BotID     string `json:"aibotid"`
				ChatType  string `json:"chattype"`
				ChatID    string `json:"chatid"`
				From      struct {
					UserID string `json:"userid"`
				} `json:"from"`
				MessageType string `json:"msgtype"`
				Text        struct {
					Content string `json:"content"`
				} `json:"text"`
			} `json:"body"`
		} `json:"frame"`
	}
	if !utf8.Valid(data) || json.Unmarshal(data, &envelope) != nil {
		return nil, fmt.Errorf("invalid WeCom bot bridge envelope")
	}
	body := envelope.Frame.Body
	if body.BotID != binding.AccountID {
		return nil, fmt.Errorf("WeCom bot callback account mismatch")
	}
	if envelope.Frame.Cmd == "aibot_event_callback" {
		return nil, ErrIgnoredInbound
	}
	if envelope.Frame.Cmd != "aibot_msg_callback" {
		return nil, fmt.Errorf("unsupported WeCom bot callback command")
	}
	if body.MessageType != "text" || strings.TrimSpace(body.Text.Content) == "" {
		return nil, ErrIgnoredInbound
	}
	_, offset := envelope.ReceivedAt.Zone()
	if envelope.ReceivedAt.IsZero() || envelope.ReceivedAt.Unix() <= 0 || offset != 0 ||
		!validBotIdentity(body.MessageID, 256) || !validBotIdentity(envelope.Frame.Headers.RequestID, 256) || !validBotIdentity(body.From.UserID, 256) {
		return nil, fmt.Errorf("WeCom bot callback identity or receipt timestamp is invalid")
	}
	if len(body.Text.Content) > 32*1024 || strings.ContainsRune(body.Text.Content, 0) {
		return nil, fmt.Errorf("WeCom bot text exceeds platform limits")
	}
	conversation, group := body.From.UserID, false
	switch body.ChatType {
	case "single":
	case "group":
		if !validBotIdentity(body.ChatID, 256) {
			return nil, fmt.Errorf("WeCom bot group identity is missing or invalid")
		}
		conversation, group = body.ChatID, true
	default:
		return nil, fmt.Errorf("WeCom bot conversation type is invalid")
	}
	return &InboundMessage{
		ChannelType: string(ChannelTypeWeComBot), ExternalUserID: body.From.UserID,
		ConversationID: conversation, MessageID: body.MessageID, ReplyToID: envelope.Frame.Headers.RequestID,
		Content: body.Text.Content, Timestamp: envelope.ReceivedAt, IsGroupChat: group,
	}, nil
}

func validBotIdentity(value string, maxBytes int) bool {
	if value == "" || len(value) > maxBytes || strings.TrimSpace(value) != value || !utf8.ValidString(value) {
		return false
	}
	for _, ch := range value {
		if unicode.IsControl(ch) || unicode.Is(unicode.Cf, ch) {
			return false
		}
	}
	return true
}

func (a *WeComBotAdapter) SendReply(ctx context.Context, binding *tenant.ChannelBinding, msg *OutboundMessage) error {
	if a == nil || a.client == nil || a.endpoint == "" || binding == nil || tenant.ValidateWeComBotBinding(*binding) != nil || !tenant.IsValidWeComBotBridgeToken(binding.Token) {
		return permanentCredentialError("WeComBot")
	}
	if msg == nil || !validBotIdentity(msg.ConversationID, 256) || !validBotIdentity(msg.ReplyToID, 256) || !validBotIdentity(msg.DeliveryID, maxDeliveryIDBytes) || strings.TrimSpace(msg.Content) == "" || len(msg.Content) > MaxWeComBotReplyBytes || !utf8.ValidString(msg.Content) || strings.ContainsRune(msg.Content, 0) || len(msg.Attachments) != 0 || (msg.ContentType != "" && msg.ContentType != "text" && msg.ContentType != "markdown") {
		return invalidOutboundMessageError("WeComBot")
	}
	cursor, err := OutboundDeliveryCursor(msg)
	if err != nil || cursor != 0 {
		return invalidOutboundMessageError("WeComBot")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	payload, err := json.Marshal(struct {
		BotID          string `json:"bot_id"`
		RequestID      string `json:"request_id"`
		ConversationID string `json:"conversation_id"`
		Content        string `json:"content"`
		DeliveryID     string `json:"delivery_id"`
	}{binding.AccountID, msg.ReplyToID, msg.ConversationID, msg.Content, msg.DeliveryID})
	if err != nil {
		return permanentRequestBuildError("WeComBot")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpoint, bytes.NewReader(payload))
	if err != nil {
		return permanentRequestBuildError("WeComBot")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(WeComBotBridgeTokenHeader, binding.Token)
	telemetry.InjectHTTP(ctx, req)
	resp, dispatched, err := doProviderRequest(a.client, req)
	if err != nil || resp == nil {
		return providerTransportFailure("WeComBot", dispatched)
	}
	defer resp.Body.Close()
	data, readErr := io.ReadAll(io.LimitReader(resp.Body, 4097))
	var result struct {
		Status string `json:"status"`
	}
	validResult := readErr == nil && len(data) <= 4096 && json.Unmarshal(data, &result) == nil
	switch {
	case resp.StatusCode == http.StatusOK && validResult && result.Status == "sent":
		SetOutboundDeliveryProgress(msg, 1, true)
		return nil
	case resp.StatusCode == http.StatusServiceUnavailable && validResult && result.Status == "not_sent":
		return retryableTransportError("WeComBot")
	case resp.StatusCode == http.StatusTooManyRequests && validResult && result.Status == "not_sent":
		return RateLimitedDeliveryError(retryableTransportError("WeComBot"), weWorkRetryAfter(resp.Header, time.Now()))
	case resp.StatusCode == http.StatusTooManyRequests:
		return UnknownDeliveryError(retryableTransportError("WeComBot"))
	case resp.StatusCode == http.StatusConflict || resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode >= 500 || resp.StatusCode == http.StatusOK:
		return UnknownDeliveryError(retryableTransportError("WeComBot"))
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		return PermanentDeliveryError(fmt.Errorf("WeCom bot bridge rejected reply: status=%d", resp.StatusCode))
	default:
		return UnknownDeliveryError(retryableTransportError("WeComBot"))
	}
}

func (a *WeComBotAdapter) SendStreamChunk(context.Context, *tenant.ChannelBinding, *StreamChunk) error {
	return PermanentDeliveryError(fmt.Errorf("%w: WeComBot final reply", ErrStreamingUnsupported))
}
func (a *WeComBotAdapter) SupportsStreaming() bool { return false }
func (a *WeComBotAdapter) HandleRateLimit(err error) time.Duration {
	_, delay := DeliveryFailure(err)
	return delay
}

var _ Adapter = (*WeComBotAdapter)(nil)
