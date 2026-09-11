package channels

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/delivery"
	"github.com/cyl6/trpc-agent-service/trpcservice/domain"
)

type Slack struct {
	client HTTPDoer
	now    func() time.Time
}

func NewSlack(client HTTPDoer) *Slack {
	if client == nil {
		client = http.DefaultClient
	}
	return &Slack{client: client, now: time.Now}
}

func (*Slack) Name() string { return "slack" }

func (s *Slack) Verify(r *http.Request, body []byte, binding config.ChannelConfig) error {
	secret, err := config.Secret(binding.SigningSecretEnv)
	if err != nil {
		return err
	}
	ts := r.Header.Get("X-Slack-Request-Timestamp")
	seconds, err := strconv.ParseInt(ts, 10, 64)
	if err != nil || s.now().Sub(time.Unix(seconds, 0)) > 5*time.Minute || time.Unix(seconds, 0).Sub(s.now()) > 5*time.Minute {
		return ErrInvalidSignature
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte("v0:" + ts + ":" + string(body)))
	want := "v0=" + hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(want), []byte(r.Header.Get("X-Slack-Signature"))) {
		return ErrInvalidSignature
	}
	return nil
}

type slackEnvelope struct {
	Type      string `json:"type"`
	Challenge string `json:"challenge"`
	EventID   string `json:"event_id"`
	EventTime int64  `json:"event_time"`
	TeamID    string `json:"team_id"`
	APIAppID  string `json:"api_app_id"`
	Event     struct {
		Type        string `json:"type"`
		Subtype     string `json:"subtype"`
		BotID       string `json:"bot_id"`
		User        string `json:"user"`
		Text        string `json:"text"`
		Channel     string `json:"channel"`
		ChannelType string `json:"channel_type"`
		TS          string `json:"ts"`
		ThreadTS    string `json:"thread_ts"`
		Files       []struct {
			ID       string `json:"id"`
			Name     string `json:"name"`
			MimeType string `json:"mimetype"`
			URL      string `json:"url_private"`
		} `json:"files"`
	} `json:"event"`
}

func (*Slack) Parse(body []byte, binding config.ChannelConfig) (ParsedWebhook, error) {
	var envelope slackEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return ParsedWebhook{}, fmt.Errorf("decode slack event: %w", err)
	}
	if envelope.TeamID != binding.WorkspaceID || envelope.APIAppID != binding.ApplicationID {
		return ParsedWebhook{}, ErrBindingMismatch
	}
	if envelope.Type == "url_verification" {
		return ParsedWebhook{Challenge: envelope.Challenge}, nil
	}
	e := envelope.Event
	if envelope.Type != "event_callback" || e.Type != "message" || e.BotID != "" || e.Subtype != "" || e.User == "" || e.Channel == "" {
		return ParsedWebhook{}, ErrUnsupportedEvent
	}
	attachments := make([]domain.Attachment, 0, len(e.Files))
	for _, f := range e.Files {
		attachments = append(attachments, domain.Attachment{Type: "file", FileID: f.ID, Name: f.Name, MimeType: f.MimeType, URL: f.URL})
	}
	if strings.TrimSpace(e.Text) == "" && len(attachments) == 0 {
		return ParsedWebhook{}, ErrUnsupportedEvent
	}
	scope := domain.ScopeGroup
	if e.ChannelType == "im" {
		scope = domain.ScopeDirect
	}
	threadID := e.ThreadTS
	if scope == domain.ScopeGroup && threadID == "" {
		// A later Slack reply carries thread_ts equal to the root message ts.
		// Using the root ts immediately keeps the root and all replies in the
		// same platform session instead of splitting the first turn away.
		threadID = e.TS
	}
	received := time.Unix(envelope.EventTime, 0).UTC()
	if envelope.EventTime == 0 {
		received = time.Now().UTC()
	}
	id := envelope.EventID
	if id == "" {
		id = e.Channel + ":" + e.TS
	}
	return ParsedWebhook{Messages: []domain.InboundMessage{{
		BindingID: binding.BindingID, Channel: "slack", ExternalMessageID: id,
		ExternalUserID: e.User, ConversationID: e.Channel, ThreadID: threadID,
		Scope: scope, Text: e.Text, Attachments: attachments, ReceivedAt: received,
		ReplyTarget: e.Channel,
	}}}, nil
}

func (*Slack) Plan(binding config.ChannelConfig, msg domain.OutboundMessage) ([]delivery.Part, error) {
	limit := binding.MaxMessageLength
	if limit <= 0 {
		limit = 4000
	}
	return planRuneParts(msg, limit), nil
}

type slackSendResponse struct {
	OK      bool   `json:"ok"`
	Error   string `json:"error"`
	Channel string `json:"channel"`
	TS      string `json:"ts"`
}

func (s *Slack) Deliver(ctx context.Context, binding config.ChannelConfig, request delivery.Request) delivery.Result {
	token, err := config.Secret(binding.TokenEnv)
	if err != nil {
		return delivery.Result{
			Outcome: delivery.PermanentRejected, ErrorType: "provider_auth",
			Err: fmt.Errorf("load slack token failed"),
		}
	}
	url := strings.TrimRight(binding.APIBaseURL, "/")
	if url == "" {
		url = "https://slack.com/api/chat.postMessage"
	} else {
		url += "/chat.postMessage"
	}
	msg := request.Message
	payload := map[string]any{"channel": msg.Target, "text": msg.Text}
	if msg.ThreadID != "" {
		payload["thread_ts"] = msg.ThreadID
	}
	exchange := postJSON(ctx, s.client, url, map[string]string{"Authorization": "Bearer " + token}, payload)
	classified, terminal := classifyHTTP(exchange, s.now(), "X-Slack-Req-Id", "X-Request-Id")
	var response slackSendResponse
	decodeErr := decodeJSONResponse(exchange, &response)
	if terminal {
		if decodeErr == nil {
			if code := sanitizeIdentifier(response.Error, 96); code != "" {
				classified.ProviderCode = code
			}
			if !response.OK && slackTransientError(response.Error) {
				classified.Outcome = delivery.RetryableNotSent
				classified.ErrorType = "provider_transient"
			}
		}
		return classified
	}
	if decodeErr != nil {
		return malformedSuccess(exchange, "X-Slack-Req-Id", "X-Request-Id")
	}
	result := responseMetadata(exchange, "X-Slack-Req-Id", "X-Request-Id")
	if response.OK {
		result.Outcome = delivery.Confirmed
		result.ProviderMessageID = sanitizeIdentifier(response.TS, 96)
		return result
	}
	result.ProviderCode = sanitizeIdentifier(response.Error, 96)
	if slackTransientError(response.Error) {
		result.Outcome = delivery.RetryableNotSent
		result.ErrorType = "provider_transient"
		result.RetryAfter = parseRetryAfter(exchange.header.Get("Retry-After"), s.now())
	} else {
		result.Outcome = delivery.PermanentRejected
		result.ErrorType = "provider_rejected"
	}
	return result
}

func slackTransientError(code string) bool {
	switch code {
	case "ratelimited", "internal_error", "fatal_error", "service_unavailable", "temporarily_unavailable", "request_timeout":
		return true
	default:
		return false
	}
}
