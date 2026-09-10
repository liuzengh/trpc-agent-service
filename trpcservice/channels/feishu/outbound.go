package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

const (
	maxFeishuReplyTextBytes    = 150 << 10
	maxFeishuCardBytes         = 30 << 10
	maxFeishuInboundMediaBytes = 32 << 20
)

var (
	errFeishuReplyTooLarge          = errors.New("feishu reply is too large")
	errFeishuReplyUnsupported       = errors.New("feishu reply is unsupported")
	errFeishuStreamStateLost        = errors.New("feishu stream provider message is unavailable")
	errFeishuOutboundNotInitialized = errors.New("feishu outbound client is not initialized")
	errFeishuProviderMessageID      = errors.New("feishu provider message id is missing")
	errFeishuCardTooLarge           = errors.New("feishu card is too large")
)

// ProviderSendError is the stable, body-redacted error returned by one
// Feishu provider call. Reply Outbox retry policy remains owned by IM-06.
type ProviderSendError struct {
	StatusCode      int
	Code            int
	Retryable       bool
	Uncertain       bool
	RetryAfterDelay time.Duration
	cause           error
}

// Error returns a provider-safe description without response bodies, targets,
// credentials, or other provider payload data.
func (e *ProviderSendError) Error() string {
	if e == nil {
		return "feishu provider send failed"
	}
	if e.StatusCode != 0 {
		return fmt.Sprintf("feishu provider returned status %d", e.StatusCode)
	}
	if e.Code != 0 {
		return fmt.Sprintf("feishu provider returned code %d", e.Code)
	}
	return "feishu provider send failed"
}

// Unwrap returns the non-sensitive transport or decoding cause, if present.
func (e *ProviderSendError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// IsRetryable reports whether the provider or transport indicated a temporary
// failure. ReplySender owns the actual retry decision and attempt limit.
func (e *ProviderSendError) IsRetryable() bool {
	return e != nil && e.Retryable
}

// IsSideEffectUncertain reports that a provider call may have reached Feishu
// before its result was lost. The request UUID remains available to the
// provider, but the local outbox still records the conservative state.
func (e *ProviderSendError) IsSideEffectUncertain() bool {
	return e != nil && e.Uncertain
}

// RetryAfter returns a provider-supplied minimum delay when the Feishu API
// included an HTTP Retry-After header. ReplySender owns the bounded retry.
func (e *ProviderSendError) RetryAfter() time.Duration {
	if e == nil || e.RetryAfterDelay < 0 {
		return 0
	}
	return e.RetryAfterDelay
}

// OutboundOption configures a Feishu one-call provider client.
type OutboundOption func(*outboundConfig) error

type outboundConfig struct {
	httpClient *http.Client
	baseURL    string
}

// WithHTTPClient supplies the HTTP client used by the official Feishu SDK.
// It is useful for transport policy and fake-provider tests.
func WithHTTPClient(client *http.Client) OutboundOption {
	return func(config *outboundConfig) error {
		if client == nil {
			return errors.New("http client is required")
		}
		config.httpClient = client
		return nil
	}
}

// WithBaseURL overrides both Feishu API base URLs. Production callers should
// leave it unset; tests use it to point the official SDK at a fake provider.
func WithBaseURL(baseURL string) OutboundOption {
	return func(config *outboundConfig) error {
		parsed, err := url.ParseRequestURI(baseURL)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
			return errors.New("feishu base url is invalid")
		}
		config.baseURL = strings.TrimRight(baseURL, "/")
		return nil
	}
}

// OutboundClient performs exactly one Feishu provider operation. It does not
// claim, persist, retry, or otherwise manage Reply Outbox state.
type OutboundClient struct {
	client *lark.Client
}

// NewOutboundClient creates a Feishu client backed by the binding's scoped
// App Secret. Binding.Secret is never returned or logged.
func NewOutboundClient(
	ctx context.Context,
	secrets platformsecret.SecretProvider,
	binding channels.BindingSnapshot,
	opts ...OutboundOption,
) (*OutboundClient, error) {
	if ctx == nil {
		return nil, errors.New("context is required")
	}
	if secrets == nil {
		return nil, errors.New("secret provider is required")
	}
	if err := binding.Validate(); err != nil {
		return nil, fmt.Errorf("feishu binding: %w", err)
	}
	if binding.Channel != channels.ChannelFeishu {
		return nil, errors.New("feishu outbound client received another channel")
	}
	if binding.Secret.Name == "" {
		return nil, errors.New("feishu app secret reference is required")
	}
	appSecret, err := secrets.ResolveSecret(ctx, binding.Scope(), binding.Secret)
	if err != nil {
		return nil, fmt.Errorf("resolve feishu app secret: %w", err)
	}
	if appSecret == "" {
		return nil, errors.New("feishu app secret is required")
	}
	config := outboundConfig{}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&config); err != nil {
			return nil, err
		}
	}
	clientOptions := []lark.ClientOptionFunc{
		lark.WithLogReqAtDebug(false),
	}
	if config.httpClient != nil {
		clientOptions = append(clientOptions, lark.WithHttpClient(config.httpClient))
	}
	if config.baseURL != "" {
		clientOptions = append(clientOptions,
			lark.WithOpenBaseUrl(config.baseURL),
			lark.WithOAuthBaseUrl(config.baseURL),
		)
	}
	return &OutboundClient{
		client: lark.NewClient(binding.ExternalAccount, appSecret, clientOptions...),
	}, nil
}

// DownloadMediaForMessage is the production media path. Feishu requires both
// the message ID and resource key; the former comes from ChannelInput's
// normalized external message ID and the latter from the opaque media ref.
func (c *OutboundClient) DownloadMediaForMessage(
	ctx context.Context,
	messageID string,
	media channels.ProviderMediaRef,
) (channels.DownloadedMedia, error) {
	if c == nil || c.client == nil {
		return channels.DownloadedMedia{}, errFeishuOutboundNotInitialized
	}
	if err := media.Validate(); err != nil {
		return channels.DownloadedMedia{}, err
	}
	if messageID == "" {
		return channels.DownloadedMedia{}, errors.New("feishu media message id is required")
	}
	resource := media.Reference
	resourceType := "image"
	if media.Kind == channels.MessageTypeFile {
		resourceType = "file"
	}
	request := larkim.NewGetMessageResourceReqBuilder().
		MessageId(messageID).
		FileKey(resource).
		Type(resourceType).
		Build()
	response, err := c.client.Im.MessageResource.Get(ctx, request)
	if err != nil {
		return channels.DownloadedMedia{}, transportError(err)
	}
	if response == nil {
		return channels.DownloadedMedia{}, &ProviderSendError{Retryable: true, cause: errors.New("empty feishu media response")}
	}
	if !response.Success() {
		return channels.DownloadedMedia{}, responseError(response.ApiResp, response.Code)
	}
	if response.File == nil {
		return channels.DownloadedMedia{}, errors.New("feishu media response has no file")
	}
	data, err := io.ReadAll(io.LimitReader(response.File, maxFeishuInboundMediaBytes+1))
	if err != nil {
		return channels.DownloadedMedia{}, fmt.Errorf("read feishu media: %w", err)
	}
	if len(data) > maxFeishuInboundMediaBytes {
		return channels.DownloadedMedia{}, errors.New("feishu media exceeds size limit")
	}
	filename := response.FileName
	if filename == "" {
		filename = resource
	}
	return channels.DownloadedMedia{
		Filename: filename,
		MIMEType: channels.DetectMediaMIMEType(filename, data),
		Data:     data,
	}, nil
}

// SendOnce encodes and sends one Feishu Reply through the official Open API.
// It performs no queue claim, retry, or lease operation.
func (c *OutboundClient) SendOnce(
	ctx context.Context,
	reply channels.Reply,
	providerTarget string,
) (channels.ProviderReceipt, error) {
	if c == nil || c.client == nil {
		return channels.ProviderReceipt{}, errFeishuOutboundNotInitialized
	}
	if err := reply.Validate(); err != nil {
		return channels.ProviderReceipt{}, fmt.Errorf("feishu reply: %w", err)
	}
	if reply.Channel != channels.ChannelFeishu {
		return channels.ProviderReceipt{}, errors.New("feishu outbound client received another channel")
	}
	if reply.ReplyKind() != channels.ReplyKindText {
		return channels.ProviderReceipt{}, errFeishuReplyUnsupported
	}
	content, err := encodeReply(reply)
	if err != nil {
		return channels.ProviderReceipt{}, err
	}
	kind, id, err := parseProviderTarget(providerTarget)
	if err != nil {
		return channels.ProviderReceipt{}, err
	}
	switch kind {
	case feishuTargetMessage:
		return c.replyMessage(ctx, id, content, reply.ReplyID)
	case feishuTargetUser, feishuTargetConversation:
		return c.createMessage(ctx, kind, id, content, reply.ReplyID)
	default:
		return channels.ProviderReceipt{}, errFeishuReplyUnsupported
	}
}

func encodeReply(reply channels.Reply) ([]byte, error) {
	return encodeText(reply.Text)
}

func encodeText(text string) ([]byte, error) {
	if !utf8.ValidString(text) || len([]byte(text)) > maxFeishuReplyTextBytes {
		return nil, errFeishuReplyTooLarge
	}
	content, err := json.Marshal(struct {
		Text string `json:"text"`
	}{Text: text})
	if err != nil {
		return nil, fmt.Errorf("encode feishu text: %w", err)
	}
	return content, nil
}

// SendStream sends the first frame as a reply and updates that bot message on
// later frames. The previous provider message ID is loaded from Reply Outbox,
// so recovery never creates a second visible stream message.
func (c *OutboundClient) SendStream(
	ctx context.Context,
	reply channels.Reply,
	providerTarget string,
	previousProviderMessageID string,
) (channels.ProviderReceipt, error) {
	if c == nil || c.client == nil {
		return channels.ProviderReceipt{}, errFeishuOutboundNotInitialized
	}
	if err := reply.Validate(); err != nil {
		return channels.ProviderReceipt{}, fmt.Errorf("feishu stream reply: %w", err)
	}
	if reply.Channel != channels.ChannelFeishu || reply.ReplyKind() != channels.ReplyKindStream {
		return channels.ProviderReceipt{}, errFeishuReplyUnsupported
	}
	content, err := encodeText(reply.Text)
	if err != nil {
		return channels.ProviderReceipt{}, err
	}
	if previousProviderMessageID == "" &&
		reply.StreamPhase != channels.StreamPhaseStart &&
		reply.StreamPhase != channels.StreamPhaseEnd &&
		reply.StreamPhase != channels.StreamPhaseAbort {
		return channels.ProviderReceipt{}, errFeishuStreamStateLost
	}
	if previousProviderMessageID != "" && reply.StreamPhase != channels.StreamPhaseStart {
		return c.updateTextMessage(ctx, previousProviderMessageID, content)
	}
	kind, id, err := parseProviderTarget(providerTarget)
	if err != nil {
		return channels.ProviderReceipt{}, err
	}
	switch kind {
	case feishuTargetMessage:
		return c.replyMessageWithType(ctx, id, larkim.MsgTypeText, content, reply.ReplyID)
	case feishuTargetUser, feishuTargetConversation:
		return c.createMessageWithType(ctx, kind, id, larkim.MsgTypeText, content, reply.ReplyID)
	default:
		return channels.ProviderReceipt{}, errFeishuReplyUnsupported
	}
}

// SendCard sends a native interactive card. If a provider message ID is
// supplied, Feishu's card patch API updates that card instead of creating a
// duplicate.
func (c *OutboundClient) SendCard(
	ctx context.Context,
	reply channels.Reply,
	providerTarget string,
	previousProviderMessageID string,
) (channels.ProviderReceipt, error) {
	if c == nil || c.client == nil {
		return channels.ProviderReceipt{}, errFeishuOutboundNotInitialized
	}
	if err := reply.Validate(); err != nil {
		return channels.ProviderReceipt{}, fmt.Errorf("feishu card reply: %w", err)
	}
	if reply.Channel != channels.ChannelFeishu || reply.ReplyKind() != channels.ReplyKindCard {
		return channels.ProviderReceipt{}, errFeishuReplyUnsupported
	}
	content, err := encodeCard(*reply.Card)
	if err != nil {
		return channels.ProviderReceipt{}, err
	}
	if previousProviderMessageID != "" {
		req := larkim.NewPatchMessageReqBuilder().
			MessageId(previousProviderMessageID).
			Body(larkim.NewPatchMessageReqBodyBuilder().Content(string(content)).Build()).
			Build()
		resp, err := c.client.Im.Message.Patch(ctx, req)
		if err != nil {
			return channels.ProviderReceipt{}, transportError(err)
		}
		if resp == nil {
			return channels.ProviderReceipt{}, &ProviderSendError{Retryable: true, Uncertain: true, cause: errors.New("empty feishu card patch response")}
		}
		if !resp.Success() {
			return channels.ProviderReceipt{}, responseError(resp.ApiResp, resp.Code)
		}
		return channels.ProviderReceipt{ProviderMessageID: previousProviderMessageID}, nil
	}
	kind, id, err := parseProviderTarget(providerTarget)
	if err != nil {
		return channels.ProviderReceipt{}, err
	}
	switch kind {
	case feishuTargetMessage:
		return c.replyMessageWithType(ctx, id, larkim.MsgTypeInteractive, content, reply.ReplyID)
	case feishuTargetUser, feishuTargetConversation:
		return c.createMessageWithType(ctx, kind, id, larkim.MsgTypeInteractive, content, reply.ReplyID)
	default:
		return channels.ProviderReceipt{}, errFeishuReplyUnsupported
	}
}

func encodeCard(card channels.ReplyCard) ([]byte, error) {
	type cardText struct {
		Tag     string `json:"tag"`
		Content string `json:"content"`
	}
	type cardAction struct {
		Tag   string            `json:"tag"`
		Text  cardText          `json:"text"`
		Type  string            `json:"type,omitempty"`
		Value map[string]string `json:"value,omitempty"`
	}
	type cardElement struct {
		Tag     string       `json:"tag"`
		Content string       `json:"content,omitempty"`
		Text    *cardText    `json:"text,omitempty"`
		Actions []cardAction `json:"actions,omitempty"`
	}
	type cardHeader struct {
		Title struct {
			Tag     string `json:"tag"`
			Content string `json:"content"`
		} `json:"title"`
		Template string `json:"template,omitempty"`
	}
	type cardBody struct {
		Elements []cardElement `json:"elements"`
	}
	payload := struct {
		Schema string     `json:"schema"`
		Header cardHeader `json:"header"`
		Body   cardBody   `json:"body"`
	}{}
	payload.Schema = "2.0"
	payload.Header.Title.Tag = "plain_text"
	payload.Header.Title.Content = card.Title
	payload.Header.Template = "blue"
	payload.Body.Elements = append(payload.Body.Elements, cardElement{
		Tag:     "markdown",
		Content: card.Body,
	})
	if card.Status != "" {
		payload.Body.Elements = append(payload.Body.Elements, cardElement{
			Tag:  "div",
			Text: &cardText{Tag: "plain_text", Content: "状态：" + card.Status},
		})
	}
	if len(card.Actions) > 0 {
		actions := make([]cardAction, 0, len(card.Actions))
		for _, action := range card.Actions {
			actions = append(actions, cardAction{
				Tag:  "button",
				Text: cardText{Tag: "plain_text", Content: action.Label},
				Type: "primary",
				Value: map[string]string{
					"action_id": action.ID,
					"value":     action.Value,
				},
			})
		}
		payload.Body.Elements = append(payload.Body.Elements, cardElement{Tag: "action", Actions: actions})
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode feishu card: %w", err)
	}
	if len(encoded) > maxFeishuCardBytes {
		return nil, errFeishuCardTooLarge
	}
	return encoded, nil
}

func (c *OutboundClient) createMessage(
	ctx context.Context,
	targetKind, targetID string,
	content []byte,
	uuid string,
) (channels.ProviderReceipt, error) {
	return c.createMessageWithType(ctx, targetKind, targetID, larkim.MsgTypeText, content, uuid)
}

func (c *OutboundClient) createMessageWithType(
	ctx context.Context,
	targetKind, targetID, msgType string,
	content []byte,
	uuid string,
) (channels.ProviderReceipt, error) {
	req := larkim.NewCreateMessageReqBuilder().
		ReceiveIdType(targetKind).
		Body(larkim.NewCreateMessageReqBodyBuilder().
			ReceiveId(targetID).
			MsgType(msgType).
			Content(string(content)).
			Uuid(uuid).
			Build()).
		Build()
	resp, err := c.client.Im.Message.Create(ctx, req)
	if err != nil {
		return channels.ProviderReceipt{}, transportError(err)
	}
	if resp == nil {
		return channels.ProviderReceipt{}, &ProviderSendError{Retryable: true, Uncertain: true, cause: errors.New("empty feishu response")}
	}
	if !resp.Success() {
		return channels.ProviderReceipt{}, responseError(resp.ApiResp, resp.Code)
	}
	if resp.Data != nil {
		return receiptFromID(resp.Data.MessageId)
	}
	return receiptFromID(nil)
}

func (c *OutboundClient) replyMessage(
	ctx context.Context,
	messageID string,
	content []byte,
	uuid string,
) (channels.ProviderReceipt, error) {
	return c.replyMessageWithType(ctx, messageID, larkim.MsgTypeText, content, uuid)
}

func (c *OutboundClient) replyMessageWithType(
	ctx context.Context,
	messageID, msgType string,
	content []byte,
	uuid string,
) (channels.ProviderReceipt, error) {
	req := larkim.NewReplyMessageReqBuilder().
		MessageId(messageID).
		Body(larkim.NewReplyMessageReqBodyBuilder().
			MsgType(msgType).
			Content(string(content)).
			Uuid(uuid).
			Build()).
		Build()
	resp, err := c.client.Im.Message.Reply(ctx, req)
	if err != nil {
		return channels.ProviderReceipt{}, transportError(err)
	}
	if resp == nil {
		return channels.ProviderReceipt{}, &ProviderSendError{Retryable: true, Uncertain: true, cause: errors.New("empty feishu response")}
	}
	if !resp.Success() {
		return channels.ProviderReceipt{}, responseError(resp.ApiResp, resp.Code)
	}
	if resp.Data != nil {
		return receiptFromID(resp.Data.MessageId)
	}
	return receiptFromID(nil)
}

func (c *OutboundClient) updateTextMessage(
	ctx context.Context,
	messageID string,
	content []byte,
) (channels.ProviderReceipt, error) {
	req := larkim.NewUpdateMessageReqBuilder().
		MessageId(messageID).
		Body(larkim.NewUpdateMessageReqBodyBuilder().
			MsgType(larkim.MsgTypeText).
			Content(string(content)).
			Build()).
		Build()
	resp, err := c.client.Im.Message.Update(ctx, req)
	if err != nil {
		return channels.ProviderReceipt{}, transportError(err)
	}
	if resp == nil {
		return channels.ProviderReceipt{}, &ProviderSendError{Retryable: true, Uncertain: true, cause: errors.New("empty feishu message update response")}
	}
	if !resp.Success() {
		return channels.ProviderReceipt{}, responseError(resp.ApiResp, resp.Code)
	}
	return channels.ProviderReceipt{ProviderMessageID: messageID}, nil
}

func receiptFromID(providerMessageID *string) (channels.ProviderReceipt, error) {
	if providerMessageID != nil && *providerMessageID != "" {
		return channels.ProviderReceipt{ProviderMessageID: *providerMessageID}, nil
	}
	return channels.ProviderReceipt{}, &ProviderSendError{Uncertain: true, cause: errFeishuProviderMessageID}
}

func transportError(err error) error {
	return &ProviderSendError{
		Retryable: !errors.Is(err, context.Canceled),
		Uncertain: true,
		cause:     err,
	}
}

func responseError(response *larkcore.ApiResp, code int) error {
	statusCode := 0
	retryAfter := time.Duration(0)
	if response != nil {
		statusCode = response.StatusCode
		retryAfter = parseRetryAfter(response.Header.Get("Retry-After"), time.Now())
	}
	return &ProviderSendError{
		StatusCode:      statusCode,
		Code:            code,
		Retryable:       retryableFeishuStatus(statusCode),
		Uncertain:       statusCode == http.StatusRequestTimeout || statusCode >= http.StatusInternalServerError,
		RetryAfterDelay: retryAfter,
	}
}

func retryableFeishuStatus(statusCode int) bool {
	return statusCode == http.StatusRequestTimeout || statusCode == http.StatusTooEarly ||
		statusCode == http.StatusTooManyRequests || statusCode >= http.StatusInternalServerError
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
		return 0
	}
	deadline, err := http.ParseTime(value)
	if err != nil || !deadline.After(now) {
		return 0
	}
	return deadline.Sub(now)
}

var _ channels.ProviderOutboundClient = (*OutboundClient)(nil)
