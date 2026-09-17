// Package feishu implements the service-owned Feishu Channel Adapter.
package feishu

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu/protocol"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

const routeKeyQuery = "route_key"

type TextSender interface {
	ReplyText(context.Context, channel.ReplyDestination, string, string, string) (string, error)
}
type CardSender interface {
	ReplyCard(context.Context, channel.ReplyDestination, string, []byte, string) (string, error)
}
type ImageSender interface {
	ReplyImage(context.Context, channel.ReplyDestination, string, []byte, string, string) (string, error)
}

type Adapter struct {
	Protocol protocol.Verifier
	Sender   TextSender
}

func (a *Adapter) ID() string { return "feishu" }

func (a *Adapter) Run(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (a *Adapter) PublicRoute(_ context.Context, request channel.CallbackRequest) (channel.PublicRouteHint, error) {
	routeKey := request.Query[routeKeyQuery]
	if routeKey == "" || len(request.Body) == 0 {
		return channel.PublicRouteHint{}, runtime.ErrInvalidEnvelope
	}
	timestamp := mapValue(request.Headers, "X-Lark-Request-Timestamp")
	nonce := mapValue(request.Headers, "X-Lark-Request-Nonce")
	if timestamp == "" || nonce == "" {
		return channel.PublicRouteHint{}, runtime.ErrInvalidEnvelope
	}
	attempt := sha256.Sum256(append([]byte(timestamp+"\x00"+nonce+"\x00"), request.Body...))
	return channel.PublicRouteHint{Channel: a.ID(), ExternalAccountHint: routeKey,
		RouteKeyDigest: protocol.RouteKeyDigest(routeKey), IngressAttemptID: hex.EncodeToString(attempt[:16])}, nil
}

func (a *Adapter) IsChallenge(request channel.CallbackRequest) bool {
	return protocol.IsChallengeRequest(request)
}

func (a *Adapter) PublicChallengeRoute(_ context.Context, request channel.CallbackRequest) (channel.PublicRouteHint, error) {
	routeKey := request.Query[routeKeyQuery]
	if routeKey == "" || len(request.Body) == 0 || !a.IsChallenge(request) {
		return channel.PublicRouteHint{}, runtime.ErrInvalidEnvelope
	}
	attempt := sha256.Sum256(append([]byte("feishu-challenge\x00"), request.Body...))
	return channel.PublicRouteHint{Channel: a.ID(), ExternalAccountHint: routeKey,
		RouteKeyDigest: protocol.RouteKeyDigest(routeKey), IngressAttemptID: hex.EncodeToString(attempt[:16])}, nil
}

func (a *Adapter) VerifyChallenge(ctx context.Context, request channel.CallbackRequest, handle channel.ScopedVerifierHandle) (channel.HTTPResponse, channel.VerificationReceipt, error) {
	if handle == nil {
		return channel.HTTPResponse{}, channel.VerificationReceipt{}, runtime.ErrInvariantViolation
	}
	callback, receipt, err := handle.Verify(ctx, request, a.Protocol.VerifyChallenge)
	if err != nil {
		return channel.HTTPResponse{}, channel.VerificationReceipt{}, err
	}
	return channel.HTTPResponse{ContentType: callback.Headers["content-type"], Body: callback.Body}, receipt, nil
}

func (a *Adapter) CallbackACK() channel.HTTPResponse {
	return channel.HTTPResponse{ContentType: "application/json", Body: []byte(`{"code":0}`)}
}

func (a *Adapter) Verify(ctx context.Context, request channel.CallbackRequest, handle channel.ScopedVerifierHandle) (channel.VerifiedCallback, channel.VerificationReceipt, error) {
	if handle == nil {
		return channel.VerifiedCallback{}, channel.VerificationReceipt{}, runtime.ErrInvariantViolation
	}
	return handle.Verify(ctx, request, a.Protocol.Verify)
}

func (a *Adapter) Decode(_ context.Context, callback channel.VerifiedCallback) ([]channel.ProviderEvent, error) {
	decoded, err := protocol.DecodeMessage(callback)
	if err != nil {
		return nil, err
	}
	if decoded.Ignored {
		return nil, nil
	}
	return []channel.ProviderEvent{decoded.Event}, nil
}

func (a *Adapter) Deliver(ctx context.Context, request channel.DeliveryRequest) (channel.DeliveryResult, error) {
	if a.Sender == nil || request.Event.TenantID == "" || request.Event.ChannelBindingID == "" || request.Target.Channel != "feishu" ||
		request.Target.ExternalMessageID == "" || len(request.Content) == 0 || request.ClientRequestID == "" {
		return channel.DeliveryResult{}, runtime.ErrInvariantViolation
	}
	sum := sha256.Sum256(request.Content)
	if request.ContentDigest != hex.EncodeToString(sum[:]) {
		return channel.DeliveryResult{}, runtime.ErrVersionMismatch
	}
	destination := channel.ReplyDestination{TenantID: request.Event.TenantID, Channel: request.Target.Channel,
		ChannelBindingID: request.Event.ChannelBindingID, ExternalAccountID: request.Target.ExternalAccountID,
		ConfigVersion: request.Event.ConfigVersion}
	contentType, err := messaging.NormalizeContentType(request.ContentType)
	if err != nil {
		return channel.DeliveryResult{}, channel.PermanentDeliveryError{Err: err, Class: "content_type_unsupported"}
	}
	var messageID string
	switch contentType {
	case messaging.ContentTypeText:
		messageID, err = a.Sender.ReplyText(ctx, destination, request.Target.ExternalMessageID, string(request.Content), request.ClientRequestID)
	case messaging.ContentTypeCard:
		sender, ok := a.Sender.(CardSender)
		if !ok {
			return channel.DeliveryResult{}, channel.PermanentDeliveryError{Err: runtime.ErrCapabilityUnsupported, Class: "content_type_unsupported"}
		}
		messageID, err = sender.ReplyCard(ctx, destination, request.Target.ExternalMessageID, request.Content, request.ClientRequestID)
	default:
		sender, ok := a.Sender.(ImageSender)
		if !ok {
			return channel.DeliveryResult{}, channel.PermanentDeliveryError{Err: runtime.ErrCapabilityUnsupported, Class: "content_type_unsupported"}
		}
		messageID, err = sender.ReplyImage(ctx, destination, request.Target.ExternalMessageID, request.Content, contentType, request.ClientRequestID)
	}
	if err != nil {
		return channel.DeliveryResult{}, err
	}
	if messageID == "" {
		return channel.DeliveryResult{}, channel.AmbiguousDeliveryError{Err: runtime.ErrBackendUnavailable}
	}
	return channel.DeliveryResult{ProviderMessageID: messageID, Delivered: true}, nil
}

func (a *Adapter) Capabilities() channel.Capabilities {
	_, card := a.Sender.(CardSender)
	_, image := a.Sender.(ImageSender)
	return channel.Capabilities{Text: true, Card: card, Image: image}
}

type ClientCredentials struct {
	AppID, AppSecret string
	Version          int64
}

// CredentialsResolver must resolve app credentials under channel_send scope.
type CredentialsResolver interface {
	ResolveFeishuSendCredentials(context.Context, channel.ReplyDestination) (ClientCredentials, error)
}

// ClientProvider returns a shared SDK client and owns its credential generation.
// The pinned SDK currently has a process-global token cache, but rebuilding a
// client per delivery still loses explicit lifecycle and adds avoidable work.
type ClientProvider interface {
	ResolveFeishuClient(context.Context, channel.ReplyDestination) (*lark.Client, error)
}

type cachedClient struct {
	appID         string
	configVersion int64
	secretVersion int64
	client        *lark.Client
}

// ClientCache keeps one SDK client per binding and credential generation.
// Credential rotation replaces the old entry instead of growing the cache.
type ClientCache struct {
	Credentials CredentialsResolver
	NewClient   func(string, string) *lark.Client

	mu      sync.Mutex
	clients map[string]cachedClient
}

func (c *ClientCache) ResolveFeishuClient(ctx context.Context, destination channel.ReplyDestination) (*lark.Client, error) {
	if c == nil || c.Credentials == nil || destination.TenantID == "" || destination.ChannelBindingID == "" || destination.ExternalAccountID == "" || destination.ConfigVersion < 1 {
		return nil, runtime.ErrInvariantViolation
	}
	credentials, err := c.Credentials.ResolveFeishuSendCredentials(ctx, destination)
	if err != nil {
		return nil, err
	}
	if credentials.AppID == "" || credentials.AppID != destination.ExternalAccountID || credentials.AppSecret == "" || credentials.Version < 1 {
		return nil, runtime.ErrVersionMismatch
	}
	key := destination.TenantID + "\x00" + destination.ChannelBindingID + "\x00" + destination.ExternalAccountID
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.clients[key]; ok {
		if existing.configVersion == destination.ConfigVersion && existing.secretVersion == credentials.Version &&
			existing.appID == credentials.AppID && existing.client != nil {
			return existing.client, nil
		}
	}
	newClient := c.NewClient
	if newClient == nil {
		newClient = func(id, secret string) *lark.Client { return lark.NewClient(id, secret) }
	}
	client := newClient(credentials.AppID, credentials.AppSecret)
	if client == nil {
		return nil, runtime.ErrBackendUnavailable
	}
	if c.clients == nil {
		c.clients = make(map[string]cachedClient)
	}
	c.clients[key] = cachedClient{appID: credentials.AppID, configVersion: destination.ConfigVersion,
		secretVersion: credentials.Version, client: client}
	return client, nil
}

type OfficialSender struct {
	Clients ClientProvider
	Tokens  MediaAccessTokenProvider
	Client  HTTPClient
	BaseURL string
}

func (s OfficialSender) ReplyText(ctx context.Context, destination channel.ReplyDestination, replyMessageID, text, uuid string) (string, error) {
	if s.Clients == nil || destination.TenantID == "" || destination.ChannelBindingID == "" || replyMessageID == "" || text == "" || uuid == "" {
		return "", runtime.ErrInvariantViolation
	}
	content, _ := json.Marshal(struct {
		Text string `json:"text"`
	}{Text: text})
	return s.reply(ctx, destination, replyMessageID, "text", content, uuid)
}

func (s OfficialSender) ReplyCard(ctx context.Context, destination channel.ReplyDestination, replyMessageID string, card []byte, uuid string) (string, error) {
	if !json.Valid(card) {
		return "", channel.PermanentDeliveryError{Err: runtime.ErrInvalidEnvelope, Class: "invalid_card"}
	}
	return s.reply(ctx, destination, replyMessageID, "interactive", card, uuid)
}

func (s OfficialSender) reply(ctx context.Context, destination channel.ReplyDestination, replyMessageID, messageType string, content []byte, uuid string) (string, error) {
	if s.Clients == nil || destination.TenantID == "" || destination.ChannelBindingID == "" || replyMessageID == "" || len(content) == 0 || uuid == "" {
		return "", runtime.ErrInvariantViolation
	}
	client, err := s.Clients.ResolveFeishuClient(ctx, destination)
	if err != nil {
		return "", err
	}
	if client == nil {
		return "", runtime.ErrVersionMismatch
	}
	body := larkim.NewReplyMessageReqBodyBuilder().Content(string(content)).MsgType(messageType).Uuid(uuid).Build()
	req := larkim.NewReplyMessageReqBuilder().MessageId(replyMessageID).Body(body).Build()
	resp, err := client.Im.Message.Reply(ctx, req)
	if err != nil {
		return "", channel.AmbiguousDeliveryError{Err: err}
	}
	if resp == nil {
		return "", channel.AmbiguousDeliveryError{Err: runtime.ErrBackendUnavailable}
	}
	if !resp.Success() {
		status := 0
		if resp.ApiResp != nil {
			status = resp.ApiResp.StatusCode
		}
		providerErr := fmt.Errorf("feishu reply code=%d status=%d: %s", resp.Code, status, resp.Msg)
		switch {
		case resp.Code == 99991400 || resp.Code == 99991401 || status == 429:
			delay := time.Second
			if resp.ApiResp != nil {
				if seconds, parseErr := strconv.Atoi(resp.ApiResp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
					delay = time.Duration(seconds) * time.Second
				}
			}
			return "", channel.RetryableDeliveryError{Err: providerErr, RetryAfter: delay}
		case resp.Code == 99991663 || resp.Code == 99991664 || resp.Code == 99991671 || status == 408 || status >= 500:
			return "", channel.RetryableDeliveryError{Err: providerErr}
		default:
			return "", channel.PermanentDeliveryError{Err: providerErr, Class: "provider_rejected"}
		}
	}
	if resp.Data == nil || resp.Data.MessageId == nil || *resp.Data.MessageId == "" {
		return "", channel.AmbiguousDeliveryError{Err: runtime.ErrBackendUnavailable}
	}
	return *resp.Data.MessageId, nil
}

func (s OfficialSender) ReplyImage(ctx context.Context, destination channel.ReplyDestination, replyMessageID string, image []byte, contentType, uuid string) (string, error) {
	if s.Tokens == nil || replyMessageID == "" || len(image) == 0 || !strings.HasPrefix(contentType, "image/") || uuid == "" {
		return "", runtime.ErrInvariantViolation
	}
	for attempt := 0; attempt < 2; attempt++ {
		token, err := s.Tokens.ResolveFeishuMediaAccessToken(ctx, destination, attempt == 1)
		if err != nil {
			return "", err
		}
		key, invalid, err := s.uploadImage(ctx, token, image, contentType)
		if invalid && attempt == 0 {
			continue
		}
		if err != nil {
			return "", err
		}
		content, _ := json.Marshal(map[string]string{"image_key": key})
		return s.reply(ctx, destination, replyMessageID, "image", content, uuid)
	}
	return "", channel.RetryableDeliveryError{Err: runtime.ErrBackendUnavailable}
}

func (s OfficialSender) uploadImage(ctx context.Context, token string, image []byte, contentType string) (string, bool, error) {
	base := strings.TrimRight(s.BaseURL, "/")
	if base == "" {
		base = defaultFeishuAPIBaseURL
	}
	endpoint, err := url.Parse(base + "/open-apis/im/v1/images")
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return "", false, runtime.ErrInvariantViolation
	}
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err = writer.WriteField("image_type", "message"); err != nil {
		return "", false, err
	}
	part, err := writer.CreateFormFile("image", "reply."+strings.TrimPrefix(contentType, "image/"))
	if err != nil {
		return "", false, err
	}
	if _, err = part.Write(image); err != nil {
		return "", false, err
	}
	if err = writer.Close(); err != nil {
		return "", false, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), &body)
	if err != nil {
		return "", false, runtime.ErrInvariantViolation
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	client := s.Client
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return "", false, ctx.Err()
		}
		return "", false, channel.AmbiguousDeliveryError{Err: runtime.ErrBackendUnavailable}
	}
	if response == nil || response.Body == nil {
		return "", false, channel.AmbiguousDeliveryError{Err: runtime.ErrBackendUnavailable}
	}
	defer response.Body.Close()
	var value struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			ImageKey string `json:"image_key"`
		} `json:"data"`
	}
	if err = json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&value); err != nil {
		return "", false, channel.AmbiguousDeliveryError{Err: runtime.ErrInvalidEnvelope}
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 && value.Code == 0 && value.Data.ImageKey != "" {
		return value.Data.ImageKey, false, nil
	}
	providerErr := fmt.Errorf("feishu image upload code=%d status=%d: %s", value.Code, response.StatusCode, value.Msg)
	invalid := value.Code == 99991663 || value.Code == 99991664 || value.Code == 99991671 || response.StatusCode == http.StatusUnauthorized
	if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 || value.Code == 99991400 || value.Code == 99991401 {
		return "", invalid, channel.RetryableDeliveryError{Err: providerErr, RetryAfter: retryAfterSeconds(response.Header)}
	}
	if invalid {
		return "", true, channel.RetryableDeliveryError{Err: providerErr}
	}
	return "", false, channel.PermanentDeliveryError{Err: providerErr, Class: "provider_rejected"}
}

func retryAfterSeconds(headers http.Header) time.Duration {
	seconds, err := strconv.Atoi(headers.Get("Retry-After"))
	if err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Second
}

func mapValue(values map[string]string, name string) string {
	for key, value := range values {
		if strings.EqualFold(key, name) {
			return value
		}
	}
	return ""
}

var (
	_ channel.Adapter     = (*Adapter)(nil)
	_ channel.HTTPAdapter = (*Adapter)(nil)
)
