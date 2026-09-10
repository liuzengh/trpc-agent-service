// Package wecom implements the service-owned WeCom Channel Adapter.
package wecom

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
	"time"
	"unicode/utf8"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom/protocol"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

const (
	routeKeyQuery       = "route_key"
	defaultAPIBaseURL   = "https://qyapi.weixin.qq.com"
	maxTextContentBytes = 2048
)

type TextSender interface {
	SendText(context.Context, channel.ReplyDestination, string, string, string) (string, error)
}
type ImageSender interface {
	SendImage(context.Context, channel.ReplyDestination, string, []byte, string, string) (string, error)
}

type Adapter struct {
	Protocol protocol.Verifier
	Sender   TextSender
}

func (a *Adapter) ID() string { return "wecom" }

func (a *Adapter) Run(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (a *Adapter) PublicRoute(_ context.Context, request channel.CallbackRequest) (channel.PublicRouteHint, error) {
	routeKey := mapValue(request.Query, routeKeyQuery)
	timestamp := mapValue(request.Query, "timestamp")
	nonce := mapValue(request.Query, "nonce")
	signature := mapValue(request.Query, "msg_signature")
	if routeKey == "" || timestamp == "" || nonce == "" || signature == "" || len(request.Body) == 0 {
		return channel.PublicRouteHint{}, runtime.ErrInvalidEnvelope
	}
	attempt := sha256.Sum256(append([]byte(timestamp+"\x00"+nonce+"\x00"+signature+"\x00"), request.Body...))
	return channel.PublicRouteHint{Channel: a.ID(), ExternalAccountHint: routeKey,
		RouteKeyDigest: protocol.RouteKeyDigest(routeKey), IngressAttemptID: hex.EncodeToString(attempt[:16])}, nil
}

func (a *Adapter) IsChallenge(request channel.CallbackRequest) bool {
	return protocol.IsChallengeRequest(request)
}

func (a *Adapter) PublicChallengeRoute(_ context.Context, request channel.CallbackRequest) (channel.PublicRouteHint, error) {
	routeKey := mapValue(request.Query, routeKeyQuery)
	timestamp := mapValue(request.Query, "timestamp")
	nonce := mapValue(request.Query, "nonce")
	signature := mapValue(request.Query, "msg_signature")
	echo := mapValue(request.Query, "echostr")
	if routeKey == "" || timestamp == "" || nonce == "" || signature == "" || echo == "" {
		return channel.PublicRouteHint{}, runtime.ErrInvalidEnvelope
	}
	attempt := sha256.Sum256([]byte("wecom-challenge\x00" + timestamp + "\x00" + nonce + "\x00" + signature + "\x00" + echo))
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
	return channel.HTTPResponse{ContentType: "text/plain; charset=utf-8", Body: []byte("success")}
}

func (a *Adapter) Verify(ctx context.Context, request channel.CallbackRequest, handle channel.ScopedVerifierHandle) (channel.VerifiedCallback, channel.VerificationReceipt, error) {
	if handle == nil {
		return channel.VerifiedCallback{}, channel.VerificationReceipt{}, runtime.ErrInvariantViolation
	}
	return handle.Verify(ctx, request, a.Protocol.Verify)
}

func (a *Adapter) Decode(_ context.Context, callback channel.VerifiedCallback) ([]channel.ProviderEvent, error) {
	event, err := protocol.DecodeMessage(callback)
	if err != nil {
		return nil, err
	}
	return []channel.ProviderEvent{event}, nil
}

func (a *Adapter) Deliver(ctx context.Context, request channel.DeliveryRequest) (channel.DeliveryResult, error) {
	if a.Sender == nil || request.Event.TenantID == "" || request.Event.ChannelBindingID == "" ||
		request.Target.Channel != "wecom" || request.Target.ExternalAccountID == "" || request.Target.ExternalUserID == "" ||
		len(request.Content) == 0 || request.ClientRequestID == "" {
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
	if contentType == messaging.ContentTypeText {
		messageID, err = a.Sender.SendText(ctx, destination, request.Target.ExternalUserID, string(request.Content), request.ClientRequestID)
	} else {
		sender, ok := a.Sender.(ImageSender)
		if !ok || !strings.HasPrefix(contentType, "image/") {
			return channel.DeliveryResult{}, channel.PermanentDeliveryError{Err: runtime.ErrCapabilityUnsupported, Class: "content_type_unsupported"}
		}
		messageID, err = sender.SendImage(ctx, destination, request.Target.ExternalUserID, request.Content, contentType, request.ClientRequestID)
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
	_, image := a.Sender.(ImageSender)
	return channel.Capabilities{Text: true, Image: image}
}

// MaxTextBytes reflects the provider's per-message text content limit. The
// delivery service uses this value to segment a terminal reply durably.
func (*Adapter) MaxTextBytes() int { return maxTextContentBytes }

// AccessTokenProvider owns credential resolution and the shared token cache.
// forceRefresh is true only after WeCom explicitly rejects the first token.
type AccessTokenProvider interface {
	ResolveWeComAccessToken(context.Context, channel.ReplyDestination, bool) (accessToken string, agentID int64, err error)
}

type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

type OfficialSender struct {
	Tokens  AccessTokenProvider
	Client  HTTPClient
	BaseURL string
}

func (s OfficialSender) SendText(ctx context.Context, destination channel.ReplyDestination, externalUserID, text, clientRequestID string) (string, error) {
	if s.Tokens == nil || destination.TenantID == "" || destination.ChannelBindingID == "" || destination.ExternalAccountID == "" || destination.ConfigVersion < 1 ||
		externalUserID == "" || text == "" || clientRequestID == "" {
		return "", runtime.ErrInvariantViolation
	}
	if !utf8.ValidString(text) || len([]byte(text)) > maxTextContentBytes {
		return "", channel.PermanentDeliveryError{Err: runtime.ErrInvalidEnvelope, Class: "invalid_text"}
	}
	for attempt := 0; attempt < 2; attempt++ {
		token, agentID, err := s.Tokens.ResolveWeComAccessToken(ctx, destination, attempt == 1)
		if err != nil {
			return "", err
		}
		if token == "" || agentID <= 0 {
			return "", runtime.ErrVersionMismatch
		}
		messageID, invalidToken, err := s.send(ctx, token, agentID, externalUserID, "text", text, "")
		if invalidToken && attempt == 0 {
			continue
		}
		return messageID, err
	}
	return "", channel.RetryableDeliveryError{Err: runtime.ErrBackendUnavailable}
}

func (s OfficialSender) SendImage(ctx context.Context, destination channel.ReplyDestination, externalUserID string, image []byte, contentType, clientRequestID string) (string, error) {
	if s.Tokens == nil || destination.TenantID == "" || destination.ChannelBindingID == "" || destination.ExternalAccountID == "" || destination.ConfigVersion < 1 ||
		externalUserID == "" || len(image) == 0 || !strings.HasPrefix(contentType, "image/") || clientRequestID == "" {
		return "", runtime.ErrInvariantViolation
	}
	for attempt := 0; attempt < 2; attempt++ {
		token, agentID, err := s.Tokens.ResolveWeComAccessToken(ctx, destination, attempt == 1)
		if err != nil {
			return "", err
		}
		mediaID, invalidToken, err := s.uploadImage(ctx, token, image, contentType)
		if invalidToken && attempt == 0 {
			continue
		}
		if err != nil {
			return "", err
		}
		messageID, invalidToken, err := s.send(ctx, token, agentID, externalUserID, "image", "", mediaID)
		if invalidToken && attempt == 0 {
			continue
		}
		return messageID, err
	}
	return "", channel.RetryableDeliveryError{Err: runtime.ErrBackendUnavailable}
}

func (s OfficialSender) uploadImage(ctx context.Context, token string, image []byte, contentType string) (string, bool, error) {
	baseURL := strings.TrimRight(s.BaseURL, "/")
	if baseURL == "" {
		baseURL = defaultAPIBaseURL
	}
	endpoint, err := url.Parse(baseURL + "/cgi-bin/media/upload")
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return "", false, runtime.ErrInvariantViolation
	}
	query := endpoint.Query()
	query.Set("access_token", token)
	query.Set("type", "image")
	endpoint.RawQuery = query.Encode()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("media", "reply."+strings.TrimPrefix(contentType, "image/"))
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
	var result struct {
		ErrorCode    int64  `json:"errcode"`
		ErrorMessage string `json:"errmsg"`
		MediaID      string `json:"media_id"`
	}
	if err = json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result); err != nil {
		return "", false, channel.AmbiguousDeliveryError{Err: runtime.ErrInvalidEnvelope}
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 && result.ErrorCode == 0 && result.MediaID != "" {
		return result.MediaID, false, nil
	}
	providerErr := providerError(response.StatusCode, result.ErrorCode, result.ErrorMessage)
	invalid := result.ErrorCode == 40014 || result.ErrorCode == 42001 || result.ErrorCode == 42007 || response.StatusCode == http.StatusUnauthorized
	if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 || result.ErrorCode == -1 || result.ErrorCode == 45009 {
		return "", invalid, channel.RetryableDeliveryError{Err: providerErr, RetryAfter: retryAfter(response.Header)}
	}
	if invalid {
		return "", true, channel.RetryableDeliveryError{Err: providerErr}
	}
	return "", false, channel.PermanentDeliveryError{Err: providerErr, Class: "provider_rejected"}
}

func (s OfficialSender) send(ctx context.Context, token string, agentID int64, externalUserID, messageType, text, mediaID string) (string, bool, error) {
	payload := map[string]any{"touser": externalUserID, "msgtype": messageType, "agentid": agentID, "enable_duplicate_check": 1, "duplicate_check_interval": 1800}
	if messageType == "text" {
		payload["text"] = map[string]string{"content": text}
	} else {
		payload["image"] = map[string]string{"media_id": mediaID}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", false, err
	}
	baseURL := strings.TrimRight(s.BaseURL, "/")
	if baseURL == "" {
		baseURL = defaultAPIBaseURL
	}
	endpoint, err := url.Parse(baseURL + "/cgi-bin/message/send")
	if err != nil {
		return "", false, runtime.ErrInvariantViolation
	}
	query := endpoint.Query()
	query.Set("access_token", token)
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return "", false, err
	}
	request.Header.Set("Content-Type", "application/json")
	client := s.Client
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return "", false, ctx.Err()
		}
		// A net/http url.Error includes the full URL, including access_token.
		// Preserve the ambiguous classification without allowing that secret to
		// escape through logs or a Delivery Ledger error string.
		return "", false, channel.AmbiguousDeliveryError{Err: runtime.ErrBackendUnavailable}
	}
	if response == nil {
		return "", false, channel.AmbiguousDeliveryError{Err: runtime.ErrBackendUnavailable}
	}
	defer response.Body.Close()
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if readErr != nil {
		return "", false, channel.AmbiguousDeliveryError{Err: readErr}
	}
	if response.StatusCode == http.StatusTooManyRequests {
		return "", false, channel.RetryableDeliveryError{Err: providerError(response.StatusCode, 0, "rate limited"), RetryAfter: retryAfter(response.Header)}
	}
	if response.StatusCode >= 500 {
		return "", false, channel.RetryableDeliveryError{Err: providerError(response.StatusCode, 0, "server error")}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", false, channel.PermanentDeliveryError{Err: providerError(response.StatusCode, 0, "http request rejected"), Class: "provider_rejected"}
	}
	var result struct {
		ErrorCode      int64  `json:"errcode"`
		ErrorMessage   string `json:"errmsg"`
		MessageID      string `json:"msgid"`
		InvalidUser    string `json:"invaliduser"`
		UnlicensedUser string `json:"unlicenseduser"`
	}
	decoder := json.NewDecoder(bytes.NewReader(responseBody))
	if err := decoder.Decode(&result); err != nil {
		return "", false, channel.AmbiguousDeliveryError{Err: runtime.ErrInvalidEnvelope}
	}
	if result.ErrorCode == 0 {
		if result.InvalidUser != "" || result.UnlicensedUser != "" {
			return "", false, channel.PermanentDeliveryError{Err: providerError(response.StatusCode, 0, "recipient rejected"), Class: "invalid_recipient"}
		}
		if result.MessageID == "" {
			return "", false, channel.AmbiguousDeliveryError{Err: runtime.ErrBackendUnavailable}
		}
		return result.MessageID, false, nil
	}
	err = providerError(response.StatusCode, result.ErrorCode, result.ErrorMessage)
	switch result.ErrorCode {
	case 40014, 42001, 42007:
		return "", true, channel.RetryableDeliveryError{Err: err}
	case -1, 45002, 45009, 45011:
		return "", false, channel.RetryableDeliveryError{Err: err}
	default:
		return "", false, channel.PermanentDeliveryError{Err: err, Class: "provider_rejected"}
	}
}

func providerError(status int, code int64, message string) error {
	return fmt.Errorf("wecom send status=%d errcode=%d: %s", status, code, message)
}

func retryAfter(headers http.Header) time.Duration {
	seconds, err := strconv.Atoi(headers.Get("Retry-After"))
	if err != nil || seconds <= 0 {
		return time.Second
	}
	return time.Duration(seconds) * time.Second
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
