package wecom

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	"github.com/XnLemon/trpc-agent-service/trpcservice/outbox"
	storage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

// Provider delivers replies through a WeCom self-built application.
// Access tokens are cached in memory and are never persisted or logged.
type Provider struct {
	CorpID      string
	AgentID     string
	AppSecret   string
	HTTPClient  *http.Client
	BaseURL     string
	Now         func() time.Time
	Attachments attachment.Reader
	mu          sync.Mutex
	token       string
	tokenExpiry time.Time
	receipts    map[string]string
}

var _ outbox.Provider = (*Provider)(nil)

const maximumTextBytes = 2048

// HTTPMediaDownloader fetches verified WeCom media through the official API
// and keeps per-binding access-token caches in memory.
type HTTPMediaDownloader struct {
	HTTPClient *http.Client
	BaseURL    string
	Now        func() time.Time

	mu        sync.Mutex
	providers map[string]*Provider
}

var _ MediaDownloader = (*HTTPMediaDownloader)(nil)

// Download resolves a short-lived access token from the verified Binding
// context and returns the provider response body for the handler to validate
// and persist.
func (downloader *HTTPMediaDownloader) Download(ctx context.Context, request MediaDownloadRequest) (io.ReadCloser, error) {
	request, err := normalizeMediaDownloadRequest(request)
	if err != nil || downloader == nil || ctx == nil {
		return nil, ErrAttachment
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return downloader.provider(request).downloadMedia(ctx, request)
}

func (downloader *HTTPMediaDownloader) provider(request MediaDownloadRequest) *Provider {
	key := request.TenantID + "\x00" + request.BindingID + "\x00" + request.CorpID + "\x00" + request.AgentID
	downloader.mu.Lock()
	defer downloader.mu.Unlock()
	if downloader.providers == nil {
		downloader.providers = make(map[string]*Provider)
	}
	if provider := downloader.providers[key]; provider != nil && provider.AppSecret == request.AppSecret {
		return provider
	}
	provider := &Provider{
		CorpID: request.CorpID, AgentID: request.AgentID, AppSecret: request.AppSecret,
		HTTPClient: downloader.HTTPClient, BaseURL: downloader.BaseURL, Now: downloader.Now,
	}
	downloader.providers[key] = provider
	return provider
}

func normalizeMediaDownloadRequest(request MediaDownloadRequest) (MediaDownloadRequest, error) {
	request.TenantID = strings.TrimSpace(request.TenantID)
	request.BindingID = strings.TrimSpace(request.BindingID)
	request.CorpID = strings.TrimSpace(request.CorpID)
	request.AgentID = strings.TrimSpace(request.AgentID)
	request.AppSecret = strings.TrimSpace(request.AppSecret)
	request.MediaID = strings.TrimSpace(request.MediaID)
	request.MIMEType = strings.ToLower(strings.TrimSpace(request.MIMEType))
	if request.MaximumBytes == 0 {
		request.MaximumBytes = defaultAttachmentBytes
	}
	if request.TenantID == "" || request.BindingID == "" || request.CorpID == "" || request.AgentID == "" || request.AppSecret == "" || request.MediaID == "" || request.MaximumBytes < 1 || request.MaximumBytes > maximumAttachmentBytes {
		return MediaDownloadRequest{}, ErrAttachment
	}
	if request.Kind != attachment.KindImage && request.Kind != attachment.KindDocument && request.Kind != attachment.KindAudio && request.Kind != attachment.KindVideo {
		return MediaDownloadRequest{}, ErrAttachment
	}
	for _, value := range []string{request.TenantID, request.BindingID, request.CorpID, request.AgentID, request.AppSecret, request.MediaID, request.MIMEType} {
		if hasControl(value) {
			return MediaDownloadRequest{}, ErrAttachment
		}
	}
	return request, nil
}

// Deliver sends one durable reply segment through the WeCom application API.
//
//nolint:gocyclo
func (p *Provider) Deliver(ctx context.Context, value storage.ReplyOutbox) (string, error) {
	if p == nil || strings.TrimSpace(p.CorpID) == "" || strings.TrimSpace(p.AgentID) == "" || strings.TrimSpace(p.AppSecret) == "" || ctx == nil {
		return "", &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	normalized, err := normalizeDeliveryReply(value)
	if err != nil {
		return "", err
	}
	value = normalized
	if (value.ReplyTarget.ConversationKind != "direct" && value.ReplyTarget.ConversationKind != "group") || value.ReplyTarget.ReceiverID == "" {
		return "", &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	agentID, parseErr := strconv.Atoi(strings.TrimSpace(p.AgentID))
	if parseErr != nil || agentID <= 0 || strconv.Itoa(agentID) != strings.TrimSpace(p.AgentID) {
		return "", &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	nativeMedia := nativeWeComMedia(value.Kind) && p.Attachments != nil
	text := ""
	if !nativeMedia {
		text, err = deliveryText(value)
		if err != nil {
			return "", err
		}
		if len([]byte(text)) == 0 || len([]byte(text)) > maximumTextBytes {
			return "", &outbox.DeliveryError{Class: "invalid", Retryable: false}
		}
	}
	key := deliveryKey(value)
	p.mu.Lock()
	if value.ReplyID != "" && p.receipts != nil {
		if receipt := p.receipts[key]; receipt != "" {
			p.mu.Unlock()
			return receipt, nil
		}
	}
	p.mu.Unlock()
	token, err := p.accessToken(ctx)
	if err != nil {
		return "", err
	}
	var receipt string
	if nativeMedia {
		receipt, err = p.deliverMedia(ctx, token, agentID, value)
	} else {
		receipt, err = p.deliverText(ctx, token, agentID, value, text)
	}
	if err != nil {
		return "", err
	}
	p.mu.Lock()
	if value.ReplyID != "" {
		if p.receipts == nil {
			p.receipts = make(map[string]string)
		}
		p.receipts[key] = receipt
	}
	p.mu.Unlock()
	return receipt, nil
}

type wecomSendPayload struct {
	ToUser  string             `json:"touser,omitempty"`
	ChatID  string             `json:"chatid,omitempty"`
	MsgType string             `json:"msgtype"`
	AgentID int                `json:"agentid"`
	Text    *wecomTextPayload  `json:"text,omitempty"`
	Image   *wecomMediaPayload `json:"image,omitempty"`
	File    *wecomMediaPayload `json:"file,omitempty"`
	Safe    int                `json:"safe"`
}

type wecomTextPayload struct {
	Content string `json:"content"`
}

type wecomMediaPayload struct {
	MediaID string `json:"media_id"`
}

func normalizeDeliveryReply(value storage.ReplyOutbox) (storage.ReplyOutbox, error) {
	normalized, err := storage.NormalizeReplyOutbox(value)
	if err != nil {
		return storage.ReplyOutbox{}, &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	return normalized, nil
}

func (p *Provider) deliverText(ctx context.Context, token string, agentID int, value storage.ReplyOutbox, text string) (string, error) {
	payload := newSendPayload(value, agentID, "text")
	payload.Text = &wecomTextPayload{Content: text}
	return p.sendMessage(ctx, token, payload)
}

func (p *Provider) deliverMedia(ctx context.Context, token string, agentID int, value storage.ReplyOutbox) (string, error) {
	content, err := p.Attachments.Load(ctx, value.TenantID, value.EventID, value.Attachment)
	if err != nil {
		return "", attachmentLoadError(ctx, err)
	}
	if err := content.Validate(value.Attachment); err != nil {
		return "", &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	uploadType, msgType := wecomMediaTypes(value.Kind)
	mediaID, err := p.uploadTempMedia(ctx, token, uploadType, value.Attachment.Name, content.Data)
	if err != nil {
		return "", err
	}
	payload := newSendPayload(value, agentID, msgType)
	switch msgType {
	case "image":
		payload.Image = &wecomMediaPayload{MediaID: mediaID}
	case "file":
		payload.File = &wecomMediaPayload{MediaID: mediaID}
	default:
		return "", &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	return p.sendMessage(ctx, token, payload)
}

func nativeWeComMedia(kind storage.ReplyKind) bool {
	return kind == storage.ReplyKindImage || kind == storage.ReplyKindDocument
}

func wecomMediaTypes(kind storage.ReplyKind) (string, string) {
	switch kind {
	case storage.ReplyKindImage:
		return "image", "image"
	case storage.ReplyKindDocument:
		return "file", "file"
	default:
		return "", ""
	}
}

func newSendPayload(value storage.ReplyOutbox, agentID int, msgType string) wecomSendPayload {
	payload := wecomSendPayload{MsgType: msgType, AgentID: agentID, Safe: 0}
	if value.ReplyTarget.ConversationKind == "group" {
		payload.ChatID = value.ReplyTarget.ReceiverID
	} else {
		payload.ToUser = value.ReplyTarget.ReceiverID
	}
	return payload
}

func (p *Provider) uploadTempMedia(ctx context.Context, token, mediaType, name string, data []byte) (string, error) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("media", attachmentFileName(name))
	if err != nil {
		return "", &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	if _, err := part.Write(data); err != nil {
		return "", &outbox.DeliveryError{Class: "provider_error", Retryable: true}
	}
	if err := writer.Close(); err != nil {
		return "", &outbox.DeliveryError{Class: "provider_error", Retryable: true}
	}
	endpoint := p.baseURL() + "/cgi-bin/media/upload?access_token=" + url.QueryEscape(token) + "&type=" + url.QueryEscape(mediaType)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, &body)
	if err != nil {
		return "", &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	request.Header.Set("Content-Type", writer.FormDataContentType())
	response, err := p.client().Do(request)
	if err != nil {
		return "", transportDeliveryError(err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", &outbox.DeliveryError{Class: "unavailable", Retryable: true}
	}
	var result struct {
		ErrCode int    `json:"errcode"`
		MediaID string `json:"media_id"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&result); err != nil {
		return "", &outbox.DeliveryError{Class: "provider_error", Retryable: true}
	}
	if result.ErrCode != 0 {
		return "", p.providerResultError(result.ErrCode, response.StatusCode)
	}
	if result.MediaID == "" {
		return "", &outbox.DeliveryError{Class: "provider_error", Retryable: true}
	}
	return result.MediaID, nil
}

func (p *Provider) downloadMedia(ctx context.Context, download MediaDownloadRequest) (io.ReadCloser, error) {
	if p == nil || ctx == nil || strings.TrimSpace(download.MediaID) == "" || hasControl(download.MediaID) {
		return nil, ErrAttachment
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	token, err := p.accessToken(ctx)
	if err != nil {
		return nil, wecomAttachmentError(ctx)
	}
	endpoint := p.baseURL() + "/cgi-bin/media/get?access_token=" + url.QueryEscape(token) + "&media_id=" + url.QueryEscape(download.MediaID)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, ErrAttachment
	}
	response, err := p.client().Do(request)
	if err != nil {
		return nil, wecomAttachmentError(ctx)
	}
	data, err := readWeComMediaResponse(ctx, response, download)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func readWeComMediaResponse(ctx context.Context, response *http.Response, download MediaDownloadRequest) ([]byte, error) {
	if response == nil || response.Body == nil {
		return nil, ErrAttachment
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, ErrAttachment
	}
	contentType := strings.ToLower(strings.TrimSpace(response.Header.Get("Content-Type")))
	data, err := io.ReadAll(io.LimitReader(response.Body, download.MaximumBytes+1))
	if err != nil {
		return nil, wecomAttachmentError(ctx)
	}
	if int64(len(data)) == 0 || int64(len(data)) > download.MaximumBytes || providerJSONError(contentType, data) || !downloadContentTypeMatches(download.Kind, contentType) {
		return nil, ErrAttachment
	}
	return data, nil
}

func wecomAttachmentError(ctx context.Context) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return ErrAttachment
}

func providerJSONError(contentType string, _ []byte) bool {
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		mediaType = strings.ToLower(strings.TrimSpace(contentType))
	}
	return mediaType == "application/json" || strings.HasSuffix(mediaType, "+json")
}

func downloadContentTypeMatches(kind attachment.Kind, contentType string) bool {
	mediaType, _, err := mime.ParseMediaType(contentType)
	if strings.TrimSpace(contentType) == "" || err != nil || mediaType == "application/octet-stream" {
		return true
	}
	switch kind {
	case attachment.KindImage:
		return strings.HasPrefix(mediaType, "image/")
	case attachment.KindVideo:
		return strings.HasPrefix(mediaType, "video/")
	case attachment.KindAudio:
		return strings.HasPrefix(mediaType, "audio/")
	case attachment.KindDocument:
		return !strings.HasPrefix(mediaType, "image/") && !strings.HasPrefix(mediaType, "video/") && !strings.HasPrefix(mediaType, "audio/")
	default:
		return false
	}
}

func (p *Provider) sendMessage(ctx context.Context, token string, payload wecomSendPayload) (string, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return "", &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	endpoint := p.baseURL() + "/cgi-bin/message/send?access_token=" + url.QueryEscape(token)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return "", &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := p.client().Do(request)
	if err != nil {
		return "", transportDeliveryError(err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", &outbox.DeliveryError{Class: "unavailable", Retryable: true}
	}
	var result struct {
		ErrCode int    `json:"errcode"`
		MsgID   string `json:"msgid"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&result); err != nil {
		return "", &outbox.DeliveryError{Class: "provider_error", Retryable: true}
	}
	if result.ErrCode != 0 {
		return "", p.providerResultError(result.ErrCode, response.StatusCode)
	}
	if result.MsgID == "" {
		return "", &outbox.DeliveryError{Class: "provider_error", Retryable: true}
	}
	return result.MsgID, nil
}

func attachmentFileName(name string) string {
	if strings.TrimSpace(name) == "" {
		return "attachment"
	}
	return name
}

func attachmentLoadError(ctx context.Context, err error) error {
	if contextErr := ctx.Err(); contextErr != nil {
		return transportDeliveryError(contextErr)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return transportDeliveryError(err)
	}
	if errors.Is(err, storage.ErrNotFound) || errors.Is(err, attachment.ErrInvalid) {
		return &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	return &outbox.DeliveryError{Class: "unavailable", Retryable: true}
}

func transportDeliveryError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return &outbox.DeliveryError{Class: "timeout", Retryable: true}
	}
	if errors.Is(err, context.Canceled) {
		return &outbox.DeliveryError{Class: "canceled", Retryable: true}
	}
	return &outbox.DeliveryError{Class: "unavailable", Retryable: true}
}

func (p *Provider) providerResultError(code, status int) error {
	class, retryable := classifyWeCom(code, status)
	if class == "unauthenticated" {
		p.clearToken()
	}
	return &outbox.DeliveryError{Class: class, Retryable: retryable}
}

func (p *Provider) clearToken() {
	p.mu.Lock()
	p.token = ""
	p.tokenExpiry = time.Time{}
	p.mu.Unlock()
}

func deliveryText(value storage.ReplyOutbox) (string, error) {
	if value.Kind == "" || value.Kind == storage.ReplyKindText {
		return value.Payload, nil
	}
	normalized, err := storage.NormalizeReplyOutbox(value)
	if err != nil {
		return "", &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	return normalized.Fallback, nil
}

// Reconcile reports unknown because WeCom does not expose a stable receipt query for app text sends.
func (p *Provider) Reconcile(_ context.Context, value storage.ReplyOutbox) (outbox.DeliveryStatus, string, error) {
	if p == nil {
		return outbox.DeliveryUnknown, "", nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if receipt := p.receipts[deliveryKey(value)]; receipt != "" {
		return outbox.DeliveryAccepted, receipt, nil
	}
	return outbox.DeliveryUnknown, "", nil
}

func deliveryKey(value storage.ReplyOutbox) string {
	return value.TenantID + "\x00" + value.ReplyID + "\x00" + strconv.Itoa(value.SegmentIndex)
}

func (p *Provider) accessToken(ctx context.Context) (string, error) {
	now := time.Now().UTC()
	if p.Now != nil {
		now = p.Now().UTC()
	}
	p.mu.Lock()
	if p.token != "" && now.Before(p.tokenExpiry) {
		token := p.token
		p.mu.Unlock()
		return token, nil
	}
	p.mu.Unlock()
	endpoint := p.baseURL() + "/cgi-bin/gettoken?corpid=" + url.QueryEscape(p.CorpID) + "&corpsecret=" + url.QueryEscape(p.AppSecret)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	response, err := p.client().Do(request)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return "", &outbox.DeliveryError{Class: "timeout", Retryable: true}
		}
		return "", &outbox.DeliveryError{Class: "unavailable", Retryable: true}
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", &outbox.DeliveryError{Class: "unavailable", Retryable: true}
	}
	var result struct {
		ErrCode     int    `json:"errcode"`
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&result); err != nil {
		return "", &outbox.DeliveryError{Class: "provider_error", Retryable: true}
	}
	if result.ErrCode != 0 || result.AccessToken == "" {
		class, retryable := classifyWeCom(result.ErrCode, response.StatusCode)
		return "", &outbox.DeliveryError{Class: class, Retryable: retryable}
	}
	expires := now.Add(time.Duration(result.ExpiresIn) * time.Second)
	if result.ExpiresIn <= 60 {
		expires = now.Add(30 * time.Second)
	} else {
		expires = expires.Add(-30 * time.Second)
	}
	p.mu.Lock()
	p.token, p.tokenExpiry = result.AccessToken, expires
	p.mu.Unlock()
	return result.AccessToken, nil
}

func (p *Provider) baseURL() string {
	if strings.TrimSpace(p.BaseURL) != "" {
		return strings.TrimRight(p.BaseURL, "/")
	}
	return "https://qyapi.weixin.qq.com"
}
func (p *Provider) client() *http.Client {
	if p.HTTPClient != nil {
		return p.HTTPClient
	}
	return http.DefaultClient
}
func classifyWeCom(code, status int) (string, bool) {
	switch {
	case code == 40014 || code == 42001:
		return "unauthenticated", true
	case code == 45009 || status == http.StatusTooManyRequests:
		return "rate_limited", true
	case code == 0 && status >= 500:
		return "unavailable", true
	case code != 0:
		return "provider_error", false
	default:
		return "provider_error", true
	}
}
