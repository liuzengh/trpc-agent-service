package feishu

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

const (
	defaultAPIBaseURL = "https://open.feishu.cn"
	maxTextBytes      = 4096
)

// Feishu platform error codes that mean the tenant_access_token is no longer
// accepted and must be refreshed once.
var tokenErrorCodes = map[int]bool{99991661: true, 99991663: true, 99991664: true}

// TokenSource returns and invalidates a cached tenant_access_token.
type TokenSource interface {
	Token(context.Context) (string, error)
	Invalidate(string)
}

// AppTokenSource exchanges an App ID and App Secret for a cached
// tenant_access_token. Secret values and tokens never appear in errors. The
// mutex serializes refreshes, so concurrent sends share one token request.
type AppTokenSource struct {
	AppID, AppSecret string
	BaseURL          string
	Client           *http.Client

	mu      sync.Mutex
	token   string
	expires time.Time
}

// Token implements TokenSource. The cache is refreshed one minute before the
// platform expiry.
func (source *AppTokenSource) Token(ctx context.Context) (string, error) {
	if source == nil || source.AppID == "" || source.AppSecret == "" {
		return "", errors.New("feishu: app id and app secret are required")
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.token != "" && time.Now().Before(source.expires) {
		return source.token, nil
	}
	body, err := json.Marshal(map[string]string{"app_id": source.AppID, "app_secret": source.AppSecret})
	if err != nil {
		return "", errors.New("feishu: create access token request")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, source.baseURL()+"/open-apis/auth/v3/tenant_access_token/internal", bytes.NewReader(body))
	if err != nil {
		return "", errors.New("feishu: create access token request")
	}
	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	response, err := source.client().Do(request)
	if err != nil {
		return "", fmt.Errorf("feishu: request access token: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		return "", fmt.Errorf("feishu: access token HTTP status %d", response.StatusCode)
	}
	var payload struct {
		Code    int    `json:"code"`
		Token   string `json:"tenant_access_token"`
		Expires int    `json:"expire"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&payload); err != nil {
		return "", errors.New("feishu: decode access token response")
	}
	if payload.Code != 0 || payload.Token == "" || payload.Expires <= 0 {
		return "", fmt.Errorf("feishu: access token rejected with code %d", payload.Code)
	}
	source.token = payload.Token
	// Refresh one minute before the platform expiry; a shorter TTL is treated
	// as already expired so a token never dies mid-send.
	ttl := time.Duration(payload.Expires) * time.Second
	margin := time.Minute
	if ttl <= margin {
		margin = ttl
	}
	source.expires = time.Now().Add(ttl - margin)
	return source.token, nil
}

// Invalidate drops only the token that caused an API rejection.
func (source *AppTokenSource) Invalidate(token string) {
	if source == nil {
		return
	}
	source.mu.Lock()
	if source.token == token {
		source.token = ""
		source.expires = time.Time{}
	}
	source.mu.Unlock()
}

func (source *AppTokenSource) client() *http.Client {
	var configured *http.Client
	if source.Client != nil {
		configured = source.Client
	} else {
		configured = &http.Client{Timeout: 10 * time.Second}
	}
	copy := *configured
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &copy
}

func (source *AppTokenSource) baseURL() string {
	if strings.TrimRight(source.BaseURL, "/") != "" {
		return strings.TrimRight(source.BaseURL, "/")
	}
	return defaultAPIBaseURL
}

// APIError preserves provider error classification without including tokens,
// secrets, or response bodies.
type APIError struct {
	Code      int
	Retryable bool
}

func (err *APIError) Error() string { return fmt.Sprintf("feishu: API error code %d", err.Code) }

// DeliveryRetryable exposes the definitive Feishu response classification.
func (err *APIError) DeliveryRetryable() bool { return err != nil && err.Retryable }

// Sender delivers durable Outbox text through the Feishu message API.
type Sender struct {
	Tokens   TokenSource
	BaseURL  string
	Client   *http.Client
	MaxBytes int
	Limiter  channels.SendLimiter
}

// SetDeliveryLimiter installs the shared limiter used before every provider
// API call, including each chunk of a long reply.
func (sender *Sender) SetDeliveryLimiter(limiter channels.SendLimiter) {
	if sender != nil {
		sender.Limiter = limiter
	}
}

// SendText delivers replies with im/v1/messages. Group chats reply to the
// pinned chat_id; direct chats reply to the user open_id. Each chunk respects
// the UTF-8 safe byte limit.
func (sender *Sender) SendText(ctx context.Context, outbound gateway.OutboundMessage) error {
	if sender == nil || sender.Tokens == nil {
		return errors.New("feishu: sender token source is required")
	}
	if outbound.ExternalUserID == "" || strings.TrimSpace(outbound.Text) == "" {
		return errors.New("feishu: outbound external user and text are required")
	}
	chunks, err := SplitText(outbound.Text, sender.maxBytes())
	if err != nil {
		return err
	}
	for index, chunk := range chunks {
		if err := sender.sendChunk(ctx, outbound, chunk); err != nil {
			if index > 0 && !isUncertain(err) {
				return &channels.UncertainError{Cause: fmt.Errorf("feishu: partial chunk delivery: %w", err)}
			}
			return err
		}
	}
	return nil
}

// sendChunk refreshes and retries exactly once when the platform rejects the
// cached token.
func (sender *Sender) sendChunk(ctx context.Context, outbound gateway.OutboundMessage, text string) error {
	for attempt := 0; attempt < 2; attempt++ {
		if sender.Limiter != nil {
			if err := sender.Limiter.Wait(ctx, outbound); err != nil {
				return err
			}
		}
		token, err := sender.Tokens.Token(ctx)
		if err != nil {
			return err
		}
		code, err := sender.post(ctx, token, outbound, text)
		if err != nil {
			return err
		}
		if code == 0 {
			return nil
		}
		if tokenErrorCodes[code] && attempt == 0 {
			sender.Tokens.Invalidate(token)
			continue
		}
		return &APIError{Code: code, Retryable: isRetryableCode(code)}
	}
	return errors.New("feishu: exhausted token refresh")
}

func (sender *Sender) post(ctx context.Context, token string, outbound gateway.OutboundMessage, text string) (int, error) {
	receiveIDType, receiveID := "open_id", outbound.ExternalUserID
	if outbound.ConversationID != "" {
		receiveIDType, receiveID = "chat_id", outbound.ConversationID
	}
	messageType := "text"
	var content []byte
	var err error
	if outbound.ReplyFormat == "card" {
		messageType = "interactive"
		content, err = json.Marshal(map[string]any{
			"config": map[string]bool{"wide_screen_mode": true},
			"elements": []any{map[string]any{
				"tag":  "div",
				"text": map[string]string{"tag": "lark_md", "content": text},
			}},
		})
	} else {
		content, err = json.Marshal(map[string]string{"text": text})
	}
	if err != nil {
		return 0, err
	}
	body, err := json.Marshal(map[string]any{
		"receive_id": receiveID,
		"msg_type":   messageType,
		"content":    string(content),
	})
	if err != nil {
		return 0, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, sender.baseURL()+"/open-apis/im/v1/messages?receive_id_type="+receiveIDType, bytes.NewReader(body))
	if err != nil {
		return 0, errors.New("feishu: create send request")
	}
	// The token travels in the Authorization header and is never formatted
	// into errors, logs, or URLs.
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	response, err := sender.client().Do(request)
	if err != nil {
		return 0, &channels.UncertainError{Cause: fmt.Errorf("feishu: send message: %w", err)}
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		return 0, &APIError{Code: response.StatusCode, Retryable: response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500}
	}
	var result struct {
		Code int `json:"code"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result); err != nil {
		return 0, &channels.UncertainError{Cause: errors.New("feishu: decode send response")}
	}
	return result.Code, nil
}

func (sender *Sender) client() *http.Client {
	var configured *http.Client
	if sender.Client != nil {
		configured = sender.Client
	} else {
		configured = &http.Client{Timeout: 10 * time.Second}
	}
	copy := *configured
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &copy
}

func (sender *Sender) baseURL() string {
	if strings.TrimRight(sender.BaseURL, "/") != "" {
		return strings.TrimRight(sender.BaseURL, "/")
	}
	return defaultAPIBaseURL
}

func (sender *Sender) maxBytes() int {
	if sender.MaxBytes > 0 && sender.MaxBytes <= maxTextBytes {
		return sender.MaxBytes
	}
	return maxTextBytes
}

// SplitText splits on rune boundaries without exceeding a byte limit, so a
// multi-byte character is never truncated.
func SplitText(text string, limit int) ([]string, error) {
	if strings.TrimSpace(text) == "" || limit <= 0 || limit > maxTextBytes {
		return nil, errors.New("feishu: non-empty text and a valid byte limit are required")
	}
	var chunks []string
	for len(text) > 0 {
		if len(text) <= limit {
			chunks = append(chunks, text)
			break
		}
		cut := limit
		for cut > 0 && !utf8.RuneStart(text[cut]) {
			cut--
		}
		if cut == 0 {
			return nil, errors.New("feishu: byte limit cannot fit one UTF-8 rune")
		}
		chunks = append(chunks, text[:cut])
		text = text[cut:]
	}
	return chunks, nil
}

// isRetryableCode classifies Feishu business error codes. Feishu codes are
// large numbers, so unlike HTTP statuses there is no numeric retryable range:
// only the known transient codes retry; everything else is permanent.
func isRetryableCode(code int) bool {
	return tokenErrorCodes[code] || code == 99991400
}

func isUncertain(err error) bool {
	var classified channels.OutcomeClassifier
	return errors.As(err, &classified) && classified.DeliveryOutcomeUncertain()
}
