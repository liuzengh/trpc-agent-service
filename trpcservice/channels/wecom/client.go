package wecom

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

const (
	defaultAPIBaseURL = "https://qyapi.weixin.qq.com"
	maxTextBytes      = 2048
)

// TokenSource returns and invalidates a cached WeCom access token.
type TokenSource interface {
	Token(context.Context) (string, error)
	Invalidate(string)
}

// CredentialTokenSource exchanges a CorpID and application secret for a
// cached access token. Secret values are never included in returned errors.
type CredentialTokenSource struct {
	CorpID, CorpSecret string
	BaseURL            string
	Client             *http.Client

	mu      sync.Mutex
	token   string
	expires time.Time
}

// Token implements TokenSource.
func (source *CredentialTokenSource) Token(ctx context.Context) (string, error) {
	if source == nil || source.CorpID == "" || source.CorpSecret == "" {
		return "", errors.New("wecom: CorpID and application secret are required")
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.token != "" && time.Now().Before(source.expires) {
		return source.token, nil
	}
	endpoint, err := url.Parse(source.baseURL() + "/cgi-bin/gettoken")
	if err != nil {
		return "", errors.New("wecom: invalid API base URL")
	}
	query := endpoint.Query()
	query.Set("corpid", source.CorpID)
	query.Set("corpsecret", source.CorpSecret)
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return "", errors.New("wecom: create access token request")
	}
	response, err := source.client().Do(request)
	if err != nil {
		return "", fmt.Errorf("wecom: request access token: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		return "", fmt.Errorf("wecom: access token HTTP status %d", response.StatusCode)
	}
	var payload struct {
		ErrCode     int    `json:"errcode"`
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&payload); err != nil {
		return "", errors.New("wecom: decode access token response")
	}
	if payload.ErrCode != 0 || payload.AccessToken == "" || payload.ExpiresIn <= 0 {
		return "", fmt.Errorf("wecom: access token rejected with code %d", payload.ErrCode)
	}
	source.token = payload.AccessToken
	ttl := time.Duration(payload.ExpiresIn) * time.Second
	if ttl > time.Minute {
		ttl -= time.Minute
	}
	source.expires = time.Now().Add(ttl)
	return source.token, nil
}

// Invalidate drops only the token that caused an API rejection.
func (source *CredentialTokenSource) Invalidate(token string) {
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

func (source *CredentialTokenSource) client() *http.Client {
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

func (source *CredentialTokenSource) baseURL() string {
	if strings.TrimRight(source.BaseURL, "/") != "" {
		return strings.TrimRight(source.BaseURL, "/")
	}
	return defaultAPIBaseURL
}

// APIError preserves provider error classification without including tokens or
// response bodies.
type APIError struct {
	Code      int
	Retryable bool
}

func (err *APIError) Error() string { return fmt.Sprintf("wecom: API error code %d", err.Code) }

// DeliveryRetryable exposes the definitive WeCom response classification.
func (err *APIError) DeliveryRetryable() bool { return err != nil && err.Retryable }

// Sender delivers durable Outbox text through the WeCom application API.
type Sender struct {
	AgentID  int64
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

// SendText delivers direct replies with message/send and group replies with
// appchat/send. Each UTF-8 chunk is within WeCom's 2048-byte text limit.
func (sender *Sender) SendText(ctx context.Context, outbound gateway.OutboundMessage) error {
	if sender == nil || sender.AgentID <= 0 || sender.Tokens == nil {
		return errors.New("wecom: sender AgentID and token source are required")
	}
	if outbound.ExternalUserID == "" || strings.TrimSpace(outbound.Text) == "" {
		return errors.New("wecom: outbound external user and text are required")
	}
	chunks, err := SplitText(outbound.Text, sender.maxBytes())
	if err != nil {
		return err
	}
	for index, chunk := range chunks {
		if err := sender.sendChunk(ctx, outbound, chunk); err != nil {
			if index > 0 && !isUncertain(err) {
				return &channels.UncertainError{Cause: fmt.Errorf("wecom: partial chunk delivery: %w", err)}
			}
			return err
		}
	}
	return nil
}

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
		if isTokenError(code) && attempt == 0 {
			sender.Tokens.Invalidate(token)
			continue
		}
		return &APIError{Code: code, Retryable: isRetryableCode(code)}
	}
	return errors.New("wecom: exhausted token refresh")
}

func (sender *Sender) post(ctx context.Context, token string, outbound gateway.OutboundMessage, text string) (int, error) {
	path := "/cgi-bin/message/send"
	payload := map[string]any{
		"touser":  outbound.ExternalUserID,
		"msgtype": "text",
		"agentid": sender.AgentID,
		"text":    map[string]string{"content": text},
	}
	if outbound.ConversationID != "" {
		path = "/cgi-bin/appchat/send"
		payload = map[string]any{
			"chatid":  outbound.ConversationID,
			"msgtype": "text",
			"text":    map[string]string{"content": text},
			"safe":    0,
		}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	endpoint, err := url.Parse(sender.baseURL() + path)
	if err != nil {
		return 0, errors.New("wecom: invalid API base URL")
	}
	query := endpoint.Query()
	query.Set("access_token", token)
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return 0, errors.New("wecom: create send request")
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := sender.client().Do(request)
	if err != nil {
		return 0, &channels.UncertainError{Cause: fmt.Errorf("wecom: send message: %w", err)}
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		return 0, &APIError{Code: response.StatusCode, Retryable: response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500}
	}
	var result struct {
		ErrCode int `json:"errcode"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result); err != nil {
		return 0, &channels.UncertainError{Cause: errors.New("wecom: decode send response")}
	}
	return result.ErrCode, nil
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

// SplitText splits on rune boundaries without exceeding a byte limit.
func SplitText(text string, limit int) ([]string, error) {
	if strings.TrimSpace(text) == "" || limit <= 0 || limit > maxTextBytes {
		return nil, errors.New("wecom: non-empty text and a valid byte limit are required")
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
			return nil, errors.New("wecom: byte limit cannot fit one UTF-8 rune")
		}
		chunks = append(chunks, text[:cut])
		text = text[cut:]
	}
	return chunks, nil
}

func isTokenError(code int) bool { return code == 40014 || code == 42001 }

func isRetryableCode(code int) bool {
	return code == -1 || code == 45009 || isTokenError(code) || code == http.StatusTooManyRequests || code >= 500
}

func isUncertain(err error) bool {
	var classified channels.OutcomeClassifier
	return errors.As(err, &classified) && classified.DeliveryOutcomeUncertain()
}
