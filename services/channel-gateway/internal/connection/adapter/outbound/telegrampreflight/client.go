// Package telegrampreflight confines Telegram diagnostics to two read-only
// methods. SDK values, tokens, remote URLs and provider text never leave it.
package telegrampreflight

import (
	"context"
	"errors"
	protocol "github.com/liuzengh/trpc-agent-service/platform/im/telegram"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-telegram/bot"
	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/preflight"
)

const callTimeout = 5 * time.Second

// Client is concurrent-safe and has no mutable per-account cache.
type Client struct {
	transport http.RoundTripper
	closeIdle func()
}

// New uses only the deployment's ordinary outbound proxy configuration. Neither
// the account nor the caller can supply an arbitrary endpoint or proxy. A test
// account selects only the fixed Compose laboratory endpoint.
func New() *Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DialContext = (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	tr.TLSHandshakeTimeout = 3 * time.Second
	tr.ResponseHeaderTimeout = callTimeout
	tr.MaxResponseHeaderBytes = maxHeaderBytes
	tr.MaxConnsPerHost = 4
	tr.MaxIdleConnsPerHost = 4
	tr.MaxIdleConns = 4
	tr.IdleConnTimeout = 30 * time.Second
	tr.DisableCompression = true
	return &Client{transport: tr, closeIdle: tr.CloseIdleConnections}
}

// Close closes local idle connections; it never calls Telegram's close API.
func (c *Client) Close() {
	if c != nil && c.closeIdle != nil {
		c.closeIdle()
	}
}

func (c *Client) newBot(token string) (*bot.Bot, error) { return c.newBotAt(token, "") }
func (c *Client) newBotAt(token, profile string) (*bot.Bot, error) {
	endpoint, e := protocol.Endpoint(profile, "")
	if e != nil {
		return nil, app.ErrInvalid
	}
	if c == nil || c.transport == nil || !validToken(token) {
		return nil, app.ErrInvalid
	}
	h := &http.Client{
		Transport:     &readOnlyTransport{next: c.transport, token: token, endpoint: endpoint},
		Timeout:       callTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	b, err := bot.New(token, bot.WithServerURL(endpoint), bot.WithSkipGetMe(), bot.WithHTTPClient(callTimeout, h),
		bot.WithErrorsHandler(func(error) {}), bot.WithDebugHandler(func(string, ...any) {}))
	if err != nil {
		return nil, app.ErrInvalid
	}
	return b, nil
}

// Inspect uses the caller's bounded task context and returns only declassified
// observations. ExpectedWebhook is comparison data, never a network target.
func (c *Client) Inspect(ctx context.Context, r app.ProbeRequest) (app.ProbeResult, error) {
	out := app.ProbeResult{IdentityCode: "NOT_EXECUTED", WebhookCode: "NOT_EXECUTED", Relation: "UNKNOWN"}
	if ctx == nil || !validIdentity(r.ExpectedIdentity) {
		return out, app.ErrInvalid
	}
	if ctx.Err() != nil {
		return out, app.ErrExpired
	}
	// Stored credentials are opaque. A malformed token is a local credential
	// failure, not a broken claim to retry until timeout. This is not an observed
	// HTTP 401, and no token-bearing request is constructed for unsafe syntax.
	if !validToken(r.Token.Reveal()) {
		out.IdentityCode = "TOKEN_REJECTED"
		return out, nil
	}
	b, err := c.newBotAt(r.Token.Reveal(), r.EndpointProfile)
	if err != nil {
		return out, err
	}
	defer b.SetToken("")
	op, cancel := context.WithTimeout(ctx, callTimeout)
	u, err := b.GetMe(op)
	cancel()
	if err != nil {
		out.IdentityCode = failureCode(err)
		return out, nil
	}
	if u == nil || u.ID <= 0 || u.ID > maxSafeInteger {
		out.IdentityCode = "PROVIDER_RESPONSE_INVALID"
		return out, nil
	}
	match := u.IsBot && strconv.FormatInt(u.ID, 10) == r.ExpectedIdentity
	out.IdentityMatch = &match
	if !match {
		out.IdentityCode = "BOT_IDENTITY_MISMATCH"
		return out, nil
	}
	out.IdentityCode = "BOT_IDENTITY_MATCH"
	if ctx.Err() != nil {
		return out, app.ErrExpired
	}
	op, cancel = context.WithTimeout(ctx, callTimeout)
	w, err := b.GetWebhookInfo(op)
	cancel()
	if err != nil {
		out.WebhookCode = failureCode(err)
		return out, nil
	}
	if w == nil {
		out.WebhookCode = "PROVIDER_RESPONSE_INVALID"
		return out, nil
	}
	present := w.URL != ""
	out.Presence = &present
	switch {
	case !present:
		out.WebhookCode, out.Relation = "WEBHOOK_NONE", "NONE"
	case r.ExpectedWebhook == nil:
		out.WebhookCode = "WEBHOOK_COMPARISON_UNAVAILABLE"
	case w.URL == *r.ExpectedWebhook:
		out.WebhookCode, out.Relation = "WEBHOOK_MATCH", "MATCH"
	default:
		out.WebhookCode, out.Relation = "WEBHOOK_DIFFERENT", "DIFFERENT"
	}
	pending := int64(w.PendingUpdateCount)
	out.PendingUpdates = &pending
	hasError := w.LastErrorDate > 0 || w.LastErrorMessage != ""
	out.HasLastError = &hasError
	if w.LastErrorDate > 0 {
		at := time.Unix(int64(w.LastErrorDate), 0).UTC()
		out.LastErrorAt = &at
	}
	return out, nil
}

func validIdentity(s string) bool {
	// Control permits a canonical decimal identifier up to 1024 bytes. A saved
	// but non-existent huge ID must produce an identity mismatch, not a timeout.
	return len(s) > 0 && len(s) <= 1024 && s[0] >= '1' && s[0] <= '9' && strings.Trim(s, "0123456789") == ""
}

func validToken(s string) bool {
	if len(s) == 0 || len(s) > 16<<10 {
		return false
	}
	id, secret, found := strings.Cut(s, ":")
	if !found || !validIdentity(id) || secret == "" {
		return false
	}
	for _, ch := range secret {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '_' || ch == '-') {
			return false
		}
	}
	return true
}

type providerError struct{ code string }

func (e *providerError) Error() string { return e.code }

func failureCode(err error) string {
	var p *providerError
	if errors.As(err, &p) {
		return p.code
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "PROVIDER_TIMEOUT"
	}
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		return "PROVIDER_TIMEOUT"
	}
	// Network failures are classified in the transport. Remaining SDK failures
	// are response decoding failures; never publish their original text.
	return "PROVIDER_RESPONSE_INVALID"
}

var _ app.TelegramProbe = (*Client)(nil)
