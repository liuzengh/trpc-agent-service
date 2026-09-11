// Package telegram provides bounded, explicit Telegram Bot API requests.
// It has no polling loop, cursor, account ownership, or application side effects
// in its constructor. The caller owns admission and update acknowledgement.
package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	ErrConfig     = errors.New("telegram: invalid client configuration")
	ErrRequest    = errors.New("telegram: invalid request")
	ErrTransport  = errors.New("telegram: transport unavailable")
	ErrProtocol   = errors.New("telegram: invalid response")
	tokenPattern  = regexp.MustCompile(`^[1-9][0-9]*:[A-Za-z0-9_-]{5,}$`)
	secretPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,256}$`)
)

const maxResponseBytes = 8 << 20

// APIError deliberately excludes provider descriptions and URLs, which may
// contain secrets. RetryAfter is a bounded number of seconds, not a deadline.
type APIError struct {
	Code       int
	RetryAfter int
}

func (e *APIError) Error() string { return "telegram: API error " + strconv.Itoa(e.Code) }

type Options struct {
	Token string
	// BaseURL defaults to the official HTTPS endpoint. HTTP is accepted only
	// for literal loopback addresses, to support deterministic protocol tests.
	BaseURL    string
	HTTPClient *http.Client
}

type Client struct {
	client    *http.Client
	endpoint  string
	closeIdle func()
}

func New(o Options) (*Client, error) {
	if len(o.Token) > 256 || !tokenPattern.MatchString(o.Token) {
		return nil, ErrConfig
	}
	if o.BaseURL == "" {
		o.BaseURL = "https://api.telegram.org"
	}
	u, err := url.Parse(o.BaseURL)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" || (u.Path != "" && u.Path != "/") {
		return nil, ErrConfig
	}
	ip := net.ParseIP(u.Hostname())
	if u.Scheme != "https" && !(u.Scheme == "http" && ((ip != nil && ip.IsLoopback()) || o.BaseURL == TestAPIURL)) {
		return nil, ErrConfig
	}
	var h http.Client
	if o.HTTPClient != nil {
		h = *o.HTTPClient
	} else {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.ResponseHeaderTimeout = 25 * time.Second
		transport.MaxResponseHeaderBytes = 64 << 10
		h.Transport = transport
	}
	// Never follow an API redirect carrying the token in its path. Clone the
	// client rather than mutating a client shared by other application code.
	h.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if h.Timeout == 0 || h.Timeout > 25*time.Second {
		h.Timeout = 25 * time.Second
	}
	return &Client{client: &h, endpoint: strings.TrimSuffix(o.BaseURL, "/") + "/bot" + o.Token + "/", closeIdle: h.CloseIdleConnections}, nil
}

// Formatting a client must not disclose the token-bearing endpoint.
func (*Client) String() string               { return "telegram.Client([REDACTED])" }
func (*Client) GoString() string             { return "telegram.Client([REDACTED])" }
func (*Client) MarshalJSON() ([]byte, error) { return []byte(`"[REDACTED]"`), nil }

func (c *Client) Close() {
	if c != nil && c.closeIdle != nil {
		c.closeIdle()
	}
}

func (c *Client) call(ctx context.Context, method string, params any, result any) error {
	if ctx == nil || c == nil || c.client == nil {
		return ErrRequest
	}
	body, err := json.Marshal(params)
	if err != nil {
		return ErrRequest
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+method, bytes.NewReader(body))
	if err != nil {
		return ErrRequest
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return errors.Join(ErrTransport, ctx.Err())
		}
		return ErrTransport
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		return ErrProtocol
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return ErrTransport
	}
	if len(raw) > maxResponseBytes {
		return ErrProtocol
	}
	var envelope struct {
		OK         *bool           `json:"ok"`
		Result     json.RawMessage `json:"result"`
		Code       int             `json:"error_code"`
		Parameters struct {
			RetryAfter int `json:"retry_after"`
		} `json:"parameters"`
	}
	if json.Unmarshal(raw, &envelope) != nil || envelope.OK == nil {
		return ErrProtocol
	}
	if !*envelope.OK {
		if envelope.Code < 400 || envelope.Code > 599 || envelope.Parameters.RetryAfter < 0 || envelope.Parameters.RetryAfter > 86400 {
			return ErrProtocol
		}
		return &APIError{envelope.Code, envelope.Parameters.RetryAfter}
	}
	if resp.StatusCode != http.StatusOK || len(envelope.Result) == 0 || bytes.Equal(envelope.Result, []byte("null")) || json.Unmarshal(envelope.Result, result) != nil {
		return ErrProtocol
	}
	return nil
}

func (c *Client) Identity(ctx context.Context) (string, error) {
	var user struct {
		ID    int64 `json:"id"`
		IsBot bool  `json:"is_bot"`
	}
	if err := c.call(ctx, "getMe", struct{}{}, &user); err != nil {
		return "", err
	}
	if user.ID < 1 || user.ID > 9007199254740991 || !user.IsBot {
		return "", ErrProtocol
	}
	return strconv.FormatInt(user.ID, 10), nil
}
