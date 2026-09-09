// Package telegramingress forwards Telegram long-poll updates through the
// existing authenticated Gateway, retaining its admission and durable queue.
package telegramingress

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	ErrConfiguration = errors.New("telegram ingress configuration invalid")
	ErrBotIdentity   = errors.New("telegram bot identity mismatch")
	ErrWebhookActive = errors.New("telegram webhook is active; explicit operator cutover required")
	ErrProvider      = errors.New("telegram provider rejected request")
	ErrProtocol      = errors.New("telegram provider response invalid")
	ErrGateway       = errors.New("gateway rejected telegram update; offset retained")
	ErrStateRead     = errors.New("telegram ingress state unreadable or invalid")
	ErrStateBinding  = errors.New("telegram ingress state belongs to a different bot or gateway route")
	ErrStateWrite    = errors.New("telegram ingress state persistence failed; stop before confirming offset")
	ErrAlreadyLocked = errors.New("telegram ingress state is locked by another process")
)

const maxUpdateBytes = 1 << 20
const maxAPIResponseBytes = 16 << 20

var tokenPattern = regexp.MustCompile(`^[1-9][0-9]{5,19}:[A-Za-z0-9_-]{30,128}$`)
var secretPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{32,256}$`)
var routePattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

// Config contains operator-owned configuration, never incoming message data.
// StatePath must be on a durable local filesystem shared by this bot's poller
// restarts. A single-writer OS lock is held throughout Run.
type Config struct {
	BotToken       string
	WebhookSecret  string
	GatewayBaseURL string
	RouteKey       string
	StatePath      string
	ExpectedBotID  int64
	PollTimeout    time.Duration
}

// Event exposes only fixed operation names, HTTP status and provider update
// IDs. It deliberately excludes credentials, URLs, chat identities and text.
type Event struct {
	Operation string
	Outcome   string
	Status    int
	UpdateID  int64
}

type Poller struct {
	config        Config
	gatewayURL    string
	routeHash     string
	apiClient     *http.Client
	gatewayClient *http.Client
	notify        func(Event)
	wait          func(context.Context, time.Duration) error
	readState     func(string) (*checkpoint, error)
	writeState    func(string, *checkpoint) error
}

// New validates configuration without making network requests. HTTP proxy
// environment variables are honored by both clients; redirects are rejected
// so Bot API tokens and Gateway authentication never cross a redirect.
func New(config Config, notify func(Event)) (*Poller, error) {
	if !tokenPattern.MatchString(config.BotToken) || !secretPattern.MatchString(config.WebhookSecret) ||
		!routePattern.MatchString(config.RouteKey) || config.StatePath == "" || config.ExpectedBotID < 0 {
		return nil, ErrConfiguration
	}
	if config.PollTimeout == 0 {
		config.PollTimeout = 30 * time.Second
	}
	if config.PollTimeout < time.Second || config.PollTimeout > 50*time.Second || config.PollTimeout%time.Second != 0 {
		return nil, ErrConfiguration
	}
	base, err := url.Parse(config.GatewayBaseURL)
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Hostname() == "" ||
		base.User != nil || base.RawQuery != "" || base.Fragment != "" || base.Opaque != "" ||
		(base.Path != "" && base.Path != "/") || strings.ContainsAny(config.GatewayBaseURL, "\r\n\x00") {
		return nil, ErrConfiguration
	}
	base.Path, base.RawPath = "/webhook", ""
	base.RawQuery = url.Values{"token": []string{config.RouteKey}}.Encode()
	absolute, err := filepath.Abs(config.StatePath)
	if err != nil {
		return nil, ErrConfiguration
	}
	config.StatePath = absolute
	digest := sha256.Sum256([]byte(base.String()))
	noRedirect := func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	return &Poller{
		config: config, gatewayURL: base.String(), routeHash: hex.EncodeToString(digest[:]),
		apiClient:     &http.Client{Timeout: config.PollTimeout + 15*time.Second, CheckRedirect: noRedirect},
		gatewayClient: &http.Client{Timeout: 20 * time.Second, CheckRedirect: noRedirect},
		notify:        notify, wait: waitContext, readState: loadCheckpoint, writeState: saveCheckpoint,
	}, nil
}

// Run verifies the bot and webhook state before polling. A received update is
// durably spooled before forwarding, and its exact bytes survive a crash after
// Gateway commit. Only a Gateway 200 permits the next offset to be committed.
// The next getUpdates call acknowledges that committed offset to Telegram.
func (p *Poller) Run(ctx context.Context) error {
	if p == nil || ctx == nil {
		return ErrConfiguration
	}
	lock, err := acquireStateLock(p.config.StatePath)
	if err != nil {
		return err
	}
	defer lock.Close()
	var identity struct {
		ID    int64 `json:"id"`
		IsBot bool  `json:"is_bot"`
	}
	if err := p.callAPI(ctx, "getMe", struct{}{}, &identity); err != nil {
		return err
	}
	if !identity.IsBot || identity.ID <= 0 || (p.config.ExpectedBotID != 0 && p.config.ExpectedBotID != identity.ID) {
		return ErrBotIdentity
	}
	var webhook struct {
		URL *string `json:"url"`
	}
	if err := p.callAPI(ctx, "getWebhookInfo", struct{}{}, &webhook); err != nil {
		return err
	}
	if webhook.URL == nil {
		return ErrProtocol
	}
	if *webhook.URL != "" {
		return ErrWebhookActive
	}
	state, err := p.readState(p.config.StatePath)
	if err != nil {
		return err
	}
	if state == nil {
		state = &checkpoint{Version: 1, BotID: identity.ID, RouteHash: p.routeHash}
		if err := p.writeState(p.config.StatePath, state); err != nil {
			return ErrStateWrite
		}
	} else if state.BotID != identity.ID || state.RouteHash != p.routeHash {
		return ErrStateBinding
	}
	p.emit(Event{Operation: "startup", Outcome: "ready"})
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if state.Pending != nil {
			if err := p.deliverPending(ctx, state); err != nil {
				return err
			}
		}
		var updates []json.RawMessage
		request := struct {
			Offset         int64    `json:"offset"`
			Limit          int      `json:"limit"`
			Timeout        int      `json:"timeout"`
			AllowedUpdates []string `json:"allowed_updates"`
		}{state.Offset, 100, int(p.config.PollTimeout / time.Second), []string{"message"}}
		if err := p.callAPI(ctx, "getUpdates", request, &updates); err != nil {
			return err
		}
		if len(updates) > 100 {
			return ErrProtocol
		}
		for _, raw := range updates {
			id, err := updateIdentity(raw)
			if err != nil || id < state.Offset {
				return ErrProtocol
			}
			pending := *state
			pending.Pending = &pendingUpdate{ID: id, Body: append(json.RawMessage(nil), raw...)}
			if err := p.writeState(p.config.StatePath, &pending); err != nil {
				return ErrStateWrite
			}
			*state = pending
			if err := p.deliverPending(ctx, state); err != nil {
				return err
			}
		}
		// A healthy Telegram long poll normally blocks server-side. Prevent a
		// misbehaving proxy or fixture returning empty arrays from hot looping.
		if len(updates) == 0 {
			if err := p.wait(ctx, 100*time.Millisecond); err != nil {
				return err
			}
		}
	}
}

func (p *Poller) deliverPending(ctx context.Context, state *checkpoint) error {
	pending := state.Pending
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.gatewayURL, bytes.NewReader(pending.Body))
		if err != nil {
			return ErrConfiguration
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Telegram-Bot-Api-Secret-Token", p.config.WebhookSecret)
		response, err := p.gatewayClient.Do(req)
		if ctx.Err() != nil {
			closeResponse(response)
			return ctx.Err()
		}
		status := 0
		var delay time.Duration
		if response != nil {
			status = response.StatusCode
			delay = gatewayRetryAfter(response.Header.Get("Retry-After"))
		}
		closeResponse(response)
		if err == nil && status == http.StatusOK {
			next := *state
			next.Offset, next.Pending = pending.ID+1, nil
			if err := p.writeState(p.config.StatePath, &next); err != nil {
				return ErrStateWrite
			}
			*state = next
			p.emit(Event{Operation: "gateway", Outcome: "acknowledged", Status: status, UpdateID: pending.ID})
			return nil
		}
		if err == nil && status != http.StatusTooManyRequests && status < 500 {
			return fmt.Errorf("%w (status %d)", ErrGateway, status)
		}
		p.emit(Event{Operation: "gateway", Outcome: "retry", Status: status, UpdateID: pending.ID})
		if err := p.wait(ctx, retryDelay(attempt, delay)); err != nil {
			return err
		}
	}
}

type apiResponse struct {
	OK         bool            `json:"ok"`
	ErrorCode  int             `json:"error_code"`
	Result     json.RawMessage `json:"result"`
	Parameters struct {
		RetryAfter int64 `json:"retry_after"`
	} `json:"parameters"`
}

func (p *Poller) callAPI(ctx context.Context, method string, input, output any) error {
	// method is always one of the three compile-time call sites above. The
	// provider host is fixed; configuration cannot redirect bot credentials.
	endpoint := "https://api.telegram.org/bot" + p.config.BotToken + "/" + method
	body, _ := json.Marshal(input)
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return ErrConfiguration
		}
		req.Header.Set("Content-Type", "application/json")
		response, err := p.apiClient.Do(req)
		if ctx.Err() != nil {
			closeResponse(response)
			return ctx.Err()
		}
		if err != nil {
			closeResponse(response)
			p.emit(Event{Operation: method, Outcome: "retry"})
			if err := p.wait(ctx, retryDelay(attempt, 0)); err != nil {
				return err
			}
			continue
		}
		status := response.StatusCode
		raw, readErr := io.ReadAll(io.LimitReader(response.Body, maxAPIResponseBytes+1))
		_ = response.Body.Close()
		var decoded apiResponse
		decodeErr := json.Unmarshal(raw, &decoded)
		if status == http.StatusTooManyRequests || status >= 500 || decoded.ErrorCode == http.StatusTooManyRequests || decoded.ErrorCode >= 500 || readErr != nil {
			delay := time.Duration(0)
			if decoded.Parameters.RetryAfter > 0 {
				if decoded.Parameters.RetryAfter > math.MaxInt64/int64(time.Second) {
					return ErrProtocol
				}
				delay = time.Duration(decoded.Parameters.RetryAfter) * time.Second
			}
			p.emit(Event{Operation: method, Outcome: "retry", Status: status})
			if err := p.wait(ctx, retryDelay(attempt, delay)); err != nil {
				return err
			}
			continue
		}
		if status != http.StatusOK {
			return fmt.Errorf("%w (status %d)", ErrProvider, status)
		}
		if decodeErr != nil || len(raw) > maxAPIResponseBytes {
			return ErrProtocol
		}
		if !decoded.OK {
			return fmt.Errorf("%w (code %d)", ErrProvider, decoded.ErrorCode)
		}
		if len(decoded.Result) == 0 || bytes.Equal(bytes.TrimSpace(decoded.Result), []byte("null")) || json.Unmarshal(decoded.Result, output) != nil {
			return ErrProtocol
		}
		return nil
	}
}

func updateIdentity(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 || len(raw) > maxUpdateBytes {
		return 0, ErrProtocol
	}
	var value struct {
		ID *int64 `json:"update_id"`
	}
	if json.Unmarshal(raw, &value) != nil || value.ID == nil || *value.ID < 0 || *value.ID == math.MaxInt64 {
		return 0, ErrProtocol
	}
	return *value.ID, nil
}

func (p *Poller) emit(event Event) {
	if p.notify != nil {
		p.notify(event)
	}
}

func closeResponse(response *http.Response) {
	if response != nil && response.Body != nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		_ = response.Body.Close()
	}
}

func gatewayRetryAfter(value string) time.Duration {
	seconds, err := strconv.ParseInt(value, 10, 64)
	if err == nil && seconds > 0 && seconds <= math.MaxInt64/int64(time.Second) {
		return time.Duration(seconds) * time.Second
	}
	if stamp, err := http.ParseTime(value); err == nil {
		if delay := time.Until(stamp); delay > 0 {
			return delay
		}
	}
	return 0
}

func retryDelay(attempt int, provider time.Duration) time.Duration {
	if provider > 0 {
		return provider
	}
	if attempt > 5 {
		attempt = 5
	}
	return time.Second << attempt
}

func waitContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
