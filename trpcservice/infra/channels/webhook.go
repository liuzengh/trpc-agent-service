// HTTP callback ingress for IM channels.
//
// Two ways exist to receive IM events. The default is a connection we open and
// hold (WeCom's AI-bot WSS, Feishu's ws.Client); the alternative is the platform
// POSTing each event to an HTTP endpoint we expose — Feishu's "事件订阅 → 请求网址"
// mode. The second mode matters in production because it needs no long-lived
// socket: it survives restarts, works behind an ordinary reverse proxy, and is
// the only mode some platforms offer at all (WeChat customer service, official
// accounts).
//
// The ingress is deliberately not a second message pipeline. It authenticates
// the callback, hands the raw payload to the very same adapter the long
// connection would feed, and lets the adapter's dedup / media fetch / routing /
// idempotency do the rest. Replies are unchanged: they already go out over each
// platform's send API.
package channels

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

// Callback authentication failures. Every one of them is fail-closed: an
// endpoint reachable by the public internet must never fall back to "accept it
// anyway", so a missing secret, an unserved channel and a bad signature are all
// refusals.
var (
	// ErrWebhookUnsupported means this channel has no HTTP callback mode (its
	// platform only supports a connection we open), or the running node was not
	// configured to serve callbacks.
	ErrWebhookUnsupported = errors.New("channels: channel has no HTTP callback mode")
	// ErrWebhookUnauthorized means the callback could not be authenticated.
	ErrWebhookUnauthorized = errors.New("channels: callback authentication failed")
	// ErrWebhookBusy means the callback was authenticated but the adapter could
	// not accept it right now. The HTTP layer answers 503 so the platform
	// retries instead of losing the event.
	ErrWebhookBusy = errors.New("channels: callback not accepted, retry later")
)

// WebhookCallback is one platform callback: the raw body plus the signature
// metadata the platform sent alongside it. Header names are platform-specific,
// so the HTTP layer extracts them and the channel's verifier interprets them.
type WebhookCallback struct {
	Body      []byte
	Timestamp string
	Nonce     string
	Signature string
}

// WebhookEvent is the result of authenticating a callback.
type WebhookEvent struct {
	// Payload is the raw event to feed the adapter's pipeline. Empty when the
	// callback carried no event (a handshake, or an event type this platform
	// delivers only to acknowledge it).
	Payload []byte
	// Challenge is echoed back verbatim by the HTTP layer to complete the
	// platform's URL-verification handshake. Empty for a normal callback.
	Challenge string
}

// WebhookVerifier authenticates one callback with the binding's verification
// secret and returns the event to feed the adapter.
//
// The verifier owns its platform's protocol, including decryption and the
// URL-verification handshake, because both are platform-specific: Feishu signs
// sha256(timestamp + nonce + encrypt_key + body) and may AES-encrypt the body,
// while another platform uses a different scheme entirely.
//
// A verifier must fail closed: an unverifiable callback returns an error and
// never an empty event (see ErrWebhookUnauthorized).
type WebhookVerifier func(secret string, cb WebhookCallback) (WebhookEvent, error)

// WebhookAdapter is an Adapter whose inbound events arrive over HTTP rather than
// from a connection the adapter reads.
type WebhookAdapter interface {
	Adapter
	// Feed hands one authenticated callback payload to the adapter's
	// decode/dedup pipeline. It reports false when the adapter cannot accept it
	// right now, so the caller can answer 503 and let the platform retry.
	Feed(payload []byte) bool
}

// WebhookBuilder builds the webhook-mode adapter of a binding. It mirrors
// AdapterBuilder; the credential is the same one the long connection would use,
// because sending replies and downloading attachments still go through the
// platform's API.
type WebhookBuilder func(ctx context.Context, b ChannelBinding, secret string) (WebhookAdapter, error)

// WebhookConfig enables the HTTP callback ingress.
type WebhookConfig struct {
	// Channels selects which channels are served over HTTP. A channel listed
	// here is NOT connected by a long connection; a binding of any other channel
	// keeps using its connection.
	Channels map[string]bool
	// Verifiers supplies each channel's callback authentication.
	Verifiers map[string]WebhookVerifier
	// Builders supplies each channel's webhook-mode adapter.
	Builders map[string]WebhookBuilder
}

// EnableWebhook installs the HTTP callback ingress. It reports a configuration
// error for a channel that has no verifier or builder, rather than accepting
// callbacks it could never authenticate or route.
func (m *Manager) EnableWebhook(cfg WebhookConfig) error {
	for channel := range cfg.Channels {
		if cfg.Verifiers[channel] == nil {
			return fmt.Errorf("channels: webhook channel %q has no verifier", channel)
		}
		if cfg.Builders[channel] == nil {
			return fmt.Errorf("channels: webhook channel %q has no adapter builder", channel)
		}
	}
	m.mu.Lock()
	m.webhook = &cfg
	m.mu.Unlock()
	slog.Info("channels: HTTP callback ingress enabled", "channels", cfg.Channels)
	return nil
}

// WebhookChannelServed reports whether a channel is served over HTTP callbacks,
// i.e. whether the reconciler must leave it unconnected.
func (m *Manager) WebhookChannelServed(channel string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.webhook != nil && m.webhook.Channels[channel]
}

// webhookConfig returns the installed ingress configuration (nil when the
// ingress is off).
func (m *Manager) webhookConfig() *WebhookConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.webhook
}

// HandleWebhook authenticates and ingests one platform callback for a binding.
//
// The returned WebhookEvent is what the HTTP layer needs to answer (a challenge
// for the handshake, or an empty acknowledgement). Errors map onto HTTP statuses
// in the caller: ErrBindingNotFound → 404, ErrWebhookUnsupported → 501,
// ErrWebhookUnauthorized → 401, ErrWebhookBusy → 503.
func (m *Manager) HandleWebhook(ctx context.Context, bindingID string, cb WebhookCallback) (WebhookEvent, error) {
	wh := m.webhookConfig()
	if wh == nil || m.bindings == nil {
		return WebhookEvent{}, ErrWebhookUnsupported
	}
	b, err := m.bindings.Get(ctx, bindingID)
	if err != nil {
		return WebhookEvent{}, err
	}
	if !wh.Channels[b.Channel] {
		// The binding exists but runs on a long connection: a callback for it
		// would be a duplicate delivery of the same event. Refuse rather than
		// ingest it twice.
		return WebhookEvent{}, ErrWebhookUnsupported
	}
	secret, err := m.callbackSecret(ctx, *b)
	if err != nil {
		return WebhookEvent{}, err
	}
	event, err := wh.Verifiers[b.Channel](secret, cb)
	if err != nil {
		slog.Warn("channels: callback rejected", "binding", bindingID, "channel", b.Channel, "err", err)
		return WebhookEvent{}, fmt.Errorf("%w: %v", ErrWebhookUnauthorized, err)
	}
	if event.Challenge != "" || len(event.Payload) == 0 {
		// A handshake, or an event type we do not handle: acknowledge it so the
		// platform does not retry forever.
		return event, nil
	}
	adapter, err := m.webhookAdapter(ctx, *b, wh.Builders[b.Channel])
	if err != nil {
		return WebhookEvent{}, err
	}
	if !adapter.Feed(event.Payload) {
		return WebhookEvent{}, ErrWebhookBusy
	}
	return event, nil
}

// callbackSecret resolves the secret a callback is authenticated with. An unset
// or unresolvable reference is a hard error: accepting an unauthenticated
// callback would let anyone drive the agent.
func (m *Manager) callbackSecret(ctx context.Context, b ChannelBinding) (string, error) {
	if b.VerificationTokenRef == "" {
		return "", fmt.Errorf("%w: binding %s has no verification secret", ErrWebhookUnauthorized, b.BindingID)
	}
	if m.creds == nil {
		return "", fmt.Errorf("%w: credential source unavailable", ErrWebhookUnauthorized)
	}
	secret, err := m.creds.Get(ctx, b.VerificationTokenRef)
	if err != nil {
		return "", fmt.Errorf("%w: resolve verification secret: %v", ErrWebhookUnauthorized, err)
	}
	if secret == "" {
		return "", fmt.Errorf("%w: verification secret is empty", ErrWebhookUnauthorized)
	}
	return secret, nil
}

// liveWebhook reports whether the binding already has a live adapter.
// ok=false with err=nil means "not live yet"; a non-nil err means the binding is
// connected by a long connection and cannot be served over HTTP as well.
func (m *Manager) liveWebhook(bindingID string) (WebhookAdapter, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	h, ok := m.conns[bindingID]
	if !ok {
		return nil, false, nil
	}
	if wa, ok := h.adapter.(WebhookAdapter); ok {
		return wa, true, nil
	}
	return nil, false, fmt.Errorf("channels: binding %s is connected by a long connection", bindingID)
}

// webhookAdapter returns the binding's webhook-mode adapter, building and
// attaching it on first use. The adapter is registered like any other live
// connection so reply routing (which resolves a binding back to its adapter)
// finds it, and so shutdown closes it.
//
// Construction is serialized on its own mutex: it performs network calls (bot
// info, token exchange), so it must neither block the reconciler nor let two
// concurrent callbacks build the same adapter twice.
func (m *Manager) webhookAdapter(ctx context.Context, b ChannelBinding, build WebhookBuilder) (WebhookAdapter, error) {
	if wa, ok, err := m.liveWebhook(b.BindingID); ok || err != nil {
		return wa, err
	}
	m.whMu.Lock()
	defer m.whMu.Unlock()
	// Re-check under the build lock: another callback may have won the race.
	if wa, ok, err := m.liveWebhook(b.BindingID); ok || err != nil {
		return wa, err
	}
	if build == nil {
		return nil, ErrWebhookUnsupported
	}
	if m.creds == nil {
		return nil, errors.New("channels: credential source unavailable")
	}
	secret, err := m.creds.Get(ctx, b.CredentialRef)
	if err != nil {
		return nil, fmt.Errorf("channels: resolve credential %q: %w", b.CredentialRef, err)
	}
	if secret == "" {
		return nil, fmt.Errorf("channels: empty credential for %q", b.CredentialRef)
	}
	adapter, err := build(ctx, b, secret)
	if err != nil {
		return nil, err
	}
	// Same lifecycle as a long-connection adapter: its own context (so an HTTP
	// request cancel cannot tear it down), a supervised read loop, and the
	// gateway attach that pumps its inbound messages into the bus.
	actx, cancel := context.WithCancel(context.Background())
	go m.supervise(actx, b, adapter)
	m.gw.Attach(actx, adapter, Attach{BindingID: b.BindingID, AccountID: b.AccountID, AgentID: b.AgentID})
	m.mu.Lock()
	m.conns[b.BindingID] = connHandle{adapter: adapter, cancel: cancel}
	m.mu.Unlock()
	slog.Info("channels: webhook adapter attached", "binding", b.BindingID, "channel", b.Channel)
	return adapter, nil
}
