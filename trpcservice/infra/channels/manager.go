// Binding-driven IM connection management.
//
// The Manager reconciles live IM adapters against the binding store: Reload
// starts an adapter for each binding that has a resolvable credential and
// stops adapters whose binding was removed. Credentials are resolved from the
// secret store by reference (never plaintext in the binding).
//
// This replaces the earlier config-driven wiring: the admin channel page is
// now the single place where an IM account is bound, credentialled and routed
// to a tenant/agent, and the gateway connects as soon as the binding is saved.
package channels

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// CredentialSource resolves a credential value by secret-store reference. The
// secret store satisfies it directly.
type CredentialSource interface {
	Get(ctx context.Context, key string) (string, error)
}

// AdapterBuilder builds a channel adapter from a binding plus a resolved
// credential value. Implemented by main: the wecom/feishu adapters live in
// their own packages, and channels cannot import them without a cycle.
type AdapterBuilder func(ctx context.Context, b ChannelBinding, secret string) (Adapter, error)

// connHandle tracks one live adapter connection.
type connHandle struct {
	adapter Adapter
	cancel  context.CancelFunc
}

func (h connHandle) stop() {
	h.cancel()
	if h.adapter != nil {
		_ = h.adapter.Stop(context.Background())
	}
}

// Manager drives IM connections from the binding store.
type Manager struct {
	bus      Bus
	bindings BindingStore
	creds    CredentialSource
	build    AdapterBuilder
	gw       *Gateway

	// webhook is the installed HTTP callback ingress (nil when disabled); whMu
	// serializes its adapter construction, which performs network calls.
	webhook *WebhookConfig
	whMu    sync.Mutex

	mu    sync.RWMutex
	conns map[string]connHandle
}

// NewManager returns a binding-driven IM connection manager. creds may be nil
// (no bindings will connect until one is configured); build must not be nil.
func NewManager(b Bus, bindings BindingStore, creds CredentialSource, build AdapterBuilder) *Manager {
	m := &Manager{
		bus:      b,
		bindings: bindings,
		creds:    creds,
		build:    build,
		gw:       NewGateway(b, bindings),
		conns:    make(map[string]connHandle),
	}
	// The gateway needs to resolve a persisted route's binding back to a live
	// adapter; the Manager owns the connections, so it answers.
	m.gw.SetAdapterLookup(m.adapterByBinding)
	return m
}

// adapterByBinding resolves a live adapter by its binding id.
func (m *Manager) adapterByBinding(bindingID string) (Adapter, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.conns[bindingID]
	if !ok || h.adapter == nil {
		return nil, false
	}
	return h.adapter, true
}

// Reload reconciles live adapters with the current binding set. It is safe to
// call repeatedly and after every binding create/delete. Bindings without a
// credential are skipped with a warning, not an error.
func (m *Manager) Reload(ctx context.Context) error {
	bindings, err := m.bindings.List(ctx, "", "")
	if err != nil {
		return err
	}
	want := make(map[string]ChannelBinding, len(bindings))
	for _, b := range bindings {
		want[b.BindingID] = b
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for id, h := range m.conns {
		if _, ok := want[id]; !ok {
			h.stop()
			delete(m.conns, id)
			slog.Info("channels: stopped adapter", "binding", id)
		}
	}
	for id, b := range want {
		if m.webhook != nil && m.webhook.Channels[b.Channel] {
			// This channel is served over HTTP callbacks (see webhook.go): its
			// adapter is built on the first callback, so opening a long
			// connection here would deliver every event twice.
			continue
		}
		if _, ok := m.conns[id]; ok {
			continue
		}
		if b.CredentialRef == "" {
			slog.Warn("channels: binding has no credential, skipping", "binding", id, "channel", b.Channel)
			continue
		}
		h, err := m.start(ctx, b)
		if err != nil {
			slog.Error("channels: start adapter failed", "binding", id, "channel", b.Channel, "err", err)
			continue
		}
		m.conns[id] = h
		slog.Info("channels: started adapter", "binding", id, "channel", b.Channel, "account", b.AccountID)
	}
	return nil
}

func (m *Manager) start(ctx context.Context, b ChannelBinding) (connHandle, error) {
	if m.creds == nil {
		return connHandle{}, fmt.Errorf("credential source unavailable")
	}
	secret, err := m.creds.Get(ctx, b.CredentialRef)
	if err != nil {
		return connHandle{}, fmt.Errorf("resolve credential %q: %w", b.CredentialRef, err)
	}
	if secret == "" {
		return connHandle{}, fmt.Errorf("empty credential for %q", b.CredentialRef)
	}
	adapter, err := m.build(ctx, b, secret)
	if err != nil {
		return connHandle{}, err
	}
	// The connection outlives the Reload request: give it its own context so
	// a short-lived HTTP request cancel does not tear the adapter down.
	actx, cancel := context.WithCancel(context.Background())
	// Start pumps raw events from the Conn into the adapter's inbound channel;
	// Attach then pumps inbound into the bus. Both are required — without
	// Start the Conn's events are never consumed and nothing reaches the
	// worker (the IM-no-reply bug).
	//
	// A supervised restart keeps a binding connected for the process lifetime:
	// adapters return from Start when their connection ends (which used to
	// leave the binding silently dead until the next manual Reload).
	go m.supervise(actx, b, adapter)
	m.gw.Attach(actx, adapter, Attach{BindingID: b.BindingID, AccountID: b.AccountID, AgentID: b.AgentID})
	return connHandle{adapter: adapter, cancel: cancel}, nil
}

// adapterRestartMaxBackoff caps the restart delay.
const adapterRestartMaxBackoff = 30 * time.Second

// supervise runs one adapter's read loop, restarting it with exponential
// backoff while the binding lives. Start returning is not an error of the
// platform: it means the underlying connection ended (network drop, server
// drain, credential rotation), and the only correct reaction is to connect
// again. Retrying stops as soon as the binding's context is cancelled.
func (m *Manager) supervise(ctx context.Context, b ChannelBinding, adapter Adapter) {
	backoff := time.Second
	for {
		err := adapter.Start(ctx)
		if ctx.Err() != nil {
			return
		}
		slog.Warn("channels: adapter stopped, reconnecting",
			"channel", b.Channel, "binding", b.BindingID, "in", backoff, "err", err)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
		if backoff < adapterRestartMaxBackoff {
			if backoff *= 2; backoff > adapterRestartMaxBackoff {
				backoff = adapterRestartMaxBackoff
			}
		}
	}
}

// Close stops every live adapter and releases their connections. It is called
// on shutdown: without it a node that is re-created (or a gateway whose config
// changed) leaves IM connections open on the platform side until they time out.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, h := range m.conns {
		h.stop()
		delete(m.conns, id)
		slog.Info("channels: closed adapter", "binding", id)
	}
	return nil
}

// SetRateLimiter installs an inbound rate limiter shared by all adapters
// (nil disables limiting).
func (m *Manager) SetRateLimiter(rl RateLimiter) {
	m.gw.SetRateLimiter(rl)
}

// Run follows the outbound stream and dispatches replies (blocks until ctx is
// done).
func (m *Manager) Run(ctx context.Context) error {
	return m.gw.Run(ctx)
}
