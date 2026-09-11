package channels

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/bus"
)

// fakeWebhookAdapter records the payloads fed to it, standing in for a
// webhook-mode channel adapter.
type fakeWebhookAdapter struct {
	fakeAdapter
	feeds chan []byte
	full  bool
}

func (a *fakeWebhookAdapter) Feed(payload []byte) bool {
	if a.full {
		return false
	}
	if a.feeds != nil {
		a.feeds <- payload
	}
	return true
}

// okVerifier accepts anything and echoes the body as the event payload.
func okVerifier(_ string, cb WebhookCallback) (WebhookEvent, error) {
	return WebhookEvent{Payload: cb.Body}, nil
}

// newWebhookManager wires a manager with one webhook-served channel.
func newWebhookManager(t *testing.T, verifier WebhookVerifier, adapter WebhookAdapter) (*Manager, BindingStore) {
	t.Helper()
	store := NewMemBindingStore()
	m := NewManager(&fakeBus{published: make(chan *bus.Message, 8)}, store,
		&fakeCreds{values: map[string]string{"sec-1": "app-secret", "vt-1": "encrypt-key"}}, nil)
	if verifier == nil {
		verifier = okVerifier
	}
	build := func(context.Context, ChannelBinding, string) (WebhookAdapter, error) {
		return adapter, nil
	}
	if err := m.EnableWebhook(WebhookConfig{
		Channels:  map[string]bool{ChannelFeishu: true},
		Verifiers: map[string]WebhookVerifier{ChannelFeishu: verifier},
		Builders:  map[string]WebhookBuilder{ChannelFeishu: build},
	}); err != nil {
		t.Fatalf("EnableWebhook: %v", err)
	}
	return m, store
}

func bindingFixture(t *testing.T, store BindingStore) {
	t.Helper()
	if err := store.Create(context.Background(), ChannelBinding{
		BindingID: "b1", TenantID: "t1", AgentID: "a1",
		Channel: ChannelFeishu, AccountID: "cli_1",
		CredentialRef: "sec-1", VerificationTokenRef: "vt-1",
	}); err != nil {
		t.Fatalf("create binding: %v", err)
	}
}

func TestWebhookIngressDisabledByDefault(t *testing.T) {
	m := NewManager(&fakeBus{}, NewMemBindingStore(), nil, nil)
	if _, err := m.HandleWebhook(context.Background(), "b1", WebhookCallback{}); !errors.Is(err, ErrWebhookUnsupported) {
		t.Fatalf("err = %v, want ErrWebhookUnsupported", err)
	}
	if m.WebhookChannelServed(ChannelFeishu) {
		t.Error("no channel may be served before the ingress is enabled")
	}
}

// TestWebhookEnableRequiresVerifierAndBuilder: a half-installed ingress would
// accept callbacks it cannot authenticate or route, so it is a config error.
func TestWebhookEnableRequiresVerifierAndBuilder(t *testing.T) {
	m := NewManager(&fakeBus{}, NewMemBindingStore(), nil, nil)
	err := m.EnableWebhook(WebhookConfig{Channels: map[string]bool{ChannelFeishu: true}})
	if err == nil {
		t.Fatal("a channel without verifier/builder must be rejected")
	}
	if !strings.Contains(err.Error(), ChannelFeishu) {
		t.Errorf("error should name the channel: %v", err)
	}
}

func TestWebhookFeedsAdapter(t *testing.T) {
	adapter := &fakeWebhookAdapter{
		fakeAdapter: fakeAdapter{name: ChannelFeishu, inbound: make(chan *InboundMessage, 8), sent: make(chan *OutboundMessage, 8)},
		feeds:       make(chan []byte, 4),
	}
	m, store := newWebhookManager(t, nil, adapter)
	bindingFixture(t, store)

	body := []byte(`{"header":{"event_id":"ev1"}}`)
	event, err := m.HandleWebhook(context.Background(), "b1", WebhookCallback{Body: body, Signature: "x"})
	if err != nil {
		t.Fatalf("HandleWebhook: %v", err)
	}
	if event.Challenge != "" {
		t.Errorf("challenge = %q, want none", event.Challenge)
	}
	select {
	case got := <-adapter.feeds:
		if string(got) != string(body) {
			t.Errorf("fed payload = %s, want %s", got, body)
		}
	case <-time.After(time.Second):
		t.Fatal("payload was never handed to the adapter")
	}
	// The lazily built adapter must be registered so replies can be routed back
	// and shutdown can stop it.
	if _, ok := m.adapterByBinding("b1"); !ok {
		t.Error("webhook adapter must be registered as the binding's live adapter")
	}
}

// TestWebhookReloadLeavesServedChannelDisconnected: serving callbacks and
// opening a long connection at the same time would deliver every event twice.
func TestWebhookReloadLeavesServedChannelDisconnected(t *testing.T) {
	adapter := &fakeWebhookAdapter{
		fakeAdapter: fakeAdapter{name: ChannelFeishu, inbound: make(chan *InboundMessage, 8), sent: make(chan *OutboundMessage, 8)},
		feeds:       make(chan []byte, 4),
	}
	built := make(chan string, 4)
	store := NewMemBindingStore()
	m := NewManager(&fakeBus{}, store, &fakeCreds{values: map[string]string{"sec-1": "app-secret", "vt-1": "k"}},
		func(_ context.Context, b ChannelBinding, _ string) (Adapter, error) {
			built <- b.BindingID
			return adapter, nil
		})
	if err := m.EnableWebhook(WebhookConfig{
		Channels:  map[string]bool{ChannelFeishu: true},
		Verifiers: map[string]WebhookVerifier{ChannelFeishu: okVerifier},
		Builders:  map[string]WebhookBuilder{ChannelFeishu: func(context.Context, ChannelBinding, string) (WebhookAdapter, error) { return adapter, nil }},
	}); err != nil {
		t.Fatal(err)
	}
	bindingFixture(t, store)
	if err := m.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-built:
		t.Fatalf("long connection opened for a webhook-served binding: %s", id)
	case <-time.After(200 * time.Millisecond):
		// expected
	}
}

func TestWebhookRejectsUnservedChannel(t *testing.T) {
	adapter := &fakeWebhookAdapter{fakeAdapter: fakeAdapter{name: ChannelFeishu}}
	m, store := newWebhookManager(t, nil, adapter)
	// A binding on a channel that runs on a long connection must not be
	// callable over HTTP: that would double-ingest every event.
	if err := store.Create(context.Background(), ChannelBinding{
		BindingID: "b2", TenantID: "t1", AgentID: "a1",
		Channel: ChannelWeCom, AccountID: "corp-1", CredentialRef: "sec-1",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.HandleWebhook(context.Background(), "b2", WebhookCallback{Body: []byte("{}")}); !errors.Is(err, ErrWebhookUnsupported) {
		t.Fatalf("err = %v, want ErrWebhookUnsupported", err)
	}
}

func TestWebhookUnknownBinding(t *testing.T) {
	adapter := &fakeWebhookAdapter{fakeAdapter: fakeAdapter{name: ChannelFeishu}}
	m, _ := newWebhookManager(t, nil, adapter)
	if _, err := m.HandleWebhook(context.Background(), "nope", WebhookCallback{Body: []byte("{}")}); !errors.Is(err, ErrBindingNotFound) {
		t.Fatalf("err = %v, want ErrBindingNotFound", err)
	}
}

// TestWebhookFailsClosedWithoutSecret: an endpoint on the public internet must
// refuse a callback it cannot authenticate — never fall through to "accept it".
func TestWebhookFailsClosedWithoutSecret(t *testing.T) {
	adapter := &fakeWebhookAdapter{fakeAdapter: fakeAdapter{name: ChannelFeishu}}
	m, store := newWebhookManager(t, nil, adapter)
	if err := store.Create(context.Background(), ChannelBinding{
		BindingID: "b3", TenantID: "t1", AgentID: "a1",
		Channel: ChannelFeishu, AccountID: "cli_3", CredentialRef: "sec-1", // no VerificationTokenRef
	}); err != nil {
		t.Fatal(err)
	}
	_, err := m.HandleWebhook(context.Background(), "b3", WebhookCallback{Body: []byte("{}")})
	if !errors.Is(err, ErrWebhookUnauthorized) {
		t.Fatalf("err = %v, want ErrWebhookUnauthorized", err)
	}

	// A missing secret in the store is equally fatal.
	if err := store.Create(context.Background(), ChannelBinding{
		BindingID: "b4", TenantID: "t1", AgentID: "a1",
		Channel: ChannelFeishu, AccountID: "cli_4", CredentialRef: "sec-1", VerificationTokenRef: "missing-ref",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.HandleWebhook(context.Background(), "b4", WebhookCallback{Body: []byte("{}")}); !errors.Is(err, ErrWebhookUnauthorized) {
		t.Fatalf("err = %v, want ErrWebhookUnauthorized", err)
	}
}

func TestWebhookVerifierFailureIsUnauthorized(t *testing.T) {
	reject := func(string, WebhookCallback) (WebhookEvent, error) {
		return WebhookEvent{}, errors.New("signature mismatch")
	}
	adapter := &fakeWebhookAdapter{fakeAdapter: fakeAdapter{name: ChannelFeishu}, feeds: make(chan []byte, 1)}
	m, store := newWebhookManager(t, reject, adapter)
	bindingFixture(t, store)
	_, err := m.HandleWebhook(context.Background(), "b1", WebhookCallback{Body: []byte("{}")})
	if !errors.Is(err, ErrWebhookUnauthorized) {
		t.Fatalf("err = %v, want ErrWebhookUnauthorized", err)
	}
	select {
	case <-adapter.feeds:
		t.Error("a rejected callback must never reach the adapter")
	default:
	}
}

func TestWebhookChallengeIsEchoedWithoutFeeding(t *testing.T) {
	handshake := func(string, WebhookCallback) (WebhookEvent, error) {
		return WebhookEvent{Challenge: "chal-1"}, nil
	}
	adapter := &fakeWebhookAdapter{fakeAdapter: fakeAdapter{name: ChannelFeishu}, feeds: make(chan []byte, 1)}
	m, store := newWebhookManager(t, handshake, adapter)
	bindingFixture(t, store)
	event, err := m.HandleWebhook(context.Background(), "b1", WebhookCallback{Body: []byte("{}")})
	if err != nil {
		t.Fatalf("HandleWebhook: %v", err)
	}
	if event.Challenge != "chal-1" {
		t.Errorf("challenge = %q, want chal-1", event.Challenge)
	}
	select {
	case <-adapter.feeds:
		t.Error("a handshake must not be fed to the adapter as an event")
	default:
	}
}

// TestWebhookBusyAsksForRetry: when the adapter cannot accept the event the
// caller must learn it, so the platform retries instead of losing the message.
func TestWebhookBusyAsksForRetry(t *testing.T) {
	adapter := &fakeWebhookAdapter{fakeAdapter: fakeAdapter{name: ChannelFeishu}, full: true}
	m, store := newWebhookManager(t, nil, adapter)
	bindingFixture(t, store)
	if _, err := m.HandleWebhook(context.Background(), "b1", WebhookCallback{Body: []byte("{}")}); !errors.Is(err, ErrWebhookBusy) {
		t.Fatalf("err = %v, want ErrWebhookBusy", err)
	}
}

// TestWebhookAdapterBuildFailureSurfaces: a missing app credential must be a
// visible error, not a silently dropped event.
func TestWebhookAdapterBuildFailureSurfaces(t *testing.T) {
	adapter := &fakeWebhookAdapter{fakeAdapter: fakeAdapter{name: ChannelFeishu}}
	store := NewMemBindingStore()
	m := NewManager(&fakeBus{}, store, &fakeCreds{values: map[string]string{"vt-1": "k"}}, nil)
	served := map[string]bool{ChannelFeishu: true}
	if err := m.EnableWebhook(WebhookConfig{
		Channels:  served,
		Verifiers: map[string]WebhookVerifier{ChannelFeishu: okVerifier},
		Builders:  map[string]WebhookBuilder{ChannelFeishu: func(context.Context, ChannelBinding, string) (WebhookAdapter, error) { return adapter, nil }},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(context.Background(), ChannelBinding{
		BindingID: "b9", TenantID: "t1", AgentID: "a1",
		Channel: ChannelFeishu, AccountID: "cli_9", CredentialRef: "missing", VerificationTokenRef: "vt-1",
	}); err != nil {
		t.Fatal(err)
	}
	_, err := m.HandleWebhook(context.Background(), "b9", WebhookCallback{Body: []byte("{}")})
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("err = %v, want a credential resolution failure", err)
	}
	if errors.Is(err, ErrWebhookBusy) || errors.Is(err, ErrWebhookUnauthorized) {
		t.Errorf("a credential failure must not masquerade as %v", err)
	}
}

// Verify the fake reaches the Manager's feed path through the interface.
var _ WebhookAdapter = (*fakeWebhookAdapter)(nil)
