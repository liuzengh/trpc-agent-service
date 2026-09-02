package channels

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/bus"
)

type fakeCreds struct {
	values map[string]string
}

func (f *fakeCreds) Get(_ context.Context, key string) (string, error) {
	if v, ok := f.values[key]; ok {
		return v, nil
	}
	return "", fmt.Errorf("credential not found: %s", key)
}

func TestManagerReloadStartsAndStops(t *testing.T) {
	ctx := context.Background()
	b := &fakeBus{published: make(chan *bus.Message, 8)}
	store := NewMemBindingStore()
	creds := &fakeCreds{values: map[string]string{"sec-1": "the-secret"}}
	built := make(chan string, 8)
	build := func(_ context.Context, b ChannelBinding, secret string) (Adapter, error) {
		built <- b.BindingID
		return &fakeAdapter{name: b.Channel, inbound: make(chan *InboundMessage, 8), sent: make(chan *OutboundMessage, 8)}, nil
	}
	m := NewManager(b, store, creds, build)

	_ = store.Create(ctx, ChannelBinding{
		BindingID: "b1", TenantID: "t1", AgentID: "a1",
		Channel: ChannelWeCom, AccountID: "corp-1", CredentialRef: "sec-1",
	})
	if err := m.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-built:
		if id != "b1" {
			t.Errorf("built binding = %q, want b1", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("adapter not built for binding b1")
	}

	// Deleting the binding and reloading must stop the adapter.
	_ = store.Delete(ctx, "b1")
	if err := m.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	n := len(m.conns)
	m.mu.Unlock()
	if n != 0 {
		t.Errorf("conns after delete = %d, want 0", n)
	}
}

func TestManagerSkipsBindingWithoutCredential(t *testing.T) {
	ctx := context.Background()
	b := &fakeBus{published: make(chan *bus.Message, 8)}
	store := NewMemBindingStore()
	creds := &fakeCreds{values: map[string]string{}}
	built := make(chan string, 8)
	build := func(_ context.Context, b ChannelBinding, secret string) (Adapter, error) {
		built <- b.BindingID
		return &fakeAdapter{name: b.Channel}, nil
	}
	m := NewManager(b, store, creds, build)

	_ = store.Create(ctx, ChannelBinding{
		BindingID: "b1", TenantID: "t1", AgentID: "a1",
		Channel: ChannelWeCom, AccountID: "corp-1", // no CredentialRef
	})
	if err := m.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-built:
		t.Errorf("adapter built for a binding without credential: %q", id)
	case <-time.After(200 * time.Millisecond):
		// expected: nothing built
	}
}

// startedAdapter records that Start was invoked — the regression guard for the
// IM-no-reply bug where Reload attached the adapter but never started its
// event-pumping loop.
type startedAdapter struct {
	fakeAdapter
	started chan struct{}
}

func (a *startedAdapter) Start(context.Context) error {
	close(a.started)
	return nil
}

func TestManagerReloadStartsAdapterLoop(t *testing.T) {
	ctx := context.Background()
	b := &fakeBus{published: make(chan *bus.Message, 8)}
	store := NewMemBindingStore()
	creds := &fakeCreds{values: map[string]string{"sec-1": "s"}}
	started := make(chan struct{})
	build := func(_ context.Context, b ChannelBinding, secret string) (Adapter, error) {
		return &startedAdapter{
			fakeAdapter: fakeAdapter{name: b.Channel, inbound: make(chan *InboundMessage, 8), sent: make(chan *OutboundMessage, 8)},
			started:     started,
		}, nil
	}
	m := NewManager(b, store, creds, build)

	_ = store.Create(ctx, ChannelBinding{
		BindingID: "b1", TenantID: "t1", AgentID: "a1",
		Channel: ChannelWeCom, AccountID: "corp-1", CredentialRef: "sec-1",
	})
	if err := m.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
		// adapter.Start was invoked: raw Conn events now flow into inbound.
	case <-time.After(2 * time.Second):
		t.Fatal("adapter.Start was not called after Reload (IM-no-reply bug)")
	}
}
