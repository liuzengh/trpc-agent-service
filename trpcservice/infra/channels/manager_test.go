package channels

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/bus"
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
// event-pumping loop. It then blocks like a live connection: Start returning is
// what the manager treats as "connection ended, reconnect", so a double that
// returns immediately would look like a flapping connection.
type startedAdapter struct {
	fakeAdapter
	started chan struct{}
}

func (a *startedAdapter) Start(ctx context.Context) error {
	select {
	case a.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return nil
}

func TestManagerReloadStartsAdapterLoop(t *testing.T) {
	ctx := context.Background()
	b := &fakeBus{published: make(chan *bus.Message, 8)}
	store := NewMemBindingStore()
	creds := &fakeCreds{values: map[string]string{"sec-1": "s"}}
	started := make(chan struct{}, 8)
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

// reconnectingAdapter returns from Start immediately, standing in for a
// connection that ended (network drop, server drain).
type reconnectingAdapter struct {
	fakeAdapter
	starts   chan struct{}
	stopOnce sync.Once
	stopped  chan struct{}
}

func (a *reconnectingAdapter) Start(context.Context) error {
	a.starts <- struct{}{}
	return errors.New("connection closed")
}

func (a *reconnectingAdapter) Stop(context.Context) error {
	a.stopOnce.Do(func() { close(a.stopped) })
	return nil
}

// TestManagerRestartsAdapterAfterDisconnect covers the silent-death gap: an
// adapter whose connection ends used to stay dead until an operator triggered a
// Reload, so the binding looked configured while its bot had gone deaf.
func TestManagerRestartsAdapterAfterDisconnect(t *testing.T) {
	ctx := context.Background()
	b := &fakeBus{published: make(chan *bus.Message, 8)}
	store := NewMemBindingStore()
	creds := &fakeCreds{values: map[string]string{"sec-1": "s"}}
	adapter := &reconnectingAdapter{
		fakeAdapter: fakeAdapter{name: ChannelWeCom, inbound: make(chan *InboundMessage, 8), sent: make(chan *OutboundMessage, 8)},
		starts:      make(chan struct{}, 8),
		stopped:     make(chan struct{}),
	}
	m := NewManager(b, store, creds, func(context.Context, ChannelBinding, string) (Adapter, error) {
		return adapter, nil
	})
	_ = store.Create(ctx, ChannelBinding{
		BindingID: "b1", TenantID: "t1", AgentID: "a1",
		Channel: ChannelWeCom, AccountID: "corp-1", CredentialRef: "sec-1",
	})
	if err := m.Reload(ctx); err != nil {
		t.Fatal(err)
	}

	// Two starts prove the loop: the first from Reload, the second from the
	// supervised restart after the connection ended.
	for i := 1; i <= 2; i++ {
		select {
		case <-adapter.starts:
		case <-time.After(5 * time.Second):
			t.Fatalf("adapter was started %d time(s), want it reconnected after the disconnect", i-1)
		}
	}

	// Close stops the adapter and its restart loop.
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-adapter.stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not stop the adapter")
	}
	// After Close no further restart may happen: the node is shutting down.
	drained := len(adapter.starts)
	time.Sleep(1200 * time.Millisecond)
	if got := len(adapter.starts); got > drained {
		t.Errorf("adapter restarted %d more time(s) after Close", got-drained)
	}
}
