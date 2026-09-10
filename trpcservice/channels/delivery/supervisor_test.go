package delivery

import (
	"context"
	"sync"
	"testing"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
)

type catalogStub struct {
	mu           sync.RWMutex
	destinations []channel.ReplyDestination
}

func (c *catalogStub) ListDeliveryDestinations(context.Context) ([]channel.ReplyDestination, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return append([]channel.ReplyDestination(nil), c.destinations...), nil
}
func (c *catalogStub) set(values ...channel.ReplyDestination) {
	c.mu.Lock()
	c.destinations = append([]channel.ReplyDestination(nil), values...)
	c.mu.Unlock()
}

type supervisedRunner struct {
	started chan<- string
	stopped chan<- string
	key     string
}

type runnerFunc func(context.Context) error

func (f runnerFunc) Run(ctx context.Context) error { return f(ctx) }

func (r supervisedRunner) Run(ctx context.Context) error {
	r.started <- r.key
	<-ctx.Done()
	r.stopped <- r.key
	return ctx.Err()
}

func TestSupervisorAddsRemovesAndDrainsAccountConsumers(t *testing.T) {
	first := channel.ReplyDestination{TenantID: "tenant", Channel: "feishu", ChannelBindingID: "binding-a", ExternalAccountID: "app"}
	second := channel.ReplyDestination{TenantID: "tenant", Channel: "wecom", ChannelBindingID: "binding-b", ExternalAccountID: "corp"}
	catalog := &catalogStub{}
	catalog.set(first)
	started, stopped := make(chan string, 4), make(chan string, 4)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	supervisor := Supervisor{Catalog: catalog, RefreshInterval: 5 * time.Millisecond, NewConsumer: func(destination channel.ReplyDestination) (ConsumerRunner, error) {
		key, _ := deliveryDestinationKey(destination)
		return supervisedRunner{started: started, stopped: stopped, key: key}, nil
	}}
	go func() { done <- supervisor.Run(ctx) }()
	firstKey, _ := deliveryDestinationKey(first)
	secondKey, _ := deliveryDestinationKey(second)
	if got := waitValue(t, started); got != firstKey {
		t.Fatalf("started=%q", got)
	}
	catalog.set(second)
	seen := map[string]bool{waitValue(t, started): true, waitValue(t, stopped): true}
	if !seen[secondKey] || !seen[firstKey] {
		t.Fatalf("transitions=%v", seen)
	}
	cancel()
	if got := waitValue(t, stopped); got != secondKey {
		t.Fatalf("stopped=%q", got)
	}
	if err := <-done; err != context.Canceled {
		t.Fatalf("err=%v", err)
	}
}

func TestSupervisorRejectsDuplicateOrVersionedCatalogEntries(t *testing.T) {
	destination := channel.ReplyDestination{TenantID: "tenant", Channel: "feishu", ChannelBindingID: "binding", ExternalAccountID: "app"}
	for _, values := range [][]channel.ReplyDestination{{destination, destination}, {{TenantID: "tenant", Channel: "feishu", ChannelBindingID: "binding", ExternalAccountID: "app", ConfigVersion: 1}}} {
		catalog := &catalogStub{destinations: values}
		err := (Supervisor{Catalog: catalog, RefreshInterval: time.Second, NewConsumer: func(channel.ReplyDestination) (ConsumerRunner, error) { return supervisedRunner{}, nil }}).Run(context.Background())
		if err == nil {
			t.Fatalf("values=%#v accepted", values)
		}
	}
}

func TestSupervisorRestartsUnexpectedlyExitedConsumer(t *testing.T) {
	destination := channel.ReplyDestination{TenantID: "tenant", Channel: "feishu", ChannelBindingID: "binding", ExternalAccountID: "app"}
	catalog := &catalogStub{destinations: []channel.ReplyDestination{destination}}
	starts := make(chan struct{}, 3)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	supervisor := Supervisor{Catalog: catalog, RefreshInterval: 5 * time.Millisecond, NewConsumer: func(channel.ReplyDestination) (ConsumerRunner, error) {
		return runnerFunc(func(context.Context) error { starts <- struct{}{}; return context.DeadlineExceeded }), nil
	}}
	go func() { done <- supervisor.Run(ctx) }()
	for index := 0; index < 2; index++ {
		select {
		case <-starts:
		case <-time.After(time.Second):
			t.Fatal("consumer was not restarted")
		}
	}
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("err=%v", err)
	}
}

func waitValue(t *testing.T, values <-chan string) string {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(time.Second):
		t.Fatal("timed out")
		return ""
	}
}
