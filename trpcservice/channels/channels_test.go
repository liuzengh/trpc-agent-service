package channels

import (
	"context"
	"net/http"
	"testing"

	"github.com/Violet2314/trpc-agent-service/trpcservice/gateway"
	"github.com/Violet2314/trpc-agent-service/trpcservice/reply"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

func TestRegistryRunsAndDispatches(t *testing.T) {
	registry := NewRegistry()
	adapter := &fakeAdapter{channelType: "test", replier: &fakeReplier{}}
	if err := registry.Register(adapter); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := registry.Register(adapter); err == nil {
		t.Fatal("Register() accepted duplicate adapter")
	}
	if err := registry.Run(
		context.Background(),
		http.NewServeMux(),
		func(context.Context, gateway.InboundMessage) (gateway.Result, error) {
			return gateway.Result{}, nil
		},
	); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if adapter.runCalls != 1 {
		t.Fatalf("adapter run calls = %d, want 1", adapter.runCalls)
	}
	events := make(chan reply.Event, 1)
	events <- reply.Event{Type: "done"}
	close(events)
	if err := registry.Dispatch(
		context.Background(),
		tenant.Snapshot{},
		"session",
		gateway.InboundMessage{Channel: "test"},
		events,
	); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	if adapter.replier.(*fakeReplier).events != 1 {
		t.Fatalf("replier event count = %d, want 1", adapter.replier.(*fakeReplier).events)
	}
}

func TestRegistryUnknownAdapterDrains(t *testing.T) {
	registry := NewRegistry()
	events := make(chan reply.Event, 2)
	events <- reply.Event{Type: "text_delta"}
	events <- reply.Event{Type: "done"}
	close(events)
	if err := registry.Dispatch(
		context.Background(), tenant.Snapshot{}, "session",
		gateway.InboundMessage{Channel: "missing"}, events,
	); err == nil {
		t.Fatal("Dispatch() accepted unknown channel")
	}
	if _, ok := <-events; ok {
		t.Fatal("Dispatch() did not drain unknown-channel events")
	}
}

type fakeAdapter struct {
	channelType string
	replier     Replier
	runCalls    int
	runErr      error
}

func (f *fakeAdapter) Type() string { return f.channelType }
func (f *fakeAdapter) Run(context.Context, *http.ServeMux, Sink) error {
	f.runCalls++
	return f.runErr
}
func (f *fakeAdapter) NewReplier(tenant.Snapshot) Replier { return f.replier }

type fakeReplier struct {
	events int
	err    error
}

func (f *fakeReplier) Reply(
	_ context.Context,
	_ string,
	_ gateway.InboundMessage,
	events <-chan reply.Event,
) error {
	for range events {
		f.events++
	}
	return f.err
}
