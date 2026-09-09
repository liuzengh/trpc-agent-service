package openclaw

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/cluster"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
	servicelog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessioncoord"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

type producerBackend struct{ added int }

func (*producerBackend) CreateConsumerGroup(context.Context, string, string) error { return nil }
func (backend *producerBackend) AddStream(context.Context, string, []byte) error {
	backend.added++
	return nil
}
func (*producerBackend) ReadGroup(context.Context, string, string, string, string, int64, time.Duration) ([]cluster.StreamMessage, error) {
	panic("gateway-only component must not consume")
}
func (*producerBackend) AckStream(context.Context, string, string, string) error {
	panic("gateway-only component must not ack")
}

func roleFixture(t *testing.T) (*config.File, Routes, *idempotency.MemoryStore) {
	t.Helper()
	file := &config.File{Tenants: []tenant.Tenant{{ID: "tenant", Enabled: true, ConfigVersion: 1}}}
	routes, err := NewStaticRoutes(Route{TenantID: "tenant", AppID: "app", BindingID: "binding", ChannelType: tenant.ChannelTypeHTTP, ConfigVersion: 1, Credential: "credential"})
	if err != nil {
		t.Fatal(err)
	}
	return file, routes, idempotency.NewMemoryStore()
}

func TestGatewayModeHasNoWorkerRuntimeOrConsumers(t *testing.T) {
	file, routes, inbox := roleFixture(t)
	component, err := NewComponentForMode(context.Background(), "127.0.0.1:0", file, routes, ComponentDependencies{
		Inbox: inbox, QueueBackend: &producerBackend{}, WorkerID: "gateway-a",
	}, GatewayMode)
	if err != nil {
		t.Fatal(err)
	}
	defer component.Close(context.Background())
	if component.server == nil || component.queue != nil || component.poller != nil || component.runtimes != nil || component.cancelBus != nil {
		t.Fatalf("gateway role leaked worker components: %+v", component)
	}
}

func TestGatewayModeDecoratorFailureCleansUpWithoutWorkerState(t *testing.T) {
	file, routes, inbox := roleFixture(t)
	_, err := NewComponentForMode(context.Background(), "127.0.0.1:0", file, routes, ComponentDependencies{
		Inbox: inbox, QueueBackend: &producerBackend{}, WorkerID: "gateway-a",
	}, GatewayMode, func(*Handler, http.Handler) (http.Handler, error) {
		return nil, errors.New("decorator failed")
	})
	if err == nil || err.Error() != "decorator failed" {
		t.Fatalf("error=%v", err)
	}
}

func TestComponentModeRejectsUnknownBits(t *testing.T) {
	file, routes, inbox := roleFixture(t)
	if _, err := NewComponentForMode(context.Background(), "127.0.0.1:0", file, routes, ComponentDependencies{Inbox: inbox}, ComponentMode(8)); err == nil {
		t.Fatal("unknown component mode accepted")
	}
}

func TestWorkerModeHasNoHTTPServer(t *testing.T) {
	file, routes, inbox := roleFixture(t)
	writes := sessioncoord.NewMemoryWriteStore()
	coordinator, err := sessioncoord.NewCoordinator(writes)
	if err != nil {
		t.Fatal(err)
	}
	component, err := NewComponentForMode(context.Background(), "", file, routes, ComponentDependencies{
		Inbox: inbox, Coordinator: coordinator, Writes: writes,
		RuntimeFactory: worker.TestRuntimeFactory(writes),
		Audit:          audit.NewMemoryStore(servicelog.NewRedactor(nil, nil)),
		WorkerID:       "worker-a",
	}, WorkerMode)
	if err != nil {
		t.Fatal(err)
	}
	defer component.Close(context.Background())
	if component.server != nil || component.poller == nil || component.runtimes == nil || component.dispatcher == nil {
		t.Fatalf("worker role boundary is incomplete: %+v", component)
	}
}
