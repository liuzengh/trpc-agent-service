package openclaw

import (
	"context"
	"errors"
	"net/http"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/cluster"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/dispatcher"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
	servicelog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	servicemetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/policy"
	serviceruntime "github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessioncoord"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// LocalComponent is a minimal runnable single-process composition for development.
// Production should inject SQLStore, RedisCoordinator, SQLWriteStore, and a queue.
type LocalComponent struct {
	server     *Server
	poller     *worker.InboxPoller
	dispatcher *dispatcher.Dispatcher
	queue      *cluster.WorkQueue
	cancelBus  *cluster.CancelBus
	runtimes   *serviceruntime.Manager
}

// ComponentMode selects which process responsibilities are started. Gateway
// processes own HTTP ingress and only produce queue entries; Worker processes
// own Runner consumption and Inbox recovery. Combined preserves the local and
// Compose-compatible all-in-one topology.
type ComponentMode uint8

const (
	GatewayMode ComponentMode = 1 << iota
	WorkerMode
	CombinedMode = GatewayMode | WorkerMode
)

func (mode ComponentMode) gatewayEnabled() bool { return mode&GatewayMode != 0 }
func (mode ComponentMode) workerEnabled() bool  { return mode&WorkerMode != 0 }
func (mode ComponentMode) valid() bool {
	return mode != 0 && mode&^CombinedMode == 0
}

// HandlerDecorator mounts a provider adapter around the shared local Gateway.
type HandlerDecorator func(*Handler, http.Handler) (http.Handler, error)

// ComponentDependencies selects durable stores without coupling the HTTP
// protocol package to concrete database clients.
type ComponentDependencies struct {
	Inbox             idempotency.ReadyStore
	Coordinator       sessioncoord.LeaseCoordinator
	Writes            worker.PlatformStore
	RuntimeFactory    serviceruntime.Factory
	Audit             audit.Store
	EventBus          EventBus
	Status            StatusStore
	QueueBackend      cluster.StreamBackend
	ControlBackend    cluster.PubSubBackend
	Cancellations     worker.CancellationStore
	RunLimiter        worker.RunLimiter
	Admission         AdmissionLimiter
	Policy            *policy.Engine
	WorkerID          string
	WorkerConcurrency int
	// Readiness checks production dependencies without exposing driver errors.
	// Nil is appropriate only for the self-contained offline component.
	Readiness func(context.Context) error
	// Snapshots optionally resolves pinned published versions from the
	// control-plane store. Nil falls back to the static startup file, which
	// is only appropriate for offline development.
	Snapshots gateway.SnapshotResolver
}

// NewLocalComponent wires the complete offline HTTP -> Runner -> Outbox chain.
func NewLocalComponent(parent context.Context, address string, file *config.File, routes Routes, decorators ...HandlerDecorator) (*LocalComponent, error) {
	if parent == nil || address == "" || file == nil || routes == nil {
		return nil, errors.New("openclaw: context, address, config, and routes are required")
	}
	writes := sessioncoord.NewMemoryWriteStore()
	coordinator, err := sessioncoord.NewCoordinator(writes)
	if err != nil {
		return nil, err
	}
	inbox := idempotency.NewMemoryStore()
	redactor := servicelog.NewRedactor(nil, nil)
	return NewComponent(parent, address, file, routes, ComponentDependencies{
		Inbox: inbox, Coordinator: coordinator, Writes: writes,
		RuntimeFactory: worker.TestRuntimeFactory(writes), Audit: audit.NewMemoryStore(redactor),
		WorkerID: "local-worker",
	}, decorators...)
}

// NewComponent wires a single-process Gateway and Worker to injected durable
// stores. The caller owns the database and Redis clients behind dependencies.
func NewComponent(parent context.Context, address string, file *config.File, routes Routes, dependencies ComponentDependencies, decorators ...HandlerDecorator) (*LocalComponent, error) {
	return NewComponentForMode(parent, address, file, routes, dependencies, CombinedMode, decorators...)
}

// NewComponentForMode constructs a real role boundary rather than merely
// hiding endpoints. A Gateway-only component has no Runtime Manager, queue
// consumer, cancel subscription, or Inbox recovery poller. A Worker-only
// component has no listener or channel/Admin HTTP surface.
func NewComponentForMode(parent context.Context, address string, file *config.File, routes Routes, dependencies ComponentDependencies, mode ComponentMode, decorators ...HandlerDecorator) (*LocalComponent, error) {
	if parent == nil || file == nil || !mode.valid() {
		return nil, errors.New("openclaw: context, config, and at least one process role are required")
	}
	if dependencies.Inbox == nil {
		return nil, errors.New("openclaw: inbox is required")
	}
	if mode.gatewayEnabled() && (address == "" || routes == nil) {
		return nil, errors.New("openclaw: gateway address and routes are required")
	}
	if mode.workerEnabled() && (dependencies.Coordinator == nil || dependencies.Writes == nil || dependencies.RuntimeFactory == nil || dependencies.Audit == nil) {
		return nil, errors.New("openclaw: worker coordinator, writes, runtime factory, and audit are required")
	}
	if dependencies.WorkerID == "" {
		dependencies.WorkerID = "worker"
	}
	bus := dependencies.EventBus
	if bus == nil {
		bus = NewHub()
	}
	status := dependencies.Status
	if status == nil {
		status = NewRegistry()
	}
	telemetry, err := servicemetrics.New("trpc-agent-service")
	if err != nil {
		return nil, err
	}
	redactor := servicelog.NewRedactor(nil, nil)
	policyEngine := dependencies.Policy
	if policyEngine == nil {
		policyEngine = &policy.Engine{
			Identity:           policy.AuthenticatedIdentityAuthorizer{},
			Budgets:            policy.NewMemoryBudget(),
			Approvals:          policy.NewMemoryApprovals(),
			CostMicrosPerToken: 1, // deterministic local accounting for the mock model.
		}
	}
	snapshots := dependencies.Snapshots
	if snapshots == nil {
		snapshots = gateway.FileSnapshotResolver{File: file}
	}
	component := &LocalComponent{}
	var processor *worker.Processor
	if mode.workerEnabled() {
		component.runtimes, err = serviceruntime.NewManager(dependencies.RuntimeFactory)
		if err != nil {
			return nil, err
		}
		processor = &worker.Processor{WorkerID: dependencies.WorkerID, Inbox: dependencies.Inbox, Coordinator: dependencies.Coordinator, Writes: dependencies.Writes, Runtimes: component.runtimes, Snapshots: snapshots, Publisher: MultiPublisher{bus, status}, Policy: policyEngine, Audit: dependencies.Audit, Redactor: redactor, Telemetry: telemetry, Cancellations: dependencies.Cancellations, RunLimiter: dependencies.RunLimiter}
	}
	var cancelBus *cluster.CancelBus
	var canceler Canceler
	if mode.workerEnabled() && dependencies.ControlBackend != nil {
		durable, ok := status.(cluster.DurableCanceler)
		if !ok {
			_ = component.runtimes.Close(context.Background())
			return nil, errors.New("openclaw: shared control backend requires a durable cancel status store")
		}
		cancelBus, err = cluster.NewCancelBus(parent, dependencies.ControlBackend, durable, processor.Cancel, "")
		if err != nil {
			_ = component.runtimes.Close(context.Background())
			return nil, err
		}
		if mode.gatewayEnabled() {
			canceler = cancelBus
		}
	} else if mode.gatewayEnabled() && dependencies.ControlBackend != nil {
		durable, ok := status.(cluster.DurableCanceler)
		if !ok {
			return nil, errors.New("openclaw: shared control backend requires a durable cancel status store")
		}
		canceler, err = cluster.NewCancelPublisher(dependencies.ControlBackend, durable, "")
		if err != nil {
			return nil, err
		}
	} else if mode.gatewayEnabled() {
		canceler = processor
	}
	var dispatch *dispatcher.Dispatcher
	var queue *cluster.WorkQueue
	var submitter worker.RequestSubmitter
	if mode.workerEnabled() && dependencies.QueueBackend != nil {
		queue, err = cluster.NewWorkQueue(parent, dependencies.QueueBackend, processor.Process, cluster.WorkQueueConfig{NodeID: dependencies.WorkerID, Concurrency: dependencies.WorkerConcurrency})
		if err != nil {
			_ = cancelBus.Close()
			_ = component.runtimes.Close(context.Background())
			return nil, err
		}
		submitter = queue
	} else if mode.workerEnabled() {
		dispatch, err = dispatcher.New(parent, processor.Process)
		if err != nil {
			_ = cancelBus.Close()
			_ = component.runtimes.Close(context.Background())
			return nil, err
		}
		submitter = dispatch
	}
	if mode.gatewayEnabled() && !mode.workerEnabled() {
		if dependencies.QueueBackend == nil {
			_ = cancelBus.Close()
			return nil, errors.New("openclaw: gateway-only role requires a shared queue backend")
		}
		submitter, err = cluster.NewWorkSubmitter(parent, dependencies.QueueBackend, "")
		if err != nil {
			_ = cancelBus.Close()
			return nil, err
		}
	}
	if mode.workerEnabled() {
		component.poller, err = worker.NewInboxPoller(parent, dependencies.Inbox, submitter, worker.InboxPollerConfig{Owner: dependencies.WorkerID + ":inbox"})
		if err != nil {
			if dispatch != nil {
				_ = dispatch.Close(context.Background())
			}
			if queue != nil {
				_ = queue.Close(context.Background())
			}
			_ = cancelBus.Close()
			_ = component.runtimes.Close(context.Background())
			return nil, err
		}
	}
	if mode.gatewayEnabled() {
		core := &Handler{Routes: routes, Inbox: dependencies.Inbox, Submitter: submitter, Hub: bus, Status: status, Canceler: canceler, Approver: policyEngine, ClaimOwner: dependencies.WorkerID + ":gateway", Telemetry: telemetry, Readiness: dependencies.Readiness, Admission: dependencies.Admission}
		var handler http.Handler = core.RoutesHandler()
		for _, decorate := range decorators {
			if decorate == nil {
				continue
			}
			handler, err = decorate(core, handler)
			if err != nil {
				if component.poller != nil {
					_ = component.poller.Close(context.Background())
				}
				if dispatch != nil {
					_ = dispatch.Close(context.Background())
				}
				if queue != nil {
					_ = queue.Close(context.Background())
				}
				_ = cancelBus.Close()
				if component.runtimes != nil {
					_ = component.runtimes.Close(context.Background())
				}
				return nil, err
			}
		}
		// Instrument the final surface so provider callbacks and every channel
		// adapter extract traceparent before entering the shared Inbox pipeline.
		// Keep one bounded span name instead of using tenant-bearing URL paths.
		handler = otelhttp.NewHandler(handler, "http.server", otelhttp.WithFilter(func(request *http.Request) bool {
			return request.URL.Path != "/healthz" && request.URL.Path != "/readyz"
		}))
		component.server = &Server{Address: address, Handler: handler}
	}
	component.dispatcher, component.queue, component.cancelBus = dispatch, queue, cancelBus
	return component, nil
}

// Start starts the local HTTP gateway.
func (component *LocalComponent) Start(ctx context.Context) error {
	if component.server == nil {
		return nil
	}
	return component.server.Start(ctx)
}

// Close drains ingress, queued requests, and Runtime Bundles in dependency order.
func (component *LocalComponent) Close(ctx context.Context) error {
	var serverErr, pollerErr, dispatchErr, queueErr, runtimeErr error
	if component.server != nil {
		serverErr = component.server.Close(ctx)
	}
	if component.poller != nil {
		pollerErr = component.poller.Close(ctx)
	}
	if component.dispatcher != nil {
		dispatchErr = component.dispatcher.Close(ctx)
	}
	if component.queue != nil {
		queueErr = component.queue.Close(ctx)
	}
	if component.runtimes != nil {
		runtimeErr = component.runtimes.Close(ctx)
	}
	return errors.Join(serverErr, pollerErr, dispatchErr, queueErr, component.cancelBus.Close(), runtimeErr)
}
