package bootstrap

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/inbound/httpadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/backendmigration"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/finalartifacthttp"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/manifestadapter"
	ledgerpg "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/postgresadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/runtimeadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/trpcagent"
	execution "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/infra/natsadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/infra/telemetry"
	workermanagement "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/management"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/adapter/outbound/controlhttp"
	projectionpg "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/adapter/outbound/postgresadapter"
	manifestapp "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/toolapproval"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type poller interface {
	Poll(context.Context) (bool, error)
}
type processor interface {
	Advance(context.Context, domain.Run) error
}
type readyLedger interface {
	Scheduled(context.Context, int) ([]execution.ScheduledRun, error)
}
type App struct {
	tracing                                                                          *telemetrytrace.Runtime
	config                                                                           Config
	pool                                                                             *pgxpool.Pool
	nc                                                                               *nats.Conn
	controlTransport                                                                 *http.Transport
	ledger                                                                           *ledgerpg.Ledger
	projection                                                                       manifestapp.Projection
	observation                                                                      *telemetry.Recorder
	executor                                                                         processor
	runs, manifests                                                                  poller
	reply                                                                            *natsadapter.ReplyRelay
	exporter                                                                         *controlhttp.Exporter
	manifestStream                                                                   jetstream.Stream
	manifestBroker                                                                   jetstream.Consumer
	runStream                                                                        jetstream.Stream
	replyStream                                                                      jetstream.Stream
	replyCreated                                                                     time.Time
	runBroker                                                                        jetstream.Consumer
	manifestCreated, manifestConsumerCreated, runCreated, runConsumerCreated         time.Time
	healthServer, proofServer                                                        *http.Server
	proofTLSConfig                                                                   *tls.Config
	initialized, storageHealthy, runHealthy, manifestHealthy, replyHealthy, draining atomic.Bool
	active                                                                           atomic.Int64
	work                                                                             sync.WaitGroup
	attempts                                                                         sync.WaitGroup
	addresses                                                                        atomic.Pointer[listenAddresses]
	closed                                                                           sync.Once
}

func New(ctx context.Context, c Config) (*App, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	a := &App{config: c}
	success := false
	defer func() {
		if !success {
			a.closeOwned()
		}
	}()
	var err error
	a.tracing, err = telemetrytrace.New(ctx, c.Tracing, telemetrytrace.Identity{Service: "agent-worker", Instance: c.WorkerID, Environment: os.Getenv("DEPLOYMENT_ENVIRONMENT")})
	if err != nil {
		return nil, err
	}
	a.observation, err = telemetry.New(ctx, c.WorkerID, os.Stdout, c.Telemetry.Export(), a.tracing.Resource())
	if err != nil {
		return nil, err
	}
	a.proofTLSConfig, err = c.proofTLS()
	if err != nil {
		return nil, err
	}
	client, tr, err := c.controlClient()
	if err != nil {
		return nil, err
	}
	a.controlTransport = tr
	a.pool, err = openDatabase(ctx, c)
	if err != nil {
		return nil, err
	}
	a.ledger = ledgerpg.New(a.pool)
	a.ledger.Tracer = a.tracing.Tracer("agent-worker/execution-v1")
	a.projection = observedProjection{Projection: projectionpg.New(a.pool), observer: a.observation}
	reader := manifestadapter.Reader{Tracer: a.tracing.Tracer("agent-worker/execution-v1"), Projection: a.projection, ContractDigest: c.PlatformContractDigest}
	approvals, err := toolapproval.New(a.pool)
	if err != nil {
		return nil, errors.New("configure tool approval store")
	}
	if err = approvals.ReconcileInterrupted(ctx, c.WorkerID); err != nil {
		return nil, errors.New("reconcile interrupted tool approvals")
	}
	factory, err := runtimeadapter.New(runtimeadapter.Options{ArtifactPool: a.pool, ApprovalStore: approvals, Tracer: a.tracing.Tracer("agent-worker/execution-v1"), BaseURL: c.ControlURL, Client: client, RequestTimeout: c.Timing.RequestTimeout.Value(), MaxResponseBytes: c.Limits.MaxCredentialResponseBytes, SnapshotCapacityBytes: c.Limits.MaxSnapshotBytes, DrainTimeout: c.Timing.SDKDrainTimeout.Value(), MaxTrackedAttempts: c.Limits.MaxTrackedAttempts, Observer: a.observation})
	if err != nil {
		return nil, errors.New("configure Worker runtime adapter")
	}
	a.executor, err = execution.NewProcessor(a.ledger, reader, factory, c.WorkerID, c.Limits.MaxActiveAttempts, a.observation)
	if err != nil {
		return nil, err
	}
	a.executor = tracedProcessor{delegate: a.executor, tracer: a.tracing.Tracer("agent-worker/execution-v1")}
	acceptor, err := execution.NewAcceptor(a.ledger, c.Policy.Domain(), domain.IntakeLimits{MaxQueuedRuns: c.Limits.MaxQueuedRuns, MaxRetainedRuns: c.Limits.MaxRetainedRuns}, a.observation)
	if err != nil {
		return nil, err
	}
	a.nc, err = c.connectNATS()
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(a.nc)
	if err != nil {
		return nil, errors.New("configure Worker JetStream")
	}
	// Bind the already-retained Manifest durable BEFORE owner export. No runtime
	// branch creates or resets topology, even for an empty Control collection.
	a.manifests, err = natsadapter.BindManifest(ctx, js, a.projection, observedRejector{a.ledger, a.observation}, c.Limits.MaxManifests)
	if err != nil {
		return nil, err
	}
	a.manifestStream, err = js.Stream(ctx, natsadapter.ManifestStream)
	if err != nil {
		return nil, natsadapter.ErrUnavailable
	}
	a.manifestBroker, err = a.manifestStream.Consumer(ctx, natsadapter.ManifestDurable)
	if err != nil {
		return nil, natsadapter.ErrUnavailable
	}
	mi, err := a.manifestStream.Info(ctx)
	if err != nil {
		return nil, natsadapter.ErrUnavailable
	}
	mci, err := a.manifestBroker.Info(ctx)
	if err != nil {
		return nil, natsadapter.ErrUnavailable
	}
	a.manifestCreated = mi.Created
	a.manifestConsumerCreated = mci.Created
	runs, err := natsadapter.BindRun(ctx, js, acceptor, observedRejector{a.ledger, a.observation})
	if err != nil {
		return nil, err
	}
	runs.Tracer = a.tracing.Tracer("agent-worker")
	a.runs = runs
	a.runStream, err = js.Stream(ctx, natsadapter.RunStream)
	if err != nil {
		return nil, natsadapter.ErrUnavailable
	}
	a.runBroker, err = a.runStream.Consumer(ctx, natsadapter.RunDurable)
	if err != nil {
		return nil, natsadapter.ErrUnavailable
	}
	ri, err := a.runStream.Info(ctx)
	if err != nil {
		return nil, natsadapter.ErrUnavailable
	}
	rci, err := a.runBroker.Info(ctx)
	if err != nil {
		return nil, natsadapter.ErrUnavailable
	}
	a.runCreated = ri.Created
	a.runConsumerCreated = rci.Created
	a.replyStream, err = js.Stream(ctx, natsadapter.ReplyStream)
	if err != nil {
		return nil, natsadapter.ErrUnavailable
	}
	replyInfo, err := a.replyStream.Info(ctx)
	if err != nil {
		return nil, natsadapter.ErrUnavailable
	}
	if !validReplyStream(replyInfo) {
		return nil, natsadapter.ErrTopology
	}
	a.replyCreated = replyInfo.Created
	reply, err := natsadapter.NewReplyRelay(a.ledger, js, c.Limits.ReplyBatch)
	if err != nil {
		return nil, err
	}
	a.exporter, err = controlhttp.New(controlhttp.Options{BaseURL: c.ControlURL, Client: client, RequestTimeout: c.Timing.RequestTimeout.Value()})
	if err != nil {
		return nil, err
	}
	reply.Tracer = a.tracing.Tracer("agent-worker")
	a.reply = reply
	finalArtifactCredentials, err := finalartifacthttp.New(finalartifacthttp.Options{BaseURL: c.ControlURL, WorkerID: c.WorkerID, Client: client, Timeout: c.Timing.RequestTimeout.Value(), MaxResponseBytes: c.Limits.MaxCredentialResponseBytes})
	if err != nil {
		return nil, errors.New("configure completed Artifact credential adapter")
	}
	replyArtifacts := replyArtifactQueries{pool: a.pool, ledger: a.ledger, manifests: reader, credentials: replyArtifactCredentialResolver{projection: a.projection, client: finalArtifactCredentials}}
	queries := proofQueries{ledger: a.ledger, manifests: reader}
	handler, err := httpadapter.New(queries, queries, httpadapter.Options{ReplyArtifacts: replyArtifacts, Knowledge: knowledgeQueries{manifests: reader}, Artifacts: artifactQueries{pool: a.pool, ledger: a.ledger, manifests: reader}, Management: workermanagement.NewReader(a.pool), BackendMigrations: backendmigration.MemoryExecutor{Scopes: a.ledger}, Approvals: approvals, Tracer: a.tracing.Tracer("agent-worker"), ControlPrincipals: c.ControlPrincipals, GatewayPrincipals: c.GatewayPrincipals, Timeout: c.Timing.ProofTimeout.Value(), MaxConcurrent: c.Limits.MaxProofQueries})
	if err != nil {
		return nil, err
	}
	a.healthServer = &http.Server{Addr: c.HealthAddress, Handler: a.healthHandler(), ReadHeaderTimeout: c.Timing.OperationTimeout.Value()}
	a.proofServer = &http.Server{Addr: c.InternalAddress, Handler: handler, ReadHeaderTimeout: c.Timing.OperationTimeout.Value(), ReadTimeout: c.Timing.ProofTimeout.Value(), WriteTimeout: max(time.Minute, c.Timing.ProofTimeout.Value()) + c.Timing.OperationTimeout.Value(), MaxHeaderBytes: 64 * 1024}
	trpcagent.BindLogging(a.observation.SDKLog)
	if c.Tracing != nil {
		trpcagent.BindTracing(a.tracing.Provider())
	}
	success = true
	return a, nil
}
func (a *App) healthHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if a.Ready() {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
	return mux
}
func (a *App) Ready() bool {
	return a != nil && a.initialized.Load() && a.storageHealthy.Load() && a.runHealthy.Load() && a.manifestHealthy.Load() && a.replyHealthy.Load() && !a.draining.Load()
}
func (a *App) closeOwned() {
	a.closed.Do(func() {
		defer func() {
			if a.tracing != nil {
				ctx, cancel := context.WithTimeout(context.Background(), a.config.Timing.HTTPShutdownTimeout.Value())
				_ = a.tracing.Shutdown(ctx)
				cancel()
			}
			if a.observation != nil {
				ctx, cancel := context.WithTimeout(context.Background(), a.config.Timing.HTTPShutdownTimeout.Value())
				defer cancel()
				_ = a.observation.Shutdown(ctx)
			}
		}()
		if a.nc != nil {
			a.nc.Close()
		}
		if a.controlTransport != nil {
			a.controlTransport.CloseIdleConnections()
		}
		if a.pool != nil {
			done := make(chan struct{})
			go func() { a.pool.Close(); close(done) }()
			select {
			case <-done:
			case <-time.After(a.config.Timing.ShutdownCancelTimeout.Value()):
			}
		}
	})
}

// Run keeps the proof listener and renewal contexts alive during draining.
// Cancellation first stops intake/scanning, then allows in-flight Attempts to
// finish, then cancels them and tears down HTTP/transport resources.
func (a *App) Run(ctx context.Context) error {
	if a == nil || a.healthServer == nil || a.proofServer == nil {
		return errors.New("Worker app is not assembled")
	}
	defer a.closeOwned()
	health, err := net.Listen("tcp", a.config.HealthAddress)
	if err != nil {
		return errors.New("Worker health listener unavailable")
	}
	proof, err := net.Listen("tcp", a.config.InternalAddress)
	if err != nil {
		health.Close()
		return errors.New("Worker proof listener unavailable")
	}
	a.addresses.Store(&listenAddresses{health: health.Addr().String(), proof: proof.Addr().String()})
	a.healthServer.Addr = health.Addr().String()
	a.proofServer.Addr = proof.Addr().String()
	failures := make(chan error, 8)
	serve := func(server *http.Server, listener net.Listener) {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			select {
			case failures <- errors.New("Worker listener stopped"):
			default:
			}
		}
	}
	go serve(a.healthServer, health)
	go serve(a.proofServer, tls.NewListener(proof, a.proofTLSConfig))
	startup, startupCancel := context.WithTimeout(ctx, a.config.Timing.StartupTimeout.Value())
	start := time.Now()
	err = a.initialize(startup)
	a.observation.Observe(startup, execution.Observation{Operation: "startup", Result: execution.ObservationResult(err), Duration: time.Since(start)})
	startupCancel()
	if err != nil {
		a.draining.Store(true)
		a.shutdownHTTP()
		return err
	}
	workCtx, stopWork := context.WithCancel(context.WithoutCancel(ctx))
	defer stopWork()
	activeCtx, stopActive := context.WithCancel(context.WithoutCancel(ctx))
	defer stopActive()
	a.storageHealthy.Store(true)
	a.runHealthy.Store(true)
	a.manifestHealthy.Store(true)
	a.replyHealthy.Store(true)
	a.initialized.Store(true)
	a.launch(func() { a.consume(workCtx, a.runs, &a.runHealthy, failures) })
	a.launch(func() { a.consume(workCtx, a.manifests, &a.manifestHealthy, failures) })
	a.launch(func() { a.monitor(workCtx, failures) })
	a.launch(func() { a.observeStorage(workCtx) })
	a.launch(func() { a.schedule(workCtx, activeCtx, a.ledger) })
	relayDone := make(chan struct{})
	go func() { defer close(relayDone); a.relay(activeCtx, failures) }()
	var result error
	select {
	case <-ctx.Done():
	case result = <-failures:
	}
	a.draining.Store(true)
	drainStart := time.Now()
	defer func() {
		a.observation.Observe(context.Background(), execution.Observation{Operation: "drain", Result: execution.ObservationResult(result), Duration: time.Since(drainStart)})
	}()
	stopWork()
	completed := make(chan struct{})
	go func() { a.work.Wait(); a.attempts.Wait(); close(completed) }()
	drain := time.NewTimer(a.config.Timing.ShutdownDrainTimeout.Value())
	select {
	case <-completed:
		if !drain.Stop() {
			<-drain.C
		}
	case <-drain.C:
		stopActive()
		cancelled := time.NewTimer(a.config.Timing.ShutdownCancelTimeout.Value())
		select {
		case <-completed:
			if !cancelled.Stop() {
				<-cancelled.C
			}
		case <-cancelled.C:
			result = errors.Join(result, errors.New("Worker Attempt cancellation deadline exceeded"))
		}
	}
	// A final bounded outbox step can publish already committed results, but a
	// broker outage never requires re-running the accepted model completion.
	flush, flushCancel := context.WithTimeout(context.Background(), a.config.Timing.OperationTimeout.Value())
	_, _ = a.reply.Tick(flush)
	flushCancel()
	stopActive()
	select {
	case <-relayDone:
	case <-time.After(a.config.Timing.ShutdownCancelTimeout.Value()):
		result = errors.Join(result, errors.New("Worker relay shutdown deadline exceeded"))
	}
	a.shutdownHTTP()
	return result
}
func (a *App) launch(run func()) { a.work.Add(1); go func() { defer a.work.Done(); run() }() }
func (a *App) shutdownHTTP() {
	ctx, cancel := context.WithTimeout(context.Background(), a.config.Timing.HTTPShutdownTimeout.Value())
	defer cancel()
	for _, server := range []*http.Server{a.proofServer, a.healthServer} {
		if server != nil {
			if server.Shutdown(ctx) != nil {
				_ = server.Close()
			}
		}
	}
}
func wait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
func (a *App) consume(ctx context.Context, c poller, healthy *atomic.Bool, failures chan<- error) {
	for {
		if ctx.Err() != nil {
			return
		}
		op, cancel := context.WithTimeout(ctx, a.config.Timing.OperationTimeout.Value())
		found, err := c.Poll(op)
		cancel()
		healthy.Store(err == nil)
		if errors.Is(err, natsadapter.ErrTopology) {
			select {
			case failures <- natsadapter.ErrTopology:
			default:
			}
			return
		}
		if err != nil || !found {
			if !wait(ctx, a.config.Timing.PollInterval.Value()) {
				return
			}
		}
	}
}
func (a *App) relay(ctx context.Context, failures chan<- error) {
	for {
		if ctx.Err() != nil {
			return
		}
		op, cancel := context.WithTimeout(ctx, a.config.Timing.OperationTimeout.Value())
		start := time.Now()
		published, err := a.reply.Tick(op)
		if published > 0 || err != nil {
			a.observation.Observe(op, execution.Observation{Operation: "reply_publish", Result: execution.ObservationResult(err), Duration: time.Since(start)})
		}
		cancel()
		a.replyHealthy.Store(err == nil)
		if errors.Is(err, natsadapter.ErrIntegrity) {
			select {
			case failures <- natsadapter.ErrIntegrity:
			default:
			}
			return
		}
		if !wait(ctx, a.config.Timing.PollInterval.Value()) {
			return
		}
	}
}
func (a *App) schedule(ctx, activeCtx context.Context, ledger readyLedger) {
	completed := make(chan string, a.config.Limits.MaxActiveAttempts)
	running := map[string]bool{}
	ticker := time.NewTicker(a.config.Timing.PollInterval.Value())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case key := <-completed:
			delete(running, key)
		case <-ticker.C:
			if !a.storageHealthy.Load() || len(running) >= a.config.Limits.MaxActiveAttempts {
				continue
			}
			op, cancel := context.WithTimeout(ctx, a.config.Timing.OperationTimeout.Value())
			rows, err := ledger.Scheduled(op, a.config.Limits.ScanBatch)
			cancel()
			if err != nil {
				a.storageHealthy.Store(false)
				continue
			}
			for _, run := range rows {
				if ctx.Err() != nil {
					return
				}
				key := run.Request.Route.TenantID + "\x00" + run.Request.RunID
				if running[key] || len(running) >= a.config.Limits.MaxActiveAttempts {
					continue
				}
				running[key] = true
				a.attempts.Add(1)
				a.active.Add(1)
				go func(r execution.ScheduledRun, key string) {
					defer a.attempts.Done()
					defer a.active.Add(-1)
					_ = a.executor.Advance(r.Carrier.Restore(activeCtx), r.Run)
					select {
					case completed <- key:
					case <-ctx.Done():
					}
				}(run, key)
			}
		}
	}
}

type listenAddresses struct{ health, proof string }

func (a *App) HealthAddress() string {
	if value := a.addresses.Load(); value != nil {
		return value.health
	}
	return ""
}
func (a *App) InternalAddress() string {
	if value := a.addresses.Load(); value != nil {
		return value.proof
	}
	return ""
}
