package bootstrap

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	telegram "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/adapter/inbound/telegramadapter"
	admissionpg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/adapter/outbound/postgres"
	admissionapp "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/application"
	admissiondomain "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
	catalogpg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/catalogpostgres"
	controlhttp "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/controlhttp"
	connectionpg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/postgres"
	receptionpg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/telegramreceptionpostgres"
	registrationremote "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/telegramregistration"
	connection "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application"
	accountuse "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/accountuse"
	catalogrefresh "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/catalogrefresh"
	telegramruntime "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/telegramruntime"
	replyevent "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/inbound/eventadapter"
	replyconsumer "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/inbound/nats"
	deliverypg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/outbound/postgres"
	wecomdelivery "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/outbound/wecomadapter"
	workerhttp "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/outbound/workerhttp"
	deliveryapp "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/application"
	transport "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/infra/nats"
	routeconsumer "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/adapter/inbound/nats"
	routepg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/adapter/outbound/postgres"
	routeapp "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/application"
	routedomain "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/domain"
	"golang.org/x/sync/errgroup"
)

type App struct {
	tracing                   *telemetrytrace.Runtime
	workerProof               *workerhttp.Client
	replyAcceptor             *deliveryapp.Acceptor
	replyConsumer             *replyconsumer.Consumer
	preflight                 *preflightRuntime
	deliveryRunner            *deliveryapp.Runner
	catalog                   *catalogrefresh.Service
	control                   *controlhttp.Client
	telegram                  *telegramruntime.Runtime
	use                       *accountuse.Service
	controlConfig             ControlConfig
	instanceID, instanceEpoch string
	delivery                  *deliverypg.Store
	pool                      *pgxpool.Pool
	transport                 *transport.Transport
	server, admin             *http.Server
	relay                     *admissionapp.Relay
	consumer                  *routeconsumer.Consumer
	admission                 *admissionapp.Service
	ledger                    *admissionpg.Store
	routes                    *routeapp.Service
	stopping                  atomic.Bool
	closeOnce                 sync.Once
	connections               *connection.Supervisor
	maintenance               *deliveryapp.Maintainer
}

func New(ctx context.Context, c Config) (*App, error) {
	return newWithDatabaseTarget(ctx, c, gatewayDatabaseIdentity())
}

func newWithDatabaseTarget(ctx context.Context, c Config, expected databaseIdentity) (*App, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	traces, err := telemetrytrace.New(ctx, c.Tracing, telemetrytrace.Identity{Service: "channel-gateway", Instance: c.InstanceID, Environment: os.Getenv("DEPLOYMENT_ENVIRONMENT")})
	if err != nil {
		return nil, err
	}
	assembled := false
	defer func() {
		if !assembled {
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = traces.Shutdown(shutdown)
		}
	}()
	pool, err := openDatabaseForTarget(ctx, c, expected)
	if err != nil {
		return nil, err
	}
	cleanup := func() { pool.Close() }
	n, err := transport.Connect(c.NATSURL, c.Topology, c.NATSAuth)
	if err != nil {
		cleanup()
		return nil, err
	}
	var controlClient *controlhttp.Client
	var workerProof *workerhttp.Client
	fail := func(err error) (*App, error) {
		if workerProof != nil {
			workerProof.Close()
		}
		if controlClient != nil {
			controlClient.Close()
		}
		n.Close()
		cleanup()
		return nil, err
	}
	if err = n.Verify(ctx); err != nil {
		return fail(err)
	}
	routes := routepg.NewStore(pool)
	routing, err := routeapp.NewService(routes)
	if err != nil {
		return fail(err)
	}
	leases := connectionpg.NewStore(pool)
	var catalog *catalogrefresh.Service
	var use *accountuse.Service
	var catalogStore *catalogpg.Store
	var boot string
	if c.AccountSource == "control" {
		var id [16]byte
		if _, err = rand.Read(id[:]); err != nil {
			return fail(errors.New("instance epoch unavailable"))
		}
		id[6] = (id[6] & 0x0f) | 0x40
		id[8] = (id[8] & 0x3f) | 0x80
		boot = fmt.Sprintf("%x-%x-%x-%x-%x", id[0:4], id[4:6], id[6:8], id[8:10], id[10:16])
		controlClient, err = c.Control.client(c.InstanceID)
		if err != nil {
			return fail(err)
		}
		catalogStore, err = catalogpg.New(pool, c.Control.ScopeID, c.Control.SourceEpoch, c.InstanceID, boot)
		if err != nil {
			return fail(err)
		}
		catalog, err = catalogrefresh.New(controlClient, catalogStore)
		if err != nil {
			return fail(err)
		}
		use = &accountuse.Service{Directory: catalog, Issuer: catalogStore, Credentials: credentialTransport{controlClient}, Owner: leases}
	}

	// Maintenance owns no ingress authorization or Provider capability. It must
	// also run on replicas with zero accounts and no locally held connections.
	deliveryLedger, err := deliverypg.NewStore(pool, leases, deliverypg.Options{})
	if err != nil {
		return fail(err)
	}
	if catalogStore != nil {
		deliveryLedger = deliveryLedger.WithAccountUseGuard(accountGuardBridge{store: catalogStore})
	}
	maintenance, err := deliveryapp.NewMaintainer(deliveryLedger, deliveryapp.MaintenanceOptions{})
	if err != nil {
		return fail(err)
	}
	ledger := admissionpg.NewStore(pool, routes).WithConnectionGuard(leases).WithTracing(traces.Tracer("channel-gateway"))
	if catalogStore != nil {
		ledger = ledger.WithAccountUseGuard(accountGuardBridge{store: catalogStore, ingress: true}).WithTelegramGuard(telegramGuardBridge{receptionpg.New(pool, catalogStore)})
	}
	acceptor := admissionapp.New(ledger, routeBridge{routing}, traces.Tracer("channel-gateway"))
	if controlClient != nil {
		acceptor.WithUsagePolicies(controlClient)
	}
	stream, err := n.JS.Stream(ctx, transport.RouteStream)
	if err != nil {
		return fail(err)
	}
	consumer, err := stream.Consumer(ctx, transport.RouteConsumer)
	if err != nil {
		return fail(err)
	}
	app := &App{tracing: traces, catalog: catalog, control: controlClient, use: use, controlConfig: c.Control, instanceID: c.InstanceID, instanceEpoch: boot, delivery: deliveryLedger, pool: pool, transport: n, maintenance: maintenance, ledger: ledger, admission: acceptor, routes: routing, relay: admissionapp.NewRelay(ledger, n), consumer: routeconsumer.New(consumer, stream, routing)}

	app.relay.Tracer = traces.Tracer("channel-gateway")
	if err := app.consumer.Initialize(ctx); err != nil {
		return fail(err)
	}
	if catalog != nil {
		options := c.ConnectionOptions
		options.InstanceID = c.InstanceID
		options.MaxAccounts = 1000
		app.connections, err = connection.NewSupervisor(leases, catalog, controlWeComCredentials{use}, &controlWeComFactory{base: wecomClientFactory{acceptor: acceptor, url: c.WeComURL}, use: use}, options)
		if err != nil {
			return fail(err)
		}
	} else if len(c.WeComAccounts) > 0 || c.WeComAccountsFile != "" {
		options := c.ConnectionOptions
		options.InstanceID = c.InstanceID
		if options.InstanceID == "" {
			var id [16]byte
			if _, err = rand.Read(id[:]); err != nil {
				return fail(errors.New("Gateway instance identity unavailable"))
			}
			options.InstanceID = hex.EncodeToString(id[:])
		}
		app.connections, err = connection.NewSupervisor(leases, accountFileSource{path: c.WeComAccountsFile, initial: c.WeComAccounts}, envWeComCredentials{}, wecomClientFactory{acceptor: acceptor, url: c.WeComURL}, options)
		if err != nil {
			return fail(err)
		}
	}
	if catalog != nil {
		workerProof, err = c.Worker.client()
		if err != nil {
			return fail(err)
		}
		workerProof.Tracer = traces.Tracer("channel-gateway")
		app.workerProof = workerProof
		app.replyAcceptor, err = deliveryapp.NewAcceptor(deliveryLedger, admissionDeliveryReader{ledger}, workerProof, deliveryapp.AcceptOptions{})
		if err != nil {
			return fail(err)
		}
		handler, e := replyevent.NewHandler(app.replyAcceptor)
		if e != nil {
			return fail(e)
		}
		replyStream, e := n.JS.Stream(ctx, transport.ReplyStream)
		if e != nil {
			return fail(e)
		}
		replyDurable, e := replyStream.Consumer(ctx, transport.ReplyConsumer)
		if e != nil {
			return fail(e)
		}
		app.replyConsumer, e = replyconsumer.New(replyDurable, replyStream, handler, deliveryLedger)
		if e != nil {
			return fail(e)
		}
		app.replyConsumer.Tracer = traces.Tracer("channel-gateway")
		provider, e := wecomdelivery.NewProvider(connectionDeliverySource{app.connections})
		if e != nil {
			return fail(e)
		}
		dispatcher, e := deliveryapp.NewDispatcher(deliveryLedger, controlSenders{artifacts: workerProof, use: use, wecom: provider, telegramAPIURL: c.TelegramAPIURL}, deliveryapp.DispatchOptions{Tracer: traces.Tracer("channel-gateway")})
		if e != nil {
			return fail(e)
		}
		bridge := &controlDelivery{use: use, owner: app.connections, instance: c.InstanceID, dispatcher: dispatcher}
		app.deliveryRunner, e = deliveryapp.NewRunner(deliveryLedger, bridge, bridge, maintenance, deliveryapp.RunnerOptions{InstanceID: c.InstanceID})
		if e != nil {
			return fail(e)
		}
	}
	mux := http.NewServeMux()
	admin := http.NewServeMux()
	admin.HandleFunc("GET /livez", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	admin.HandleFunc("GET /readyz", app.ready)
	// Per-account reception status: the Gateway image is distroless and writes no
	// log stream, so this is the only way to see why an enabled account is not
	// ready, which in turn makes the whole Gateway report unready.
	admin.HandleFunc("GET /accounts", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		accounts := []telegramruntime.AccountStatus{}
		if app.telegram != nil {
			accounts = app.telegram.AccountStatuses()
		}
		if err := json.NewEncoder(w).Encode(map[string]any{"telegram": accounts}); err != nil {
			return
		}
	})
	if catalog != nil {
		registry := &dynamicTelegram{handlers: map[string]http.Handler{}, use: use, acceptor: acceptor}
		factory := c.telegramFactory
		if factory == nil {
			factory = registrationremote.Factory{ServerURL: c.TelegramAPIURL}
		}
		app.telegram, err = telegramruntime.New(catalog, use, registry, receptionpg.New(pool, catalogStore), factory, telegramPollingIntake{acceptor}, strings.TrimSuffix(c.Control.PublicOrigin, "/"))
		if err != nil {
			return fail(err)
		}
		mux.Handle("/v1/telegram/", registry)
	}
	for _, account := range c.Accounts {
		handler, err := telegram.NewHandler(account.ID, account.Secret, acceptor)
		if err != nil {
			return fail(err)
		}
		mux.Handle("/v1/telegram/"+account.ID, handler)
	}
	app.preflight, err = newPreflight(c, boot)
	if err != nil {
		return fail(err)
	}
	app.server = newServer(c.HTTPAddress, traces.IMHandler(mux))
	app.admin = newServer(c.AdminAddress, admin)
	assembled = true
	return app, nil
}
func newServer(address string, h http.Handler) *http.Server {
	return &http.Server{Addr: address, Handler: h, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16384}
}
func (a *App) Handler() http.Handler      { return a.server.Handler }
func (a *App) AdminHandler() http.Handler { return a.admin.Handler }
func (a *App) Close() {
	a.closeOnce.Do(func() {
		defer func() {
			if a.tracing != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = a.tracing.Shutdown(ctx)
			}
		}()

		if a.workerProof != nil {
			a.workerProof.Close()
		}
		if a.preflight != nil {
			a.preflight.Close()
		}
		if a.catalog != nil {
			a.catalog.Close()
		}
		if a.control != nil {
			a.control.Close()
		}
		a.transport.Close()
		a.pool.Close()
	})
}
func (a *App) ready(w http.ResponseWriter, r *http.Request) {
	// The failing check is named in a response header: the Gateway image is
	// distroless, so an operator has no shell or log stream to inspect a 503.
	failed := make([]string, 0, 4)
	if a.stopping.Load() {
		failed = append(failed, "stopping")
	}
	if a.catalog != nil && !a.catalog.Ready() {
		failed = append(failed, "catalog")
	}
	if a.telegram != nil && !a.telegram.Ready() {
		failed = append(failed, "telegram")
	}
	if a.connections != nil && !a.connections.Ready() {
		failed = append(failed, "connections")
	}
	if len(failed) > 0 {
		unready(w, failed...)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	health, err := a.routes.QueryProjectionHealth(ctx)
	switch {
	case err != nil:
		unready(w, "routes:error")
		return
	case !health.Initialized:
		unready(w, "routes:uninitialized")
		return
	case health.Stale:
		unready(w, "routes:stale")
		return
	case health.BlockedReason != "":
		unready(w, "routes:blocked:"+health.BlockedReason,
			fmt.Sprintf("contiguous=%d highest=%d target=%d", health.ContiguousSequence, health.HighestSequence, health.TargetSequence))
		return
	}
	budget, err := a.ledger.Health(ctx)
	if err != nil {
		unready(w, "ledger:error")
		return
	}
	if budget.Saturated {
		unready(w, "ledger:saturated")
		return
	}
	if !a.transport.Conn.IsConnected() {
		w.Header().Set("X-Gateway-State", "degraded")
	}
	w.WriteHeader(http.StatusNoContent)
}

// unready reports an operator-visible reason for a 503 without exposing state
// beyond the Gateway's own health counters.
func unready(w http.ResponseWriter, reasons ...string) {
	w.Header().Set("X-Gateway-Ready-Failed", strings.Join(reasons, ","))
	w.WriteHeader(http.StatusServiceUnavailable)
}
func (a *App) Run(ctx context.Context) error {
	// Bind both before starting background work, so a bad listen address cannot
	// leave a partially started workload or goroutines waiting forever.
	public, err := net.Listen("tcp", a.server.Addr)
	if err != nil {
		return errors.New("listen public HTTP failed")
	}
	admin, err := net.Listen("tcp", a.admin.Addr)
	if err != nil {
		public.Close()
		return errors.New("listen administrative HTTP failed")
	}
	g, runCtx := errgroup.WithContext(ctx)
	serve := func(server *http.Server, l net.Listener) func() error {
		return func() error {
			err := server.Serve(l)
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		}
	}
	g.Go(serve(a.server, public))
	g.Go(serve(a.admin, admin))
	g.Go(func() error { return a.relay.Run(runCtx) })
	g.Go(func() error { return a.consumer.Run(runCtx) })
	if a.replyConsumer != nil {
		g.Go(func() error { return a.replyConsumer.Run(runCtx) })
	}
	if a.preflight != nil {
		g.Go(func() error { return a.preflight.Run(runCtx) })
	}
	if a.deliveryRunner != nil {
		g.Go(func() error { return a.deliveryRunner.Run(runCtx) })
	} else {
		g.Go(func() error { return a.maintenance.Run(runCtx) })
	}
	if a.catalog != nil {
		g.Go(func() error { return a.catalog.Run(runCtx) })
		g.Go(func() error { return a.telegram.Run(runCtx) })
		g.Go(func() error { return a.reportAccounts(runCtx) })
	}
	if a.connections != nil {
		g.Go(func() error { return a.connections.Run(runCtx) })
	}
	g.Go(func() error {
		<-runCtx.Done()
		a.stopping.Store(true)
		a.admission.Stop()
		if a.replyAcceptor != nil {
			a.replyAcceptor.Stop()
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		shutdownGroup, _ := errgroup.WithContext(shutdownCtx)
		for _, server := range []*http.Server{a.server, a.admin} {
			server := server
			shutdownGroup.Go(func() error {
				if err := server.Shutdown(shutdownCtx); err != nil {
					_ = server.Close()
					return err
				}
				return nil
			})
		}
		return shutdownGroup.Wait()
	})
	err = g.Wait()
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

type routeBridge struct{ service *routeapp.Service }

func (b routeBridge) Resolve(ctx context.Context, provider, account string) (admissiondomain.RouteSnapshot, error) {
	r, err := b.service.Resolve(ctx, provider, account)
	return admissiondomain.RouteSnapshot{Provider: r.Provider, AccountID: r.AccountID, TenantID: r.TenantID, BindingID: r.BindingID, Generation: r.Generation, DeploymentRevisionID: r.DeploymentRevisionID, ManifestRef: r.ManifestRef, ManifestDigest: r.ManifestDigest}, err
}

func (b routeBridge) ResolveFor(ctx context.Context, provider, account, conversation, thread, sender string) (admissiondomain.RouteSnapshot, error) {
	r, _, err := b.service.ResolveFor(ctx, provider, account, routedomain.Cohort{ConversationID: conversation, ThreadID: thread, SenderID: sender})
	return admissiondomain.RouteSnapshot{Provider: r.Provider, AccountID: r.AccountID, TenantID: r.TenantID, BindingID: r.BindingID, Generation: r.Generation, DeploymentRevisionID: r.DeploymentRevisionID, ManifestRef: r.ManifestRef, ManifestDigest: r.ManifestDigest, RolloutID: r.RolloutID, RolloutVariant: r.RolloutVariant}, err
}
