package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/control"
	"github.com/liuzengh/trpc-agent-service/trpcservice/executor"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/httpapi"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/routing"
	"github.com/liuzengh/trpc-agent-service/trpcservice/scheduler"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

const shutdownTimeout = 10 * time.Second

func main() {
	fmt.Printf("trpc-agent-service %s\n", trpcservice.Version)
	fmt.Println("multi-tenant node-based agent platform on tRPC-Agent-Go")
	if len(os.Args) <= 1 {
		return
	}
	if os.Args[1] == "-h" || os.Args[1] == "--help" {
		printUsage(os.Stderr)
		return
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve(os.Args[2:])
	case "gateway":
		err = runGateway(os.Args[2:])
	case "worker":
		err = runWorker(os.Args[2:])
	case "sql-init":
		err = runSQLInit(os.Args[2:], false)
	case "sql-ready":
		err = runSQLInit(os.Args[2:], true)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		printUsage(os.Stderr)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func printUsage(output io.Writer) {
	fmt.Fprintf(output, "usage:\n  %s serve [-addr 127.0.0.1:8080]\n  %s gateway [-addr 127.0.0.1:8080] [-consumer name]\n  %s worker [-health-addr :8081] [-consumer name]\n  %s sql-init -kind postgres|mysql|all\n  %s sql-ready -kind postgres|mysql|all\n", os.Args[0], os.Args[0], os.Args[0], os.Args[0], os.Args[0])
}

func runSQLInit(args []string, readyOnly bool) error {
	flags := flag.NewFlagSet("sql", flag.ContinueOnError)
	kind := flags.String("kind", "", "postgres|mysql|all")
	if err := flags.Parse(args); err != nil {
		return err
	}
	requested := strings.ToLower(strings.TrimSpace(*kind))
	if requested != "postgres" && requested != "mysql" && requested != "all" {
		return errors.New("-kind must be postgres, mysql, or all")
	}
	cfg, err := config.LoadForRole(config.RoleWorker)
	if err != nil {
		return err
	}
	catalog, resolver, err := cfg.RuntimeCatalog()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	results, err := storage.InitializeSQLProfiles(ctx, catalog.StorageProfiles, resolver, requested, readyOnly)
	if err != nil {
		return err
	}
	for _, result := range results {
		fmt.Printf("sql-%s ok tenant=%s profile=%s kind=%s\n", map[bool]string{true: "ready", false: "init"}[readyOnly], result.TenantID, result.ProfileID, result.Kind)
	}
	return nil
}

func serve(args []string) error {
	addr, help, err := parseServeArgs(args, os.Stderr)
	if err != nil || help {
		return err
	}
	cfg, err := config.LoadForRole(config.RoleServe)
	if err != nil {
		return err
	}
	telemetryProvider, err := initTelemetry(cfg)
	if err != nil {
		return err
	}
	defer telemetryProvider.Close(context.Background())
	store, err := messaging.NewStore(*cfg.Messaging)
	if err != nil {
		return err
	}
	defer store.Close()
	controlRepository, err := newControlRepository(cfg)
	if err != nil {
		return err
	}
	if controlRepository != nil {
		defer controlRepository.Close()
	}
	router, err := newRouter(cfg)
	if err != nil {
		return err
	}
	adapters, err := newAdapters(cfg)
	if err != nil {
		return err
	}
	runtime, err := executor.New(cfg)
	if err != nil {
		return err
	}
	defer runtime.Close()
	workerConsumer, err := consumerName("worker")
	if err != nil {
		return err
	}
	gatewayConsumer, err := consumerName("gateway")
	if err != nil {
		return err
	}
	workerService, err := worker.New(store, runtime, workerConsumer)
	if controlRepository != nil {
		assignmentScheduler, schedulerErr := scheduler.New(controlRepository, store, cfg.ControlPlane.NodeAssignmentEnabled)
		if schedulerErr != nil {
			return schedulerErr
		}
		assignmentController, controllerErr := scheduler.NewController(controlRepository, store, cfg.ControlPlane, strings.TrimSpace(os.Getenv("NODE_ID")), trpcservice.Version)
		if controllerErr != nil {
			return controllerErr
		}
		if cfg.ControlPlane.NodeAssignmentEnabled {
			workerConsumer = assignmentController.ConsumerName()
		}
		workerService, controllerErr = worker.NewWithController(store, runtime, workerConsumer, assignmentController)
		if controllerErr != nil {
			return controllerErr
		}
		gatewayService, gatewayErr := gateway.NewWithScheduler(router, store, gatewayConsumer, assignmentScheduler, adapters...)
		if gatewayErr != nil {
			return gatewayErr
		}
		gatewaySinks, gatewayErr := configureGatewayGovernance(cfg, controlRepository, gatewayService)
		if gatewayErr != nil {
			return gatewayErr
		}
		defer gatewaySinks.Close()
		return runCombined(addr, gatewayService, workerService, runtime, adminHTTPConfig(cfg, controlRepository, assignmentScheduler, store))
	}
	if err != nil {
		return err
	}
	gatewayService, err := gateway.NewWithAdapters(router, store, gatewayConsumer, adapters...)
	if err != nil {
		return err
	}
	return runCombined(addr, gatewayService, workerService, runtime, httpapi.AdminConfig{})
}

func runGateway(args []string) error {
	addr, consumer, help, err := parseGatewayArgs(args, os.Stderr)
	if err != nil || help {
		return err
	}
	if consumer == "" {
		consumer, err = consumerName("gateway")
		if err != nil {
			return err
		}
	}
	cfg, err := config.LoadForRole(config.RoleGateway)
	if err != nil {
		return err
	}
	telemetryProvider, err := initTelemetry(cfg)
	if err != nil {
		return err
	}
	defer telemetryProvider.Close(context.Background())
	store, err := messaging.NewStore(*cfg.Messaging)
	if err != nil {
		return err
	}
	defer store.Close()
	controlRepository, err := newControlRepository(cfg)
	if err != nil {
		return err
	}
	if controlRepository != nil {
		defer controlRepository.Close()
	}
	router, err := newRouter(cfg)
	if err != nil {
		return err
	}
	adapters, err := newAdapters(cfg)
	if err != nil {
		return err
	}
	var service *gateway.Service
	var assignmentScheduler *scheduler.Service
	if controlRepository != nil {
		var schedulerErr error
		assignmentScheduler, schedulerErr = scheduler.New(controlRepository, store, cfg.ControlPlane.NodeAssignmentEnabled)
		if schedulerErr != nil {
			return schedulerErr
		}
		service, err = gateway.NewWithScheduler(router, store, consumer, assignmentScheduler, adapters...)
	} else {
		service, err = gateway.NewWithAdapters(router, store, consumer, adapters...)
	}
	if err != nil {
		return err
	}
	if controlRepository != nil {
		gatewaySinks, governanceErr := configureGatewayGovernance(cfg, controlRepository, service)
		if governanceErr != nil {
			return governanceErr
		}
		defer gatewaySinks.Close()
	}
	return runGatewayServer(addr, service, adminHTTPConfig(cfg, controlRepository, assignmentScheduler, store))
}

type gatewaySinks struct {
	audit         *metrics.AsyncAuditSink
	metric        *metrics.AsyncMetricSink
	restoreMetric func()
}

func (s *gatewaySinks) Close() error {
	if s == nil {
		return nil
	}
	if s.restoreMetric != nil {
		s.restoreMetric()
	}
	return errors.Join(s.metric.Close(), s.audit.Close())
}

func configureGatewayGovernance(cfg config.Config, repository control.Repository, service *gateway.Service) (*gatewaySinks, error) {
	cache, err := governance.NewPolicyCache(repository, cfg.ControlPlane.PolicyCacheTTL)
	if err != nil {
		return nil, err
	}
	auditSink := metrics.NewAuditSink(repository, 256)
	metricSink := metrics.NewMetricSink(repository, 256)
	enforcer := governance.NewPolicyEnforcer(cache, cfg.IdentitySecret, repository)
	enforcer.SetAuditSink(auditSink)
	service.SetTaskAuthorizer(enforcer)
	return &gatewaySinks{audit: auditSink, metric: metricSink, restoreMetric: telemetry.SetMetricEventSink(metricSink)}, nil
}

func runWorker(args []string) error {
	healthAddr, consumer, help, err := parseWorkerArgs(args, os.Stderr)
	if err != nil || help {
		return err
	}
	if consumer == "" {
		consumer, err = consumerName("worker")
		if err != nil {
			return err
		}
	}
	cfg, err := config.LoadForRole(config.RoleWorker)
	if err != nil {
		return err
	}
	telemetryProvider, err := initTelemetry(cfg)
	if err != nil {
		return err
	}
	defer telemetryProvider.Close(context.Background())
	store, err := messaging.NewStore(*cfg.Messaging)
	if err != nil {
		return err
	}
	defer store.Close()
	controlRepository, err := newControlRepository(cfg)
	if err != nil {
		return err
	}
	if controlRepository != nil {
		defer controlRepository.Close()
	}
	var workerMetrics *metrics.AsyncMetricSink
	var restoreWorkerMetrics func()
	if controlRepository != nil {
		workerMetrics = metrics.NewMetricSink(controlRepository, 256)
		restoreWorkerMetrics = telemetry.SetMetricEventSink(workerMetrics)
		defer func() {
			restoreWorkerMetrics()
			_ = workerMetrics.Close()
		}()
	}
	runtime, err := executor.New(cfg)
	if err != nil {
		return err
	}
	defer runtime.Close()
	var service *worker.Worker
	if controlRepository != nil {
		assignmentController, controllerErr := scheduler.NewController(controlRepository, store, cfg.ControlPlane, strings.TrimSpace(os.Getenv("NODE_ID")), trpcservice.Version)
		if controllerErr != nil {
			return controllerErr
		}
		if cfg.ControlPlane.NodeAssignmentEnabled {
			consumer = assignmentController.ConsumerName()
		}
		service, err = worker.NewWithController(store, runtime, consumer, assignmentController)
	} else {
		service, err = worker.New(store, runtime, consumer)
	}
	if err != nil {
		return err
	}
	return runWorkerServer(healthAddr, service)
}

func newRouter(cfg config.Config) (*routing.Router, error) {
	catalog, err := cfg.RoutingCatalog()
	if err != nil {
		return nil, err
	}
	repository, err := tenant.NewPresetRepository(catalog)
	if err != nil {
		return nil, err
	}
	return routing.NewWithDigestV2(repository, cfg.IdentitySecret, cfg.ControlPlane != nil && cfg.ControlPlane.TraceDigestV2Enabled)
}

func initTelemetry(cfg config.Config) (*telemetry.Provider, error) {
	obs := cfg.Observability
	return telemetry.New(context.Background(), telemetry.Config{Enabled: obs.Enabled, Endpoint: obs.Endpoint, ServiceName: obs.ServiceName, ServiceVersion: trpcservice.Version, ServiceInstanceID: strings.TrimSpace(os.Getenv("OTEL_SERVICE_INSTANCE_ID")), SampleRatio: obs.SampleRatio, SpanQueueSize: obs.SpanQueueSize, SpanBatchSize: obs.SpanBatchSize, SpanBatchTimeout: obs.ScheduleDelay, ExportTimeout: obs.ExportTimeout, MetricInterval: obs.MetricInterval, MetricExportTimeout: obs.ExportTimeout})
}

func newControlRepository(cfg config.Config) (control.Repository, error) {
	if cfg.ControlPlane == nil {
		return nil, nil
	}
	repository, err := control.NewRedisRepository(*cfg.ControlPlane)
	if err != nil {
		return nil, err
	}
	defaultTools := platformtool.NewRegistry().NonDangerousNames()
	return control.NewInitializingRepository(repository, cfg.Catalog.Tenants, defaultTools), nil
}

func adminHTTPConfig(cfg config.Config, repository control.Repository, overrider interface {
	Override(context.Context, string, string) (control.NodeAssignment, error)
}, store *messaging.Store) httpapi.AdminConfig {
	if cfg.ControlPlane == nil || repository == nil {
		return httpapi.AdminConfig{}
	}
	admin := httpapi.AdminConfig{Repository: repository, Token: cfg.ControlPlane.AdminToken, AssignmentOverrider: overrider, Catalog: cfg.Catalog}
	if store != nil {
		admin.TaskLookup = func(ctx context.Context, bindingID, messageID string) (map[string]any, error) {
			tenantID := ""
			for _, binding := range cfg.Catalog.ChannelBindings {
				if binding.ID == bindingID {
					tenantID = binding.TenantID
					break
				}
			}
			if tenantID == "" {
				return nil, tenant.ErrBindingNotFound
			}
			inbox := (message.ExecutionTask{TenantID: tenantID, ChannelBindingID: bindingID, PlatformMessageID: messageID}).InboxID()
			snapshot, err := store.Snapshot(ctx, inbox)
			if err != nil {
				return nil, err
			}
			return map[string]any{"task_id": snapshot.TaskID, "state": snapshot.State, "attempt": snapshot.Attempt, "error_code": snapshot.ErrorCode, "node_id": snapshot.NodeID, "assignment_state": snapshot.AssignmentState, "persist_attempt": snapshot.PersistAttempt}, nil
		}
		admin.OutboundLookup = func(ctx context.Context, taskID string) (map[string]any, error) {
			value, err := store.OutboundSnapshot(ctx, taskID)
			if err != nil {
				return nil, err
			}
			return map[string]any{"task_id": taskID, "state": value.Status, "attempt": value.Attempts, "error_code": value.LastError}, nil
		}
		admin.TaskList = func(ctx context.Context, limit int) ([]httpapi.AdminTask, error) {
			snapshots, err := store.ListSnapshots(ctx, limit)
			if err != nil {
				return nil, err
			}
			result := make([]httpapi.AdminTask, 0, len(snapshots))
			for _, snapshot := range snapshots {
				outboundState := ""
				if value, outboundErr := store.OutboundSnapshot(ctx, snapshot.TaskID); outboundErr == nil {
					outboundState = value.Status
				}
				result = append(result, httpapi.AdminTask{
					TaskID: snapshot.TaskID, TenantID: snapshot.TenantID, AgentAppID: snapshot.AgentAppID,
					Channel: snapshot.Channel, BindingID: snapshot.BindingID, PlatformMessageID: snapshot.PlatformMessageID, State: snapshot.State,
					Attempt: snapshot.Attempt, PersistAttempt: snapshot.PersistAttempt, NodeID: snapshot.NodeID,
					AssignmentState: snapshot.AssignmentState, ErrorCode: snapshot.ErrorCode,
					RequestID: snapshot.RequestID, TraceID: snapshot.TraceID, OutboundState: outboundState, ReceivedAt: snapshot.ReceivedAt,
				})
			}
			return result, nil
		}
	}
	admin.ReconcilerStatus = func(ctx context.Context) (map[string]any, error) {
		if err := repository.Ready(ctx); err != nil {
			return nil, err
		}
		leader := false
		if status, ok := overrider.(interface{ IsLeader() bool }); ok {
			leader = status.IsLeader()
		}
		return map[string]any{"is_leader": leader, "node_id": strings.TrimSpace(os.Getenv("NODE_ID"))}, nil
	}
	return admin
}

func newAdapters(cfg config.Config) ([]channels.Adapter, error) {
	catalog, resolver, err := cfg.GatewayCatalog()
	if err != nil {
		return nil, err
	}
	result := make([]channels.Adapter, 0, len(catalog.ChannelBindings))
	identities := make(map[[32]byte]string)
	for _, binding := range catalog.ChannelBindings {
		if !binding.Enabled || binding.Channel == "demo" {
			continue
		}
		var adapter channels.Adapter
		var identityValue string
		switch binding.Channel {
		case "telegram":
			token, resolveErr := resolver.Resolve(binding.CredentialRef)
			if resolveErr == nil {
				identityValue = token
				adapter, err = channels.NewTelegramAdapter(binding.ID, binding.ExternalAccountID, token, binding.ServerURL)
			}
			if resolveErr != nil {
				adapter = &channels.UnavailableAdapter{BindingID: binding.ID}
			}
		case "wecom_aibot":
			botID, botErr := resolver.Resolve(binding.BotIDRef)
			secret, secretErr := resolver.Resolve(binding.BotSecretRef)
			if botErr == nil && secretErr == nil {
				identityValue = botID
				adapter, err = channels.NewWeComAdapter(binding.ID, binding.ExternalAccountID, botID, secret, "")
			}
			if botErr != nil || secretErr != nil {
				adapter = &channels.UnavailableAdapter{BindingID: binding.ID}
			}
		case "feishu":
			appID, appErr := resolver.Resolve(binding.BotIDRef)
			secret, secretErr := resolver.Resolve(binding.BotSecretRef)
			if appErr == nil && secretErr == nil {
				identityValue = appID
				adapter, err = channels.NewFeishuAdapter(binding.ID, binding.ExternalAccountID, appID, secret)
			}
			if appErr != nil || secretErr != nil {
				adapter = &channels.UnavailableAdapter{BindingID: binding.ID}
			}
		}
		if err != nil {
			return nil, err
		}
		if identityValue != "" {
			digest := sha256.Sum256([]byte(binding.Channel + "\x00" + identityValue))
			if prior, exists := identities[digest]; exists {
				return nil, fmt.Errorf("enabled bindings %q and %q reuse the same IM bot", prior, binding.ID)
			}
			identities[digest] = binding.ID
		}
		result = append(result, adapter)
	}
	return result, nil
}

func runCombined(addr string, gatewayService *gateway.Service, workerService *worker.Worker, runtime *executor.Runtime, admin httpapi.AdminConfig) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	gatewayErr := make(chan error, 1)
	workerErr := make(chan error, 1)
	var loops sync.WaitGroup
	loops.Add(2)
	go func() { defer loops.Done(); gatewayErr <- gatewayService.Run(ctx) }()
	go func() { defer loops.Done(); workerErr <- workerService.Run(ctx) }()
	server := newHTTPServer(addr, httpapi.NewHandlerWithAdmin(combinedBackend{gateway: gatewayService, worker: workerService}, admin))
	serverErr := listen(server)

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-gatewayErr:
	case runErr = <-workerErr:
	case runErr = <-serverErr:
	}
	stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	serverShutdownErr := server.Shutdown(shutdownCtx)
	_ = gatewayService.Close()
	_ = workerService.Close()
	loops.Wait()
	runtimeErr := runtime.Close()
	return errors.Join(runErr, serverShutdownErr, runtimeErr)
}

type combinedBackend struct {
	gateway *gateway.Service
	worker  *worker.Worker
}

func (b combinedBackend) Ready(ctx context.Context) error {
	if err := b.gateway.Ready(ctx); err != nil {
		return err
	}
	return b.worker.Ready(ctx)
}

func (b combinedBackend) TraceDigestV2Enabled() bool {
	return b.gateway.TraceDigestV2Enabled()
}

func (b combinedBackend) Handle(ctx context.Context, inbound message.InboundMessage) (message.OutboundMessage, error) {
	return b.gateway.Handle(ctx, inbound)
}

func (b combinedBackend) Accept(ctx context.Context, inbound message.InboundMessage) (channels.AcceptResult, error) {
	return b.gateway.Accept(ctx, inbound)
}

func (b combinedBackend) Snapshot(ctx context.Context, channel, bindingID, messageID string) (messaging.Snapshot, error) {
	return b.gateway.Snapshot(ctx, channel, bindingID, messageID)
}

func runGatewayServer(addr string, service *gateway.Service, admin httpapi.AdminConfig) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	runErr := make(chan error, 1)
	var loop sync.WaitGroup
	loop.Add(1)
	go func() { defer loop.Done(); runErr <- service.Run(ctx) }()
	server := newHTTPServer(addr, httpapi.NewHandlerWithAdmin(service, admin))
	serverErr := listen(server)
	var err error
	select {
	case <-ctx.Done():
	case err = <-runErr:
	case err = <-serverErr:
	}
	stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	shutdownErr := errors.Join(server.Shutdown(shutdownCtx), service.Close())
	loop.Wait()
	return errors.Join(err, shutdownErr)
}

func runWorkerServer(addr string, service *worker.Worker) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	runErr := make(chan error, 1)
	var loop sync.WaitGroup
	loop.Add(1)
	go func() { defer loop.Done(); runErr <- service.Run(ctx) }()
	server := newHTTPServer(addr, httpapi.NewHealthHandler(service))
	serverErr := listen(server)
	var err error
	select {
	case <-ctx.Done():
	case err = <-runErr:
	case err = <-serverErr:
	}
	stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	shutdownErr := errors.Join(server.Shutdown(shutdownCtx), service.Close())
	loop.Wait()
	return errors.Join(err, shutdownErr)
}

func newHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
}

func listen(server *http.Server) <-chan error {
	result := make(chan error, 1)
	go func() {
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		result <- err
	}()
	return result
}

func consumerName(role string) (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return "", err
	}
	suffix, err := identity.RequestID()
	if err != nil {
		return "", err
	}
	hostname = strings.NewReplacer(" ", "_", ":", "_").Replace(hostname)
	return fmt.Sprintf("%s-%s-%d-%s", role, hostname, os.Getpid(), suffix), nil
}

func parseServeArgs(args []string, output io.Writer) (string, bool, error) {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.SetOutput(output)
	addr := flags.String("addr", "127.0.0.1:8080", "HTTP listen address")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return "", true, nil
		}
		return "", false, err
	}
	if flags.NArg() != 0 {
		return "", false, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	return *addr, false, nil
}

func parseGatewayArgs(args []string, output io.Writer) (string, string, bool, error) {
	flags := flag.NewFlagSet("gateway", flag.ContinueOnError)
	flags.SetOutput(output)
	addr := flags.String("addr", "127.0.0.1:8080", "HTTP listen address")
	consumer := flags.String("consumer", "", "Redis reply consumer name")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return "", "", true, nil
		}
		return "", "", false, err
	}
	if flags.NArg() != 0 {
		return "", "", false, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	return *addr, *consumer, false, nil
}

func parseWorkerArgs(args []string, output io.Writer) (string, string, bool, error) {
	flags := flag.NewFlagSet("worker", flag.ContinueOnError)
	flags.SetOutput(output)
	addr := flags.String("health-addr", ":8081", "Worker health listen address")
	consumer := flags.String("consumer", "", "Redis task consumer name")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return "", "", true, nil
		}
		return "", "", false, err
	}
	if flags.NArg() != 0 {
		return "", "", false, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	return *addr, *consumer, false, nil
}
