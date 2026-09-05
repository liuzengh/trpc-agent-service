package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/outbox"
	"github.com/liuzengh/trpc-agent-service/trpcservice/platform"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

const (
	processExitOK      = 0
	processExitFailure = 1
	processExitForced  = 2

	defaultStartupTimeout  = 30 * time.Second
	defaultShutdownTimeout = 10 * time.Second
	defaultForceCloseLimit = 500 * time.Millisecond
	startupCleanupTimeout  = 2 * time.Second
)

var (
	errProcessServeFailure    = errors.New("process serve failure")
	errProcessHTTPDrain       = errors.New("process http drain timeout")
	errProcessWorkerDrain     = errors.New("process worker drain unresolved")
	errProcessDispatcherDrain = errors.New("process dispatcher drain timeout")
	errProcessRuntimeStop     = errors.New("process runtime stop failure")
	errProcessForcedClose     = errors.New("process forced close")
	errProcessStartup         = errors.New("process startup failure")
)

// lifecycleError deliberately omits the cause from Error. Causes remain
// available to tests through errors.Is, while process logs only contain a safe
// phase and code.
type lifecycleError struct {
	phase string
	code  string
	cause error
}

func (e *lifecycleError) Error() string {
	if e == nil {
		return "lifecycle failure"
	}
	return "lifecycle phase=" + e.phase + " code=" + e.code
}

func (e *lifecycleError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func newLifecycleError(phase, code string, cause error) error {
	return &lifecycleError{phase: phase, code: code, cause: cause}
}

func lifecycleStatus(err error) string {
	switch {
	case err == nil:
		return "clean"
	case errors.Is(err, errProcessForcedClose):
		return "forced_close"
	case errors.Is(err, errProcessHTTPDrain):
		return "http_drain_timeout"
	case errors.Is(err, errProcessWorkerDrain):
		return "worker_unresolved"
	case errors.Is(err, errProcessDispatcherDrain):
		return "dispatcher_timeout"
	case errors.Is(err, errProcessServeFailure):
		return "serve_failure"
	case errors.Is(err, errProcessStartup):
		return "startup_failure"
	default:
		return "runtime_stop_failure"
	}
}

// startupReadiness is installed before the listener is served. It keeps
// healthz fail-closed while dependencies are still being assembled.
type startupReadiness struct {
	ready atomic.Bool
}

func (g *startupReadiness) Ready(ctx context.Context) error {
	if ctx == nil {
		return errors.New("startup readiness context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !g.ready.Load() {
		return errors.New("startup is not complete")
	}
	return nil
}

func (g *startupReadiness) SetReady() {
	if g != nil {
		g.ready.Store(true)
	}
}

type processRuntime interface {
	BeginDraining()
	Stop(context.Context) error
	ForceClose()
}

type processHTTPServer interface {
	Shutdown(context.Context) error
	Close() error
}

type lifecycleReadiness struct {
	mu       sync.RWMutex
	delegate web.ReadinessGate
	draining atomic.Bool
}

func (g *lifecycleReadiness) SetDelegate(delegate web.ReadinessGate) {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.delegate = delegate
	g.mu.Unlock()
}

func (g *lifecycleReadiness) BeginDraining() {
	if g != nil {
		g.draining.Store(true)
	}
}

func (g *lifecycleReadiness) Ready(ctx context.Context) error {
	if ctx == nil {
		return errors.New("readiness context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if g == nil || g.draining.Load() {
		return errors.New("server is draining")
	}
	g.mu.RLock()
	delegate := g.delegate
	g.mu.RUnlock()
	if delegate == nil {
		return errors.New("readiness is not configured")
	}
	return delegate.Ready(ctx)
}

type lifecycleCoordinatorConfig struct {
	server            processHTTPServer
	serveDone         <-chan error
	runtime           processRuntime
	beginDraining     func()
	forceSignal       <-chan struct{}
	shutdownTimeout   time.Duration
	forceCloseTimeout time.Duration
}

// runProcessLifecycle owns the already-bound listener and all process-level
// shutdown. The caller supplies a signal context; this function never exits
// the process itself.
func runProcessLifecycle(ctx context.Context, config lifecycleCoordinatorConfig) (int, error) {
	if ctx == nil || config.server == nil || config.serveDone == nil {
		return processExitFailure, newLifecycleError("process", "invalid_configuration", errProcessRuntimeStop)
	}
	if config.shutdownTimeout <= 0 {
		config.shutdownTimeout = defaultShutdownTimeout
	}
	if config.forceCloseTimeout <= 0 {
		config.forceCloseTimeout = defaultForceCloseLimit
	}

	select {
	case serveErr := <-config.serveDone:
		exitCode, shutdownErr := config.shutdown(false)
		if shutdownErr == nil {
			shutdownErr = newLifecycleError("serve", "unexpected_exit", errors.Join(errProcessServeFailure, serveErr))
		} else {
			shutdownErr = errors.Join(newLifecycleError("serve", "unexpected_exit", errors.Join(errProcessServeFailure, serveErr)), shutdownErr)
		}
		if exitCode == processExitOK {
			exitCode = processExitFailure
		}
		return exitCode, shutdownErr
	case <-config.forceSignal:
		return config.shutdown(true)
	case <-ctx.Done():
		return config.shutdown(false)
	}
}

func (c lifecycleCoordinatorConfig) shutdown(forced bool) (int, error) {
	if c.runtime != nil {
		c.runtime.BeginDraining()
	}
	if c.beginDraining != nil {
		c.beginDraining()
	}

	deadline := time.Now().Add(c.shutdownTimeout)
	var shutdownErr error
	if err, phaseForced := c.stopHTTP(deadline); err != nil {
		shutdownErr = errors.Join(shutdownErr, err)
		forced = forced || phaseForced
	}

	if err, phaseForced := c.stopRuntime(deadline, forced); err != nil {
		shutdownErr = errors.Join(shutdownErr, err)
		forced = forced || phaseForced
	}

	if serveErr := c.collectServeError(); serveErr != nil {
		shutdownErr = errors.Join(shutdownErr, serveErr)
	}
	if forced {
		shutdownErr = errors.Join(shutdownErr, newLifecycleError("process", "forced_close", errProcessForcedClose))
		return processExitForced, shutdownErr
	}
	if shutdownErr != nil {
		return processExitFailure, shutdownErr
	}
	return processExitOK, nil
}

func (c lifecycleCoordinatorConfig) stopHTTP(deadline time.Time) (error, bool) {
	phaseCtx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.server.Shutdown(phaseCtx) }()
	select {
	case err := <-done:
		if err == nil {
			return nil, false
		}
		if errors.Is(err, context.DeadlineExceeded) || phaseCtx.Err() != nil {
			closeErr := c.closeHTTP()
			return errors.Join(newLifecycleError("http", "drain_timeout", errors.Join(errProcessHTTPDrain, err)), closeErr), true
		}
		closeErr := c.closeHTTP()
		return errors.Join(newLifecycleError("http", "shutdown_failure", err), closeErr), false
	case <-c.forceSignal:
		closeErr := c.closeHTTP()
		return errors.Join(newLifecycleError("http", "forced_close", errProcessForcedClose), closeErr), true
	case <-phaseCtx.Done():
		closeErr := c.closeHTTP()
		return errors.Join(newLifecycleError("http", "drain_timeout", errors.Join(errProcessHTTPDrain, phaseCtx.Err())), closeErr), true
	}
}

func (c lifecycleCoordinatorConfig) closeHTTP() error {
	if err := c.server.Close(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return newLifecycleError("http", "close_failure", err)
	}
	return nil
}

func (c lifecycleCoordinatorConfig) stopRuntime(deadline time.Time, forced bool) (error, bool) {
	if c.runtime == nil {
		return nil, false
	}
	remaining := time.Until(deadline)
	if forced || remaining <= 0 {
		if !forced {
			forced = true
		}
		c.forceRuntimeClose()
		stopCtx, cancel := context.WithTimeout(context.Background(), c.forceCloseTimeout)
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- c.runtime.Stop(stopCtx) }()
		select {
		case err := <-done:
			if err != nil {
				return classifyRuntimeStop(err), forced
			}
			return nil, forced
		case <-stopCtx.Done():
			c.forceRuntimeClose()
			return newLifecycleError("runtime", "forced_close", errors.Join(errProcessForcedClose, stopCtx.Err())), true
		}
	}

	stopCtx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.runtime.Stop(stopCtx) }()
	select {
	case err := <-done:
		if err == nil {
			return nil, false
		}
		return classifyRuntimeStop(err), false
	case <-c.forceSignal:
		c.forceRuntimeClose()
		return newLifecycleError("runtime", "forced_close", errProcessForcedClose), true
	case <-stopCtx.Done():
		c.forceRuntimeClose()
		return newLifecycleError("runtime", "shutdown_deadline", errors.Join(errProcessRuntimeStop, stopCtx.Err())), true
	}
}

func (c lifecycleCoordinatorConfig) forceRuntimeClose() {
	if c.runtime == nil {
		return
	}
	done := make(chan struct{})
	go func() {
		c.runtime.ForceClose()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(c.forceCloseTimeout):
	}
}

func (c lifecycleCoordinatorConfig) collectServeError() error {
	select {
	case err := <-c.serveDone:
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return newLifecycleError("serve", "failure", errors.Join(errProcessServeFailure, err))
	default:
		return nil
	}
}

func classifyRuntimeStop(err error) error {
	if err == nil {
		return nil
	}
	var classified error
	if errors.Is(err, worker.ErrDrainTimeout) || errors.Is(err, worker.ErrShutdownDeadline) || errors.Is(err, errProcessWorkerDrain) {
		classified = errors.Join(classified, newLifecycleError("worker", "unresolved", errors.Join(errProcessWorkerDrain, err)))
	}
	if errors.Is(err, outbox.ErrShutdownTimeout) || errors.Is(err, errProcessDispatcherDrain) {
		classified = errors.Join(classified, newLifecycleError("dispatcher", "timeout", errors.Join(errProcessDispatcherDrain, err)))
	}
	if classified != nil {
		return classified
	}
	return newLifecycleError("runtime", "stop_failure", errors.Join(errProcessRuntimeStop, err))
}

func startSecondSignalGate(signals <-chan os.Signal) (<-chan struct{}, func()) {
	if signals == nil {
		return nil, func() {}
	}
	force := make(chan struct{})
	stop := make(chan struct{})
	var stopOnce sync.Once
	go func() {
		count := 0
		for {
			select {
			case <-stop:
				return
			case _, ok := <-signals:
				if !ok {
					return
				}
				count++
				if count >= 2 {
					close(force)
					return
				}
			}
		}
	}()
	return force, func() { stopOnce.Do(func() { close(stop) }) }
}

func lifecycleDuration(name string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func runService(ctx context.Context, signals <-chan os.Signal, stdout, stderr io.Writer) (int, error) {
	if ctx == nil {
		return processExitFailure, newLifecycleError("process", "invalid_context", errProcessStartup)
	}
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	forceSignal, stopForceSignal := startSecondSignalGate(signals)
	defer stopForceSignal()

	store := platform.NewMemoryStore()
	if err := store.SaveTenant(context.Background(), platform.Tenant{
		ID: env("DEFAULT_TENANT", "demo"), Name: "Demo tenant",
		Agent: platform.AgentConfig{
			Name:           env("DEFAULT_AGENT_APP_ID", "demo-agent"),
			Model:          env("MODEL", "openai"),
			ModelConfigRef: env("MODEL_CONFIG_REF", "env"),
			ToolPolicyRef:  env("TOOL_POLICY_REF", "default"),
		},
		Backend: platform.BackendConfig{Session: "memory", Memory: "memory", Vector: "none"},
	}); err != nil {
		return processExitFailure, newLifecycleError("startup", "memory_store", errors.Join(errProcessStartup, err))
	}
	responder, err := newResponder(env("MODEL_PROVIDER", "runner"))
	if err != nil {
		return processExitFailure, newLifecycleError("startup", "responder", errors.Join(errProcessStartup, err))
	}
	server := web.NewServer(store, platform.Runner{Store: store, Responder: responder})
	server.SetAccepting(false)
	startupGate := &startupReadiness{}
	readinessGate := &lifecycleReadiness{delegate: startupGate}
	server.SetReadiness(readinessGate)

	addr := env("HTTP_ADDR", ":8080")
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return processExitFailure, newLifecycleError("listen", "bind_failure", errors.Join(errProcessStartup, err))
	}
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- httpServer.Serve(listener) }()
	if _, writeErr := fmt.Fprintf(stdout, "trpc-agent-service %s listening on %s\n", trpcservice.Version, listener.Addr().String()); writeErr != nil {
		return failStartupService(httpServer, listener, serveDone, nil, "announce", errors.Join(errProcessStartup, writeErr))
	}
	if os.Getenv("G_D_TEST_MODE") == "1" {
		if hold := lifecycleDuration("G_D_STARTUP_HOLD", 0); hold > 0 {
			timer := time.NewTimer(hold)
			select {
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return failStartupService(httpServer, listener, serveDone, nil, "context", errors.Join(errProcessStartup, ctx.Err()))
			}
		}
	}

	var runtimeValue *productionRuntime
	if os.Getenv("DATABASE_URL") != "" {
		runnerResponder, ok := responder.(platform.RuntimeResponder)
		if !ok {
			return failStartupService(httpServer, listener, serveDone, nil, "responder", errProcessStartup)
		}
		startupCtx, startupCancel := context.WithTimeout(ctx, lifecycleDuration("STARTUP_TIMEOUT", defaultStartupTimeout))
		runtimeValue, err = assembleProduction(startupCtx, runtimeResponderAdapter{value: runnerResponder})
		startupCancel()
		if err != nil {
			return failStartupService(httpServer, listener, serveDone, nil, "production_assembly", errors.Join(errProcessStartup, err))
		}
		if err := runtimeValue.Start(ctx); err != nil {
			return failStartupService(httpServer, listener, serveDone, runtimeValue, "runtime_start", errors.Join(errProcessStartup, err))
		}
		server.Resolver = runtimeValue.resolver
		server.AsyncIngress = runtimeValue.ingress
		if middleware := runtimeValue.TelemetryWebMiddleware(); middleware != nil {
			server.SetTelemetryMiddleware(middleware)
		}
		readinessGate.SetDelegate(runtimeValue.readiness)
		runtimeValue.beginDrainingHook = server.BeginDraining
	} else {
		startupGate.SetReady()
	}
	server.SetAccepting(true)

	return runProcessLifecycle(ctx, lifecycleCoordinatorConfig{
		server:    httpServer,
		serveDone: serveDone,
		runtime:   runtimeValue,
		beginDraining: func() {
			server.BeginDraining()
			readinessGate.BeginDraining()
		},
		forceSignal:       forceSignal,
		shutdownTimeout:   lifecycleDuration("SHUTDOWN_TIMEOUT", defaultShutdownTimeout),
		forceCloseTimeout: lifecycleDuration("FORCE_CLOSE_TIMEOUT", defaultForceCloseLimit),
	})
}

func failStartupService(server processHTTPServer, _ net.Listener, serveDone <-chan error, runtime *productionRuntime, phase string, cause error) (int, error) {
	cleanupErr := error(nil)
	if runtime != nil {
		ctx, cancel := context.WithTimeout(context.Background(), startupCleanupTimeout)
		if err := runtime.Stop(ctx); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
		}
		cancel()
	}
	if err := server.Close(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		cleanupErr = errors.Join(cleanupErr, newLifecycleError("http", "close_failure", err))
	}

	select {
	case <-serveDone:
	case <-time.After(startupCleanupTimeout):
	}
	return processExitFailure, errors.Join(newLifecycleError("startup", phase, errors.Join(errProcessStartup, cause)), cleanupErr)
}

func logLifecycleResult(w io.Writer, err error) {
	if err == nil {
		if _, writeErr := fmt.Fprint(w, "service stopped status=clean\n"); writeErr != nil {
			return
		}
		return
	}
	if _, writeErr := fmt.Fprintf(w, "service stopped status=%s\n", lifecycleStatus(err)); writeErr != nil {
		return
	}
}

func installProcessSignals() (context.Context, <-chan os.Signal, func()) {
	ctx, stopContext := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	return ctx, signals, func() {
		stopContext()
		signal.Stop(signals)
	}
}
