package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	platformconfig "github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/security"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionrun"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
)

const shutdownTimeout = 10 * time.Second

// startupTimeout bounds everything between the first connection attempt and a
// serving process. It has to allow for a migration waiting on the advisory lock
// another booting worker is holding, and it exists so an unreachable database
// fails the process instead of hanging it. The inmemory profile never reaches
// it, having nothing to wait for.
const startupTimeout = 30 * time.Second

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "HTTP listen address")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Printf("trpc-agent-service %s\n", trpcservice.Version)
		return
	}
	if err := run(*addr); err != nil {
		log.Fatalf("trpc-agent-service: %v", err)
	}
}

// run starts the process and returns when it has stopped serving and released
// everything it owns.
//
// The error is named because shutdown contributes to it. Closing a session
// store or a connection pool can fail, and those failures used to be logged and
// dropped; joined into the result they reach the exit status, where a
// supervisor can see them.
//
// The order below is the whole lifecycle, and each step is placed where it is
// on purpose:
//
//   - The listen address is checked first. It costs nothing and it is the guard
//     that keeps the control plane off the network, so it must not sit behind
//     anything that can connect to a database.
//   - The security configuration is loaded, resolved and cross-validated in
//     full before anything is opened. It reads a file and the environment and
//     touches nothing else, so it is the cheapest possible refusal — and a
//     process whose credentials are misconfigured must not have connected to a
//     shared database or run a migration on the way to finding that out.
//   - The storage configuration is loaded and validated as a whole, before a
//     single resource is opened, so a typo in a schema name is a refusal rather
//     than a half-built process.
//   - Cleanup is registered by deferring, which makes the shutdown order the
//     reverse of the startup order for free: HTTP drain inside waitForStop,
//     then the IM channel, then the RuntimeResolver, then the storage Router,
//     then the session store and the shared pool. Each step waits for the one
//     above it to let go. The channel waits for the message it is executing or
//     answering; the resolver waits for in-flight runtimes; the Router waits for
//     the storage leases those runtimes hold, including the borrowed lease on
//     the process default; and only then is the session store closed. A runtime
//     still writing to a session store that had already been closed would lose
//     the last turn of the conversation it was serving.
func run(addr string) error {
	return runWith(addr, os.Getenv, defaultStorageDeps())
}

// runWith is run with its two sources of outside state made explicit: the
// environment it reads, and the constructors that actually touch a database.
//
// The seam exists so the startup *order* can be tested rather than merely
// documented. Given an environment that is wrong in two ways at once, a test can
// assert which refusal comes back and that no storage constructor was reached —
// which is the only way to keep "security first" from decaying into "security
// eventually" as steps are added above it.
func runWith(addr string, getenv func(string) string, deps storageDeps) (err error) {
	if err := validateListenAddr(addr); err != nil {
		return err
	}

	// Before storage, and before any deadline: the tool registry is the same
	// static one every Runtime resolves against, so a policy this manifest
	// entitles is checked against the policies that actually exist.
	securityCfg, err := security.Load(getenv, tool.Builtin())
	if err != nil {
		return err
	}
	// Safe to log: Description names configuration sources and counts, never a
	// key, a hash or a resolved value.
	log.Printf("security %s", securityCfg.Description)

	storageCfg, err := loadStorageConfig(getenv)
	if err != nil {
		return err
	}
	// Safe to log: describe reports presence, never contents.
	log.Printf("storage %s", storageCfg.describe())

	// Read here, with the other configuration, so that a misconfigured collector
	// is a refusal before anything is opened. Nothing is built yet; see
	// startWeComChannel.
	telemetryCfg, err := telemetry.Load(getenv)
	if err != nil {
		return err
	}
	// Safe to log: Describe renders the checked origin, which cannot carry a
	// credential.
	log.Printf("telemetry %s", telemetryCfg.Describe())

	// One deadline over every connection, migration and constructor between
	// here and a serving process.
	startupCtx, cancelStartup := context.WithTimeout(context.Background(), startupTimeout)
	defer cancelStartup()

	stack, err := openStorage(startupCtx, storageCfg, deps)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, stack.close()) }()

	if err := platformconfig.SeedDemo(startupCtx, stack.repository); err != nil {
		return err
	}

	runtimes, err := openRuntimeStack(storageCfg, stack, getenv, securityCfg.Revisions)
	if err != nil {
		return err
	}
	// Registered after the stack's own defer, so it runs before it: the
	// Runtimes and their storage Bundles are released while the store they hold
	// is still open. The order inside is runtimeStack.close's, not this line's.
	defer func() { err = errors.Join(err, runtimes.close()) }()

	// One run service for the whole process. HTTP is its first caller and the IM
	// channels will be the next; the lease, the pin and the Runtime lease are
	// sequenced here once, so a second entry point cannot sequence them
	// differently.
	runs, err := sessionrun.NewService(runtimes.resolver, stack.directory, stack.coordinator)
	if err != nil {
		return err
	}

	// Started here and registered after the Runtime defer, so that it stops
	// before the Runtimes it executes through and the pool it writes through are
	// released. It is off unless configured; see startWeComChannel.
	//
	// Both channel configurations are cross-checked first: two enabled channels
	// sharing one (tenant, binding) would claim each other's rows, so neither is
	// started. See checkChannelBindings.
	if err := checkChannelBindings(getenv); err != nil {
		return err
	}
	channel, err := startWeComChannel(startupCtx, storageCfg, telemetryCfg, stack, runs, getenv)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, channel.stop()) }()
	feishuChan, err := startFeishuChannel(startupCtx, storageCfg, telemetryCfg, stack, runs, getenv)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, feishuChan.stop()) }()

	api, err := web.NewPlatformServer(
		stack.repository,
		// The same repository the Router resolves through, so a profile this
		// API accepts is one the data plane can already see.
		stack.profiles,
		runs,
		securityCfg.Chat,
		securityCfg.Admin,
		securityCfg.Revisions,
	)
	if err != nil {
		return err
	}

	httpServer := &http.Server{
		Addr:              addr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		log.Printf(
			"trpc-agent-service %s listening on %s; chat=/v1/chat/completions admin=/admin/v1/tenants",
			trpcservice.Version,
			addr,
		)
		errCh <- httpServer.ListenAndServe()
	}()

	signalCtx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	return waitForStop(
		signalCtx, errCh, channel.failed(), httpServer, shutdownTimeout, feishuChan.failed())
}

// loopbackHostname is the only non-literal host accepted as loopback. Resolving
// arbitrary names would make the guard depend on whatever the resolver answers
// at startup, so anything else is refused even if it happens to point at 127/8.
const loopbackHostname = "localhost"

// validateListenAddr fails closed on every address that is not loopback.
//
// The Admin API now authenticates, so this is no longer the only thing standing
// between the control plane and the network — but it stays, and it stays
// non-overridable. What it defends is everything authentication does not: this
// process serves plain HTTP, so a routable bind would put admin Bearer tokens on
// the wire in cleartext, and the demo profile still boots with a published chat
// key. Exposure is a deployment decision that belongs to a reverse proxy
// terminating TLS, not to an environment variable read by this binary.
func validateListenAddr(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid listen address %q: %w", addr, err)
	}
	if strings.EqualFold(host, loopbackHostname) {
		return nil
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return nil
	}
	// An empty host is the wildcard form (":8080"), which is the easiest way to
	// expose this process by accident.
	return fmt.Errorf(
		"refusing to listen on %q: this process serves plain HTTP and must sit behind a "+
			"TLS-terminating proxy, so it may only bind a loopback address such as "+
			"127.0.0.1:8080, localhost:8080 or [::1]:8080",
		addr,
	)
}

type httpServerLifecycle interface {
	Shutdown(context.Context) error
	Close() error
}

// waitForStop blocks until the process is signalled or the server stops serving.
// Both paths must leave no connection open: an active SSE response still holds a
// runtime lease, and RuntimeResolver.Close waits for every lease to be released.
//
// A channel that has failed terminally is a third way to stop. It is not a
// serve error — HTTP is still healthy and still holding requests — so it takes
// the graceful path, and its own error is kept: it is the reason the process
// exited, and the deferred stop above only reports what is left in the channel.
func waitForStop(
	signalCtx context.Context,
	serveErrCh <-chan error,
	channelErrCh <-chan error,
	server httpServerLifecycle,
	timeout time.Duration,
	extraChannelErrCh ...<-chan error,
) error {
	// A nil channel blocks forever, so a process running one channel or none
	// waits on the same select as one running both.
	var secondChannelErrCh <-chan error
	if len(extraChannelErrCh) > 0 {
		secondChannelErrCh = extraChannelErrCh[0]
	}
	select {
	case <-signalCtx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		return shutdownHTTPServer(shutdownCtx, server)
	case err := <-serveErrCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return errors.Join(err, server.Close())
	case err := <-channelErrCh:
		shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		return errors.Join(err, shutdownHTTPServer(shutdownCtx, server))
	case err := <-secondChannelErrCh:
		shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		return errors.Join(err, shutdownHTTPServer(shutdownCtx, server))
	}
}

// shutdownHTTPServer drains in-flight requests and forces the remaining
// connections closed when the graceful deadline expires or Shutdown fails.
func shutdownHTTPServer(ctx context.Context, server httpServerLifecycle) error {
	if err := server.Shutdown(ctx); err != nil {
		return errors.Join(err, server.Close())
	}
	return nil
}
