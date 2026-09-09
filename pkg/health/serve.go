//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package health

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// ShutdownSignals are the signals that begin a graceful shutdown. SIGTERM is
// what Kubernetes sends before the kill deadline; SIGINT covers local runs.
var ShutdownSignals = []os.Signal{syscall.SIGINT, syscall.SIGTERM}

// ValidateDrainTimeout ensures a service can finish its longest detached
// operation and persist the resulting lease/state transition before the
// process exits. Composition roots should call this before starting the
// listener; otherwise a too-short drain window can hard-kill claimed work.
func ValidateDrainTimeout(name string, operationTimeout, drainTimeout, persistenceMargin time.Duration) error {
	if strings.TrimSpace(name) == "" {
		name = "service"
	}
	if operationTimeout <= 0 || drainTimeout <= 0 || persistenceMargin < 0 {
		return fmt.Errorf("%s timeout values must be positive (margin may be zero)", name)
	}
	if operationTimeout > time.Duration(1<<63-1)-persistenceMargin {
		return fmt.Errorf("%s operation timeout overflows drain budget", name)
	}
	required := operationTimeout + persistenceMargin
	if drainTimeout < required {
		return fmt.Errorf("%s shutdown timeout %s must be at least operation timeout %s plus persistence margin %s", name, drainTimeout, operationTimeout, persistenceMargin)
	}
	return nil
}

// ServeUntilSignal runs srv until a shutdown signal arrives, then drains it in
// the order that avoids dropping work:
//
//  1. Coordinator.BeginDrain fails readiness and rejects new tracked work.
//  2. http.Server.Shutdown stops accepting new connections and waits for
//     handlers that have already started.
//  3. Coordinator.Shutdown waits for work tracked
//     outside the HTTP handler (background execution, queued deliveries), and
//     runs cleanup hooks in reverse registration order.
//
// Server shutdown alone is not sufficient: a load balancer that has not yet observed the
// readiness change keeps sending requests, and work spawned by a handler can
// outlive it. BeginDrain alone is not sufficient either, because the listener would
// stay open. Both are required.
//
// name appears in log lines. signals defaults to ShutdownSignals when empty,
// and is injectable so tests can drive the real signal path.
func ServeUntilSignal(
	srv *http.Server,
	shutdown *Coordinator,
	name string,
	signals ...os.Signal,
) error {
	return ServeUntilSignalWithTimeout(srv, shutdown, name, DrainGracePeriod, signals...)
}

// ServeUntilSignalWithTimeout is ServeUntilSignal with a service-specific
// drain window. Agent execution and channel delivery can legitimately exceed
// the generic HTTP default, so their composition roots must choose a timeout
// greater than the maximum operation timeout plus persistence margin.
func ServeUntilSignalWithTimeout(
	srv *http.Server,
	shutdown *Coordinator,
	name string,
	drainTimeout time.Duration,
	signals ...os.Signal,
) error {
	if drainTimeout <= 0 {
		return fmt.Errorf("%s drain timeout must be positive", name)
	}
	if len(signals) == 0 {
		signals = ShutdownSignals
	}

	// Register for signals before serving, so a signal arriving during startup
	// is not lost.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, signals...)
	defer signal.Stop(quit)

	return serveUntilSignalOnChannel(srv, shutdown, name, drainTimeout, quit)
}

// serveUntilSignalOnChannel contains the listener and drain lifecycle behind
// a small package-local seam. Production callers use signal.Notify above;
// tests can inject a signal value without relying on platform-specific process
// signal delivery (Windows cannot send SIGUSR/Interrupt to itself).
func serveUntilSignalOnChannel(
	srv *http.Server,
	shutdown *Coordinator,
	name string,
	drainTimeout time.Duration,
	quit <-chan os.Signal,
) error {

	serveErr := make(chan error, 1)
	go func() {
		log.Printf("%s listening on %s", name, srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	// Exit early if the listener fails (for example, port already in use)
	// instead of waiting for a signal that will never come.
	select {
	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("%s serve: %w", name, err)
		}
		return nil
	case sig := <-quit:
		log.Printf("%s received %v, shutting down", name, sig)
	}

	// Fail readiness and reject newly tracked work before closing the listener.
	shutdown.BeginDrain()

	ctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()

	var errs []error

	if err := srv.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("http shutdown: %w", err))
	}

	// Always run the coordinator, even if the HTTP shutdown timed out, so
	// leases and connections are still released.
	if err := shutdown.Shutdown(ctx); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("%s shutdown incomplete: %w", name, errors.Join(errs...))
	}

	log.Printf("%s stopped cleanly", name)
	return nil
}
