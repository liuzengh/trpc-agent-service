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
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// freePort reserves a port and releases it so the server under test can bind it.
// Racy in principle, adequate for a single-process test.
func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("release port: %v", err)
	}
	return addr
}

func waitForServer(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			c.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server at %s never became reachable", addr)
}

// TestServeUntilSignal_SignalDrainsAndRunsHooks is the P1-5 signal test: a
// signal delivered to the lifecycle seam must stop the listener, flip
// readiness to draining, and run cleanup hooks. The production wrapper still
// uses signal.Notify; this test avoids platform-specific process signalling.
func TestServeUntilSignal_SignalDrainsAndRunsHooks(t *testing.T) {
	addr := freePort(t)
	shutdown := NewCoordinator()

	var hookRan atomic.Bool
	var drainingWhenHookRan atomic.Bool
	shutdown.OnShutdown("record-state", func(context.Context) error {
		hookRan.Store(true)
		// Readiness must already report draining by the time hooks run,
		// otherwise a load balancer could still be sending traffic.
		drainingWhenHookRan.Store(shutdown.Draining())
		return nil
	})

	srv := &http.Server{
		Addr: addr,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
	}

	done := make(chan error, 1)
	quit := make(chan os.Signal, 1)
	go func() {
		done <- serveUntilSignalOnChannel(srv, shutdown, "test-server", DrainGracePeriod, quit)
	}()

	waitForServer(t, addr)

	if shutdown.Draining() {
		t.Fatal("server reported draining before any signal was sent")
	}

	quit <- os.Interrupt

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ServeUntilSignal returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ServeUntilSignal did not return after signal")
	}

	if !hookRan.Load() {
		t.Error("cleanup hook did not run on signal shutdown")
	}
	if !drainingWhenHookRan.Load() {
		t.Error("readiness still reported ready while cleanup hooks were running")
	}
	if !shutdown.Draining() {
		t.Error("coordinator not marked draining after shutdown")
	}

	// The listener must be closed: a fresh connection should be refused.
	if c, err := net.DialTimeout("tcp", addr, 300*time.Millisecond); err == nil {
		c.Close()
		t.Error("listener still accepting connections after shutdown")
	}
}

// TestShutdownSignals_IncludesSIGTERM pins the production signal set. The drain
// test above substitutes SIGUSR1 for practical reasons, so this asserts that
// what the binaries actually register includes the signal Kubernetes sends.
func TestShutdownSignals_IncludesSIGTERM(t *testing.T) {
	var sawTERM, sawINT bool
	for _, s := range ShutdownSignals {
		switch s {
		case syscall.SIGTERM:
			sawTERM = true
		case syscall.SIGINT:
			sawINT = true
		}
	}
	if !sawTERM {
		t.Error("ShutdownSignals does not include SIGTERM; Kubernetes shutdown would not drain")
	}
	if !sawINT {
		t.Error("ShutdownSignals does not include SIGINT")
	}
}

// TestServeUntilSignal_WaitsForInFlightRequest proves a request already
// executing when the signal arrives finishes instead of being cut off.
func TestServeUntilSignal_WaitsForInFlightRequest(t *testing.T) {
	addr := freePort(t)
	shutdown := NewCoordinator()

	handlerEntered := make(chan struct{})
	var handlerCompleted atomic.Bool

	slow := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(handlerEntered)
		time.Sleep(400 * time.Millisecond)
		handlerCompleted.Store(true)
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Addr: addr, Handler: shutdown.Middleware(slow)}

	done := make(chan error, 1)
	quit := make(chan os.Signal, 1)
	go func() {
		done <- serveUntilSignalOnChannel(srv, shutdown, "test-server", DrainGracePeriod, quit)
	}()

	waitForServer(t, addr)

	respCh := make(chan int, 1)
	go func() {
		resp, err := http.Get(fmt.Sprintf("http://%s/slow", addr))
		if err != nil {
			respCh <- -1
			return
		}
		defer resp.Body.Close()
		respCh <- resp.StatusCode
	}()

	<-handlerEntered
	// Signal while the handler is still inside its sleep.
	quit <- os.Interrupt

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ServeUntilSignal returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ServeUntilSignal did not return")
	}

	if !handlerCompleted.Load() {
		t.Error("in-flight handler was cut off instead of draining")
	}

	select {
	case code := <-respCh:
		if code != http.StatusOK {
			t.Errorf("in-flight request status = %d, want 200", code)
		}
	case <-time.After(3 * time.Second):
		t.Error("in-flight request never received a response")
	}
}

// TestServeUntilSignal_ReturnsListenError guards the early-exit path: a bind
// failure must surface immediately instead of blocking on a signal forever.
func TestServeUntilSignal_ReturnsListenError(t *testing.T) {
	// Hold the port so the server under test cannot bind it.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("occupy port: %v", err)
	}
	defer l.Close()

	srv := &http.Server{Addr: l.Addr().String(), Handler: http.NewServeMux()}

	done := make(chan error, 1)
	quit := make(chan os.Signal, 1)
	go func() {
		done <- serveUntilSignalOnChannel(srv, NewCoordinator(), "test-server", DrainGracePeriod, quit)
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected a listen error, got nil")
		}
		if errors.Is(err, http.ErrServerClosed) {
			t.Errorf("listen error misreported as clean close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bind failure did not return; ServeUntilSignal is waiting for a signal")
	}
}
