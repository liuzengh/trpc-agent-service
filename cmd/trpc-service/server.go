package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const (
	serviceReadHeaderTimeout = 5 * time.Second
	serviceReadTimeout       = 15 * time.Second
	serviceIdleTimeout       = 60 * time.Second
	serviceReadinessTimeout  = 2 * time.Second
	serviceMaxHeaderBytes    = 32 << 10
)

type readinessCheck func(context.Context) error

type readinessState struct {
	ready atomic.Bool
	check readinessCheck
}

func (s *readinessState) setReady(value bool) {
	if s != nil {
		s.ready.Store(value)
	}
}

func (s *readinessState) isReady(ctx context.Context) bool {
	if s == nil || !s.ready.Load() {
		return false
	}
	if s.check == nil {
		return true
	}
	checkCtx, cancel := context.WithTimeout(ctx, serviceReadinessTimeout)
	defer cancel()
	return s.check(checkCtx) == nil
}

type serviceServer struct {
	server    *http.Server
	done      chan struct{}
	readiness *readinessState

	mu           sync.Mutex
	serveErr     error
	shutdownOnce sync.Once
	shutdownErr  error
}

func startServiceServer(
	address string,
	ingressHandler, adminHandler http.Handler,
	check readinessCheck,
) (*serviceServer, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen service server: %w", err)
	}
	readiness := &readinessState{check: check}
	service := &serviceServer{
		server: &http.Server{
			Handler:           serviceHandlerWithReadiness(ingressHandler, adminHandler, readiness),
			ReadHeaderTimeout: serviceReadHeaderTimeout,
			ReadTimeout:       serviceReadTimeout,
			IdleTimeout:       serviceIdleTimeout,
			MaxHeaderBytes:    serviceMaxHeaderBytes,
		},
		done:      make(chan struct{}),
		readiness: readiness,
	}
	go func() {
		err := service.server.Serve(listener)
		service.mu.Lock()
		service.serveErr = err
		service.mu.Unlock()
		close(service.done)
	}()
	return service, nil
}

func serviceHandler(ingressHandler, adminHandler http.Handler) http.Handler {
	readiness := &readinessState{}
	readiness.setReady(true)
	return serviceHandlerWithReadiness(ingressHandler, adminHandler, readiness)
}

func serviceHandlerWithReadiness(
	ingressHandler, adminHandler http.Handler,
	readiness *readinessState,
) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if readiness != nil && readiness.isReady(r.Context()) {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	if ingressHandler != nil {
		mux.Handle("/v1/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if readiness == nil || !readiness.isReady(r.Context()) {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			ingressHandler.ServeHTTP(w, r)
		}))
	}
	if adminHandler != nil {
		mux.Handle("/admin/v1/", adminHandler)
	}
	return mux
}

// MarkReady is called only after this process has started every role-owned
// component. readiness checks still probe PostgreSQL and Redis on each call.
func (s *serviceServer) MarkReady() {
	if s != nil {
		s.readiness.setReady(true)
	}
}

// MarkNotReady stops this process from receiving new traffic before draining.
func (s *serviceServer) MarkNotReady() {
	if s != nil {
		s.readiness.setReady(false)
	}
}

func (s *serviceServer) shutdown(ctx context.Context) error {
	if s == nil || s.server == nil {
		return nil
	}
	s.shutdownOnce.Do(func() {
		s.MarkNotReady()
		if err := s.server.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.shutdownErr = err
			return
		}
		s.shutdownErr = s.wait()
	})
	return s.shutdownErr
}

func (s *serviceServer) wait() error {
	if s == nil {
		return nil
	}
	<-s.done
	s.mu.Lock()
	err := s.serveErr
	s.mu.Unlock()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
