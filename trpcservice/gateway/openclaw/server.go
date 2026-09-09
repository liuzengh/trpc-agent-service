package openclaw

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"
)

// Server adapts the gateway handler to the root App component lifecycle.
type Server struct {
	Address           string
	Handler           http.Handler
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	MaxHeaderBytes    int
	mu                sync.Mutex
	server            *http.Server
	listener          net.Listener
}

// Start binds before returning, so lifecycle startup failures are observable.
func (component *Server) Start(_ context.Context) error {
	if component == nil || component.Handler == nil || component.Address == "" {
		return errors.New("openclaw: address and handler are required")
	}
	listener, err := net.Listen("tcp", component.Address)
	if err != nil {
		return err
	}
	component.mu.Lock()
	component.listener = listener
	component.server = &http.Server{
		Handler:           component.Handler,
		ReadHeaderTimeout: defaultDuration(component.ReadHeaderTimeout, 10*time.Second),
		ReadTimeout:       defaultDuration(component.ReadTimeout, 30*time.Second),
		// A non-zero http.Server WriteTimeout aborts long-lived SSE streams.
		// Streaming handlers enforce their own request lifetime instead.
		WriteTimeout:   component.WriteTimeout,
		IdleTimeout:    defaultDuration(component.IdleTimeout, 120*time.Second),
		MaxHeaderBytes: defaultInt(component.MaxHeaderBytes, 1<<20),
	}
	server := component.server
	component.mu.Unlock()
	go func() { _ = server.Serve(listener) }()
	return nil
}

func defaultDuration(value, fallback time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	return fallback
}

func defaultInt(value, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

// Close gracefully drains HTTP callbacks and streams.
func (component *Server) Close(ctx context.Context) error {
	component.mu.Lock()
	server := component.server
	component.mu.Unlock()
	if server == nil {
		return nil
	}
	return server.Shutdown(ctx)
}
