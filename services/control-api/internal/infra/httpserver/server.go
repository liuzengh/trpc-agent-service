// Package httpserver owns the Control API process HTTP mechanics.
package httpserver

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"time"
)

// Server wraps net/http lifecycle without owning business routes.
type Server struct {
	server *http.Server
	tls    bool
}

// New returns an HTTP server for the already assembled handler.
func New(address string, handler http.Handler) *Server {
	return &Server{server: &http.Server{
		Addr:              address,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}}
}

// ListenAndServe starts accepting HTTP traffic.
func (s *Server) ListenAndServe() error {
	var err error
	if s.tls {
		err = s.server.ListenAndServeTLS("", "")
	} else {
		err = s.server.ListenAndServe()
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown drains active requests.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

// NewTLS serves a separately assembled mTLS-only handler in the same workload.
func NewTLS(address string, handler http.Handler, config *tls.Config) *Server {
	s := New(address, handler)
	s.tls = true
	s.server.TLSConfig = config.Clone()
	s.server.ReadTimeout = 5 * time.Second
	s.server.WriteTimeout = 6 * time.Second
	return s
}
