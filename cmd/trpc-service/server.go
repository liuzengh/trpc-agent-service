package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/safego"
)

const shutdownTimeout = 10 * time.Second

// ServeHTTPServer serves until the root context is cancelled, then stops new
// requests and waits for in-flight requests to finish before returning.
func ServeHTTPServer(ctx context.Context, server *http.Server, listener net.Listener) error {
	if ctx == nil || server == nil || listener == nil {
		return fmt.Errorf("server context, HTTP server, and listener are required")
	}
	server.Handler = safego.RecoverHTTP(server.Handler)
	if server.ErrorLog == nil {
		server.ErrorLog = safego.HTTPErrorLog()
	}
	serveErrors := make(chan error, 1)
	go func() {
		if panicErr := safego.Run("HTTP server", func() { serveErrors <- server.Serve(listener) }); panicErr != nil {
			serveErrors <- panicErr
		}
	}()
	select {
	case err := <-serveErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}
	shutdownContext, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownContext); err != nil {
		return fmt.Errorf("graceful HTTP shutdown: %w", err)
	}
	err := <-serveErrors
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
