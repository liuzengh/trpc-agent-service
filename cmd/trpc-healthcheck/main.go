// Package main provides the small static health-check binary used by the
// service image. Keeping the probe outside the service command lets the
// runtime image remain distroless while Docker and Compose still get an
// in-container health check.
package main

import (
	"context"
	"io"
	"net/http"
	"os"
	"time"

	"go.uber.org/zap"
)

const (
	defaultHealthURL = "http://127.0.0.1:8080/healthz"
	healthTimeout    = 2 * time.Second
)

var exitProcess = os.Exit

func main() {
	restoreLogger, err := configureLogger(os.Stderr)
	if err != nil {
		exitProcess(1)
		return
	}
	defer restoreLogger()

	if err := run(os.Args[1:]); err != nil {
		packageLog.Error("health check failed", zap.Error(err))
		exitProcess(1)
	}
}

func run(args []string) error {
	url := defaultHealthURL
	if len(args) > 0 {
		url = args[0]
	}
	if err := check(context.Background(), http.DefaultClient, url); err != nil {
		return err
	}
	return nil
}

func check(parent context.Context, client *http.Client, url string) error {
	if parent == nil || client == nil || url == "" {
		return errUnhealthy
	}
	ctx, cancel := context.WithTimeout(parent, healthTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return errUnhealthy
	}
	response, err := client.Do(request)
	if err != nil {
		return errUnhealthy
	}
	defer func() { _ = response.Body.Close() }()
	_, _ = io.Copy(io.Discard, response.Body)
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return errUnhealthy
	}
	return nil
}
