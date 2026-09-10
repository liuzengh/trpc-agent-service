package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	servicelog "github.com/XnLemon/trpc-agent-service/trpcservice/log"
	"go.uber.org/zap"
)

func TestCheckAcceptsSuccessfulEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			t.Fatalf("method = %s", request.Method)
		}
		_, _ = io.WriteString(writer, "ok\n")
	}))
	defer server.Close()

	if err := check(context.Background(), server.Client(), server.URL); err != nil {
		t.Fatal(err)
	}
}

func TestCheckRejectsInvalidInputsAndUnhealthyResponses(t *testing.T) {
	tests := []struct {
		name    string
		handler http.Handler
		parent  context.Context
		client  *http.Client
		url     string
	}{
		{name: "nil context", parent: nil, client: http.DefaultClient, url: "http://127.0.0.1"},
		{name: "nil client", parent: context.Background(), url: "http://127.0.0.1"},
		{name: "empty URL", parent: context.Background(), client: http.DefaultClient},
		{name: "server error", parent: context.Background(), client: http.DefaultClient, handler: http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusServiceUnavailable) })},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			url := test.url
			if test.handler != nil {
				server := httptest.NewServer(test.handler)
				defer server.Close()
				url = server.URL
			}
			if err := check(test.parent, test.client, url); err == nil {
				t.Fatal("invalid or unhealthy endpoint was accepted")
			} else if !errors.Is(err, errUnhealthy) {
				t.Fatalf("error = %v, want errUnhealthy", err)
			}
		})
	}
}

func TestPackageLogAddsPrefix(t *testing.T) {
	var output strings.Builder
	restoreLogger, err := configureLogger(&output)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(restoreLogger)

	packageLog.Info("info message")
	packageLog.Warn("warn message")
	packageLog.Error("error message", zap.String("operation", "check"))

	logged := output.String()
	for _, message := range []string{
		"[trpc-healthcheck] info message",
		"[trpc-healthcheck] warn message",
		"[trpc-healthcheck] error message",
	} {
		if !strings.Contains(logged, message) {
			t.Errorf("package logger did not log %q: %s", message, logged)
		}
	}
}

func TestRunAcceptsExplicitHealthyURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/readyz" {
			t.Fatalf("path = %s, want /readyz", request.URL.Path)
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	if err := run([]string{server.URL + "/readyz"}); err != nil {
		t.Fatal(err)
	}
}

func TestRunReportsUnhealthyEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	err := run([]string{server.URL})
	if !errors.Is(err, errUnhealthy) {
		t.Fatalf("error = %v, want errUnhealthy", err)
	}
}

func TestCheckRejectsMalformedURL(t *testing.T) {
	err := check(context.Background(), http.DefaultClient, "://invalid")
	if !errors.Is(err, errUnhealthy) {
		t.Fatalf("error = %v, want errUnhealthy", err)
	}
}

func TestCheckRejectsTransportError(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("connection reset")
	})}

	err := check(context.Background(), client, "http://service.invalid/healthz")
	if !errors.Is(err, errUnhealthy) {
		t.Fatalf("error = %v, want errUnhealthy", err)
	}
}

func TestMainExitsNonZeroOnFailure(t *testing.T) {
	originalArgs := os.Args
	originalExit := exitProcess
	t.Cleanup(func() {
		os.Args = originalArgs
		exitProcess = originalExit
	})

	os.Args = []string{"trpc-healthcheck", "://invalid"}
	exitCode := 0
	exitProcess = func(code int) { exitCode = code }
	main()

	if exitCode != 1 {
		t.Fatalf("exit code = %d, want 1", exitCode)
	}
}

func TestMainExitsWhenLoggerConfigurationFails(t *testing.T) {
	originalLogger := newLogger
	originalArgs := os.Args
	originalExit := exitProcess
	t.Cleanup(func() {
		newLogger = originalLogger
		os.Args = originalArgs
		exitProcess = originalExit
	})

	newLogger = func(servicelog.Config) (*zap.Logger, error) {
		return nil, errors.New("logger unavailable")
	}
	exitCode := 0
	exitProcess = func(code int) { exitCode = code }
	os.Args = []string{"trpc-healthcheck"}

	main()
	if exitCode != 1 {
		t.Fatalf("exit code = %d, want 1", exitCode)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}
