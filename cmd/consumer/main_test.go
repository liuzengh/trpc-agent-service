package main

import (
	"errors"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/worker"
)

func TestValidateWorkerEndpointFailsClosed(t *testing.T) {
	t.Setenv("WORKER_TRANSPORT_MODE", "")
	t.Setenv("WORKER_MESH_MTLS_ASSERTED", "")
	for _, endpoint := range []string{
		"",
		"not a URL",
		"file:///tmp/worker",
		"http://user:secret@worker.internal",
		"http://worker.internal",
		"https://worker.internal/process?access_token=secret",
	} {
		t.Run(endpoint, func(t *testing.T) {
			if err := validateWorkerEndpoint(endpoint); err == nil {
				t.Fatalf("validateWorkerEndpoint(%q) accepted unsafe endpoint", endpoint)
			}
		})
	}

	if err := validateWorkerEndpoint(" https://worker.internal/base "); err != nil {
		t.Fatalf("validateWorkerEndpoint(valid endpoint): %v", err)
	}
}

func TestParseProcessTimeoutIsStrictAndLeavesWorkerMargin(t *testing.T) {
	defaultValue, err := parseProcessTimeout("")
	if err != nil {
		t.Fatal(err)
	}
	if defaultValue != 150*time.Second {
		t.Fatalf("default PROCESS_TIMEOUT=%s, want 150s", defaultValue)
	}
	for _, value := range []string{"invalid", "0s", "-1s"} {
		if _, err := parseProcessTimeout(value); err == nil {
			t.Fatalf("parseProcessTimeout(%q) accepted invalid configuration", value)
		}
	}
	if got, err := parseProcessTimeout("125s"); err != nil || got != 125*time.Second {
		t.Fatalf("parseProcessTimeout valid result=%s error=%v", got, err)
	}
}

func TestParseExpectedWorkerExecutionTimeoutUsesWorkerContract(t *testing.T) {
	got, err := parseExpectedWorkerExecutionTimeout("")
	if err != nil || got != worker.DefaultExecutionTimeout {
		t.Fatalf("default Worker timeout=%s error=%v", got, err)
	}
	for _, value := range []string{"invalid", "500ms", "16m"} {
		if _, err := parseExpectedWorkerExecutionTimeout(value); err == nil {
			t.Fatalf("parseExpectedWorkerExecutionTimeout(%q) accepted unsafe configuration", value)
		}
	}
}

func TestValidateWorkerEndpointRequiresExplicitDevelopmentOrMeshOverride(t *testing.T) {
	t.Setenv("WORKER_TRANSPORT_MODE", "")
	if err := validateWorkerEndpoint("http://worker:9090"); !errors.Is(err, worker.ErrInsecureWorkerTransport) {
		t.Fatalf("default HTTP endpoint error=%v, want ErrInsecureWorkerTransport", err)
	}

	t.Setenv("WORKER_TRANSPORT_MODE", "development")
	if err := validateWorkerEndpoint("http://worker:9090"); err != nil {
		t.Fatalf("explicit development endpoint rejected: %v", err)
	}
	if err := validateWorkerEndpoint("http://worker.internal:9090"); !errors.Is(err, worker.ErrWorkerDevelopmentEndpointInvalid) {
		t.Fatalf("non-local development endpoint error=%v, want local-boundary rejection", err)
	}

	t.Setenv("WORKER_TRANSPORT_MODE", "mesh")
	t.Setenv("WORKER_MESH_MTLS_ASSERTED", "")
	if err := validateWorkerEndpoint("http://agent-worker:9090"); !errors.Is(err, worker.ErrWorkerMeshMTLSAssertionRequired) {
		t.Fatalf("unasserted mesh endpoint error=%v, want assertion rejection", err)
	}
	t.Setenv("WORKER_MESH_MTLS_ASSERTED", "true")
	if err := validateWorkerEndpoint("http://agent-worker:9090"); err != nil {
		t.Fatalf("asserted mesh endpoint rejected: %v", err)
	}
}

func TestConfiguredWorkerTransportModeRejectsUnknownValue(t *testing.T) {
	t.Setenv("WORKER_TRANSPORT_MODE", "staging")
	if _, err := configuredWorkerTransportMode(); !errors.Is(err, worker.ErrWorkerTransportModeInvalid) {
		t.Fatalf("mode error=%v, want ErrWorkerTransportModeInvalid", err)
	}
}
