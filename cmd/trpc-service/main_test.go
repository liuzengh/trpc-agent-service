package main

import (
	"bufio"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const signalHelperEnv = "TRPC_SERVICE_SIGNAL_TEST_HELPER"

func TestRunNonBlockingFlags(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want int
	}{
		{name: "help", args: []string{"--help"}, want: 0},
		{name: "version", args: []string{"--version"}, want: 0},
		{name: "unknown flag", args: []string{"--unknown"}, want: 2},
		{name: "unexpected argument", args: []string{"unexpected"}, want: 2},
		{name: "invalid role", args: []string{"--role", "scheduler"}, want: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := run(test.args); got != test.want {
				t.Fatalf("run(%v) = %d, want %d", test.args, got, test.want)
			}
		})
	}
}

func TestParseProcessRole(t *testing.T) {
	for _, value := range []string{"all", "gateway", "worker", " GATEWAY "} {
		if _, err := parseProcessRole(value); err != nil {
			t.Fatalf("role %q: %v", value, err)
		}
	}
	if _, err := parseProcessRole(""); err == nil {
		t.Fatal("empty role accepted")
	}
}

func TestGatewayTokenEnv(t *testing.T) {
	if got := gatewayTokenEnv("demo-http.v2"); got != "TRPC_AGENT_GATEWAY_TOKEN_DEMO_HTTP_V2" {
		t.Fatalf("gatewayTokenEnv=%q", got)
	}
}

func TestResolveLocalSecretFromEnvAndFile(t *testing.T) {
	t.Setenv("TEST_WECOM_SECRET", "env-value")
	value, err := resolveLocalSecret(tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "TEST_WECOM_SECRET"})
	if err != nil || value != "env-value" {
		t.Fatalf("env value=%q err=%v", value, err)
	}
	path := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(path, []byte("file-value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	value, err = resolveLocalSecret(tenant.SecretRef{Provider: tenant.SecretProviderFile, Key: path})
	if err != nil || value != "file-value" {
		t.Fatalf("file value=%q err=%v", value, err)
	}
}

func TestPersistentCompositionRejectsMockModel(t *testing.T) {
	file := &config.File{Tenants: []tenant.Tenant{{ID: "tenant", Enabled: true, Apps: []tenant.AgentApp{{ID: "app", Enabled: true, Model: tenant.ModelProfile{Provider: "mock", Name: "fixture"}}}}}}
	err := validatePersistentProfiles(file)
	if err == nil || !strings.Contains(err.Error(), "test-only") {
		t.Fatalf("error=%v", err)
	}
}

func TestExampleConfigurationPassesPersistentProfileGate(t *testing.T) {
	t.Setenv("DEEPSEEK_API_KEY", "fixture-key")
	file, err := config.LoadFile("../../configs/example.yaml")
	if err != nil {
		t.Fatalf("LoadFile(example) error = %v", err)
	}
	if err := validatePersistentProfiles(file); err != nil {
		t.Fatalf("example configuration rejected by production gate: %v", err)
	}
}

func TestRunExitsCleanlyOnInterrupt(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("os.Interrupt delivery is not supported on Windows")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	command := exec.CommandContext(ctx, os.Args[0], "-test.run=TestSignalHelperProcess")
	command.Env = append(os.Environ(), signalHelperEnv+"=1")
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe() error = %v", err)
	}
	if err := command.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatalf("read startup output: %v", err)
	}
	if !strings.HasPrefix(line, "trpc-agent-service ") {
		t.Fatalf("startup output = %q, want service version", line)
	}
	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("Signal() error = %v", err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("process exit error = %v", err)
	}
}

func TestSignalHelperProcess(t *testing.T) {
	if os.Getenv(signalHelperEnv) != "1" {
		return
	}
	os.Exit(run(nil))
}
