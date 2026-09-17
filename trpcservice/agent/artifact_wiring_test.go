package agent

import (
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	serviceartifact "github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact"
	artifactmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker/mockmodel"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

// TestBundleWiresArtifactService proves the Runner consumes the framework
// artifact.Service contract: a configured service is handed to the Runner via
// runner.WithArtifactService, which forwards it to the Invocation where the
// code-executor and skill artifact paths read it. A nil service is omitted
// rather than failing, because artifact support is a capability, not a
// precondition for running an Agent.
func TestBundleWiresArtifactService(t *testing.T) {
	sessions := sessioninmemory.NewSessionService()
	root := llmagent.New("test-agent", llmagent.WithModel(mockmodel.New()))
	artifacts := &serviceartifact.SDKService{Store: artifactmemory.New(), MapTenant: func(appName string) string {
		tenantID, err := serviceartifact.TenantFromAppName(appName)
		if err != nil {
			return ""
		}
		return tenantID
	}}

	runnerValue, err := (&Bundle{AppName: "tenant-a/app", Root: root, Artifact: artifacts}).NewRunner(sessions)
	if err != nil {
		t.Fatalf("NewRunner with artifact service: %v", err)
	}
	if runnerValue == nil {
		t.Fatal("expected a runner when an artifact service is configured")
	}
	runnerWithout, err := (&Bundle{AppName: "tenant-a/app", Root: root}).NewRunner(sessions)
	if err != nil || runnerWithout == nil {
		t.Fatalf("NewRunner without artifact service: runner=%v err=%v", runnerWithout, err)
	}
	if _, err = (&Bundle{}).NewRunner(sessions); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("empty bundle error = %v, want ErrCapabilityUnsupported", err)
	}
}

// TestTenantFromAppNameFailsClosed guards the encoding contract on the real
// production parser: the control plane publishes AppName as "tenantID/agentAppID",
// and anything that cannot yield a non-empty tenant is rejected rather than
// treated as a tenant of its own.
func TestTenantFromAppNameFailsClosed(t *testing.T) {
	value, err := serviceartifact.TenantFromAppName("tenant-a/app")
	if err != nil || value != "tenant-a" {
		t.Fatalf("encoded app name = %q, err = %v", value, err)
	}
	for _, appName := range []string{"", "tenant-a", "/app"} {
		if _, err = serviceartifact.TenantFromAppName(appName); err == nil {
			t.Fatalf("app name %q should be rejected", appName)
		}
	}
}
