package controlplane

import (
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

func TestInTrafficBucketBoundaries(t *testing.T) {
	if InTrafficBucket("tenant", "app", "session", 0) {
		t.Fatal("zero traffic must never route to canary")
	}
	if !InTrafficBucket("tenant", "app", "session", 10000) {
		t.Fatal("full traffic must always route to canary")
	}
}

func TestVersionSnapshotRejectsEmbeddedCredential(t *testing.T) {
	snapshot := VersionSnapshot{
		Agent: tenant.AgentConfig{Name: "support", DefaultModel: "gpt"},
		Model: tenant.ModelConfig{Provider: "openai", ModelName: "gpt", APIKey: "secret"},
	}
	if err := snapshot.Validate(); err == nil {
		t.Fatal("snapshot accepted embedded model credential")
	}
}

func TestVersionSnapshotRequiresExactModelBinding(t *testing.T) {
	valid := VersionSnapshot{
		Agent: tenant.AgentConfig{Name: "support", DefaultModel: "gpt"},
		Model: tenant.ModelConfig{Provider: "openai", ModelName: "gpt"},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid snapshot rejected: %v", err)
	}
	valid.Agent.DefaultModel = "other"
	if err := valid.Validate(); err == nil {
		t.Fatal("snapshot accepted a model different from agent.defaultModel")
	}
}

func TestVersionSnapshotRejectsUnboundedAgentExecution(t *testing.T) {
	tests := []tenant.AgentConfig{
		{Name: "support", DefaultModel: "gpt-4", MaxLLMCalls: tenant.MaxConfiguredLLMCalls + 1},
		{Name: "support", DefaultModel: "gpt-4", MaxLLMCalls: 1, Tools: []string{"current_time"}},
	}
	for _, agent := range tests {
		snapshot := VersionSnapshot{
			Agent: agent,
			Model: tenant.ModelConfig{Provider: "openai", ModelName: "gpt-4"},
		}
		if err := snapshot.Validate(); err == nil {
			t.Fatalf("snapshot accepted unsafe agent execution limits: %+v", agent)
		}
	}
}

func TestResolvedDeploymentRejectsSnapshotAgentIdentityMismatch(t *testing.T) {
	resolved := &ResolvedDeployment{
		AgentAppName: "support",
		VersionID:    "version-1",
		Snapshot: VersionSnapshot{
			Agent: tenant.AgentConfig{Name: "billing", DefaultModel: "gpt-4"},
			Model: tenant.ModelConfig{Provider: "openai", ModelName: "gpt-4"},
		},
	}
	if err := validateResolvedAgentIdentity(resolved); err == nil {
		t.Fatal("resolved deployment accepted a snapshot from another agent app")
	}
}

func TestInTrafficBucketIsStableAndScoped(t *testing.T) {
	first := InTrafficBucket("tenant-a", "app-a", "session-a", 3750)
	for i := 0; i < 100; i++ {
		if got := InTrafficBucket("tenant-a", "app-a", "session-a", 3750); got != first {
			t.Fatalf("routing changed for identical key: first=%v got=%v", first, got)
		}
	}

	// Check distribution rather than assuming two chosen strings land in
	// different buckets.
	canary := 0
	for i := 0; i < 10000; i++ {
		if InTrafficBucket("tenant-a", "app-a", string(rune(i)), 2500) {
			canary++
		}
	}
	if canary < 2200 || canary > 2800 {
		t.Fatalf("unexpected 25%% bucket distribution: %d/10000", canary)
	}
}
