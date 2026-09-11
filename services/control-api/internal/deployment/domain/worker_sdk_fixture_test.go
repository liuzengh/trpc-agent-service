package domain

import (
	"encoding/json"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	deploymentv1 "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestExportWorkerSDKMemoryFixture exports actual Compile output on opt-in.
// No executable Worker platform contract or live credentials are enabled.
func TestExportWorkerSDKMemoryFixture(t *testing.T) {
	in := validCompileInput()
	zero := int64(0)
	in.Agent.Spec.Root = "assistant"
	in.Agent.Spec.Nodes = map[string]agentdomain.Node{"assistant": {Kind: agentdomain.NodeKindLLM, Instruction: "Use memory when appropriate.", ModelSlot: "primary", ToolSlots: []string{}, KnowledgeSlots: []string{}, Memory: &agentdomain.Memory{Tools: []string{"memory_add", "memory_load"}, PreloadLimit: &zero}}}
	in.Agent.Spec.Requirements.Tools = map[string]agentdomain.CapabilityRequirement{}
	in.Agent.Spec.Requirements.Knowledge = map[string]agentdomain.CapabilityRequirement{}
	in.Profile.Spec.Tools = map[string]profiledomain.ToolResource{}
	in.Profile.Spec.Knowledge = map[string]profiledomain.KnowledgeResource{}
	session := in.Profile.Spec.Storage["session"]
	session.Destination.Username = deploymentv1.WorkerV1SessionRuntimeRole
	in.Profile.Spec.Storage = map[string]profiledomain.StorageResource{"session": session, "memory": {Kind: profiledomain.StorageKindManagedMemory, BackendID: "redis-memory", BackendRevision: 1}}
	in.Platform.RuntimeDataCapabilities = []string{"memory"}
	in.Platform.StorageAdapters[profiledomain.StorageKindManagedMemory] = AdapterContract{Version: StorageAdapterManagedMemoryV1}
	in.Platform.Execution.AllowedEndpointHosts = append(in.Platform.Execution.AllowedEndpointHosts, "redis.internal")
	in.Platform.Digest, _ = in.Platform.CalculateDigest()
	in.ManagedBackends = map[string]datav1.Snapshot{"storage/memory": {SchemaVersion: "v1", TenantID: in.TenantID, BackendID: "redis-memory", BackendRevision: 1, Kind: datav1.Redis, Adapter: "managed-redis-v1", Isolation: datav1.MemoryIsolation, Limits: datav1.Limits{TimeoutMS: 1000, MaxConcurrency: 1, MaxBytes: 1024}, Redis: &datav1.RedisTarget{Host: "redis.internal", Port: 6379, Username: "runtime", TLS: true}}}
	memoryDigest, _ := in.ManagedBackends["storage/memory"].Digest()
	memoryResource := in.Profile.Spec.Storage["memory"]
	memoryResource.DSNCredentialID = credentialMemory
	memoryResource.CredentialAudienceDigest = memoryDigest
	in.Profile.Spec.Storage["memory"] = memoryResource
	// Fix real owner-canonical source digests rather than retaining fixture placeholders.
	agentRaw, _ := json.Marshal(in.Agent.Spec)
	agentCanonical, agentReport := agentdomain.ValidateForPublication(agentRaw, 1)
	if !agentReport.Valid {
		t.Fatal(agentReport)
	}
	profileRaw, _ := json.Marshal(in.Profile.Spec)
	profileCanonical, profileReport := profiledomain.ValidateForPublication(profileRaw, 1)
	if !profileReport.Valid {
		t.Fatal(profileReport)
	}
	in.Agent.SpecDigest = agentCanonical.Digest
	in.Profile.SpecDigest = profileCanonical.Digest
	compiled, report := Compile(in)
	if !report.Valid {
		t.Fatal(report.Diagnostics)
	}
	if _, err := VerifyManifestContent(compiled.CanonicalContent, compiled.ContentDigest); err != nil {
		t.Fatal(err)
	}
	envelope := RuntimeManifest{ID: "rmf_worker_sdk_memory", TenantID: in.TenantID, DeploymentID: "dpl_worker_sdk_memory", DeploymentRevisionID: "dpr_worker_sdk_memory", RevisionNumber: 1, PublishedAt: time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC), Content: compiled.CanonicalContent, ContentDigest: compiled.ContentDigest}
	envelopeRaw, err := json.MarshalIndent(envelope, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	verified, err := deploymentv1.DecodeRuntimeManifest(envelopeRaw)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := deploymentv1.VerifyRuntimeManifest(verified)
	if err != nil {
		t.Fatal(err)
	}
	n := wire.AgentPlan.Nodes["assistant"]
	if len(wire.AgentPlan.Nodes) != 1 || n.Kind != "llm" || n.Memory == nil || len(n.Memory.Tools) != 2 || n.Memory.PreloadLimit == nil || *n.Memory.PreloadLimit != 0 || n.Artifact != nil || n.AddSessionSummary != nil || wire.Runtime != nil {
		t.Fatal("unexpected capability closure")
	}
	if wire.PlatformContract.Version != PlatformContractVersionV1 {
		t.Fatal("fixture enabled production contract")
	}
	if err = deploymentv1.ValidateWorkerV1(wire, ""); err == nil {
		t.Fatal("fixture passed production gate")
	}
	out := os.Getenv("CONTROL_WORKER_SDK_FIXTURE_DIR")
	if out == "" {
		return
	}
	if !filepath.IsAbs(out) {
		t.Fatal("fixture export path must be absolute")
	}
	if err = os.MkdirAll(out, 0700); err != nil {
		t.Fatal(err)
	}
	platformRaw, _ := json.MarshalIndent(in.Platform, "", "  ")
	files := map[string][]byte{"runtime-manifest.json": append(envelopeRaw, '\n'), "content.canonical.json": compiled.CanonicalContent, "content.digest": []byte(compiled.ContentDigest + "\n"), "agent-source.canonical.json": agentCanonical.Document, "profile-source.canonical.json": profileCanonical.Document, "platform-contract.json": append(platformRaw, '\n')}
	for name, raw := range files {
		if err = os.WriteFile(filepath.Join(out, name), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("COMPILED_MEMORY_FIXTURE=%s DIGEST=%s", out, compiled.ContentDigest)
}
