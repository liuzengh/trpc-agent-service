package domain

import (
	"encoding/json"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	deploymentv1 "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"strings"
	"testing"
)

func fullDataInput() CompileInput {
	in := managedExecutableInput()
	in.Platform.RuntimeDataCapabilities = []string{"memory", "artifact", "summary"}
	in.Platform.StorageAdapters[profiledomain.StorageKindManagedArtifact] = AdapterContract{Version: StorageAdapterManagedArtifactV1}
	threshold := int64(3)
	in.Agent.Spec.Runtime = &agentdomain.Runtime{Summary: &agentdomain.Summary{Enabled: true, ModelSlot: "summarizer", EventThreshold: &threshold}}
	in.Agent.Spec.Requirements.Models["summarizer"] = agentdomain.ModelRequirement{Capabilities: []string{"chat"}}
	in.Profile.Spec.Models["summarizer"] = in.Profile.Spec.Models["primary"]
	n := in.Agent.Spec.Nodes["researcher"]
	all := int64(-1)
	yes := true
	n.Memory = &agentdomain.Memory{Tools: []string{"memory_add", "memory_load"}, PreloadLimit: &all}
	n.Artifact = &agentdomain.Artifact{Enabled: true}
	n.AddSessionSummary = &yes
	in.Agent.Spec.Nodes["researcher"] = n
	in.Profile.Spec.Storage["memory"] = profiledomain.StorageResource{Kind: profiledomain.StorageKindManagedMemory, BackendID: "redis", BackendRevision: 1}
	memory, _ := in.ManagedBackends["storage/session"].ForRole("memory")
	in.ManagedBackends["storage/memory"] = memory
	digest, _ := memory.Digest()
	mr := in.Profile.Spec.Storage["memory"]
	mr.DSNCredentialID = credentialMemory
	mr.CredentialAudienceDigest = digest
	in.Profile.Spec.Storage["memory"] = mr
	in.ManagedBackends["storage/artifact"] = datav1.Snapshot{SchemaVersion: "v1", TenantID: in.TenantID, BackendID: "s3", BackendRevision: 1, Kind: datav1.S3, Adapter: "managed-s3-v1", Isolation: "tenant-artifact-v1", Limits: datav1.Limits{TimeoutMS: 1000, MaxConcurrency: 1, MaxBytes: 1024}, S3: &datav1.S3Target{Endpoint: "https://s3.internal", Bucket: "artifacts", Region: "local", Versioning: "disabled"}}
	in.Platform.Execution.AllowedEndpointHosts = append(in.Platform.Execution.AllowedEndpointHosts, "s3.internal")
	in.Platform.Digest, _ = in.Platform.CalculateDigest()
	return in
}
func TestCompleteDataCompilationAndReadValidation(t *testing.T) {
	in := fullDataInput()
	m, r := Compile(in)
	if !r.Valid {
		t.Fatal(r.Diagnostics)
	}
	if _, err := ValidateManifestContent(m.CanonicalContent, m.ContentDigest); err != nil {
		t.Fatal("read semantic", err)
	}
	if _, err := deploymentv1.DecodeManifestContent(m.CanonicalContent); err != nil {
		t.Fatal("shared schema", err)
	}
	if m.Content.Runtime.Summary.ModelResource != "summarizer" || m.Content.ResolvedRequirements.Models["summarizer"] != "summarizer" {
		t.Fatal("summary model missing")
	}
	n := m.Content.AgentPlan.Nodes["researcher"]
	if n.Memory == nil || n.Artifact == nil || n.AddSessionSummary == nil {
		t.Fatal("capability lost")
	}
	other := m.Content.AgentPlan.Nodes["writer"]
	if other.Memory != nil || other.Artifact != nil || other.AddSessionSummary != nil {
		t.Fatal("node capability leaked")
	}
	if m.Content.Resources.Storage["artifact"].MetadataContract != ArtifactMetadataContract {
		t.Fatal("metadata contract")
	}
	public, _ := json.Marshal(NewPublicManifestView(m.Content))
	if strings.Contains(string(public), "s3.internal") || !strings.Contains(string(public), `"summary"`) {
		t.Fatal("public view", string(public))
	}
	for _, mutate := range []func(*ManifestContent){
		func(c *ManifestContent) { delete(c.Resources.Models, "summarizer") },
		func(c *ManifestContent) { c.Runtime.Summary.EventThreshold = 0 },
		func(c *ManifestContent) {
			n := c.AgentPlan.Nodes["researcher"]
			n.Memory.Resource = "session"
			c.AgentPlan.Nodes["researcher"] = n
		},
		func(c *ManifestContent) { c.Resources.Storage["memory"].Backend.Isolation = datav1.SessionIsolation },
		func(c *ManifestContent) {
			n := c.AgentPlan.Nodes["researcher"]
			n.Memory = nil
			c.AgentPlan.Nodes["researcher"] = n
		},
	} {
		c := normalizeManifestContent(m.Content)
		mutate(&c)
		_, raw, digest, err := CanonicalizeManifest(c)
		if err == nil {
			if _, err = ValidateManifestContent(raw, digest); err == nil {
				t.Fatal("tampering accepted")
			}
		}
	}
}
func TestDataCapabilityGateAndMissingResource(t *testing.T) {
	for _, mutate := range []func(*CompileInput){
		func(i *CompileInput) {
			i.Platform.RuntimeDataCapabilities = nil
			i.Platform.Digest, _ = i.Platform.CalculateDigest()
		},
		func(i *CompileInput) { delete(i.Profile.Spec.Models, "summarizer") },
		func(i *CompileInput) {
			delete(i.Profile.Spec.Storage, "memory")
			delete(i.ManagedBackends, "storage/memory")
		},
		func(i *CompileInput) {
			delete(i.Profile.Spec.Storage, "artifact")
			delete(i.ManagedBackends, "storage/artifact")
		},
		func(i *CompileInput) { i.Agent.Spec.Runtime.Summary.EventThreshold = nil },
	} {
		in := fullDataInput()
		mutate(&in)
		m, r := Compile(in)
		if r.Valid || len(m.CanonicalContent) > 0 {
			t.Fatal("invalid input compiled")
		}
	}
}
func TestFinalToolNameCollisionUsesRegisteredNames(t *testing.T) {
	if err := validateNodeCallableNames([]string{"tools/memory_add"}, []string{"memory_add"}, ProviderCallableNames); err != nil {
		t.Fatal("logical name confused with final name")
	}
	fake := func([]string) (map[string]string, error) { return map[string]string{"tools/search": "memory_add"}, nil }
	if err := validateNodeCallableNames([]string{"tools/search"}, []string{"memory_add"}, fake); err == nil {
		t.Fatal("final-name collision missed")
	}
}

func TestPostgreSQLAndRedisCompileSameSessionMemoryInterfaces(t *testing.T) {
	for _, kind := range []datav1.Kind{datav1.Redis, datav1.PostgreSQL} {
		t.Run(string(kind), func(t *testing.T) {
			in := fullDataInput()
			if kind == datav1.PostgreSQL {
				for _, role := range []string{"session", "memory"} {
					s := in.ManagedBackends["storage/"+role]
					s.Kind = kind
					s.Adapter = "managed-postgres-v1"
					s.Redis = nil
					s.PostgreSQL = &datav1.PostgresTarget{Host: "pg.internal", Port: 5432, Database: "runtime", Username: "runtime", SSLMode: "verify-full"}
					in.ManagedBackends["storage/"+role] = s
				}
			}
			if kind == datav1.PostgreSQL {
				s := in.ManagedBackends["storage/memory"]
				digest, _ := s.Digest()
				r := in.Profile.Spec.Storage["memory"]
				r.DSNCredentialID = credentialMemory
				r.CredentialAudienceDigest = digest
				in.Profile.Spec.Storage["memory"] = r
			}
			m, r := Compile(in)
			if !r.Valid {
				t.Fatal(r.Diagnostics)
			}
			if _, err := ValidateManifestContent(m.CanonicalContent, m.ContentDigest); err != nil {
				t.Fatal(err)
			}
			if m.Content.Resources.Storage["memory"].Backend.Kind != kind {
				t.Fatal("wrong adapter")
			}
		})
	}
}
func TestDisabledSourceCapabilitiesHaveNoRuntimeClosure(t *testing.T) {
	in := managedExecutableInput()
	in.Platform.RuntimeDataCapabilities = []string{"memory", "artifact", "summary"}
	in.Platform.Digest, _ = in.Platform.CalculateDigest()
	before, r := Compile(in)
	if !r.Valid {
		t.Fatal(r.Diagnostics)
	}
	zero := int64(0)
	no := false
	n := in.Agent.Spec.Nodes["writer"]
	n.Memory = &agentdomain.Memory{Tools: []string{}, PreloadLimit: &zero}
	n.Artifact = &agentdomain.Artifact{Enabled: false}
	n.AddSessionSummary = &no
	in.Agent.Spec.Nodes["writer"] = n
	in.Agent.Spec.Runtime = &agentdomain.Runtime{Summary: &agentdomain.Summary{Enabled: false}}
	after, r := Compile(in)
	if !r.Valid {
		t.Fatal(r.Diagnostics)
	}
	if before.ContentDigest != after.ContentDigest || string(before.CanonicalContent) != string(after.CanonicalContent) {
		t.Fatal("disabled fields created runtime closure")
	}
}
