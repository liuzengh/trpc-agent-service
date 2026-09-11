package domain

import (
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"reflect"
	"testing"
)

func managedInput() CompileInput {
	in := validCompileInput()
	in.Profile.Spec.Storage["session"] = profiledomain.StorageResource{Kind: profiledomain.StorageKindManagedSession, BackendID: "redis", BackendRevision: 1}
	in.Profile.Spec.Storage["memory"] = profiledomain.StorageResource{Kind: profiledomain.StorageKindManagedMemory, BackendID: "pg", BackendRevision: 1}
	in.Profile.Spec.Storage["artifact"] = profiledomain.StorageResource{Kind: profiledomain.StorageKindManagedArtifact, BackendID: "s3", BackendRevision: 1}
	k := in.Profile.Spec.Knowledge["docs"]
	k.Kind = profiledomain.KnowledgeKindManaged
	k.BackendID = "qdrant"
	k.BackendRevision = 1
	k.Host = ""
	k.Port = 0
	k.TLS = false
	k.Collection = ""
	k.QdrantAPIKeyCredentialID = ""
	in.Profile.Spec.Knowledge["docs"] = k
	unused := k
	unused.BackendID = "private-unused"
	in.Profile.Spec.Knowledge["unused"] = unused
	base := datav1.Snapshot{SchemaVersion: "v1", TenantID: in.TenantID, BackendRevision: 1, Limits: datav1.Limits{TimeoutMS: 1000, MaxConcurrency: 1, MaxBytes: 1024}}
	redis := base
	redis.BackendID = "redis"
	redis.Kind = datav1.Redis
	redis.Adapter = "managed-redis-v1"
	redis.Isolation = "tenant-session-v1"
	redis.Redis = &datav1.RedisTarget{Host: "redis.internal", Port: 6379, Username: "runtime", TLS: true}
	q := base
	q.BackendID = "qdrant"
	q.Kind = datav1.Qdrant
	q.Adapter = "managed-qdrant-v1"
	q.Isolation = "tenant-profile-resource-v1"
	q.Qdrant = &datav1.QdrantTarget{Endpoint: "https://qdrant.internal", Collection: "docs", VectorName: "content", Dimensions: k.Embedding.Dimensions, Distance: "cosine"}
	in.ManagedBackends = map[string]datav1.Snapshot{"storage/session": redis, "knowledge/docs": q}
	in.Platform.Execution.AllowedEndpointHosts = append(in.Platform.Execution.AllowedEndpointHosts, "redis.internal", "pg.internal", "qdrant.internal")
	in.Platform.Digest, _ = in.Platform.CalculateDigest()
	return in
}
func TestManagedRequestsFollowNodeAndRoleClosure(t *testing.T) {
	in := managedInput()
	requests := ManagedBackendRequests(in.Agent.Spec, in.Profile.Spec)
	keys := []string{}
	for _, r := range requests {
		keys = append(keys, r.Key())
	}
	if !reflect.DeepEqual(keys, []string{"knowledge/docs", "storage/session"}) {
		t.Fatal(keys)
	}
	node := in.Agent.Spec.Nodes["researcher"]
	node.KnowledgeSlots = nil
	in.Agent.Spec.Nodes["researcher"] = node
	if got := ManagedBackendRequests(in.Agent.Spec, in.Profile.Spec); len(got) != 1 {
		t.Fatal(got)
	}
	// Node strings alone cannot authorize an undeclared Knowledge resource.
	node.KnowledgeSlots = []string{"unused"}
	in.Agent.Spec.Nodes["researcher"] = node
	if got := ManagedBackendRequests(in.Agent.Spec, in.Profile.Spec); len(got) != 1 {
		t.Fatal(got)
	}
	in.Agent.Spec.Requirements.Knowledge["unused"] = agentdomain.CapabilityRequirement{Capability: profiledomain.CapabilityKnowledgeSearch}
	if got := ManagedBackendRequests(in.Agent.Spec, in.Profile.Spec); len(got) != 2 || got[0].BackendID != "private-unused" {
		t.Fatal(got)
	}
}
func TestManagedFixedSnapshotsValidateBeforeRuntimeGate(t *testing.T) {
	in := managedInput()
	if d := ValidateManagedSnapshots(in); len(d) != 0 {
		t.Fatal(d)
	}
	_, report := Compile(in)
	if report.Valid {
		t.Fatal("fixed targets alone enabled unimplemented runtime")
	}
	found := false
	for _, d := range report.Diagnostics {
		if d.Code == DiagnosticStorageRoleUnsupported || d.Code == DiagnosticAdapterUnsupported {
			found = true
		}
	}
	if !found {
		t.Fatal(report.Diagnostics)
	}
}
func TestManagedSnapshotRejectsForeignStaleWrongAndExtra(t *testing.T) {
	for _, tc := range []struct {
		name, code string
		change     func(*CompileInput)
	}{
		{"missing", DiagnosticBackendUnavailable, func(i *CompileInput) { delete(i.ManagedBackends, "storage/session") }},
		{"foreign", DiagnosticBackendMismatch, func(i *CompileInput) {
			s := i.ManagedBackends["storage/session"]
			s.TenantID = "other"
			i.ManagedBackends["storage/session"] = s
		}},
		{"revision", DiagnosticBackendMismatch, func(i *CompileInput) {
			s := i.ManagedBackends["storage/session"]
			s.BackendRevision = 2
			i.ManagedBackends["storage/session"] = s
		}},
		{"dimension", DiagnosticBackendMismatch, func(i *CompileInput) {
			s := i.ManagedBackends["knowledge/docs"].Clone()
			s.Qdrant.Dimensions++
			i.ManagedBackends["knowledge/docs"] = s
		}},
		{"timeout", DiagnosticLimitExceeded, func(i *CompileInput) {
			i.Platform.Execution.MaxRunSeconds = 1
			s := i.ManagedBackends["storage/session"]
			s.Limits.TimeoutMS = 1001
			i.ManagedBackends["storage/session"] = s
		}},
		{"extra", DiagnosticBackendClosure, func(i *CompileInput) { i.ManagedBackends["knowledge/unused"] = i.ManagedBackends["knowledge/docs"] }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := managedInput()
			tc.change(&in)
			ds := ValidateManagedSnapshots(in)
			found := false
			for _, d := range ds {
				if d.Code == tc.code {
					found = true
				}
			}
			if !found {
				t.Fatal(ds)
			}
		})
	}
}

func TestSameBackendIdentityCannotResolveToMixedPhysicalTargets(t *testing.T) {
	in := managedInput()
	node := in.Agent.Spec.Nodes["researcher"]
	node.Memory = &agentdomain.Memory{Tools: []string{"memory_load"}}
	in.Agent.Spec.Nodes["researcher"] = node
	in.Profile.Spec.Storage["memory"] = profiledomain.StorageResource{Kind: profiledomain.StorageKindManagedMemory, BackendID: "redis", BackendRevision: 1}
	memory, err := in.ManagedBackends["storage/session"].ForRole("memory")
	if err != nil {
		t.Fatal(err)
	}
	in.ManagedBackends["storage/memory"] = memory
	if ds := ValidateManagedSnapshots(in); len(ds) > 0 {
		t.Fatal("shared role target rejected", ds)
	}
	memory.Redis.Host = "mixed.internal"
	in.ManagedBackends["storage/memory"] = memory
	ds := ValidateManagedSnapshots(in)
	for _, d := range ds {
		if d.Code == DiagnosticBackendMismatch {
			return
		}
	}
	t.Fatal("mixed physical targets accepted", ds)
}

func TestManagedRoleDiscoveryRequiresActiveAgentDeclaration(t *testing.T) {
	in := managedInput()
	node := in.Agent.Spec.Nodes["researcher"]
	zero := int64(0)
	node.Memory = &agentdomain.Memory{Tools: []string{}, PreloadLimit: &zero}
	node.Artifact = &agentdomain.Artifact{Enabled: false}
	in.Agent.Spec.Nodes["researcher"] = node
	if got := ManagedBackendRequests(in.Agent.Spec, in.Profile.Spec); len(got) != 2 {
		t.Fatal("disabled roles selected", got)
	}
	all := int64(-1)
	node.Memory.PreloadLimit = &all
	node.Artifact.Enabled = true
	in.Agent.Spec.Nodes["researcher"] = node
	got := ManagedBackendRequests(in.Agent.Spec, in.Profile.Spec)
	if len(got) != 4 {
		t.Fatal("enabled roles missing", got)
	}
}
