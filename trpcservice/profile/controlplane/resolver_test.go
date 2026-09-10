package controlplane

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type tenantReaderStub struct{ value tenant.Tenant }

func (s tenantReaderStub) Get(context.Context, string) (tenant.Tenant, error) { return s.value, nil }

type agentReaderStub struct {
	app      agentapp.AgentApp
	revision agentapp.Revision
}

func (s agentReaderStub) Get(context.Context, string, string) (agentapp.AgentApp, error) {
	return s.app, nil
}
func (s agentReaderStub) GetRevision(context.Context, string, string, int64) (agentapp.Revision, error) {
	return s.revision, nil
}

type configReaderStub struct{ value config.Snapshot }

func (s configReaderStub) Get(context.Context, string, int64) (config.Snapshot, error) {
	return s.value, nil
}

type modelReaderStub struct{ value provider.ModelProfileSnapshot }

func (s modelReaderStub) GetModel(context.Context, string, string, int64) (provider.ModelProfileSnapshot, error) {
	return s.value, nil
}

type agentMapStub struct {
	apps      map[string]agentapp.AgentApp
	revisions map[string]agentapp.Revision
}

func (s agentMapStub) Get(_ context.Context, _, appID string) (agentapp.AgentApp, error) {
	value, ok := s.apps[appID]
	if !ok {
		return agentapp.AgentApp{}, runtime.ErrNotFound
	}
	return value, nil
}
func (s agentMapStub) GetRevision(_ context.Context, _, appID string, revision int64) (agentapp.Revision, error) {
	value, ok := s.revisions[appID]
	if !ok || value.Revision != revision {
		return agentapp.Revision{}, runtime.ErrNotFound
	}
	return value, nil
}

func TestResolverProjectsExactPublishedVersions(t *testing.T) {
	resolver, key := resolverFixture(t)
	resolved, err := resolver.Resolve(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Key != key || resolved.TenantVersion != 7 || resolved.AgentAppVersion != 11 ||
		resolved.ModelProfileRef != (profile.VersionedRef{ID: "model", Version: 3}) ||
		!resolved.BackendRequirements["atomic_turn_commit"] {
		t.Fatalf("unexpected projection: %#v", resolved)
	}
	resolved.GenerationConfig["nested"].(map[string]any)["value"] = "changed"
	agents := resolver.Agents.(agentReaderStub)
	if agents.revision.GenerationConfig["nested"].(map[string]any)["value"] != "fixed" {
		t.Fatal("projection aliases repository state")
	}
}

func TestResolverFailsClosedOnDigestPolicyAndDisabledTenant(t *testing.T) {
	resolver, key := resolverFixture(t)
	badDigest := key
	badDigest.ContentDigest = "forged"
	if _, err := resolver.Resolve(context.Background(), badDigest); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("digest: %v", err)
	}
	badPolicy := key
	badPolicy.PolicyVersion++
	if _, err := resolver.Resolve(context.Background(), badPolicy); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("policy: %v", err)
	}
	disabled := resolver
	tenantStub := disabled.Tenants.(tenantReaderStub)
	tenantStub.value.Status = tenant.StatusDisabled
	disabled.Tenants = tenantStub
	if _, err := disabled.Resolve(context.Background(), key); !errors.Is(err, runtime.ErrCancelRequested) {
		t.Fatalf("disabled tenant: %v", err)
	}
}

func TestResolverRequiresPublishedParentProofForCompositeChild(t *testing.T) {
	child := publishedRevision(t, agentapp.Revision{TenantID: "tenant-a", AgentAppID: "child", Revision: 2,
		AgentKind: agentapp.AgentKindLLM, Instruction: "child", ModelProfileID: "model", ModelProfileVersion: 3})
	node := agentapp.AgentNodeSpecV1{Key: "child", FailurePolicy: agentapp.FailurePolicyFailFast,
		AgentRef: agentapp.PublishedAgentRef{AgentAppID: "child", Revision: child.Revision, ContentDigest: child.ContentDigest}}
	parent := publishedRevision(t, agentapp.Revision{TenantID: "tenant-a", AgentAppID: "root", Revision: 1,
		AgentKind: agentapp.AgentKindChain, AgentSpec: agentapp.AgentSpecV1{Nodes: []agentapp.AgentNodeSpecV1{node}}})
	configuration := config.ConfigV1{SchemaVersion: 1, DefaultAgentAppID: "root", PolicyVersion: 5}
	configDigest, _, err := config.ContentDigest(configuration)
	if err != nil {
		t.Fatal(err)
	}
	resolver := Resolver{
		Tenants: tenantReaderStub{tenant.Tenant{TenantID: "tenant-a", Status: tenant.StatusActive, Version: 7}},
		Agents: agentMapStub{
			apps: map[string]agentapp.AgentApp{
				"root":  {TenantID: "tenant-a", AgentAppID: "root", Status: agentapp.StatusActive, Version: 2},
				"child": {TenantID: "tenant-a", AgentAppID: "child", Status: agentapp.StatusActive, Version: 4},
			},
			revisions: map[string]agentapp.Revision{"root": parent, "child": child},
		},
		Configs: configReaderStub{config.Snapshot{TenantID: "tenant-a", ConfigVersion: 4, State: config.StatePublished,
			Payload: configuration, ContentDigest: configDigest}},
		Models: modelReaderStub{provider.ModelProfileSnapshot{TenantID: "tenant-a", ProfileID: "model", Version: 3, Status: "active"}},
	}
	parentKey := profile.ExecutionProfileKey{TenantID: "tenant-a", TenantVersion: 7, AgentAppID: "root", AgentAppVersion: 2,
		AgentAppRevision: 1, ContentDigest: parent.ContentDigest, ConfigVersion: 4, PolicyVersion: 5}
	root, err := resolver.Resolve(context.Background(), parentKey)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := resolver.ResolveChild(context.Background(), root, node)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Key.AgentAppID != "child" || resolved.Key.ContentDigest != child.ContentDigest || resolved.AgentAppVersion != 4 {
		t.Fatalf("child=%#v", resolved)
	}
	forged := node
	forged.AgentRef.ContentDigest = "forged"
	if _, err := resolver.ResolveChild(context.Background(), root, forged); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("forged child: %v", err)
	}
}

func resolverFixture(t *testing.T) (Resolver, profile.ExecutionProfileKey) {
	t.Helper()
	revision := agentapp.NormalizeRevision(agentapp.Revision{
		TenantID: "tenant-a", AgentAppID: "app", Revision: 2, State: agentapp.RevisionPublished,
		DraftVersion: 1, AgentKind: agentapp.AgentKindLLM, SchemaVersion: 1, Instruction: "help",
		ModelProfileID: "model", ModelProfileVersion: 3,
		GenerationConfig: map[string]any{"nested": map[string]any{"value": "fixed"}},
	})
	digest, err := revision.ComputeContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	revision.ContentDigest = digest
	configuration := config.ConfigV1{SchemaVersion: 1, DefaultAgentAppID: "app", PolicyVersion: 5,
		BackendBindings: []config.BackendBinding{{Domain: "session", BackendProfileID: "pg", BackendVersion: 1, Required: []string{"atomic_turn_commit"}}}}
	configDigest, _, err := config.ContentDigest(configuration)
	if err != nil {
		t.Fatal(err)
	}
	key := profile.ExecutionProfileKey{TenantID: "tenant-a", TenantVersion: 7, AgentAppID: "app", AgentAppVersion: 11,
		AgentAppRevision: 2, ContentDigest: digest, ConfigVersion: 4, PolicyVersion: 5}
	return Resolver{
		Tenants: tenantReaderStub{tenant.Tenant{TenantID: "tenant-a", Status: tenant.StatusSuspended, Version: 9}},
		Agents:  agentReaderStub{app: agentapp.AgentApp{TenantID: "tenant-a", AgentAppID: "app", Status: agentapp.StatusSuspended, Version: 12}, revision: revision},
		Configs: configReaderStub{config.Snapshot{TenantID: "tenant-a", ConfigVersion: 4, State: config.StatePublished,
			Payload: configuration, ContentDigest: configDigest}},
		Models: modelReaderStub{provider.ModelProfileSnapshot{TenantID: "tenant-a", ProfileID: "model", Version: 3, Status: "suspended"}},
	}, key
}

func publishedRevision(t *testing.T, revision agentapp.Revision) agentapp.Revision {
	t.Helper()
	revision = agentapp.NormalizeRevision(revision)
	revision.State = agentapp.RevisionPublished
	revision.DraftVersion = 1
	revision.SchemaVersion = 1
	digest, err := revision.ComputeContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	revision.ContentDigest = digest
	return revision
}
