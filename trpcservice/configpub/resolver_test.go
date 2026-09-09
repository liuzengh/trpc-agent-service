package configpub

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type staticTenantResolver struct {
	context tenant.TenantContext
	err     error
}

func (r staticTenantResolver) Resolve(context.Context, tenant.ResolveRequest) (tenant.TenantContext, error) {
	return r.context, r.err
}

type staticAgentRegistry struct {
	app tenant.AgentApp
	err error
}

func (r staticAgentRegistry) Agent(context.Context, string, string) (tenant.AgentApp, error) {
	return r.app, r.err
}

func TestAssignmentResolverPinsDurableRolloutVersion(t *testing.T) {
	co, repo, _ := newTestCoordinator()
	tc := testTenantContext("tenant-a", 1)
	doc := validDocument()
	fingerprint, _ := Fingerprint(doc)
	repo.revisions[tc.TenantID] = map[int64]Revision{
		5: {TenantID: tc.TenantID, Version: 5, Status: StatusSuperseded, Document: doc, Fingerprint: fingerprint},
		7: {TenantID: tc.TenantID, Version: 7, Status: StatusPublished, Document: doc, Fingerprint: fingerprint},
	}
	repo.rollouts[tc.TenantID] = RolloutState{TenantID: tc.TenantID, ActiveVersion: 7, BaselineVersion: 5, Percentage: 30}
	resolver := AssignmentResolver{Inner: staticTenantResolver{context: tc}, Co: co}
	resolved, err := resolver.Resolve(context.Background(), tenant.ResolveRequest{Channel: tenant.ChannelLark, ExternalAppID: "app-1"})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	bucket, _ := AssignmentBucket(tc.TenantID)
	want := int64(5)
	if bucket < 30 {
		want = 7
	}
	if resolved.ConfigVersion != want {
		t.Fatalf("resolved config version=%d, want %d for bucket %d", resolved.ConfigVersion, want, bucket)
	}
	if resolved.BackendPolicy != tc.BackendPolicy {
		t.Fatalf("assignment did not retain immutable backend policy: got=%+v want=%+v", resolved.BackendPolicy, tc.BackendPolicy)
	}
	if resolved.TenantID != tc.TenantID || resolved.AgentAppID != tc.AgentAppID {
		t.Fatalf("assignment changed tenant identity: %+v", resolved)
	}
}

func TestAssignmentResolverFailsClosedOnUnsafeRollout(t *testing.T) {
	co, repo, _ := newTestCoordinator()
	tc := testTenantContext("tenant-a", 1)
	repo.rollouts[tc.TenantID] = RolloutState{TenantID: tc.TenantID, ActiveVersion: 7, Percentage: 25}
	resolver := AssignmentResolver{Inner: staticTenantResolver{context: tc}, Co: co}
	if _, err := resolver.Resolve(context.Background(), tenant.ResolveRequest{}); !errors.Is(err, ErrInvalidRollout) {
		t.Fatalf("unsafe rollout error=%v, want ErrInvalidRollout", err)
	}
}

func TestAssignmentResolverLeavesUnmanagedTenantAtRegistryVersion(t *testing.T) {
	co, _, _ := newTestCoordinator()
	tc := testTenantContext("tenant-a", 9)
	resolver := AssignmentResolver{Inner: staticTenantResolver{context: tc}, Co: co}
	resolved, err := resolver.Resolve(context.Background(), tenant.ResolveRequest{})
	if err != nil || resolved.ConfigVersion != 9 {
		t.Fatalf("unmanaged resolution=%+v err=%v", resolved, err)
	}
}

func TestSnapshotAgentResolverUsesRequestedRevisionAndRejectsMismatch(t *testing.T) {
	co, repo, _ := newTestCoordinator()
	tc := testTenantContext("tenant-a", 7)
	doc := validDocument()
	doc.Agent.ModelConfigRef = "model-v7"
	doc.Agent.ToolPolicyRef = "policy-v7"
	fingerprint, _ := Fingerprint(doc)
	repo.revisions[tc.TenantID] = map[int64]Revision{7: {
		TenantID: tc.TenantID, Version: 7, Status: StatusPublished, Document: doc, Fingerprint: fingerprint,
	}}
	registry := staticAgentRegistry{app: tenant.AgentApp{TenantID: tc.TenantID, ID: tc.AgentAppID, Name: "assistant-v7", Version: 1, Status: "active"}}
	resolver := SnapshotAgentResolver{Co: co, Registry: registry, ModelProvider: "fake"}
	ref := queue.AgentRefDTO{TenantID: tc.TenantID, AgentAppID: tc.AgentAppID, Version: tc.ConfigVersion}
	spec, err := resolver.Resolve(context.Background(), tc, ref)
	if err != nil {
		t.Fatalf("snapshot resolve: %v", err)
	}
	if spec.Version != 7 || spec.TenantID != tc.TenantID || spec.AgentAppID != tc.AgentAppID || spec.ModelConfigRef != "model-v7" || spec.ToolPolicyRef != "policy-v7" || spec.Name != "assistant-v7" {
		t.Fatalf("snapshot spec=%+v", spec)
	}
	ref.Version = 6
	if _, err := resolver.Resolve(context.Background(), tc, ref); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("version mismatch error=%v", err)
	}
	ref.Version = 7
	bad := tc
	bad.ConfigVersion = 6
	if _, err := resolver.Resolve(context.Background(), bad, queue.AgentRefDTO{TenantID: bad.TenantID, AgentAppID: bad.AgentAppID, Version: 6}); !errors.Is(err, ErrRevisionNotFound) && !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("unavailable revision error=%v", err)
	}
}

func TestSnapshotAgentResolverFailsClosedOnRegistryError(t *testing.T) {
	co, repo, _ := newTestCoordinator()
	tc := testTenantContext("tenant-a", 7)
	doc := validDocument()
	fingerprint, _ := Fingerprint(doc)
	repo.revisions[tc.TenantID] = map[int64]Revision{7: {TenantID: tc.TenantID, Version: 7, Status: StatusPublished, Document: doc, Fingerprint: fingerprint}}
	resolver := SnapshotAgentResolver{Co: co, Registry: staticAgentRegistry{err: errors.New("backend detail")}, ModelProvider: "fake"}
	_, err := resolver.Resolve(context.Background(), tc, queue.AgentRefDTO{TenantID: tc.TenantID, AgentAppID: tc.AgentAppID, Version: 7})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("registry error=%v, want unavailable class", err)
	}
	if strings.Contains(err.Error(), "backend detail") {
		t.Fatalf("raw registry detail escaped: %v", err)
	}
}
