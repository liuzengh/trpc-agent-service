package application

import (
	"context"
	"encoding/json"
	"errors"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"strings"
	"testing"
)

type backendResolverStub struct {
	calls         []domain.BackendRequest
	tenant, actor string
	snapshot      datav1.Snapshot
	err           error
}

func (b *backendResolverStub) ResolveDeploymentBackend(_ context.Context, tenant, actor string, r domain.BackendRequest) (datav1.Snapshot, error) {
	b.tenant = tenant
	b.actor = actor
	b.calls = append(b.calls, r)
	return b.snapshot, b.err
}
func TestBackendResolutionClonesOnlyRequestedSnapshots(t *testing.T) {
	r := domain.BackendRequest{Category: "storage", Name: "session", BackendID: "redis", Revision: 1, Role: "session"}
	b := &backendResolverStub{snapshot: datav1.Snapshot{Redis: &datav1.RedisTarget{Host: "original.internal"}}}
	s := &Service{deps: Dependencies{ManagedBackends: b}}
	got, ds := s.resolveManagedBackends(context.Background(), "tenant-1", "owner", []domain.BackendRequest{r})
	if len(ds) != 0 || len(got) != 1 || len(b.calls) != 1 || b.tenant != "tenant-1" || b.actor != "owner" {
		t.Fatal(got, ds, b)
	}
	b.snapshot.Redis.Host = "mutated.internal"
	if got[r.Key()].Redis.Host != "original.internal" {
		t.Fatal("adapter result aliases compiler source")
	}
	// Invalid provider results are intentionally validated again by the Compiler,
	// not trusted merely because the application adapter returned nil error.
}
func TestBackendResolutionUnavailableIsStableAndRedacted(t *testing.T) {
	r := domain.BackendRequest{Category: "storage", Name: "session", BackendID: "redis", Revision: 1, Role: "session"}
	for _, resolver := range []ManagedBackendResolver{nil, &backendResolverStub{err: errors.New("password=private target=internal")}} {
		s := &Service{deps: Dependencies{ManagedBackends: resolver}}
		got, ds := s.resolveManagedBackends(context.Background(), "tenant-1", "owner", []domain.BackendRequest{r})
		if len(got) != 0 || len(ds) != 1 || ds[0].Code != domain.DiagnosticBackendUnavailable || strings.Contains(ds[0].Message, "private") {
			t.Fatal(got, ds)
		}
	}
	b := &backendResolverStub{}
	s := &Service{deps: Dependencies{ManagedBackends: b}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.resolveManagedBackends(ctx, "tenant-1", "owner", []domain.BackendRequest{r})
	if len(b.calls) != 0 {
		t.Fatal("queried after cancellation")
	}
}

func TestValidateAndPublishResolveFixedProfileSelectionWithoutPublishingOnFailure(t *testing.T) {
	h := newHarness(t)
	created := h.mustCreate(t)
	h.profile.revisionSpec.Storage["session"] = profiledomain.StorageResource{Kind: profiledomain.StorageKindManagedSession, BackendID: "redis", BackendRevision: 7}
	h.profile.revisionSpec.Storage["artifact"] = profiledomain.StorageResource{Kind: profiledomain.StorageKindManagedArtifact, BackendID: "unused-s3", BackendRevision: 1}
	h.profile.refresh(t)
	b := &backendResolverStub{err: errors.New("private target unavailable")}
	h.service.deps.ManagedBackends = b
	report, err := h.service.ValidateDeploymentRevision(context.Background(), ValidateDeploymentCommand{TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "member", Input: deploymentInput()})
	if err != nil || report.Valid || len(report.Diagnostics) != 1 || report.Diagnostics[0].Code != domain.DiagnosticBackendUnavailable {
		t.Fatal(report, err)
	}
	if len(b.calls) != 1 || b.calls[0].BackendID != "redis" || b.calls[0].Revision != 7 || b.actor != "member" || b.tenant != "tenant-1" {
		t.Fatal(b)
	}
	if _, err := h.service.PublishDeploymentRevision(context.Background(), PublishDeploymentCommand{TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "owner", IdempotencyKey: "managed-failed", Input: deploymentInput()}); !errors.Is(err, ErrDeploymentRevisionInvalid) {
		t.Fatal(err)
	}
	if len(h.store.published) != 0 || h.checker.calls != 0 {
		t.Fatal("failed backend resolution reached publication or credential resolution")
	}
	calls := len(b.calls)
	if _, err := h.service.ValidateDeploymentRevision(context.Background(), ValidateDeploymentCommand{TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "outsider", Input: deploymentInput()}); !errors.Is(err, ErrTenantForbidden) {
		t.Fatal(err)
	}
	if len(b.calls) != calls {
		t.Fatal("unauthorized actor queried physical target")
	}
}

func TestManagedPublicationSuccessAndDelayedReceiptReplay(t *testing.T) {
	h := newHarness(t)
	created := h.mustCreate(t)
	h.profile.revisionSpec.Storage["session"] = profiledomain.StorageResource{Kind: profiledomain.StorageKindManagedSession, BackendID: "redis", BackendRevision: 1}
	h.profile.refresh(t)
	h.service.deps.Platform.StorageAdapters[profiledomain.StorageKindManagedSession] = domain.AdapterContract{Version: domain.StorageAdapterManagedSessionV1}
	h.service.deps.Platform.Execution.AllowedEndpointHosts = append(h.service.deps.Platform.Execution.AllowedEndpointHosts, "redis.private")
	h.service.deps.Platform.Digest, _ = h.service.deps.Platform.CalculateDigest()
	b := &backendResolverStub{snapshot: datav1.Snapshot{SchemaVersion: "v1", TenantID: "tenant-1", BackendID: "redis", BackendRevision: 1, Kind: datav1.Redis, Adapter: "managed-redis-v1", Isolation: "tenant-session-v1", Limits: datav1.Limits{TimeoutMS: 1000, MaxConcurrency: 1, MaxBytes: 1024}, Redis: &datav1.RedisTarget{Host: "redis.private", Port: 6379, Username: "runtime", TLS: true}}}
	h.service.deps.ManagedBackends = b
	command := PublishDeploymentCommand{TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "owner", IdempotencyKey: "managed-success", Input: deploymentInput()}
	first, err := h.service.PublishDeploymentRevision(context.Background(), command)
	if err != nil || !first.Created {
		t.Fatal(first, err)
	}
	if strings.Contains(string(first.Published.ManifestView), "redis.private") || len(h.checker.last.Uses) != 1 || h.checker.last.Uses[0].Purpose != "api_key" {
		t.Fatal("public projection or credential closure incorrect")
	}
	calls := len(b.calls)
	b.err = errors.New("target later revoked")
	replay, err := h.service.PublishDeploymentRevision(context.Background(), command)
	if err != nil || replay.Created || replay.Published.ManifestDigest != first.Published.ManifestDigest || len(b.calls) != calls {
		t.Fatal("receipt replay depends on current directory", err)
	}
}

type backendMapResolver map[string]datav1.Snapshot

func (b backendMapResolver) ResolveDeploymentBackend(_ context.Context, _, _ string, r domain.BackendRequest) (datav1.Snapshot, error) {
	return b[r.Key()].Clone(), nil
}
func TestPublishFullDataCapabilitiesUsesResolvedBackendClosure(t *testing.T) {
	h := newHarness(t)
	created := h.mustCreate(t)
	var a agentdomain.Spec
	if err := json.Unmarshal(h.agent.version.Spec, &a); err != nil {
		t.Fatal(err)
	}
	limit := int64(-1)
	threshold := int64(3)
	yes := true
	n := a.Nodes[a.Root]
	n.Memory = &agentdomain.Memory{Tools: []string{}, PreloadLimit: &limit}
	n.Artifact = &agentdomain.Artifact{Enabled: true}
	n.AddSessionSummary = &yes
	a.Nodes[a.Root] = n
	a.Runtime = &agentdomain.Runtime{Summary: &agentdomain.Summary{Enabled: true, ModelSlot: "primary", EventThreshold: &threshold}}
	h.agent.version.Spec, _ = json.Marshal(a)
	h.profile.revisionSpec.Storage["session"] = profiledomain.StorageResource{Kind: profiledomain.StorageKindManagedSession, BackendID: "redis", BackendRevision: 1}
	h.profile.revisionSpec.Storage["memory"] = profiledomain.StorageResource{Kind: profiledomain.StorageKindManagedMemory, BackendID: "redis", BackendRevision: 1}
	h.profile.revisionSpec.Storage["artifact"] = profiledomain.StorageResource{Kind: profiledomain.StorageKindManagedArtifact, BackendID: "s3", BackendRevision: 1}
	model := h.profile.revisionSpec.Models["primary"]
	model.Capabilities = []string{"chat", "tool_call"}
	h.profile.revisionSpec.Models["primary"] = model
	h.profile.refresh(t)
	p := &h.service.deps.Platform
	p.RuntimeDataCapabilities = []string{"memory", "artifact", "summary"}
	p.StorageAdapters[profiledomain.StorageKindManagedSession] = domain.AdapterContract{Version: domain.StorageAdapterManagedSessionV1}
	p.StorageAdapters[profiledomain.StorageKindManagedMemory] = domain.AdapterContract{Version: domain.StorageAdapterManagedMemoryV1}
	p.StorageAdapters[profiledomain.StorageKindManagedArtifact] = domain.AdapterContract{Version: domain.StorageAdapterManagedArtifactV1}
	p.Execution.AllowedEndpointHosts = append(p.Execution.AllowedEndpointHosts, "redis.private", "s3.private")
	p.Digest, _ = p.CalculateDigest()
	session := datav1.Snapshot{SchemaVersion: "v1", TenantID: "tenant-1", BackendID: "redis", BackendRevision: 1, Kind: datav1.Redis, Adapter: "managed-redis-v1", Isolation: datav1.SessionIsolation, Limits: datav1.Limits{TimeoutMS: 1000, MaxConcurrency: 1, MaxBytes: 1024}, Redis: &datav1.RedisTarget{Host: "redis.private", Port: 6379, Username: "runtime", TLS: true}}
	memory, _ := session.ForRole("memory")
	memoryDigest, _ := memory.Digest()
	mr := h.profile.revisionSpec.Storage["memory"]
	mr.DSNCredentialID = "crd_00000000000000000000000000000009"
	mr.CredentialAudienceDigest = memoryDigest
	h.profile.revisionSpec.Storage["memory"] = mr
	h.profile.refresh(t)
	artifact := datav1.Snapshot{SchemaVersion: "v1", TenantID: "tenant-1", BackendID: "s3", BackendRevision: 1, Kind: datav1.S3, Adapter: "managed-s3-v1", Isolation: "tenant-artifact-v1", Limits: session.Limits, S3: &datav1.S3Target{Endpoint: "https://s3.private", Bucket: "artifacts", Region: "local", Versioning: "disabled"}}
	h.service.deps.ManagedBackends = backendMapResolver{"storage/session": session, "storage/memory": memory, "storage/artifact": artifact}
	result, err := h.service.PublishDeploymentRevision(context.Background(), PublishDeploymentCommand{TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "owner", IdempotencyKey: "full-data", Input: deploymentInput()})
	if err != nil || !result.Created {
		t.Fatal(result, err)
	}
	view := string(result.Published.ManifestView)
	for _, key := range []string{`"memory"`, `"artifact"`, `"summary"`, `"metadata_contract"`} {
		if !strings.Contains(view, key) {
			t.Fatal("missing public field", key)
		}
	}
	if strings.Contains(view, "redis.private") || strings.Contains(view, "s3.private") {
		t.Fatal("private target leaked")
	}
}
