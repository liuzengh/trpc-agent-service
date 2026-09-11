package application

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	executionv1 "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
	profileapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

type migrationCredentialStub struct{}

func (migrationCredentialStub) ResolveStorageForOwner(_ context.Context, c profileapp.CheckProfileCredentialsCommand) (profileapp.CredentialBatch, error) {
	return profileapp.CredentialBatch{TenantID: c.TenantID, ProfileID: c.ProfileID, ProfileRevisionNumber: c.ProfileRevisionNumber, Credentials: []profileapp.ResolvedCredential{{Use: c.Uses[0], Value: []byte("password")}}}, nil
}

type migrationWorkerStub struct {
	calls int
	err   error
}

func (s *migrationWorkerStub) Execute(_ context.Context, request executionv1.BackendMigrationRequest) (executionv1.BackendMigrationResponse, error) {
	s.calls++
	if request.Source.Password != "password" || request.Target.Password != "password" || request.Source.Backend.Kind != datav1.Redis || request.Target.Backend.Kind != datav1.PostgreSQL {
		return executionv1.BackendMigrationResponse{}, errors.New("bad migration request")
	}
	return executionv1.BackendMigrationResponse{MemoryScopesCopied: 2}, s.err
}

func TestMigrateAndPublishPublishesOnlyAfterWorkerSuccess(t *testing.T) {
	h := newHarness(t)
	created := h.mustCreate(t)
	var spec agentdomain.Spec
	if err := json.Unmarshal(h.agent.version.Spec, &spec); err != nil {
		t.Fatal(err)
	}
	n := spec.Nodes[spec.Root]
	n.Memory = &agentdomain.Memory{Tools: []string{"memory_load"}}
	spec.Nodes[spec.Root] = n
	h.agent.version.Spec, _ = json.Marshal(spec)
	model := h.profile.revisionSpec.Models["primary"]
	model.Capabilities = []string{"chat", "tool_call"}
	h.profile.revisionSpec.Models["primary"] = model
	configure := func(id string, revision uint64) {
		r := profiledomain.StorageResource{Kind: profiledomain.StorageKindManagedMemory, BackendID: id, BackendRevision: revision, DSNCredentialID: "crd_00000000000000000000000000000009"}
		r.CredentialAudienceDigest, _ = h.service.deps.ManagedBackends.(backendMapResolver)["storage/memory"].Digest()
		h.profile.revisionSpec.Storage["memory"] = r
		h.profile.refresh(t)
	}
	limits := datav1.Limits{TimeoutMS: 1000, MaxConcurrency: 2, MaxBytes: 4096}
	redis := datav1.Snapshot{SchemaVersion: "v1", TenantID: "tenant-1", BackendID: "redis", BackendRevision: 1, Kind: datav1.Redis, Adapter: "managed-redis-v1", Isolation: datav1.MemoryIsolation, Limits: limits, Redis: &datav1.RedisTarget{Host: "redis.private", Port: 6379, Username: "runtime", TLS: true}}
	postgres := datav1.Snapshot{SchemaVersion: "v1", TenantID: "tenant-1", BackendID: "postgres", BackendRevision: 2, Kind: datav1.PostgreSQL, Adapter: "managed-postgres-v1", Isolation: datav1.MemoryIsolation, Limits: limits, PostgreSQL: &datav1.PostgresTarget{Host: "postgres.private", Port: 5432, Database: "memory", Username: "runtime", SSLMode: "require"}}
	h.service.deps.ManagedBackends = backendMapResolver{"storage/memory": redis}
	p := &h.service.deps.Platform
	p.RuntimeDataCapabilities = append(p.RuntimeDataCapabilities, "memory")
	p.StorageAdapters[profiledomain.StorageKindManagedMemory] = domain.AdapterContract{Version: domain.StorageAdapterManagedMemoryV1}
	p.Execution.AllowedEndpointHosts = append(p.Execution.AllowedEndpointHosts, "redis.private", "postgres.private")
	p.Digest, _ = p.CalculateDigest()
	configure("redis", 1)
	source, err := h.service.PublishDeploymentRevision(context.Background(), PublishDeploymentCommand{TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "owner", IdempotencyKey: "source", Input: deploymentInput()})
	if err != nil {
		t.Fatal(err, source.Validation.Diagnostics)
	}
	h.service.deps.ManagedBackends = backendMapResolver{"storage/memory": postgres}
	configure("postgres", 2)
	worker := &migrationWorkerStub{err: ErrBackendMigrationBusy}
	h.service.deps.StorageCredentials = migrationCredentialStub{}
	h.service.deps.BackendMigrations = worker
	expected := int64(1)
	command := MigrateAndPublishCommand{TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "owner", IdempotencyKey: "target", SourceRevisionNumber: source.Published.Revision.RevisionNumber, ExpectedLatestRevisionNumber: &expected, Input: deploymentInput()}
	if _, err := h.service.MigrateAndPublish(context.Background(), command); !errors.Is(err, ErrBackendMigrationBusy) || len(h.store.published) != 1 {
		t.Fatalf("failed migration published: %v count=%d", err, len(h.store.published))
	}
	worker.err = nil
	result, err := h.service.MigrateAndPublish(context.Background(), command)
	if err != nil || result.MemoryScopesCopied != 2 || !result.Publication.Created || result.Publication.Published.Revision.RevisionNumber != 2 || len(h.store.published) != 2 {
		t.Fatalf("result=%#v err=%v count=%d", result, err, len(h.store.published))
	}
}
