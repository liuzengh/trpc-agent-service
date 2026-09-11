package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
	profileapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"testing"
)

type artifactVaultStub struct {
	calls int
	last  profileapp.CheckProfileCredentialsCommand
}

func (v *artifactVaultStub) ResolveArtifactForOwner(_ context.Context, c profileapp.CheckProfileCredentialsCommand) (profileapp.CredentialBatch, error) {
	v.calls++
	v.last = c
	return profileapp.CredentialBatch{Credentials: []profileapp.ResolvedCredential{{Use: c.Uses[0], Value: []byte("access")}, {Use: c.Uses[1], Value: []byte("secret")}}}, nil
}

type artifactBackendStub struct {
	calls int
	last  ArtifactBackendRequest
	err   error
}

func (b *artifactBackendStub) ExecuteArtifact(_ context.Context, c ArtifactBackendRequest) (ArtifactResult, error) {
	b.calls++
	b.last = c
	data := []byte("hello")
	sum := sha256.Sum256(data)
	return ArtifactResult{Name: c.Name, Version: 0, SizeBytes: len(data), SHA256: hex.EncodeToString(sum[:]), Content: data, MimeType: "text/plain"}, b.err
}
func TestArtifactOwnerProxyDerivesFixedPublication(t *testing.T) {
	h := newHarness(t)
	created := h.mustCreate(t)
	var a agentdomain.Spec
	_ = json.Unmarshal(h.agent.version.Spec, &a)
	n := a.Nodes[a.Root]
	n.Artifact = &agentdomain.Artifact{Enabled: true}
	a.Nodes[a.Root] = n
	h.agent.version.Spec, _ = json.Marshal(a)
	b := datav1.Snapshot{SchemaVersion: "v1", TenantID: "tenant-1", BackendID: "s3", BackendRevision: 1, Kind: datav1.S3, Adapter: "managed-s3-v1", Isolation: "tenant-artifact-v1", Limits: datav1.Limits{TimeoutMS: 1000, MaxConcurrency: 1, MaxBytes: 1024}, S3: &datav1.S3Target{Endpoint: "https://s3.private", Bucket: "artifacts", Region: "local", Versioning: "disabled"}}
	digest, _ := b.Digest()
	m := h.profile.revisionSpec.Models["primary"]
	m.Capabilities = []string{"chat", "tool_call"}
	h.profile.revisionSpec.Models["primary"] = m
	h.profile.revisionSpec.Storage["artifact"] = profiledomain.StorageResource{Kind: profiledomain.StorageKindManagedArtifact, BackendID: "s3", BackendRevision: 1, AccessKeyIDCredentialID: "crd_88888888888888888888888888888888", SecretAccessKeyCredentialID: "crd_99999999999999999999999999999999", CredentialAudienceDigest: digest}
	h.profile.refresh(t)
	p := &h.service.deps.Platform
	p.RuntimeDataCapabilities = []string{"artifact"}
	p.StorageAdapters[profiledomain.StorageKindManagedArtifact] = domain.AdapterContract{Version: domain.StorageAdapterManagedArtifactV1}
	p.Execution.AllowedEndpointHosts = append(p.Execution.AllowedEndpointHosts, "s3.private")
	p.Digest, _ = p.CalculateDigest()
	h.service.deps.ManagedBackends = backendMapResolver{"storage/artifact": b}
	published, err := h.service.PublishDeploymentRevision(context.Background(), PublishDeploymentCommand{TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "owner", IdempotencyKey: "artifact", Input: deploymentInput()})
	if err != nil {
		t.Fatal(err)
	}
	vault := &artifactVaultStub{}
	backend := &artifactBackendStub{}
	h.service.deps.ArtifactCredentials = vault
	h.service.deps.ArtifactBackend = backend
	c := ArtifactCommand{TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "owner", RevisionNumber: 1, RunID: "run-1", Name: "note.txt", Operation: "load"}
	out, err := h.service.AccessArtifact(context.Background(), c)
	if err != nil || string(out.Content) != "hello" {
		t.Fatal(err)
	}
	if backend.last.ManifestRef != published.Published.Manifest.ID || backend.last.DeploymentRevisionID != published.Published.Revision.ID || backend.last.TenantID != "tenant-1" || backend.last.RunID != "run-1" || backend.last.Credentials.SecretAccessKey != "secret" || vault.last.Uses[0].AudienceDigest != digest {
		t.Fatal("unfixed request", backend.last)
	}
	calls := vault.calls
	c.ActorUserID = "member"
	if _, err = h.service.AccessArtifact(context.Background(), c); err != ErrTenantForbidden || vault.calls != calls {
		t.Fatal("member resolved", err)
	}
	c.ActorUserID = "owner"
	c.Operation = "save"
	c.Content = make([]byte, 1025)
	if _, err = h.service.AccessArtifact(context.Background(), c); err != ErrArtifactTooLarge || vault.calls != calls {
		t.Fatal("limit", err)
	}
	c.Content = nil
	c.Name = "../other"
	if _, err = h.service.AccessArtifact(context.Background(), c); err != ErrArtifactInvalid {
		t.Fatal("name", err)
	}
	c.Name = "note.txt"
	c.Operation = "load"
	backend.err = ErrArtifactForbidden
	if _, err = h.service.AccessArtifact(context.Background(), c); err != ErrArtifactForbidden {
		t.Fatal("Worker scope denial", err)
	}
}
