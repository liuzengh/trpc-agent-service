package deploymentv1

import (
	"encoding/json"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	"testing"
)

func workerArtifactFixture(t *testing.T) ManifestContent {
	c := workerMemoryFixture(t)
	b := datav1.Snapshot{SchemaVersion: "v1", TenantID: c.TenantID, BackendID: "s3", BackendRevision: 1, Kind: datav1.S3, Adapter: "managed-s3-v1", Isolation: "tenant-artifact-v1", Limits: datav1.Limits{MaxBytes: 1024, MaxConcurrency: 2, TimeoutMS: 1000}, S3: &datav1.S3Target{Endpoint: "https://s3.internal", Bucket: "artifacts", Region: "test", PathStyle: true, Versioning: "disabled"}}
	d, err := b.Digest()
	if err != nil {
		t.Fatal(err)
	}
	c.StorageRoles["artifact"] = "artifact"
	c.Resources.Storage["artifact"] = ManifestStorageResource{Kind: "managed_artifact", AdapterVersion: "managed-artifact-v1", Backend: &b, MetadataContract: ArtifactMetadataContract, Credentials: &ArtifactCredentials{AccessKeyID: CredentialUse{CredentialID: "crd_00000000000000000000000000000011", Purpose: "access_key_id", AudienceDigest: d}, SecretAccessKey: CredentialUse{CredentialID: "crd_00000000000000000000000000000012", Purpose: "secret_access_key", AudienceDigest: d}}}
	n := c.AgentPlan.Nodes[c.AgentPlan.Root]
	n.Artifact = &ManifestArtifact{Enabled: true, Resource: "artifact"}
	c.AgentPlan.Nodes[c.AgentPlan.Root] = n
	c.Execution.AllowedEndpointHosts = append(c.Execution.AllowedEndpointHosts, "s3.internal")
	return c
}
func TestWorkerArtifactFixedClosureAndHistoricalRead(t *testing.T) {
	c := workerArtifactFixture(t)
	if err := ValidateWorkerV1(c, c.PlatformContract.Digest); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*ManifestContent){
		"missing credentials": func(c *ManifestContent) {
			r := c.Resources.Storage["artifact"]
			r.Credentials = nil
			c.Resources.Storage["artifact"] = r
		},
		"wrong credential purpose": func(c *ManifestContent) { c.Resources.Storage["artifact"].Credentials.AccessKeyID.Purpose = "api_key" },
		"route mutation":           func(c *ManifestContent) { c.Resources.Storage["artifact"].Backend.S3.Bucket = "other" },
		"cross tenant":             func(c *ManifestContent) { c.Resources.Storage["artifact"].Backend.TenantID = "other" },
		"implicit enable": func(c *ManifestContent) {
			n := c.AgentPlan.Nodes[c.AgentPlan.Root]
			n.Artifact = nil
			c.AgentPlan.Nodes[c.AgentPlan.Root] = n
		},
		"credential collision": func(c *ManifestContent) {
			a := c.Resources.Storage["artifact"].Credentials
			a.AccessKeyID.CredentialID = a.SecretAccessKey.CredentialID
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := workerArtifactFixture(t)
			mutate(&c)
			if ValidateWorkerV1(c, c.PlatformContract.Digest) == nil {
				t.Fatal("invalid closure executed")
			}
		})
	}
	r := c.Resources.Storage["artifact"]
	r.Credentials = nil
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal("historical descriptor", err)
	}
	var decoded ManifestStorageResource
	if json.Unmarshal(b, &decoded) != nil || decoded.Credentials != nil {
		t.Fatal("historical changed")
	}
}
