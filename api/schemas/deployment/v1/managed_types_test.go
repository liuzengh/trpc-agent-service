package deploymentv1

import (
	"encoding/json"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	"testing"
)

func TestManagedArtifactMetadataAndRoleWire(t *testing.T) {
	c := aggregateFixture(t)
	b := datav1.Snapshot{SchemaVersion: "v1", TenantID: c["tenant_id"].(string), BackendID: "objects", BackendRevision: 1, Kind: datav1.S3, Adapter: "managed-s3-v1", Isolation: "tenant-artifact-v1", Limits: datav1.Limits{TimeoutMS: 1000, MaxConcurrency: 1, MaxBytes: 1024}, S3: &datav1.S3Target{Endpoint: "https://s3.internal", Bucket: "artifacts", Region: "local", Versioning: "disabled"}}
	if err := b.ValidateForRole("artifact"); err != nil {
		t.Fatal(err)
	}
	r := ManifestStorageResource{Kind: "managed_artifact", AdapterVersion: "managed-artifact-v1", Backend: &b, MetadataContract: ArtifactMetadataContract}
	resources := c["resources"].(map[string]any)["storage"].(map[string]any)
	resources["artifact"] = r
	c["storage_roles"].(map[string]any)["artifact"] = "artifact"
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	out, err := DecodeManifestContent(raw)
	if err != nil {
		t.Fatal(err)
	}
	if out.Resources.Storage["artifact"].MetadataContract != ArtifactMetadataContract {
		t.Fatal("metadata lost")
	}
	r.MetadataContract = ""
	if _, err = json.Marshal(r); err == nil {
		t.Fatal("missing metadata contract")
	}
	r.MetadataContract = ArtifactMetadataContract
	r.Kind = "managed_session"
	if _, err = json.Marshal(r); err == nil {
		t.Fatal("metadata on session")
	}
	r.MetadataContract = ""
	r.AdapterVersion = "managed-session-v1"
	delete(resources, "artifact")
	delete(c["storage_roles"].(map[string]any), "artifact")
	resources["session"] = r
	raw, _ = json.Marshal(c)
	if _, err = DecodeManifestContent(raw); err == nil {
		t.Fatal("S3 consumed as Session")
	}
}
