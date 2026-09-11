package domain

import (
	"encoding/json"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	deploymentv1 "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"testing"
)

func artifactCredentialInput() CompileInput {
	in := workerSummaryInput()
	n := in.Agent.Spec.Nodes["assistant"]
	n.Artifact = &agentdomain.Artifact{Enabled: true}
	in.Agent.Spec.Nodes["assistant"] = n
	m := in.Profile.Spec.Models["primary"]
	m.Capabilities = []string{"chat", "tool_call"}
	in.Profile.Spec.Models["primary"] = m
	b := datav1.Snapshot{SchemaVersion: "v1", TenantID: in.TenantID, BackendID: "artifact-s3", BackendRevision: 1, Kind: datav1.S3, Adapter: "managed-s3-v1", Isolation: "tenant-artifact-v1", Limits: datav1.Limits{TimeoutMS: 1000, MaxConcurrency: 1, MaxBytes: 1024}, S3: &datav1.S3Target{Endpoint: "https://s3.internal", Bucket: "artifacts", Region: "local", Versioning: "disabled"}}
	digest, _ := b.Digest()
	in.ManagedBackends = map[string]datav1.Snapshot{"storage/artifact": b}
	in.Profile.Spec.Storage["artifact"] = profiledomain.StorageResource{Kind: profiledomain.StorageKindManagedArtifact, BackendID: b.BackendID, BackendRevision: 1, AccessKeyIDCredentialID: credentialMemory, SecretAccessKeyCredentialID: "crd_99999999999999999999999999999999", CredentialAudienceDigest: digest}
	in.Platform.Execution.AllowedEndpointHosts = append(in.Platform.Execution.AllowedEndpointHosts, "s3.internal")
	in.Platform.Digest, _ = in.Platform.CalculateDigest()
	return in
}
func TestArtifactCredentialClosureAndHistoricalRead(t *testing.T) {
	in := artifactCredentialInput()
	m, r := Compile(in)
	if !r.Valid {
		t.Fatal(r.Diagnostics)
	}
	a := m.Content.Resources.Storage["artifact"]
	if !validArtifactCredentials(a.Credentials, *a.Backend) || a.MetadataContract != ArtifactMetadataContract {
		t.Fatal("artifact closure")
	}
	if len(m.CredentialUses) != 5 {
		t.Fatal("uses", m.CredentialUses)
	}
	if _, err := ValidateManifestContent(m.CanonicalContent, m.ContentDigest); err != nil {
		t.Fatal(err)
	}
	wire, err := deploymentv1.DecodeManifestContent(m.CanonicalContent)
	if err != nil {
		t.Fatal(err)
	}
	if err = deploymentv1.ValidateWorkerV1(wire, in.Platform.Digest); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*datav1.S3Target){func(s *datav1.S3Target) { s.Bucket = "other" }, func(s *datav1.S3Target) { s.Endpoint = "https://other.internal" }, func(s *datav1.S3Target) { s.Region = "other" }, func(s *datav1.S3Target) { s.PathStyle = !s.PathStyle }, func(s *datav1.S3Target) { s.Versioning = "enabled" }} {
		c := normalizeManifestContent(m.Content)
		mutate(c.Resources.Storage["artifact"].Backend.S3)
		_, raw, digest, err := CanonicalizeManifest(c)
		if err != nil {
			continue
		}
		if _, err = ValidateManifestContent(raw, digest); err == nil {
			t.Fatal("target changed")
		}
	}
	a.Credentials = nil
	m.Content.Resources.Storage["artifact"] = a
	_, raw, digest, err := CanonicalizeManifest(m.Content)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ValidateManifestContent(raw, digest); err != nil {
		t.Fatal("historical read", err)
	}
	wire, err = deploymentv1.DecodeManifestContent(raw)
	if err != nil {
		t.Fatal(err)
	}
	if deploymentv1.ValidateWorkerV1(wire, "") == nil {
		t.Fatal("historical executable")
	}
	var tree map[string]any
	_ = json.Unmarshal(raw, &tree)
	tree["resources"].(map[string]any)["storage"].(map[string]any)["artifact"].(map[string]any)["credentials"] = nil
	bad, _ := json.Marshal(tree)
	if _, err = DecodeManifestContent(bad); err == nil {
		t.Fatal("null accepted")
	}
}
func TestArtifactNewCompileRequiresBothCredentials(t *testing.T) {
	for _, mode := range []string{"access", "secret", "target"} {
		in := artifactCredentialInput()
		r := in.Profile.Spec.Storage["artifact"]
		switch mode {
		case "access":
			r.AccessKeyIDCredentialID = ""
		case "secret":
			r.SecretAccessKeyCredentialID = ""
		case "target":
			r.CredentialAudienceDigest = ""
		}
		in.Profile.Spec.Storage["artifact"] = r
		if m, report := Compile(in); report.Valid || len(m.CanonicalContent) > 0 {
			t.Fatal("incomplete artifact", mode)
		}
	}
}
