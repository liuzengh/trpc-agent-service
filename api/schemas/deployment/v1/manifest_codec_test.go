package deploymentv1

import (
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
)

func TestStrictManifestCodec(t *testing.T) {
	raw, err := os.ReadFile("examples/valid/runtime-manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = DecodeRuntimeManifest(raw); err != nil {
		t.Fatal(err)
	}
	var m RuntimeManifest
	if err = json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*RuntimeManifest){
		"digest": func(m *RuntimeManifest) { m.ContentDigest = "sha256:" + strings.Repeat("0", 64) },
		"tenant": func(m *RuntimeManifest) { m.TenantID = "another-tenant" },
		"unknown content": func(m *RuntimeManifest) {
			m.Content = append(m.Content[:len(m.Content)-1], []byte(`,"secret":"value"}`)...)
		},
	} {
		t.Run(name, func(t *testing.T) {
			var copy RuntimeManifest
			json.Unmarshal(raw, &copy)
			mutate(&copy)
			data, _ := json.Marshal(copy)
			if _, err = DecodeRuntimeManifest(data); !errors.Is(err, ErrInvalidManifest) {
				t.Fatalf("error %v", err)
			}
		})
	}
	for _, bad := range [][]byte{append(raw, []byte("{}")...), []byte(strings.Replace(string(raw), `"manifest_id":`, `"manifest_id":"duplicate","manifest_id":`, 1)), []byte(strings.Replace(string(raw), `rmf_`, `\ud800rmf_`, 1))} {
		if _, err = DecodeRuntimeManifest(bad); !errors.Is(err, ErrInvalidManifest) {
			t.Fatalf("accepted malformed manifest: %v", err)
		}
	}
}
func workerFixture(t *testing.T) ManifestContent {
	raw, err := os.ReadFile("examples/valid/runtime-manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	m, err := DecodeRuntimeManifest(raw)
	if err != nil {
		t.Fatal(err)
	}
	c, err := DecodeManifestContent(m.Content)
	if err != nil {
		t.Fatal(err)
	}
	c.PlatformContract.Version = WorkerV1PlatformVersion
	var root ManifestNode
	for _, node := range c.AgentPlan.Nodes {
		if node.Kind == "llm" {
			root = node
			break
		}
	}
	root.ToolResources = []string{}
	root.KnowledgeResources = []string{}
	root.CallableEntries = []string{}
	c.AgentPlan = AgentPlan{Root: "assistant", Nodes: map[string]ManifestNode{"assistant": root}}
	c.Resources.Models = map[string]ManifestModelResource{root.ModelResource: c.Resources.Models[root.ModelResource]}
	c.Resources.Tools = map[string]ManifestToolResource{}
	c.Resources.Knowledge = map[string]ManifestKnowledgeResource{}
	c.ResolvedRequirements = ResolvedRequirements{Models: map[string]string{root.ModelResource: root.ModelResource}, Tools: map[string]string{}, Knowledge: map[string]string{}}
	key := c.StorageRoles["session"]
	c.StorageRoles = map[string]string{"session": key}
	session := c.Resources.Storage[key]
	session.Destination.Username = WorkerV1SessionRuntimeRole
	session.Credential.AudienceDigest = CredentialAudienceDigest(session.Kind, session.Destination)
	c.Resources.Storage = map[string]ManifestStorageResource{key: session}
	endpoint, _ := url.Parse(c.Resources.Models[root.ModelResource].BaseURL)
	c.Execution.AllowedEndpointHosts = []string{endpoint.Hostname(), c.Resources.Storage[key].Destination.Host}
	return c
}
func TestWorkerV1StaticContract(t *testing.T) {
	c := workerFixture(t)
	if err := ValidateWorkerV1(c, c.PlatformContract.Digest); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*ManifestContent){
		"legacy": func(c *ManifestContent) { c.PlatformContract.Version = "platform-v1" },
		"combination": func(c *ManifestContent) {
			n := c.AgentPlan.Nodes[c.AgentPlan.Root]
			n.Kind = "parallel"
			c.AgentPlan.Nodes[c.AgentPlan.Root] = n
		},
		"memory": func(c *ManifestContent) { c.StorageRoles["memory"] = c.StorageRoles["session"] },
		"callable": func(c *ManifestContent) {
			n := c.AgentPlan.Nodes[c.AgentPlan.Root]
			n.CallableEntries = []string{"tools/extra"}
			c.AgentPlan.Nodes[c.AgentPlan.Root] = n
		},
		"no session": func(c *ManifestContent) { delete(c.StorageRoles, "session") },
		"host":       func(c *ManifestContent) { c.Execution.AllowedEndpointHosts = nil },
	} {
		t.Run(name, func(t *testing.T) {
			copy := workerFixture(t)
			mutate(&copy)
			if !errors.Is(ValidateWorkerV1(copy, c.PlatformContract.Digest), ErrUnsupportedWorkerManifest) {
				t.Fatal("unsupported manifest accepted")
			}
		})
	}
	// A different release pin is generation skew, not a capability refusal: the
	// same manifest is executable by a consumer running the producer's release.
	skewed := workerFixture(t)
	skewed.PlatformContract.Digest = "sha256:" + strings.Repeat("2", 64)
	if err := ValidateWorkerV1(skewed, c.PlatformContract.Digest); !errors.Is(err, ErrWorkerV1ContractMismatch) || errors.Is(err, ErrUnsupportedWorkerManifest) {
		t.Fatalf("release pin mismatch = %v", err)
	}
	c.ResolvedRequirements.Models = map[string]string{"logical-slot": c.AgentPlan.Nodes[c.AgentPlan.Root].ModelResource}
	if err := ValidateWorkerV1(c, c.PlatformContract.Digest); err != nil {
		t.Fatalf("different slot/resource keys: %v", err)
	}
	// The new static capability gate does not impose a cumulative token cap or
	// reduce a source generation parameter; the existing compiler owns validity.
	n := c.AgentPlan.Nodes[c.AgentPlan.Root]
	limit := int64(16384)
	c.Execution.MaxOutputTokens = limit
	n.Generation = &Generation{MaxOutputTokens: &limit}
	c.AgentPlan.Nodes[c.AgentPlan.Root] = n
	if err := ValidateWorkerV1(c, c.PlatformContract.Digest); err != nil {
		t.Fatal(err)
	}
	c.Execution.MaxOutputTokens = limit - 1
	if !errors.Is(ValidateWorkerV1(c, c.PlatformContract.Digest), ErrUnsupportedWorkerManifest) {
		t.Fatal("generation above explicit single-output policy accepted")
	}
}

func TestWorkerV1RejectsNonRuntimeSessionRole(t *testing.T) {
	for _, username := range []string{"agent", "session_migrator", "platform_admin", "SESSION_RUNTIME"} {
		t.Run(username, func(t *testing.T) {
			c := workerFixture(t)
			key := c.StorageRoles["session"]
			session := c.Resources.Storage[key]
			session.Destination.Username = username
			// Keep the destination audience valid: the failure must be the role gate.
			session.Credential.AudienceDigest = CredentialAudienceDigest(session.Kind, session.Destination)
			c.Resources.Storage[key] = session
			err := ValidateWorkerV1(c, c.PlatformContract.Digest)
			if !errors.Is(err, ErrWorkerV1SessionRuntimeRole) || !errors.Is(err, ErrUnsupportedWorkerManifest) || !strings.Contains(err.Error(), "session runtime username must be session_runtime") {
				t.Fatalf("role diagnostic = %v", err)
			}
		})
	}
}

func TestWorkerV1GoldenPassesSharedGate(t *testing.T) {
	raw, err := os.ReadFile("examples/valid/runtime-manifest-worker-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	m, err := DecodeRuntimeManifest(raw)
	if err != nil {
		t.Fatal(err)
	}
	c, err := VerifyRuntimeManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateWorkerV1(c, c.PlatformContract.Digest); err != nil {
		t.Fatal(err)
	}
}

func TestPublishedExecutionLimitDoesNotInheritNodeSchemaCeiling(t *testing.T) {
	c := workerFixture(t)
	c.Execution.MaxOutputTokens = 300000
	node := c.AgentPlan.Nodes[c.AgentPlan.Root]
	node.Generation = nil
	c.AgentPlan.Nodes[c.AgentPlan.Root] = node
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeManifestContent(raw)
	if err != nil || decoded.Execution.MaxOutputTokens != 300000 {
		t.Fatalf("positive published execution limit rejected by schema: %v", err)
	}
	if err = ValidateWorkerV1(decoded, c.PlatformContract.Digest); err != nil {
		t.Fatalf("published execution limit rejected by static gate: %v", err)
	}
	// Node max_output_tokens is a different field with an existing schema cap.
	tooLargeNode := int64(262145)
	node.Generation = &Generation{MaxOutputTokens: &tooLargeNode}
	c.AgentPlan.Nodes[c.AgentPlan.Root] = node
	raw, err = json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = DecodeManifestContent(raw); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("node schema ceiling was weakened: %v", err)
	}
	t.Log("PUBLISHED_LIMIT_CONTRACT=PASS execution=300000 accepted; node=262145 rejected by unchanged node schema")
}
