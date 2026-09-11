package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

func TestManifestSemanticValidationRejectsRedigestedInvalidSnapshots(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ManifestContent)
	}{
		{"missing node model", func(c *ManifestContent) {
			n := c.AgentPlan.Nodes["writer"]
			n.ModelResource = "missing"
			c.AgentPlan.Nodes["writer"] = n
		}},
		{"missing selected tool", func(c *ManifestContent) { delete(c.Resources.Tools, "search") }},
		{"missing selected knowledge", func(c *ManifestContent) { delete(c.Resources.Knowledge, "docs") }},
		{"orphan model", func(c *ManifestContent) { c.Resources.Models["spare"] = c.Resources.Models["primary"] }},
		{"orphan tool", func(c *ManifestContent) { c.Resources.Tools["spare"] = c.Resources.Tools["search"] }},
		{"orphan knowledge", func(c *ManifestContent) { c.Resources.Knowledge["spare"] = c.Resources.Knowledge["docs"] }},
		{"orphan storage", func(c *ManifestContent) { c.Resources.Storage["archive"] = c.Resources.Storage["session"] }},
		{"missing session role", func(c *ManifestContent) { delete(c.StorageRoles, "session") }},
		{"role aliases another resource", func(c *ManifestContent) { c.StorageRoles["session"] = "memory" }},
		{"missing optional role resource", func(c *ManifestContent) { delete(c.Resources.Storage, "memory") }},
		{"unknown runtime role", func(c *ManifestContent) {
			c.StorageRoles["archive"] = "archive"
			c.Resources.Storage["archive"] = c.Resources.Storage["session"]
		}},
		{"undeclared callable on sibling", func(c *ManifestContent) {
			n := c.AgentPlan.Nodes["writer"]
			n.CallableEntries = []string{"tools/search"}
			c.AgentPlan.Nodes["writer"] = n
		}},
		{"selected tool lacks callable", func(c *ManifestContent) {
			n := c.AgentPlan.Nodes["researcher"]
			n.CallableEntries = []string{"knowledge/docs"}
			c.AgentPlan.Nodes["researcher"] = n
		}},
		{"selected knowledge lacks callable", func(c *ManifestContent) {
			n := c.AgentPlan.Nodes["researcher"]
			n.CallableEntries = []string{"tools/search"}
			c.AgentPlan.Nodes["researcher"] = n
		}},
		{"unknown callable", func(c *ManifestContent) {
			n := c.AgentPlan.Nodes["researcher"]
			n.CallableEntries = append(n.CallableEntries, "executor/shell")
			c.AgentPlan.Nodes["researcher"] = n
		}},
		{"model lacks derived tool call", func(c *ManifestContent) {
			r := c.Resources.Models["primary"]
			r.Capabilities = []string{profiledomain.CapabilityChat}
			c.Resources.Models["primary"] = r
		}},
		{"resolved tool aliases another key", func(c *ManifestContent) { c.ResolvedRequirements.Tools["search"] = "other" }},
		{"resolved model missing", func(c *ManifestContent) { delete(c.ResolvedRequirements.Models, "primary") }},
		{"resolved knowledge extra", func(c *ManifestContent) { c.ResolvedRequirements.Knowledge["extra"] = "extra" }},
		{"model adapter", func(c *ManifestContent) {
			r := c.Resources.Models["primary"]
			r.AdapterVersion = "future"
			c.Resources.Models["primary"] = r
		}},
		{"tool adapter", func(c *ManifestContent) {
			r := c.Resources.Tools["search"]
			r.AdapterVersion = "future"
			c.Resources.Tools["search"] = r
		}},
		{"knowledge adapter", func(c *ManifestContent) {
			r := c.Resources.Knowledge["docs"]
			r.AdapterVersion = "future"
			c.Resources.Knowledge["docs"] = r
		}},
		{"storage adapter", func(c *ManifestContent) {
			r := c.Resources.Storage["session"]
			r.AdapterVersion = "future"
			c.Resources.Storage["session"] = r
		}},
		{"model kind", func(c *ManifestContent) {
			r := c.Resources.Models["primary"]
			r.Kind = "future"
			c.Resources.Models["primary"] = r
		}},
		{"tool kind", func(c *ManifestContent) {
			r := c.Resources.Tools["search"]
			r.Kind = "command"
			c.Resources.Tools["search"] = r
		}},
		{"knowledge capability", func(c *ManifestContent) {
			r := c.Resources.Knowledge["docs"]
			r.Capability = "other"
			c.Resources.Knowledge["docs"] = r
		}},
		{"storage kind", func(c *ManifestContent) {
			r := c.Resources.Storage["session"]
			r.Kind = "future"
			c.Resources.Storage["session"] = r
		}},
		{"missing child", func(c *ManifestContent) {
			n := c.AgentPlan.Nodes["main"]
			n.Children = []string{"missing", "writer"}
			c.AgentPlan.Nodes["main"] = n
		}},
		{"unreachable node", func(c *ManifestContent) { c.AgentPlan.Nodes["orphan"] = c.AgentPlan.Nodes["writer"] }},
		{"cycle", func(c *ManifestContent) {
			n := c.AgentPlan.Nodes["main"]
			n.Children = append(n.Children, "main")
			c.AgentPlan.Nodes["main"] = n
		}},
		{"multiple parent", func(c *ManifestContent) {
			c.AgentPlan.Nodes["group"] = ManifestNode{Kind: agentdomain.NodeKindSequence, Children: []string{"writer"}}
			n := c.AgentPlan.Nodes["main"]
			n.Children = append(n.Children, "group")
			c.AgentPlan.Nodes["main"] = n
		}},
		{"invalid loop body", func(c *ManifestContent) {
			c.AgentPlan.Nodes["main"] = ManifestNode{Kind: agentdomain.NodeKindLoop, Body: "missing", MaxIterations: 2}
		}},
		{"model audience mismatch", func(c *ManifestContent) {
			r := c.Resources.Models["primary"]
			r.Credential.AudienceDigest = audienceDigest("other")
			c.Resources.Models["primary"] = r
		}},
		{"extra endpoint permission", func(c *ManifestContent) {
			c.Execution.AllowedEndpointHosts = append(c.Execution.AllowedEndpointHosts, "extra.example.test")
		}},
		{"missing endpoint permission", func(c *ManifestContent) { c.Execution.AllowedEndpointHosts = c.Execution.AllowedEndpointHosts[1:] }},
		{"oversized tenant identity", func(c *ManifestContent) { c.TenantID = strings.Repeat("x", 129) }},
		{"oversized source identity", func(c *ManifestContent) { c.Sources.Profile.RevisionID = strings.Repeat("界", 129) }},
		{"too many allowed hosts", func(c *ManifestContent) { c.Execution.AllowedEndpointHosts = make([]string, 129) }},
		{"unknown backend", func(c *ManifestContent) { c.Execution.Backend = "arbitrary" }},
		{"unknown platform version", func(c *ManifestContent) { c.PlatformContract.Version = "future" }},
		{"generation exceeds manifest limit", func(c *ManifestContent) {
			n := c.AgentPlan.Nodes["writer"]
			limit := c.Execution.MaxOutputTokens + 1
			n.Generation = &agentdomain.Generation{MaxOutputTokens: &limit}
			c.AgentPlan.Nodes["writer"] = n
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			compiled, report := Compile(validCompileInput())
			if !report.Valid {
				t.Fatalf("fixture: %#v", report)
			}
			test.mutate(&compiled.Content)
			_, raw, digest, err := CanonicalizeManifest(compiled.Content)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ValidateManifestContent(raw, digest); !errors.Is(err, ErrInvalidManifestContent) {
				t.Fatalf("ValidateManifestContent = %v", err)
			}
			if _, err := VerifyManifestContent(raw, digest); !errors.Is(err, ErrInvalidManifestContent) {
				t.Fatalf("VerifyManifestContent = %v", err)
			}
			if _, err := PublicManifestViewFromJSON(raw); !errors.Is(err, ErrInvalidManifestContent) {
				t.Fatalf("PublicManifestViewFromJSON = %v", err)
			}
		})
	}
}

func TestManifestSemanticValidationAcceptsEquivalentJSONBAndOptionalResources(t *testing.T) {
	input := validCompileInput()
	delete(input.Profile.Spec.Storage, "memory")
	knowledge := input.Profile.Spec.Knowledge["docs"]
	knowledge.QdrantAPIKeyCredentialID = ""
	input.Profile.Spec.Knowledge["docs"] = knowledge
	compiled, report := Compile(input)
	if !report.Valid {
		t.Fatalf("Compile = %#v", report)
	}
	if _, err := VerifyManifestContent(compiled.CanonicalContent, compiled.ContentDigest); err != nil {
		t.Fatal(err)
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, compiled.CanonicalContent, "", "  "); err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateManifestContent(pretty.Bytes(), compiled.ContentDigest); err != nil {
		t.Fatalf("JSONB = %v", err)
	}
	if _, err := VerifyManifestContent(pretty.Bytes(), compiled.ContentDigest); !errors.Is(err, ErrInvalidManifestContent) {
		t.Fatalf("noncanonical wire accepted: %v", err)
	}
	if _, err := ValidateManifestContent(pretty.Bytes(), "sha256:incorrect"); !errors.Is(err, ErrInvalidManifestContent) {
		t.Fatalf("digest mismatch accepted: %v", err)
	}
}
