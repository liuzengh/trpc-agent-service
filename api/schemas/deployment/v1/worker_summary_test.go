package deploymentv1

import (
	"testing"
)

func workerSummaryFixture(t *testing.T) ManifestContent {
	c := workerFixture(t)
	yes := true
	n := c.AgentPlan.Nodes[c.AgentPlan.Root]
	n.AddSessionSummary = &yes
	c.AgentPlan.Nodes[c.AgentPlan.Root] = n
	c.Runtime = &ManifestRuntime{Summary: &ManifestSummary{Enabled: true, ModelResource: n.ModelResource, EventThreshold: 1}}
	return c
}
func TestWorkerV1SummaryClosedModelAndCredentialSet(t *testing.T) {
	for _, separate := range []bool{false, true} {
		c := workerSummaryFixture(t)
		if separate {
			m := c.Resources.Models[c.Runtime.Summary.ModelResource]
			m.Model = "summary-model"
			m.BaseURL = "https://summary.example.test/v1"
			m.Credential.CredentialID = "summary-key"
			m.Credential.AudienceDigest = CredentialAudienceDigest(m.Kind, m.BaseURL)
			c.Resources.Models["summarizer"] = m
			c.ResolvedRequirements.Models["summarizer"] = "summarizer"
			c.Execution.AllowedEndpointHosts = append(c.Execution.AllowedEndpointHosts, "summary.example.test")
			c.Runtime.Summary.ModelResource = "summarizer"
		}
		if err := ValidateWorkerV1(c, c.PlatformContract.Digest); err != nil {
			t.Fatal(separate, err)
		}
	}
}
func TestWorkerV1SummaryRejectsIncompleteOrExpandedClosure(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*ManifestContent)
	}{
		{"disabled", func(c *ManifestContent) { c.Runtime.Summary.Enabled = false }},
		{"threshold", func(c *ManifestContent) { c.Runtime.Summary.EventThreshold = 0 }},
		{"missing model", func(c *ManifestContent) { c.Runtime.Summary.ModelResource = "missing" }},
		{"extra model", func(c *ManifestContent) {
			c.Resources.Models["extra"] = c.Resources.Models[c.Runtime.Summary.ModelResource]
		}},
		{"extra host", func(c *ManifestContent) {
			c.Execution.AllowedEndpointHosts = append(c.Execution.AllowedEndpointHosts, "extra.test")
		}},
		{"missing host", func(c *ManifestContent) { c.Execution.AllowedEndpointHosts = nil }},
		{"credential audience", func(c *ManifestContent) {
			k := c.Runtime.Summary.ModelResource
			m := c.Resources.Models[k]
			m.Credential.AudienceDigest = CredentialAudienceDigest("wrong")
			c.Resources.Models[k] = m
		}},
		{"dsn as model key", func(c *ManifestContent) {
			k := c.Runtime.Summary.ModelResource
			m := c.Resources.Models[k]
			m.Credential.CredentialID = c.Resources.Storage[c.StorageRoles["session"]].Credential.CredentialID
			c.Resources.Models[k] = m
		}},
		{"memory", func(c *ManifestContent) {
			n := c.AgentPlan.Nodes[c.AgentPlan.Root]
			n.Memory = &ManifestMemory{}
			c.AgentPlan.Nodes[c.AgentPlan.Root] = n
		}},
		{"artifact", func(c *ManifestContent) {
			n := c.AgentPlan.Nodes[c.AgentPlan.Root]
			n.Artifact = &ManifestArtifact{}
			c.AgentPlan.Nodes[c.AgentPlan.Root] = n
		}},
		{"contract pin", func(c *ManifestContent) { c.PlatformContract.Digest = CredentialAudienceDigest("other-release") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := workerSummaryFixture(t)
			pin := c.PlatformContract.Digest
			tc.change(&c)
			if ValidateWorkerV1(c, pin) == nil {
				t.Fatal("expanded summary contract accepted")
			}
		})
	}
}
