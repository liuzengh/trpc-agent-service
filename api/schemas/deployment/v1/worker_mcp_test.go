package deploymentv1

import "testing"

func workerMCPFixture(t *testing.T) ManifestContent {
	c := workerFixture(t)
	n := c.AgentPlan.Nodes[c.AgentPlan.Root]
	n.ToolResources = []string{"calculate"}
	n.CallableEntries = []string{"tools/calculate"}
	c.AgentPlan.Nodes[c.AgentPlan.Root] = n
	m := c.Resources.Models[n.ModelResource]
	m.Capabilities = []string{"chat", "tool_call"}
	c.Resources.Models[n.ModelResource] = m
	c.Resources.Tools = map[string]ManifestToolResource{"calculate": {Kind: "mcp_streamable_http", AdapterVersion: "mcp-web-search-v1", ServerURL: "https://mcp.example.test/mcp", ToolsetName: "math", ToolName: "add", Capability: "calculator.add", Auth: ManifestToolAuth{Kind: "none"}}}
	c.ResolvedRequirements.Tools = map[string]string{"calculate": "calculate"}
	c.Execution.AllowedEndpointHosts = append(c.Execution.AllowedEndpointHosts, "mcp.example.test")
	c.Execution.MaxToolCalls = 16
	return c
}
func TestWorkerV1MCPSelectedClosure(t *testing.T) {
	for _, bearer := range []bool{false, true} {
		c := workerMCPFixture(t)
		if bearer {
			r := c.Resources.Tools["calculate"]
			r.Auth.Kind = "bearer"
			r.Auth.Credential = &CredentialUse{CredentialID: "mcp-key", Purpose: "bearer_token", AudienceDigest: CredentialAudienceDigest(r.Kind, r.ServerURL, r.Auth.Kind)}
			c.Resources.Tools["calculate"] = r
		}
		if err := ValidateWorkerV1(c, c.PlatformContract.Digest); err != nil {
			t.Fatal(err)
		}
	}
}
func TestWorkerV1MCPRejectExpandedAuthority(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*ManifestContent)
	}{
		{"unselected", func(c *ManifestContent) { c.Resources.Tools["extra"] = c.Resources.Tools["calculate"] }},
		{"callable", func(c *ManifestContent) {
			n := c.AgentPlan.Nodes[c.AgentPlan.Root]
			n.CallableEntries = []string{"tools/other"}
			c.AgentPlan.Nodes[c.AgentPlan.Root] = n
		}},
		{"binding", func(c *ManifestContent) { c.ResolvedRequirements.Tools["calculate"] = "missing" }},
		{"no model tools", func(c *ManifestContent) {
			n := c.AgentPlan.Nodes[c.AgentPlan.Root]
			m := c.Resources.Models[n.ModelResource]
			m.Capabilities = []string{"chat"}
			c.Resources.Models[n.ModelResource] = m
		}},
		{"none credential", func(c *ManifestContent) {
			r := c.Resources.Tools["calculate"]
			r.Auth.Credential = &CredentialUse{}
			c.Resources.Tools["calculate"] = r
		}},
		{"bearer missing", func(c *ManifestContent) {
			r := c.Resources.Tools["calculate"]
			r.Auth.Kind = "bearer"
			c.Resources.Tools["calculate"] = r
		}},
		{"endpoint", func(c *ManifestContent) {
			r := c.Resources.Tools["calculate"]
			r.ServerURL = "https://other.test/mcp"
			c.Resources.Tools["calculate"] = r
		}},
		{"tool missing", func(c *ManifestContent) {
			r := c.Resources.Tools["calculate"]
			r.ToolName = ""
			c.Resources.Tools["calculate"] = r
		}},
		{"call budget", func(c *ManifestContent) { c.Execution.MaxToolCalls = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := workerMCPFixture(t)
			tc.change(&c)
			if ValidateWorkerV1(c, c.PlatformContract.Digest) == nil {
				t.Fatal("expanded MCP accepted")
			}
		})
	}
}
