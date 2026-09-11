package domain

import (
	"bytes"
	"encoding/json"
	deploymentv1 "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"testing"
)

func workspaceInput(save bool) CompileInput {
	in := workerSummaryInput()
	if save {
		in = artifactCredentialInput()
	}
	in.Agent.Spec.Requirements.Executors = map[string]agentdomain.CapabilityRequirement{"shell": {Capability: "workspace"}}
	in.Profile.Spec.Executors = map[string]profiledomain.ExecutorResource{"shell": {Kind: "sdk_sandbox"}, "extra": {Kind: "sdk_sandbox"}}
	n := in.Agent.Spec.Nodes["assistant"]
	n.Workspace = &agentdomain.Workspace{ExecutorSlot: "shell", Tools: []string{"workspace_exec"}}
	if save {
		n.Workspace.Tools = append(n.Workspace.Tools, "workspace_save_artifact")
	}
	in.Agent.Spec.Nodes["assistant"] = n
	return in
}
func TestWorkspaceCompilationAndConsumerGate(t *testing.T) {
	for _, save := range []bool{false, true} {
		in := workspaceInput(save)
		m, report := Compile(in)
		if !report.Valid {
			t.Fatal(report.Diagnostics)
		}
		view := NewManifestView(m.Content)
		if len(view.Resources.Executors) != 1 || view.AgentPlan.Nodes["assistant"].Workspace == nil {
			t.Fatal("public projection dropped workspace")
		}
		viewBytes, e := json.Marshal(view)
		if e != nil {
			t.Fatal(e)
		}
		var schemaDoc, viewDoc any
		json.Unmarshal(deploymentv1.ManifestViewSchema, &schemaDoc)
		json.Unmarshal(viewBytes, &viewDoc)
		compiler := jsonschema.NewCompiler()
		if e := compiler.AddResource("https://jfsas.dev/schemas/deployment/v1/runtime-manifest-view.schema.json", schemaDoc); e != nil {
			t.Fatal(e)
		}
		schema, e := compiler.Compile("https://jfsas.dev/schemas/deployment/v1/runtime-manifest-view.schema.json")
		if e != nil {
			t.Fatal(e)
		}
		if e = schema.Validate(viewDoc); e != nil {
			t.Fatal(e)
		}
		if _, e = PublicManifestViewFromJSON(m.CanonicalContent); e != nil {
			t.Fatal(e)
		}
		if len(m.Content.Resources.Executors) != 1 || m.Content.ResolvedRequirements.Executors["shell"] != "shell" || m.Content.AgentPlan.Nodes["assistant"].Workspace == nil {
			t.Fatal("closure lost")
		}
		if _, err := VerifyManifestContent(m.CanonicalContent, m.ContentDigest); err != nil {
			t.Fatal(err)
		}
		wire, err := deploymentv1.DecodeManifestContent(m.CanonicalContent)
		if err != nil {
			t.Fatal(err)
		}
		if err = deploymentv1.ValidateWorkerV1(wire, in.Platform.Digest); err != nil {
			t.Fatal(err)
		}
		for name, mutate := range map[string]func(*deploymentv1.ManifestContent){"extra": func(c *deploymentv1.ManifestContent) { c.Resources.Executors["extra"] = c.Resources.Executors["shell"] }, "missing": func(c *deploymentv1.ManifestContent) { delete(c.Resources.Executors, "shell") }, "binding": func(c *deploymentv1.ManifestContent) { c.ResolvedRequirements.Executors["shell"] = "other" }, "empty": func(c *deploymentv1.ManifestContent) {
			n := c.AgentPlan.Nodes["assistant"]
			n.Workspace.Tools = []string{}
			c.AgentPlan.Nodes["assistant"] = n
		}, "wrong adapter": func(c *deploymentv1.ManifestContent) {
			c.Resources.Executors["shell"] = deploymentv1.ManifestExecutorResource{Kind: "sdk_sandbox", AdapterVersion: "other"}
		}, "save without artifact": func(c *deploymentv1.ManifestContent) {
			n := c.AgentPlan.Nodes["assistant"]
			n.Artifact = nil
			n.Workspace.Tools = []string{"workspace_save_artifact"}
			c.AgentPlan.Nodes["assistant"] = n
		}} {
			t.Run(name, func(t *testing.T) {
				c, _ := deploymentv1.DecodeManifestContent(m.CanonicalContent)
				mutate(&c)
				if deploymentv1.ValidateWorkerV1(c, in.Platform.Digest) == nil {
					t.Fatal("accepted")
				}
			})
		}
		bad := bytes.Replace(m.CanonicalContent, []byte(`"executor_resource":"shell"`), []byte(`"executor_resource":"shell","path":"/tmp"`), 1)
		if _, err := deploymentv1.DecodeManifestContent(bad); err == nil {
			t.Fatal("path accepted")
		}
	}
}
func TestWorkspaceCompilationRejectsInvalidDependencies(t *testing.T) {
	for name, mutate := range map[string]func(*CompileInput){"missing": func(i *CompileInput) { delete(i.Profile.Spec.Executors, "shell") }, "model": func(i *CompileInput) {
		m := i.Profile.Spec.Models["primary"]
		m.Capabilities = []string{"chat"}
		i.Profile.Spec.Models["primary"] = m
		i.Agent.Spec.Requirements.Models["primary"] = agentdomain.ModelRequirement{Capabilities: []string{"chat"}}
	}, "save": func(i *CompileInput) {
		n := i.Agent.Spec.Nodes["assistant"]
		n.Workspace.Tools = []string{"workspace_save_artifact"}
		i.Agent.Spec.Nodes["assistant"] = n
	}, "platform": func(i *CompileInput) {
		i.Platform.RuntimeDataCapabilities = []string{"summary", "memory", "artifact"}
		i.Platform.Digest, _ = i.Platform.CalculateDigest()
	}} {
		t.Run(name, func(t *testing.T) {
			in := workspaceInput(false)
			mutate(&in)
			_, r := Compile(in)
			if r.Valid {
				t.Fatal("accepted")
			}
		})
	}
}
func TestWorkspaceNodeAuthorityAndLegacyCanonical(t *testing.T) {
	in := workspaceInput(false)
	n := in.Agent.Spec.Nodes["assistant"]
	other := n
	other.Workspace = nil
	in.Agent.Spec.Nodes["other"] = other
	in.Agent.Spec.Root = "root"
	in.Agent.Spec.Nodes["root"] = agentdomain.Node{Kind: agentdomain.NodeKindSequence, Children: []string{"assistant", "other"}}
	m, r := Compile(in)
	if !r.Valid {
		t.Fatal(r.Diagnostics)
	}
	if m.Content.AgentPlan.Nodes["other"].Workspace != nil {
		t.Fatal("sibling authority leaked")
	}
	legacy, r := Compile(workerSummaryInput())
	if !r.Valid {
		t.Fatal(r.Diagnostics)
	}
	var doc map[string]any
	json.Unmarshal(legacy.CanonicalContent, &doc)
	if bytes.Contains(legacy.CanonicalContent, []byte(`"executors"`)) || bytes.Contains(legacy.CanonicalContent, []byte(`"workspace"`)) {
		t.Fatal("legacy shape changed")
	}
}
