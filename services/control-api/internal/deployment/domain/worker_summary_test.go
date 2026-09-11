package domain

import (
	"encoding/json"
	deploymentv1 "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func workerSummaryInput() CompileInput {
	in := validCompileInput()
	in.Platform = WorkerV1PlatformExecutionContract()
	threshold := int64(1)
	yes := true
	in.Agent.Spec.Root = "assistant"
	in.Agent.Spec.Nodes = map[string]agentdomain.Node{"assistant": {Kind: agentdomain.NodeKindLLM, Instruction: "Answer.", ModelSlot: "primary", ToolSlots: []string{}, KnowledgeSlots: []string{}, AddSessionSummary: &yes}}
	in.Agent.Spec.Requirements.Tools = map[string]agentdomain.CapabilityRequirement{}
	in.Agent.Spec.Requirements.Knowledge = map[string]agentdomain.CapabilityRequirement{}
	in.Agent.Spec.Runtime = &agentdomain.Runtime{Summary: &agentdomain.Summary{Enabled: true, ModelSlot: "summarizer", EventThreshold: &threshold}}
	in.Agent.Spec.Requirements.Models["summarizer"] = agentdomain.ModelRequirement{Capabilities: []string{"chat"}}
	summary := in.Profile.Spec.Models["primary"]
	summary.Model = "summary-model"
	summary.APIKeyCredentialID = credentialReplacement
	in.Profile.Spec.Models["summarizer"] = summary
	in.Profile.Spec.Tools = map[string]profiledomain.ToolResource{}
	in.Profile.Spec.Knowledge = map[string]profiledomain.KnowledgeResource{}
	session := in.Profile.Spec.Storage["session"]
	session.Destination.Username = deploymentv1.WorkerV1SessionRuntimeRole
	in.Profile.Spec.Storage = map[string]profiledomain.StorageResource{"session": session}
	return in
}

func TestWorkerSummaryCompilationClosure(t *testing.T) {
	in := workerSummaryInput()
	compiled, report := Compile(in)
	if !report.Valid {
		t.Fatal(report.Diagnostics)
	}
	if len(compiled.Content.Resources.Models) != 2 || len(compiled.CredentialUses) != 3 || compiled.Content.ResolvedRequirements.Models["summarizer"] != "summarizer" || compiled.Content.Runtime.Summary.ModelResource != "summarizer" {
		t.Fatal("summary closure missing")
	}
	expected := CredentialUse{CredentialID: credentialReplacement, Purpose: CredentialPurposeAPIKey, AudienceDigest: deploymentv1.CredentialAudienceDigest("openai_compatible", in.Profile.Spec.Models["summarizer"].BaseURL)}
	found := false
	for _, use := range compiled.CredentialUses {
		if use == expected {
			found = true
		}
	}
	if !found {
		t.Fatal("summary credential missing from publication export", compiled.CredentialUses)
	}
	if _, err := VerifyManifestContent(compiled.CanonicalContent, compiled.ContentDigest); err != nil {
		t.Fatal(err)
	}
	wire, err := deploymentv1.DecodeManifestContent(compiled.CanonicalContent)
	if err != nil {
		t.Fatal(err)
	}
	if err = deploymentv1.ValidateWorkerV1(wire, in.Platform.Digest); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerSummaryReusesPrimaryModelWithoutExtraResource(t *testing.T) {
	in := workerSummaryInput()
	in.Agent.Spec.Runtime.Summary.ModelSlot = "primary"
	delete(in.Agent.Spec.Requirements.Models, "summarizer")
	// Unselected Profile resources must not leak into the fixed closure.
	n := in.Agent.Spec.Nodes["assistant"]
	n.AddSessionSummary = nil
	in.Agent.Spec.Nodes["assistant"] = n
	compiled, report := Compile(in)
	if !report.Valid {
		t.Fatal(report.Diagnostics)
	}
	if len(compiled.Content.Resources.Models) != 1 || len(compiled.CredentialUses) != 2 || compiled.Content.Runtime.Summary.ModelResource != "primary" || compiled.Content.AgentPlan.Nodes["assistant"].AddSessionSummary != nil {
		t.Fatal("shared model or explicit consumption contract changed")
	}
}

func TestWorkerSummaryRejectsUnexecutableClosure(t *testing.T) {
	cases := map[string]func(*CompileInput){
		"missing model": func(in *CompileInput) { delete(in.Profile.Spec.Models, "summarizer") },
		"non chat": func(in *CompileInput) {
			m := in.Profile.Spec.Models["summarizer"]
			m.Capabilities = []string{"embedding"}
			in.Profile.Spec.Models["summarizer"] = m
		},
		"missing session": func(in *CompileInput) { delete(in.Profile.Spec.Storage, "session") },
		"memory": func(in *CompileInput) {
			n := in.Agent.Spec.Nodes["assistant"]
			n.Memory = &agentdomain.Memory{Tools: []string{"memory_load"}}
			in.Agent.Spec.Nodes["assistant"] = n
		},
		"artifact": func(in *CompileInput) {
			n := in.Agent.Spec.Nodes["assistant"]
			n.Artifact = &agentdomain.Artifact{Enabled: true}
			in.Agent.Spec.Nodes["assistant"] = n
		},

		"knowledge": func(in *CompileInput) {
			n := in.Agent.Spec.Nodes["assistant"]
			n.KnowledgeSlots = []string{"docs"}
			in.Agent.Spec.Nodes["assistant"] = n
			in.Agent.Spec.Requirements.Knowledge["docs"] = validCompileInput().Agent.Spec.Requirements.Knowledge["docs"]
			in.Profile.Spec.Knowledge["docs"] = validCompileInput().Profile.Spec.Knowledge["docs"]
		},
		"managed session": func(in *CompileInput) {
			in.Profile.Spec.Storage["session"] = profiledomain.StorageResource{Kind: profiledomain.StorageKindManagedSession, BackendID: "pg", BackendRevision: 1}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			in := workerSummaryInput()
			mutate(&in)
			m, r := Compile(in)
			if r.Valid || len(m.CanonicalContent) != 0 {
				t.Fatal("unexecutable closure compiled", r)
			}
		})
	}
}

// Opt-in export contains compiler-produced immutable wire data, not credentials.
func TestExportWorkerSummaryFixture(t *testing.T) {
	in := workerSummaryInput()
	agentRaw, _ := json.Marshal(in.Agent.Spec)
	agentCanonical, ar := agentdomain.ValidateForPublication(agentRaw, 1)
	if !ar.Valid {
		t.Fatal(ar)
	}
	profileRaw, _ := json.Marshal(in.Profile.Spec)
	profileCanonical, pr := profiledomain.ValidateForPublication(profileRaw, 1)
	if !pr.Valid {
		t.Fatal(pr)
	}
	in.Agent.SpecDigest = agentCanonical.Digest
	in.Profile.SpecDigest = profileCanonical.Digest
	compiled, report := Compile(in)
	if !report.Valid {
		t.Fatal(report.Diagnostics)
	}
	envelope := RuntimeManifest{ID: "rmf_worker_summary", TenantID: in.TenantID, DeploymentID: "dpl_worker_summary", DeploymentRevisionID: "dpr_worker_summary", RevisionNumber: 1, PublishedAt: time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC), Content: compiled.CanonicalContent, ContentDigest: compiled.ContentDigest}
	raw, err := json.MarshalIndent(envelope, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := deploymentv1.DecodeRuntimeManifest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = deploymentv1.VerifyRuntimeManifest(decoded); err != nil {
		t.Fatal(err)
	}
	out := os.Getenv("CONTROL_WORKER_SUMMARY_FIXTURE_DIR")
	if out == "" {
		return
	}
	if !filepath.IsAbs(out) {
		t.Fatal("absolute export path required")
	}
	if err = os.MkdirAll(out, 0700); err != nil {
		t.Fatal(err)
	}
	platform, _ := json.MarshalIndent(in.Platform, "", "  ")
	uses, _ := json.MarshalIndent(compiled.CredentialUses, "", "  ")
	for name, data := range map[string][]byte{"runtime-manifest.json": raw, "content.canonical.json": compiled.CanonicalContent, "content.digest": []byte(compiled.ContentDigest + "\n"), "platform-contract.json": platform, "credential-uses.json": uses, "agent-source.canonical.json": agentCanonical.Document, "profile-source.canonical.json": profileCanonical.Document} {
		if err = os.WriteFile(filepath.Join(out, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("COMPILED_SUMMARY_FIXTURE=%s DIGEST=%s", out, compiled.ContentDigest)
}
