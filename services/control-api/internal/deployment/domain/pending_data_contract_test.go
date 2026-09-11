package domain

import (
	"encoding/json"
	agent "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	"os"
	"testing"
)

func TestLegacyManifestCanonicalAndDigestP0(t *testing.T) {
	raw, err := os.ReadFile("testdata/p0-legacy-canonical.json")
	if err != nil {
		t.Fatal(err)
	}
	var want struct {
		Canonical string `json:"canonical"`
		Digest    string `json:"digest"`
	}
	if err = json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	m, r := Compile(validCompileInput())
	if !r.Valid || string(m.CanonicalContent) != want.Canonical || m.ContentDigest != want.Digest {
		t.Fatal("legacy manifest bytes/digest changed", r)
	}
}
func TestPendingDataDeclarationsNeverDisappear(t *testing.T) {
	for _, name := range []string{"memory", "artifact", "runtime", "consume"} {
		t.Run(name, func(t *testing.T) {
			in := validCompileInput()
			n := in.Agent.Spec.Nodes[in.Agent.Spec.Root]
			switch name {
			case "memory":
				n.Memory = &agent.Memory{Tools: []string{"memory_load"}}
			case "artifact":
				n.Artifact = &agent.Artifact{Enabled: true}
			case "runtime":
				in.Agent.Spec.Runtime = &agent.Runtime{}
			case "consume":
				v := true
				n.AddSessionSummary = &v
			}
			in.Agent.Spec.Nodes[in.Agent.Spec.Root] = n
			m, r := Compile(in)
			if r.Valid || len(m.CanonicalContent) != 0 {
				t.Fatal("new declaration silently dropped")
			}
			found := false
			for _, d := range r.Diagnostics {
				if d.Code == DiagnosticEntrypointUnsupported {
					found = true
				}
			}
			if !found {
				t.Fatal(r)
			}
		})
	}
}

func TestStableAgentIdentityAcrossRevisionsP0(t *testing.T) {
	in := validCompileInput()
	first, report := Compile(in)
	if !report.Valid {
		t.Fatal(report)
	}
	in.Agent.VersionNumber++
	in.Agent.VersionID = "next-agent-version"
	in.Profile.RevisionNumber++
	in.Profile.RevisionID = "next-profile-revision"
	second, report := Compile(in)
	if !report.Valid {
		t.Fatal(report)
	}
	if first.Content.Sources.Agent.AgentID != second.Content.Sources.Agent.AgentID || second.Content.Sources.Agent.AgentID != in.Agent.AgentID {
		t.Fatal("revision changed stable Agent identity")
	}
	if first.ContentDigest == second.ContentDigest {
		t.Fatal("changed source revisions must change manifest digest")
	}
}
