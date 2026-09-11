package runtimeadapter

import (
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"testing"
)

func TestWorkspacePlanNodeAuthorityAndClone(t *testing.T) {
	_, p := sequenceRuntimeFixture()
	p.Executors = map[string]domain.ExecutorPlan{"shell": {Kind: "sdk_sandbox", AdapterVersion: "sdk-sandbox-v1"}}
	n := p.Nodes["first"]
	n.Workspace = &domain.WorkspacePlan{ExecutorResource: "shell", Tools: []string{"workspace_exec"}}
	p.Nodes["first"] = n
	if err := validateSequencePlan(p); err != nil {
		t.Fatal(err)
	}
	cloned := cloneSequencePlan(p)
	n.Workspace.Tools[0] = "workspace_save_artifact"
	if err := validateSequencePlan(p); err == nil {
		t.Fatal("save without explicit same-node artifact accepted")
	}
	if cloned.Nodes["first"].Workspace.Tools[0] != "workspace_exec" || cloned.Nodes["last"].Workspace != nil {
		t.Fatal("sibling inherited workspace or mutable plan alias")
	}
	n.Workspace.Tools[0] = "workspace_exec"
	p.Executors["unused"] = p.Executors["shell"]
	if err := validateSequencePlan(p); err == nil {
		t.Fatal("unused executor resource authorized")
	}
	delete(p.Executors, "unused")
	p.Executors["shell"] = domain.ExecutorPlan{Kind: "localexec", AdapterVersion: "sdk-sandbox-v1"}
	if err := validateSequencePlan(p); err == nil {
		t.Fatal("unapproved executor accepted")
	}
	if cloned.Executors["shell"].Kind != "sdk_sandbox" {
		t.Fatal("executor map alias")
	}
}
