package domain_test

import (
	"bytes"
	"encoding/json"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	"testing"
)

const workspaceAgent = `{"schema_version":"v1","root":"main","requirements":{"models":{"primary":{"capabilities":["chat","tool_call"]}},"tools":{},"knowledge":{},"executors":{"shell":{"capability":"workspace"}}},"nodes":{"main":{"kind":"llm","instruction":"Use tools.","model_slot":"primary","tool_slots":[],"knowledge_slots":[],"workspace":{"executor_slot":"shell","tools":["workspace_exec"]}}}}`

func TestWorkspaceAgentContract(t *testing.T) {
	c, report := domain.ValidateForPublication([]byte(workspaceAgent), 1)
	if !report.Valid {
		t.Fatal(report.Diagnostics)
	}
	var doc any
	json.Unmarshal(c.Document, &doc)
	if err := compilePublicSchema(t).Validate(doc); err != nil {
		t.Fatal(err)
	}
	var s domain.Spec
	if err := json.Unmarshal(c.Document, &s); err != nil || s.Nodes["main"].Workspace.ExecutorSlot != "shell" {
		t.Fatal(err, s)
	}
	cases := map[string]string{"empty": `"tools":[]`, "duplicate": `"tools":["workspace_exec","workspace_exec"]`, "unknown": `"tools":["host_exec"]`, "save without artifact": `"tools":["workspace_save_artifact"]`}
	for name, replacement := range cases {
		t.Run(name, func(t *testing.T) {
			b := bytes.Replace([]byte(workspaceAgent), []byte(`"tools":["workspace_exec"]`), []byte(replacement), 1)
			_, report := domain.ValidateForPublication(b, 1)
			if report.Valid {
				t.Fatal("accepted", name)
			}
		})
	}
	for _, pair := range [][2]string{{`"executor_slot":"shell"`, `"executor_slot":"missing"`}, {`"capability":"workspace"`, `"capability":"shell"`}, {`"executor_slot":"shell"`, `"executor_slot":"shell","path":"/tmp"`}} {
		b := bytes.Replace([]byte(workspaceAgent), []byte(pair[0]), []byte(pair[1]), 1)
		_, report := domain.ValidateForPublication(b, 1)
		if report.Valid {
			t.Fatal("accepted", pair)
		}
	}
	b := bytes.Replace([]byte(workspaceAgent), []byte(`"workspace_exec"`), []byte(`"workspace_save_artifact"`), 1)
	b = bytes.Replace(b, []byte(`"workspace":`), []byte(`"artifact":{"enabled":true},"workspace":`), 1)
	if _, report := domain.ValidateForPublication(b, 1); !report.Valid {
		t.Fatal(report.Diagnostics)
	}
}
