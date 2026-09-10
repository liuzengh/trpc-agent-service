package upstream

import (
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/graph"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestAwaitUserReplyRoutePersistsInSessionState(t *testing.T) {
	route := agent.AwaitUserReplyRoute{AgentName: "approval-agent", LookupPath: "root/approval-agent"}
	state, err := route.State()
	if err != nil {
		t.Fatal(err)
	}
	sess := session.NewSession("app", "user", "session")
	for key, value := range state {
		sess.SetState(key, value)
	}
	actual, ok, err := agent.PendingAwaitUserReplyRoute(sess)
	if err != nil || !ok || actual != route {
		t.Fatalf("route=%#v ok=%t err=%v", actual, ok, err)
	}
}

func TestGraphAndLLMContinuationValuesRemainAddressable(t *testing.T) {
	command := graph.NewResumeCommand().AddResumeValue("approval-task", map[string]any{"approved": true})
	if command.ResumeMap["approval-task"] == nil {
		t.Fatalf("resume command=%#v", command)
	}
	message := model.NewToolMessage("call-1", "approve", `{"approved":true}`)
	if message.ToolID != "call-1" || message.ToolName != "approve" {
		t.Fatalf("tool continuation=%#v", message)
	}
}
