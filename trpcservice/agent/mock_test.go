package agent

import (
	"context"
	"fmt"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestToolsPolicyAndCalculator(t *testing.T) {
	selected, names, err := Tools([]string{"echo", "calculator"}, []string{"echo"})
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 1 || len(names) != 1 || names[0] != "calculator" {
		t.Fatalf("tools=%v names=%v", selected, names)
	}
	callable, ok := selected[0].(tool.CallableTool)
	if !ok {
		t.Fatal("calculator is not callable")
	}
	result, err := callable.Call(context.Background(), []byte(`{"a":6,"b":7,"operation":"multiply"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(result); got != "{42}" {
		t.Fatalf("calculator result=%s", got)
	}
}

func TestToolsRejectUnknown(t *testing.T) {
	if _, _, err := Tools([]string{"missing"}, nil); err == nil {
		t.Fatal("unknown tool accepted")
	}
}
