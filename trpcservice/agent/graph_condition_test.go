package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent/condition"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"trpc.group/trpc-go/trpc-agent-go/graph"
)

func TestCompileGraphConditionsMapsFiniteBranchesAtRuntime(t *testing.T) {
	ref := &agentapp.VersionedRef{ID: condition.LastResponsePresence, Version: condition.Version}
	compiled, err := compileGraphConditions(context.Background(), "tenant-a", []agentapp.AgentEdgeSpecV1{
		{From: "decide", To: "answer", ConditionRef: ref, Branch: condition.BranchPresent},
		{From: "decide", To: "fallback", ConditionRef: ref, Branch: condition.BranchEmpty},
	}, condition.DefaultRegistry())
	if err != nil || len(compiled) != 1 || compiled[0].pathMap[condition.BranchPresent] != "answer" || compiled[0].pathMap[condition.BranchEmpty] != "fallback" {
		t.Fatalf("compiled=%#v err=%v", compiled, err)
	}
	branch, err := compiled[0].condition.Select(context.Background(), graph.State{graph.StateKeyLastResponse: "done"})
	if err != nil || branch != condition.BranchPresent {
		t.Fatalf("present branch=%q err=%v", branch, err)
	}
	branch, err = compiled[0].condition.Select(context.Background(), graph.State{})
	if err != nil || branch != condition.BranchEmpty {
		t.Fatalf("empty branch=%q err=%v", branch, err)
	}
}

func TestCompileGraphConditionsRejectsIncompleteAndMixedEdges(t *testing.T) {
	ref := &agentapp.VersionedRef{ID: condition.LastResponsePresence, Version: condition.Version}
	if _, err := compileGraphConditions(context.Background(), "tenant-a", []agentapp.AgentEdgeSpecV1{
		{From: "decide", To: "answer", ConditionRef: ref, Branch: condition.BranchPresent},
	}, condition.DefaultRegistry()); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("incomplete branch map error=%v", err)
	}
	if _, err := compileGraphConditions(context.Background(), "tenant-a", []agentapp.AgentEdgeSpecV1{
		{From: "decide", To: "answer"},
		{From: "decide", To: "fallback", ConditionRef: ref, Branch: condition.BranchEmpty},
	}, condition.DefaultRegistry()); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("mixed edge modes error=%v", err)
	}
}
