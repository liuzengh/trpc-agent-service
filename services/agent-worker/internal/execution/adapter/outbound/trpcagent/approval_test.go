package trpcagent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	approvalv1 "github.com/liuzengh/trpc-agent-service/api/runtime/approval/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/toolapproval"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

type approvalFake struct {
	proposal  toolapproval.Proposal
	deny      bool
	finished  int
	finishErr error
	proposed  chan struct{}
	allow     chan struct{}
	once      sync.Once
}

func (f *approvalFake) Propose(_ context.Context, p toolapproval.Proposal) (approvalv1.Operation, error) {
	f.proposal = p
	if f.proposed != nil {
		f.once.Do(func() { close(f.proposed) })
	}
	return approvalv1.Operation{OperationID: "tap_fixed", TenantID: p.TenantID}, nil
}
func (f *approvalFake) AwaitAndBegin(context.Context, string, string, time.Duration) (approvalv1.Operation, []byte, error) {
	if f.deny {
		return approvalv1.Operation{Status: approvalv1.StatusRejected}, nil, toolapproval.ErrDenied
	}
	if f.allow != nil {
		<-f.allow
	}
	return approvalv1.Operation{Status: approvalv1.StatusExecuting}, nil, nil
}

type approvalTicketTool struct{ calls atomic.Int32 }

func (t *approvalTicketTool) Declaration() *tool.Declaration {
	return &tool.Declaration{Name: "update_ticket", InputSchema: &tool.Schema{Type: "object", Properties: map[string]*tool.Schema{"ticket_id": {Type: "string"}, "status": {Type: "string"}}, Required: []string{"ticket_id", "status"}, AdditionalProperties: false}}
}
func (t *approvalTicketTool) Call(context.Context, []byte) (any, error) {
	t.calls.Add(1)
	return map[string]any{"updated": true}, nil
}
func (f *approvalFake) Finish(_ context.Context, tenant, id string, result []byte, err error) error {
	f.finished++
	f.finishErr = err
	if tenant != f.proposal.TenantID || id != "tap_fixed" {
		return errors.New("identity")
	}
	return nil
}

func approvalCallbackFixture(store ApprovalStore) *approvalState {
	return &approvalState{store: store, tenantID: "tenant", runID: "run", attemptID: "attempt", nodeID: "assistant", tools: map[string]approvalTool{"mcp_ticket": {resource: "ticket", capability: approvalCapability}}}
}
func approvalContextFixture() context.Context {
	return agent.NewInvocationContext(context.Background(), &agent.Invocation{InvocationID: "invocation", AgentName: "assistant"})
}

func TestApprovalBindsExactToolCallAndFinishes(t *testing.T) {
	store := &approvalFake{}
	state := approvalCallbackFixture(store)
	before, err := state.before(approvalContextFixture(), &tool.BeforeToolArgs{ToolCallID: "call-1", ToolName: "mcp_ticket", Arguments: []byte(`{"ticket_id":"T-7","status":"resolved"}`)})
	if err != nil || before == nil || before.Context == nil || before.CustomResult != nil {
		t.Fatal(before, err)
	}
	if store.proposal.TenantID != "tenant" || store.proposal.AttemptID != "attempt" || store.proposal.ToolCallID != "call-1" || store.proposal.Capability != approvalCapability {
		t.Fatalf("proposal %#v", store.proposal)
	}
	if err = state.after(before.Context, &tool.AfterToolArgs{ToolCallID: "call-1", ToolName: "mcp_ticket", Result: map[string]any{"updated": true}}); err != nil || store.finished != 1 || store.finishErr != nil {
		t.Fatal(err, store.finished, store.finishErr)
	}
}
func TestRejectedApprovalSkipsToolExecution(t *testing.T) {
	store := &approvalFake{deny: true}
	state := approvalCallbackFixture(store)
	before, err := state.before(approvalContextFixture(), &tool.BeforeToolArgs{ToolCallID: "call-1", ToolName: "mcp_ticket", Arguments: []byte(`{"ticket_id":"T-7","status":"closed"}`)})
	if err != nil || before == nil || before.CustomResult == nil || before.Context != nil {
		t.Fatal(before, err)
	}
}
func TestUnknownToolResultFailsAttemptWithoutRetrySignal(t *testing.T) {
	store := &approvalFake{}
	state := approvalCallbackFixture(store)
	before, _ := state.before(approvalContextFixture(), &tool.BeforeToolArgs{ToolCallID: "call-1", ToolName: "mcp_ticket", Arguments: []byte(`{"ticket_id":"T-7","status":"closed"}`)})
	providerErr := errors.New("response lost")
	if err := state.after(before.Context, &tool.AfterToolArgs{ToolCallID: "call-1", ToolName: "mcp_ticket", Error: providerErr}); !errors.Is(err, ErrApproval) || !errors.Is(store.finishErr, providerErr) {
		t.Fatal(err, store.finishErr)
	}
}

func TestRealSDKWaitsForApprovalAndExecutesExactToolOnce(t *testing.T) {
	sum := sha256.Sum256([]byte("tools/ticket"))
	providerName := "fn_" + hex.EncodeToString(sum[:])[:60]
	fixture, server := newMemoryHTTPFixture(t, []string{providerName},
		memoryHTTPRound{tool: providerName, args: `{"ticket_id":"TEST-42","status":"resolved"}`},
		memoryHTTPRound{final: "ticket updated"})
	remote := &approvalTicketTool{}
	store := &approvalFake{proposed: make(chan struct{}), allow: make(chan struct{})}
	executor := testExecutor()
	executor.Approvals = store
	req := testRequest(server.URL)
	req.MaxToolCalls = 2
	req.Tools = []MCPToolConfig{{Resource: "ticket", Capability: approvalCapability, Tool: remote}}
	type outcome struct {
		result Result
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := executor.Execute(context.Background(), req)
		done <- outcome{result: result, err: err}
	}()
	select {
	case <-store.proposed:
	case <-time.After(5 * time.Second):
		t.Fatal("SDK did not propose approval")
	}
	if remote.calls.Load() != 0 {
		t.Fatal("tool executed before approval")
	}
	close(store.allow)
	select {
	case got := <-done:
		if got.err != nil || got.result.FinalText != "ticket updated" || remote.calls.Load() != 1 || store.finished != 1 {
			t.Fatal(got.result, got.err, remote.calls.Load(), store.finished, store.finishErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SDK did not finish after approval")
	}
	if store.proposal.Arguments == nil || string(store.proposal.Arguments) != `{"ticket_id":"TEST-42","status":"resolved"}` {
		t.Fatal("approval did not bind model arguments", string(store.proposal.Arguments))
	}
	fixture.check(t, 2)
}
