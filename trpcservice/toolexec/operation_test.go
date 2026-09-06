package toolexec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
)

type faultyProvider struct {
	OperationProvider
	loseResponse bool
	missing      bool
	unsafe       bool
	calls        atomic.Int32
}

func (p *faultyProvider) Idempotent() bool { return !p.unsafe }
func (p *faultyProvider) Execute(ctx context.Context, op Operation, payload json.RawMessage) (json.RawMessage, error) {
	p.calls.Add(1)
	if p.missing {
		return nil, context.DeadlineExceeded
	}
	result, err := p.OperationProvider.Execute(ctx, op, payload)
	if p.loseResponse && err == nil {
		return nil, context.DeadlineExceeded
	}
	return result, err
}
func (p *faultyProvider) Lookup(ctx context.Context, op Operation) (json.RawMessage, bool, error) {
	if p.missing {
		return nil, false, nil
	}
	return p.OperationProvider.Lookup(ctx, op)
}

type failFinishStore struct {
	OperationStore
	failed atomic.Bool
}

func (s *failFinishStore) Finish(ctx context.Context, tenantID, id, status string, result json.RawMessage, errorType string) (Operation, error) {
	if s.failed.CompareAndSwap(false, true) {
		return Operation{}, errors.New("private-database-error-canary")
	}
	return s.OperationStore.Finish(ctx, tenantID, id, status, result, errorType)
}

func operationInput(t *testing.T, journal Journal, requestID, key, title string) OperationInput {
	t.Helper()
	payload, _ := json.Marshal(WorkItemPayload{Title: title})
	exec := Execution{TenantID: "tutorial-tenant", RequestID: requestID, RevisionID: "tutorial-revision-1", ToolCallID: "call-1", ToolName: WorkItemTool, ArgumentsHash: Hash(payload)}
	started, err := journal.Start(context.Background(), exec)
	if err != nil {
		t.Fatal(err)
	}
	return OperationInput{TenantID: exec.TenantID, AppID: "tutorial-app", UserID: "alice", ToolName: WorkItemTool,
		BusinessKey: key, Payload: payload, RequestID: requestID, ExecutionID: started.Execution.ID}
}

func testOperationsContract(t *testing.T, store OperationStore, journal Journal, backend OperationProvider) {
	t.Helper()
	ctx := context.Background()
	writer := audit.NewMemoryWriter()
	service, err := NewOperations(store, journal, writer, backend)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("concurrent_retry_new_call_ids", func(t *testing.T) {
		const count = 16
		inputs := make([]OperationInput, count)
		for i := range inputs {
			inputs[i] = operationInput(t, journal, fmt.Sprintf("request-%d", i), "case-001", "one work item")
		}
		var group sync.WaitGroup
		results := make(chan Operation, count)
		for _, input := range inputs {
			group.Add(1)
			go func(input OperationInput) {
				defer group.Done()
				op, err := service.Execute(ctx, input)
				if err != nil {
					t.Error(err)
				}
				results <- op
			}(input)
		}
		group.Wait()
		close(results)
		id := ""
		for op := range results {
			if id == "" {
				id = op.ID
			}
			if op.ID != id || op.Status != StatusSucceeded {
				t.Fatalf("concurrent outcome=%+v", op)
			}
		}
		for _, input := range inputs {
			execution, err := journal.Get(ctx, input.TenantID, input.ExecutionID)
			if err != nil || execution.OperationID != id || execution.Status != StatusSucceeded {
				t.Fatalf("execution=%+v err=%v", execution, err)
			}
		}
		items, err := service.List(ctx, "tutorial-tenant", "", "", 100)
		if err != nil || len(items) != 1 {
			t.Fatalf("operations=%d err=%v", len(items), err)
		}
		conflict := operationInput(t, journal, "conflict-request", "case-001", "DIFFERENT content")
		if _, err := service.Execute(ctx, conflict); !errors.Is(err, ErrOperationConflict) {
			t.Fatalf("conflict=%v", err)
		}
		if _, err := service.Get(ctx, "foreign-tenant", id); !errors.Is(err, ErrNotFound) {
			t.Fatalf("cross-tenant query=%v", err)
		}
		if _, err := service.Reconcile(ctx, "foreign-tenant", id); !errors.Is(err, ErrNotFound) {
			t.Fatalf("cross-tenant reconcile=%v", err)
		}
		otherUser := operationInput(t, journal, "bob-request", "case-001", "one work item")
		otherUser.UserID = "bob"
		bob, err := service.Execute(ctx, otherUser)
		if err != nil || bob.ID == id {
			t.Fatalf("user isolation=%+v %v", bob, err)
		}
	})
	t.Run("lost_response_reconciles_without_reexecution", func(t *testing.T) {
		provider := &faultyProvider{OperationProvider: backend, loseResponse: true}
		service, _ := NewOperations(store, journal, writer, provider)
		input := operationInput(t, journal, "response-lost", "case-002", "committed work")
		op, err := service.Execute(ctx, input)
		if !errors.Is(err, ErrOperationUnknown) || op.Status != StatusUnknown {
			t.Fatalf("uncertain=%+v %v", op, err)
		}
		op, err = service.Reconcile(ctx, op.TenantID, op.ID)
		if err != nil || op.Status != StatusSucceeded || provider.calls.Load() != 1 {
			t.Fatalf("reconcile=%+v %v calls=%d", op, err, provider.calls.Load())
		}
		exec, _ := journal.Get(ctx, input.TenantID, input.ExecutionID)
		if exec.Status != StatusSucceeded {
			t.Fatalf("tool journal not repaired: %+v", exec)
		}
		late, err := store.Finish(ctx, op.TenantID, op.ID, StatusUnknown, nil, "timeout")
		if err != nil || late.Status != StatusSucceeded {
			t.Fatal("late timeout overwrote committed outcome")
		}
	})
	t.Run("platform_commit_lost_restart_and_reconcile", func(t *testing.T) {
		failureStore := &failFinishStore{OperationStore: store}
		provider := &faultyProvider{OperationProvider: backend}
		beforeCrash, _ := NewOperations(failureStore, journal, writer, provider)
		input := operationInput(t, journal, "platform-write-lost", "case-003", "committed before journal")
		op, err := beforeCrash.Execute(ctx, input)
		if !errors.Is(err, ErrOperationUnknown) || strings.Contains(err.Error(), "canary") {
			t.Fatalf("write failure: %v", err)
		}
		afterRestart, _ := NewOperations(store, journal, writer, provider)
		op, err = afterRestart.Reconcile(ctx, op.TenantID, op.ID)
		if err != nil || op.Status != StatusSucceeded || provider.calls.Load() != 1 {
			t.Fatalf("restart reconciliation: %+v %v", op, err)
		}
	})
	t.Run("non_idempotent_not_found_does_not_authorize_retry", func(t *testing.T) {
		provider := &faultyProvider{OperationProvider: backend, missing: true, unsafe: true}
		service, _ := NewOperations(store, journal, writer, provider)
		input := operationInput(t, journal, "unsafe-first", "case-004", "never blindly repeat")
		op, err := service.Execute(ctx, input)
		if !errors.Is(err, ErrOperationUnknown) {
			t.Fatalf("first: %v", err)
		}
		retry := operationInput(t, journal, "unsafe-retry", "case-004", "never blindly repeat")
		if _, err := service.Execute(ctx, retry); !errors.Is(err, ErrOperationUnknown) {
			t.Fatalf("retry: %v", err)
		}
		if _, err := service.Reconcile(ctx, op.TenantID, op.ID); !errors.Is(err, ErrOperationUnknown) {
			t.Fatalf("reconcile: %v", err)
		}
		if provider.calls.Load() != 1 {
			t.Fatal("unsafe provider executed more than once")
		}
	})
	for _, event := range writer.Events() {
		encoded, _ := json.Marshal(event)
		if strings.Contains(string(encoded), "one work item") || strings.Contains(string(encoded), "committed work") || strings.Contains(string(encoded), "case-001") {
			t.Fatal("business payload/key leaked to audit")
		}
	}
}

func TestMemoryBusinessOperations(t *testing.T) {
	testOperationsContract(t, NewMemoryOperations(), NewMemoryJournal(), NewWorkItems(nil))
}

func TestOperationCanonicalJSON(t *testing.T) {
	got, err := canonicalJSON([]byte(`{ "id": 9007199254740993, "name":"test" }`))
	if err != nil || string(got) != `{"id":9007199254740993,"name":"test"}` {
		t.Fatalf("canonical=%s %v", got, err)
	}
	if _, err := canonicalJSON([]byte(`{} {}`)); err == nil {
		t.Fatal("multiple JSON values accepted")
	}
}
