package assembly

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type auditProbeModel struct {
	responses <-chan *model.Response
	err       error
	calls     int
}

func (m *auditProbeModel) GenerateContent(context.Context, *model.Request) (<-chan *model.Response, error) {
	m.calls++
	return m.responses, m.err
}

func (*auditProbeModel) Info() model.Info { return model.Info{Name: "audit-probe"} }

type auditIterProbeModel struct {
	auditProbeModel
	iterCalls int
	iterErr   error
}

func (m *auditIterProbeModel) GenerateContentIter(context.Context, *model.Request) (model.Seq[*model.Response], error) {
	m.iterCalls++
	if m.iterErr != nil {
		return nil, m.iterErr
	}
	return func(yield func(*model.Response) bool) {
		yield(&model.Response{Done: true})
	}, nil
}

func auditInvocationContext(parent context.Context) context.Context {
	return governance.WithInvocation(parent, governance.Invocation{
		Execution: governance.ExecutionContext{
			TenantID: "tenant-a", AppCode: "support", Role: "member", TraceID: "trace-audit", RequestID: "request-audit",
			Channel: "web", BindingID: "web-console", AgentName: "assistant", SessionID: "tenant-a/support/session/1", PolicyVersion: "1",
		},
		Budget: governance.NewCallBudget(2),
	})
}

func TestAuditedModelBypassesAuditOutsideInvocationAndPreservesInfo(t *testing.T) {
	responses := make(chan *model.Response)
	close(responses)
	inner := &auditProbeModel{responses: responses}
	audits := storage.NewMemoryStateStore()
	wrapped := newAuditedModel(inner, audits)
	if wrapped.Info().Name != "audit-probe" {
		t.Fatalf("Info() = %#v", wrapped.Info())
	}
	stream, err := wrapped.GenerateContent(context.Background(), &model.Request{})
	if err != nil {
		t.Fatal(err)
	}
	for range stream {
	}
	if inner.calls != 1 {
		t.Fatalf("provider calls = %d", inner.calls)
	}
	events, err := audits.ListAudit(context.Background(), "tenant-a", "")
	if err != nil || len(events) != 0 {
		t.Fatalf("unexpected audit outside invocation = %#v, %v", events, err)
	}
	if got := newAuditedModel(inner, nil); got != inner {
		t.Fatal("nil audit recorder unexpectedly wrapped model")
	}
	if got := newAuditedModel(nil, audits); got != nil {
		t.Fatalf("nil model wrapper = %#v", got)
	}
}

func TestAuditedModelRecordsProviderFailureWithoutContent(t *testing.T) {
	wantErr := errors.New("provider unavailable")
	inner := &auditProbeModel{err: wantErr}
	audits := storage.NewMemoryStateStore()
	wrapped := newAuditedModel(inner, audits)
	request := &model.Request{Messages: []model.Message{model.NewUserMessage("private prompt must not be audited")}}
	if _, err := wrapped.GenerateContent(auditInvocationContext(context.Background()), request); !errors.Is(err, wantErr) {
		t.Fatalf("GenerateContent() error = %v", err)
	}
	events, err := audits.ListAudit(context.Background(), "tenant-a", "trace-audit")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Action != "model.requested" || events[0].Result != "running" || events[1].Action != "model.failed" || events[1].ErrorType != "provider_error" {
		t.Fatalf("provider failure audits = %+v", events)
	}
	for _, event := range events {
		if strings.Contains(event.Detail, "private prompt") || event.RequestID != "request-audit" || event.SessionID != "tenant-a/support/session/1" {
			t.Fatalf("audit leaked content or lost identity: %+v", event)
		}
	}
}

func TestAuditedModelRecordsCancellationAfterProviderStreamCloses(t *testing.T) {
	responses := make(chan *model.Response)
	inner := &auditProbeModel{responses: responses}
	audits := storage.NewMemoryStateStore()
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := newAuditedModel(inner, audits).GenerateContent(auditInvocationContext(ctx), &model.Request{})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	close(responses)
	for range stream {
	}
	events, err := audits.ListAudit(context.Background(), "tenant-a", "trace-audit")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[1].Action != "model.failed" || events[1].ErrorType != "context_canceled" {
		t.Fatalf("canceled audits = %+v", events)
	}
}

func TestAuditedModelCancellationUnblocksUndeliveredResponse(t *testing.T) {
	responses := make(chan *model.Response)
	inner := &auditProbeModel{responses: responses}
	audits := storage.NewMemoryStateStore()
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := newAuditedModel(inner, audits).GenerateContent(auditInvocationContext(ctx), &model.Request{})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	sent := make(chan struct{})
	go func() {
		responses <- &model.Response{Done: true}
		close(responses)
		close(sent)
	}()
	for range stream {
	}
	<-sent
	events, err := audits.ListAudit(context.Background(), "tenant-a", "trace-audit")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[1].Action != "model.failed" || events[1].ErrorType != "context_canceled" {
		t.Fatalf("blocked response cancellation audits = %+v", events)
	}
}

func TestAuditedModelPreservesIterModelAndAuditsCompletion(t *testing.T) {
	inner := &auditIterProbeModel{}
	audits := storage.NewMemoryStateStore()
	wrapped := newAuditedModel(inner, audits)
	iter, ok := wrapped.(model.IterModel)
	if !ok {
		t.Fatal("audit wrapper hid model.IterModel")
	}
	sequence, err := iter.GenerateContentIter(auditInvocationContext(context.Background()), &model.Request{})
	if err != nil {
		t.Fatal(err)
	}
	responses := 0
	sequence(func(*model.Response) bool {
		responses++
		return true
	})
	if inner.iterCalls != 1 || responses != 1 {
		t.Fatalf("iter calls/responses = %d/%d, want 1/1", inner.iterCalls, responses)
	}
	events, err := audits.ListAudit(context.Background(), "tenant-a", "trace-audit")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Action != "model.requested" || events[1].Action != "model.completed" {
		t.Fatalf("iter audit events = %+v", events)
	}
}

func TestAuditedModelIterRecordsProviderFailure(t *testing.T) {
	wantErr := errors.New("iter provider unavailable")
	inner := &auditIterProbeModel{iterErr: wantErr}
	audits := storage.NewMemoryStateStore()
	iter := newAuditedModel(inner, audits).(model.IterModel)
	if _, err := iter.GenerateContentIter(auditInvocationContext(context.Background()), &model.Request{}); !errors.Is(err, wantErr) {
		t.Fatalf("GenerateContentIter() error = %v, want %v", err, wantErr)
	}
	events, err := audits.ListAudit(context.Background(), "tenant-a", "trace-audit")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[1].Action != "model.failed" || events[1].ErrorType != "provider_error" {
		t.Fatalf("iter failure audit events = %+v", events)
	}
}
