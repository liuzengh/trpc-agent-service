package gateway

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type gatewayQueue struct {
	accepted bool
	job      queue.AgentJob
}

func (q *gatewayQueue) Enqueue(_ context.Context, job queue.AgentJob) (queue.QueueReceipt, error) {
	q.job = job
	return queue.QueueReceipt{Accepted: q.accepted, ReceiptID: "receipt-1", JobID: job.JobID, Attempt: job.Attempt}, nil
}
func (*gatewayQueue) Receive(context.Context, time.Duration) (queue.Delivery, error) {
	return queue.Delivery{}, queue.ErrQueueClosed
}
func (*gatewayQueue) Ack(context.Context, queue.Delivery) error                     { return nil }
func (*gatewayQueue) Nack(context.Context, queue.Delivery, queue.NackOptions) error { return nil }
func (*gatewayQueue) ExtendVisibility(context.Context, queue.Delivery, time.Duration) error {
	return nil
}
func (*gatewayQueue) Close() error { return nil }

func gatewayRequest() GatewayRequest {
	tc := tenant.TenantContext{
		TenantID: "tenant-gateway", AgentAppID: "agent-gateway", BindingID: "binding-gateway", Channel: "web",
		SessionID: "session-gateway", RequestID: "request-gateway", MessageID: "message-gateway", TraceID: "trace-gateway",
		ConfigVersion: 3, BackendPolicy: tenant.BackendPolicy{Session: "memory", Memory: "memory", Vector: "none", Object: "none"},
	}
	return GatewayRequest{
		TenantContext: tc,
		Agent:         agent.AgentSpec{TenantID: tc.TenantID, AgentAppID: tc.AgentAppID, Version: tc.ConfigVersion, Name: "assistant", ModelProvider: "fake"},
		Input:         agent.Message{ID: tc.MessageID, Role: "user", Content: "hello", CreatedAt: time.Now().UTC()},
		Deadline:      time.Now().UTC().Add(time.Minute),
	}
}

func TestSubmitOnlyAcknowledgesAcceptedReceipt(t *testing.T) {
	q := &gatewayQueue{accepted: true}
	g, err := New(q)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := g.Submit(context.Background(), gatewayRequest())
	if err != nil || !accepted.Accepted {
		t.Fatalf("submit = %#v, %v", accepted, err)
	}
	if q.job.JobID == "" || q.job.ExecutionID == "" || q.job.Trace.ExecutionID != q.job.ExecutionID {
		t.Fatalf("job identifiers were not generated consistently: %#v", q.job)
	}
	if accepted.JobID != q.job.JobID || accepted.ExecutionID != q.job.ExecutionID || accepted.ReceiptID != "receipt-1" {
		t.Fatalf("accepted metadata = %#v", accepted)
	}
}

func TestSubmitPreservesBoundedHistorySnapshot(t *testing.T) {
	request := gatewayRequest()
	request.History = []agent.Message{
		{ID: "history-1", Role: "user", Content: "previous", CreatedAt: time.Now().UTC(), ToolCalls: []agent.ToolCall{{ID: "tool-1", Name: "lookup", Arguments: "{}"}}},
		{ID: "history-2", Role: "assistant", Content: "answer", CreatedAt: time.Now().UTC()},
	}
	q := &gatewayQueue{accepted: true}
	g, err := New(q)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Submit(context.Background(), request); err != nil {
		t.Fatalf("submit history: %v", err)
	}
	request.History[0].Content = "mutated"
	request.History[0].ToolCalls[0].Arguments = "mutated"
	if len(q.job.History) != 2 || q.job.History[0].Content != "previous" || q.job.History[0].ToolCalls[0].Arguments != "{}" || q.job.History[1].ID != "history-2" {
		t.Fatalf("history snapshot mismatch: schema=%d count=%d first=%s", q.job.SchemaVersion, len(q.job.History), q.job.History[0].ID)
	}
}

func TestSubmitRejectsOversizedHistoryBeforeEnqueue(t *testing.T) {
	request := gatewayRequest()
	request.History = []agent.Message{{ID: "history-large", Role: "user", Content: strings.Repeat("x", 300<<10), CreatedAt: time.Now().UTC()}}
	q := &gatewayQueue{accepted: true}
	g, err := New(q)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Submit(context.Background(), request); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected invalid history error: %v", err)
	}
	if q.job.JobID != "" {
		t.Fatalf("oversized history was enqueued: job=%s", q.job.JobID)
	}
}

func TestSubmitRejectsUnsupportedHistoryJobSchema(t *testing.T) {
	request := gatewayRequest()
	request.History = []agent.Message{{ID: "history-1", Role: "user", Content: "previous", CreatedAt: time.Now().UTC()}}
	q := &gatewayQueue{accepted: true}
	g, err := New(q)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Submit(context.Background(), request); err != nil {
		t.Fatalf("schema v2 history submit: %v", err)
	}
	if q.job.SchemaVersion != queue.SchemaVersion || len(q.job.History) != 1 {
		t.Fatalf("unexpected history schema: schema=%d history=%d", q.job.SchemaVersion, len(q.job.History))
	}
}

func TestSubmitRejectsUnacceptedReceipt(t *testing.T) {
	q := &gatewayQueue{}
	g, _ := New(q)
	_, err := g.Submit(context.Background(), gatewayRequest())
	if !errors.Is(err, ErrQueueRejected) {
		t.Fatalf("expected queue rejection, got %v", err)
	}
}

func TestSubmitRejectsCrossTenantAndDoesNotEnqueue(t *testing.T) {
	q := &gatewayQueue{accepted: true}
	g, _ := New(q)
	request := gatewayRequest()
	request.Agent.TenantID = "other"
	_, err := g.Submit(context.Background(), request)
	if !errors.Is(err, ErrInvalidRequest) || q.job.JobID != "" {
		t.Fatalf("expected pre-enqueue validation error, job=%#v err=%v", q.job, err)
	}
}

func TestSubmitCancellationDoesNotStartBackgroundSubmission(t *testing.T) {
	q := &gatewayQueue{accepted: true}
	g, _ := New(q)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := g.Submit(ctx, gatewayRequest())
	if !errors.Is(err, context.Canceled) || q.job.JobID != "" {
		t.Fatalf("expected cancellation before enqueue, job=%#v err=%v", q.job, err)
	}
}
