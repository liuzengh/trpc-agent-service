package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func validTestJob(clock *testClock) AgentJob {
	tc := tenant.TenantContext{
		TenantID: "tenant-a", AgentAppID: "agent-a", BindingID: "binding-a", Channel: "web",
		ExternalUser: "user-a", ExternalChat: "chat-a", SessionID: "session-a",
		RequestID: "request-a", MessageID: "message-a", TraceID: "trace-a", ConfigVersion: 7,
		Permissions: []string{"agent.run"}, BackendPolicy: tenant.BackendPolicy{Session: "memory", Memory: "memory", Vector: "none", Object: "memory"},
	}
	return AgentJob{
		SchemaVersion: SchemaVersion, JobID: "job-a", ExecutionID: "execution-a",
		Tenant:    TenantContextDTOFromContext(tc),
		Agent:     AgentRefDTO{TenantID: "tenant-a", AgentAppID: "agent-a", Version: 3},
		Message:   MessageDTO{ID: "message-a", Role: "user", Content: "hello", CreatedAt: clock.Now()},
		Trace:     TraceContextDTO{TraceID: "trace-a", RequestID: "request-a", MessageID: "message-a", ExecutionID: "execution-a"},
		CreatedAt: clock.Now(), Deadline: clock.Now().Add(time.Hour), Attempt: 1,
	}
}

func newTestQueue() (*FakeQueue, *testClock) {
	clock := &testClock{now: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	return NewFakeQueue(FakeQueueConfig{Now: clock.Now, MaxJobAge: 2 * time.Hour}), clock
}

func TestAgentJobJSONRoundTripAndDTORestore(t *testing.T) {
	q, clock := newTestQueue()
	job := validTestJob(clock)
	if err := job.ValidateAt(clock.Now(), 2*time.Hour); err != nil {
		t.Fatalf("validate job: %v", err)
	}
	envelope, err := EncodeJobAt(job, clock.Now(), 2*time.Hour)
	if err != nil {
		t.Fatalf("encode job: %v", err)
	}
	decoded, err := DecodeJobAt(envelope, clock.Now(), 2*time.Hour)
	if err != nil {
		t.Fatalf("decode job: %v", err)
	}
	if decoded.JobID != job.JobID || decoded.ExecutionID != job.ExecutionID || decoded.SchemaVersion != SchemaVersion {
		t.Fatalf("round trip changed identity: %#v", decoded)
	}
	if !reflect.DeepEqual(decoded.Tenant, job.Tenant) || !reflect.DeepEqual(decoded.Agent, job.Agent) || !reflect.DeepEqual(decoded.History, job.History) || !reflect.DeepEqual(decoded.Trace, job.Trace) || !reflect.DeepEqual(decoded.Message, job.Message) {
		t.Fatalf("round trip changed DTOs: schema=%d history=%d message=%s", decoded.SchemaVersion, len(decoded.History), decoded.Message.ID)
	}
	restored, err := decoded.Tenant.Restore()
	if err != nil {
		t.Fatalf("restore tenant context: %v", err)
	}
	if restored.TenantID != "tenant-a" || restored.SessionID != "session-a" || !restored.HasPermission("agent.run") {
		t.Fatalf("restored context incomplete: %#v", restored)
	}

	body, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	serialized := string(body)
	for _, forbidden := range []string{"context.Context", "secret", "token", "password", "authorization", "runner", "provider"} {
		if strings.Contains(strings.ToLower(serialized), forbidden) {
			t.Fatalf("serialized job contains forbidden term %q: %s", forbidden, serialized)
		}
	}
	if q == nil {
		t.Fatal("test queue was not constructed")
	}
}

func TestAgentJobHistoryLimitsAndSchemaVersion(t *testing.T) {
	_, clock := newTestQueue()
	job := validTestJob(clock)
	job.History = make([]MessageDTO, maxHistoryMessages+1)
	for i := range job.History {
		job.History[i] = MessageDTO{ID: fmt.Sprintf("history-%d", i), Role: "user", Content: "previous", CreatedAt: clock.Now()}
	}
	if err := job.ValidateAt(clock.Now(), 2*time.Hour); !errors.Is(err, ErrInvalidJob) {
		t.Fatalf("expected history count rejection: %v", err)
	}
	job.History = []MessageDTO{{ID: "history-large", Role: "user", Content: strings.Repeat("x", maxHistoryBytes), CreatedAt: clock.Now()}}
	if err := job.ValidateAt(clock.Now(), 2*time.Hour); !errors.Is(err, ErrInvalidJob) {
		t.Fatalf("expected history size rejection: %v", err)
	}
	job = validTestJob(clock)
	job.SchemaVersion = 1
	if err := job.ValidateAt(clock.Now(), 2*time.Hour); !errors.Is(err, ErrInvalidJob) {
		t.Fatalf("expected old schema rejection: %v", err)
	}
}

func TestAgentJobHistoryIsCopiedByJSONEnvelope(t *testing.T) {
	_, clock := newTestQueue()
	job := validTestJob(clock)
	job.History = []MessageDTO{{ID: "history-1", Role: "user", Content: "before", CreatedAt: clock.Now(), ToolCalls: []ToolCallDTO{{ID: "tool-1", Name: "lookup", Arguments: "{}"}}}}
	envelope, err := EncodeJobAt(job, clock.Now(), 2*time.Hour)
	if err != nil {
		t.Fatalf("encode history job: %v", err)
	}
	job.History[0].Content = "after"
	job.History[0].ToolCalls[0].Arguments = "changed"
	decoded, err := DecodeJobAt(envelope, clock.Now(), 2*time.Hour)
	if err != nil {
		t.Fatalf("decode history job: %v", err)
	}
	if decoded.History[0].Content != "before" || decoded.History[0].ToolCalls[0].Arguments != "{}" {
		t.Fatalf("encoded history changed after caller mutation: history=%d first=%s", len(decoded.History), decoded.History[0].ID)
	}
}

func TestAgentJobRejectsInconsistentAndInvalidValues(t *testing.T) {
	_, clock := newTestQueue()
	cases := map[string]func(*AgentJob){
		"schema":                func(job *AgentJob) { job.SchemaVersion = 99 },
		"tenant agent mismatch": func(job *AgentJob) { job.Agent.TenantID = "other-tenant" },
		"trace mismatch":        func(job *AgentJob) { job.Trace.ExecutionID = "other-execution" },
		"expired deadline":      func(job *AgentJob) { job.Deadline = clock.Now().Add(-time.Second) },
		"long deadline":         func(job *AgentJob) { job.Deadline = clock.Now().Add(3 * time.Hour) },
		"missing session":       func(job *AgentJob) { job.Tenant.SessionID = "" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			job := validTestJob(clock)
			mutate(&job)
			if err := job.ValidateAt(clock.Now(), 2*time.Hour); !errors.Is(err, ErrInvalidJob) {
				t.Fatalf("ValidateAt error = %v, want ErrInvalidJob", err)
			}
		})
	}
}

func TestFakeQueueVisibilityRedeliveryAndOldToken(t *testing.T) {
	q, clock := newTestQueue()
	job := validTestJob(clock)
	if receipt, err := q.Enqueue(context.Background(), job); err != nil || !receipt.Accepted {
		t.Fatalf("enqueue receipt = %#v, err = %v", receipt, err)
	}
	first, err := q.Receive(context.Background(), 10*time.Second)
	if err != nil {
		t.Fatalf("first receive: %v", err)
	}
	clock.Advance(11 * time.Second)
	second, err := q.Receive(context.Background(), 10*time.Second)
	if err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if second.Attempt != 2 || second.DeliveryID == first.DeliveryID {
		t.Fatalf("redelivery = %#v, want attempt 2 and a new token", second)
	}
	if err := q.Ack(context.Background(), first); !errors.Is(err, ErrDeliveryFinished) {
		t.Fatalf("old Ack error = %v, want ErrDeliveryFinished", err)
	}
	if err := q.Ack(context.Background(), second); err != nil {
		t.Fatalf("ack second delivery: %v", err)
	}
	if err := q.Ack(context.Background(), second); err != nil {
		t.Fatalf("idempotent ack: %v", err)
	}
	if err := q.Ack(context.Background(), first); !errors.Is(err, ErrDeliveryExpired) {
		t.Fatalf("old ack after completion = %v, want ErrDeliveryExpired", err)
	}
}

func TestFakeQueueNackDelayAndDiscard(t *testing.T) {
	q, clock := newTestQueue()
	job := validTestJob(clock)
	if _, err := q.Enqueue(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	first, err := q.Receive(context.Background(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Nack(context.Background(), first, NackOptions{Requeue: true, RetryAfter: 30 * time.Second, Reason: "temporary"}); err != nil {
		t.Fatalf("nack requeue: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := q.Receive(ctx, time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("receive with canceled context = %v", err)
	}
	clock.Advance(29 * time.Second)
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer shortCancel()
	if _, err := q.Receive(shortCtx, time.Minute); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("receive before nack delay = %v", err)
	}
	clock.Advance(2 * time.Second)
	second, err := q.Receive(context.Background(), time.Minute)
	if err != nil || second.Attempt != 2 {
		t.Fatalf("delayed receive = %#v, err = %v", second, err)
	}
	if err := q.Nack(context.Background(), second, NackOptions{}); err != nil {
		t.Fatalf("discard nack: %v", err)
	}
	finalCtx, finalCancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer finalCancel()
	if _, err := q.Receive(finalCtx, time.Minute); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("receive after discard = %v", err)
	}
}

func TestFakeQueueCloseWakesReceiveAndIsIdempotent(t *testing.T) {
	q, _ := newTestQueue()
	result := make(chan error, 1)
	go func() {
		_, err := q.Receive(context.Background(), time.Minute)
		result <- err
	}()
	select {
	case <-result:
		t.Fatal("Receive returned before Close")
	case <-time.After(10 * time.Millisecond):
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrQueueClosed) {
			t.Fatalf("Receive after close = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Receive was not woken by Close")
	}
	if _, err := q.Enqueue(context.Background(), validTestJob(&testClock{now: time.Now().UTC()})); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("enqueue after close = %v", err)
	}
}

func TestFakeQueueVisibilityExtension(t *testing.T) {
	q, clock := newTestQueue()
	if _, err := q.Enqueue(context.Background(), validTestJob(clock)); err != nil {
		t.Fatal(err)
	}
	delivery, err := q.Receive(context.Background(), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.ExtendVisibility(context.Background(), delivery, time.Minute); err != nil {
		t.Fatal(err)
	}
	clock.Advance(20 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if _, err := q.Receive(ctx, time.Minute); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("delivery became visible after extension: %v", err)
	}
	if err := q.Ack(context.Background(), delivery); err != nil {
		t.Fatal(err)
	}
}
