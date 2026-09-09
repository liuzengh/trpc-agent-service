package gateway

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/admission"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// blockingQueue gates Enqueue behind a test-owned channel so the ingress can
// be held between admission and the fast ACK deterministically.
type blockingQueue struct {
	release  chan struct{}
	once     sync.Once
	enqueued atomic.Int64
}

func (q *blockingQueue) Enqueue(ctx context.Context, job queue.AgentJob) (queue.QueueReceipt, error) {
	q.enqueued.Add(1)
	<-q.release
	if err := ctx.Err(); err != nil {
		return queue.QueueReceipt{}, err
	}
	return queue.QueueReceipt{Accepted: true, ReceiptID: "receipt-blocking", JobID: job.JobID, Attempt: 1}, nil
}
func (q *blockingQueue) Receive(ctx context.Context, visibilityTimeout time.Duration) (queue.Delivery, error) {
	return queue.Delivery{}, queue.ErrQueueClosed
}
func (q *blockingQueue) Ack(ctx context.Context, delivery queue.Delivery) error { return nil }
func (q *blockingQueue) Nack(ctx context.Context, delivery queue.Delivery, options queue.NackOptions) error {
	return nil
}
func (q *blockingQueue) ExtendVisibility(ctx context.Context, delivery queue.Delivery, extension time.Duration) error {
	return nil
}
func (q *blockingQueue) Close() error { return nil }

type recordingClaims struct {
	inner    storage.ClaimStore
	claims   atomic.Int64
	complete atomic.Int64
}

func (r *recordingClaims) Claim(ctx context.Context, tc tenant.TenantContext, key storage.DedupKey, ttl time.Duration, owner string) (storage.Claim, error) {
	r.claims.Add(1)
	return r.inner.Claim(ctx, tc, key, ttl, owner)
}
func (r *recordingClaims) Complete(ctx context.Context, tc tenant.TenantContext, key storage.DedupKey, owner, jobID string, guard storage.OperationGuard) error {
	r.complete.Add(1)
	return r.inner.Complete(ctx, tc, key, owner, jobID, guard)
}
func (r *recordingClaims) Fail(ctx context.Context, tc tenant.TenantContext, key storage.DedupKey, owner string, guard storage.OperationGuard, requeue bool) error {
	return r.inner.Fail(ctx, tc, key, owner, guard, requeue)
}

type admissionTelemetryRecorder struct {
	outcomes []string
	mu       sync.Mutex
}

func (r *admissionTelemetryRecorder) IngressAdmission(outcome string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.outcomes = append(r.outcomes, outcome)
}

func capacityIngress(t *testing.T, jobQueue queue.JobQueue, gate *admission.Gate, telemetry IngressAdmissionTelemetry) (*Ingress, *recordingClaims) {
	t.Helper()
	gateway, err := New(jobQueue)
	if err != nil {
		t.Fatal(err)
	}
	claims := &recordingClaims{inner: storage.NewFakeCoordinationStore()}
	tc := ingressTestContext()
	ingress, err := NewIngress(IngressConfig{
		Resolver: ingressTestResolver{context: tc},
		Claims:   claims,
		Gateway:  gateway,
		ResolveAgent: func(context.Context, tenant.TenantContext) (agent.AgentSpec, error) {
			return agent.AgentSpec{TenantID: tc.TenantID, AgentAppID: tc.AgentAppID, Version: tc.ConfigVersion, Name: "assistant", ModelProvider: "fake"}, nil
		},
		Adapters:  map[string]WebhookAdapter{"telegram": ingressTestAdapter{incoming: channels.Incoming{ID: "message-ingress", UserID: "user-ingress", ChatID: "chat-ingress", Text: "hello"}}},
		OwnerID:   "owner-ingress",
		Now:       func() time.Time { return time.Now().UTC() },
		Admission: gate,
		Telemetry: telemetry,
	})
	if err != nil {
		t.Fatal(err)
	}
	return ingress, claims
}

func capacityRequest(ctx context.Context) *http.Request {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "/webhook/telegram/app", strings.NewReader(`{}`))
	if err != nil {
		panic(err)
	}
	return request
}

// TestIngressCapacityExhaustedRejectsBeforeClaim proves the overload
// outcome: with the global budget held by one blocked enqueue, a second
// request is rejected with the bounded 429 before any dedup claim, and the
// held slot is released when the first request finishes.
func TestIngressCapacityExhaustedRejectsBeforeClaim(t *testing.T) {
	blocked := &blockingQueue{release: make(chan struct{})}
	gate, err := admission.New(1, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	recorder := &admissionTelemetryRecorder{}
	ingress, claims := capacityIngress(t, blocked, gate, recorder)

	done := make(chan WebhookResult, 1)
	go func() {
		done <- ingress.Handle(context.Background(), "telegram", "app", capacityRequest(context.Background()), []byte(`{}`))
	}()
	// Event-driven wait: the first request is inside the blocked enqueue.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && blocked.enqueued.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	if blocked.enqueued.Load() != 1 {
		t.Fatalf("first request never reached the enqueue")
	}
	result := ingress.Handle(context.Background(), "telegram", "app", capacityRequest(context.Background()), []byte(`{}`))
	if result.Status != http.StatusTooManyRequests {
		t.Fatalf("capacity rejection status: %d", result.Status)
	}
	if !strings.Contains(string(result.Body), "capacity_exhausted") {
		t.Fatalf("capacity rejection body: %s", result.Body)
	}
	if result.RetryAfter != time.Second {
		t.Fatalf("capacity retry hint: %s", result.RetryAfter)
	}
	if claims.claims.Load() != 1 {
		t.Fatalf("rejected request reached the dedup claim: %d", claims.claims.Load())
	}
	close(blocked.release)
	first := <-done
	if first.Status != http.StatusAccepted {
		t.Fatalf("first request outcome: %d %s", first.Status, first.Body)
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && gate.ActiveGlobal() != 0 {
		time.Sleep(time.Millisecond)
	}
	if gate.ActiveGlobal() != 0 {
		t.Fatalf("admission slot leaked after fast ACK: %d", gate.ActiveGlobal())
	}
	// The freed budget admits the next request again.
	third := ingress.Handle(context.Background(), "telegram", "app", capacityRequest(context.Background()), []byte(`{}`))
	if third.Status != http.StatusAccepted {
		t.Fatalf("request after release: %d", third.Status)
	}
	found := false
	recorder.mu.Lock()
	for _, outcome := range recorder.outcomes {
		if outcome == "capacity_exhausted" {
			found = true
		}
	}
	recorder.mu.Unlock()
	if !found {
		t.Fatalf("capacity outcome not observed: %v", recorder.outcomes)
	}
}

// TestIngressAdmissionReleaseOnEnqueueFailure proves a durable enqueue
// failure never returns accepted and never leaks the admission slot.
func TestIngressAdmissionReleaseOnEnqueueFailure(t *testing.T) {
	gate, _ := admission.New(1, 1, 1)
	ingress, _ := capacityIngress(t, &failingEnqueueQueue{}, gate, nil)
	failed := ingress.Handle(context.Background(), "telegram", "app", capacityRequest(context.Background()), []byte(`{}`))
	if failed.Status != http.StatusServiceUnavailable {
		t.Fatalf("enqueue failure status: %d", failed.Status)
	}
	if strings.Contains(string(failed.Body), "accepted") {
		t.Fatalf("enqueue failure returned accepted: %s", failed.Body)
	}
	if gate.ActiveGlobal() != 0 {
		t.Fatalf("admission slot leaked on enqueue failure: %d", gate.ActiveGlobal())
	}
	ok := ingress.Handle(context.Background(), "telegram", "app", capacityRequest(context.Background()), []byte(`{}`))
	if ok.Status != http.StatusAccepted {
		t.Fatalf("request after failure release: %d", ok.Status)
	}
}

// TestIngressAdmissionReleaseOnCancel proves a cancelled request releases
// its slot (defer-owned release) instead of stalling the budget.
func TestIngressAdmissionReleaseOnCancel(t *testing.T) {
	gate, _ := admission.New(1, 1, 1)
	ingress, _ := capacityIngress(t, queue.NewFakeQueue(queue.FakeQueueConfig{}), gate, nil)
	ctx, cancel := context.WithCancel(context.Background())
	result := ingress.Handle(ctx, "telegram", "app", capacityRequest(ctx), []byte(`{}`))
	cancel()
	_ = result
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && gate.ActiveGlobal() != 0 {
		time.Sleep(time.Millisecond)
	}
	if gate.ActiveGlobal() != 0 {
		t.Fatalf("admission slot leaked on cancelled request: %d", gate.ActiveGlobal())
	}
}

type failingEnqueueQueue struct {
	queue.JobQueue
	failures atomic.Int64
}

func (q *failingEnqueueQueue) Enqueue(ctx context.Context, job queue.AgentJob) (queue.QueueReceipt, error) {
	if q.failures.Add(1) == 1 {
		return queue.QueueReceipt{}, errors.New("durable enqueue unavailable")
	}
	return queue.QueueReceipt{Accepted: true, ReceiptID: "receipt-after-failure", JobID: job.JobID, Attempt: 1}, nil
}

func TestClampRetryAfterIsBounded(t *testing.T) {
	if got := clampRetryAfter(0); got != minRateLimitRetryAfter {
		t.Fatalf("zero clamp: %s", got)
	}
	if got := clampRetryAfter(-time.Second); got != minRateLimitRetryAfter {
		t.Fatalf("negative clamp: %s", got)
	}
	if got := clampRetryAfter(3 * time.Second); got != 3*time.Second {
		t.Fatalf("in-window clamp: %s", got)
	}
	if got := clampRetryAfter(48 * time.Hour); got != maxRateLimitRetryAfter {
		t.Fatalf("oversized clamp: %s", got)
	}
}
