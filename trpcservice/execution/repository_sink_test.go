package execution

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestRepositorySinkRequiresRepository(t *testing.T) {
	if _, err := NewRepositorySink(nil); !errors.Is(err, ErrMissingDependency) {
		t.Fatalf("nil repository error=%v", err)
	}
	if err := (&RepositorySink{}).Commit(context.Background(), ExecutionCommit{}); !errors.Is(err, ErrMissingDependency) {
		t.Fatalf("nil sink repository error=%v", err)
	}
}

func TestValidateRepositoryCommitRejectsMismatchedExecutionIdentity(t *testing.T) {
	commit := validRepositoryCommitForTest()
	commit.JobID = "different-job"
	if err := validateRepositoryCommit(commit); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("mismatched job error=%v", err)
	}
	commit = validRepositoryCommitForTest()
	commit.ExecutionID = "different-execution"
	if err := validateRepositoryCommit(commit); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("mismatched execution error=%v", err)
	}
}

func TestValidateRepositoryCommitRejectsTenantAndFenceMismatch(t *testing.T) {
	commit := validRepositoryCommitForTest()
	commit.TenantID = "different-tenant"
	if err := validateRepositoryCommit(commit); !errors.Is(err, storage.ErrTenantMismatch) {
		t.Fatalf("tenant mismatch error=%v", err)
	}
	commit = validRepositoryCommitForTest()
	commit.FenceToken++
	if err := validateRepositoryCommit(commit); !errors.Is(err, storage.ErrFenceRejected) {
		t.Fatalf("fence mismatch error=%v", err)
	}
}

func validRepositoryCommitForTest() ExecutionCommit {
	now := time.Now().UTC()
	tc := tenant.TenantContext{TenantID: "tenant-sink", SessionID: "session-sink", RequestID: "request-sink", MessageID: "message-sink", TraceID: "trace-sink"}
	job := queue.AgentJob{JobID: "job-sink", ExecutionID: "execution-sink", Tenant: queue.TenantContextDTOFromContext(tc)}
	lease := storage.Lease{TenantID: tc.TenantID, SessionID: tc.SessionID, ResourceID: tc.SessionID, OwnerID: "owner-sink", Epoch: 2, FenceToken: 7, ExpiresAt: now.Add(time.Minute)}
	return ExecutionCommit{
		JobID: job.JobID, ExecutionID: job.ExecutionID, TenantID: tc.TenantID, SessionID: tc.SessionID,
		OwnerID: lease.OwnerID, Epoch: lease.Epoch, FenceToken: lease.FenceToken,
		Job: job, TenantContext: tc, Input: agent.AgentInput{TenantContext: tc}, Lease: lease,
	}
}

func TestAtomicCompletionRequestForIncludesDurableReply(t *testing.T) {
	commit := validRepositoryCommitForTest()
	commit.Result.Text = "reply"
	request, err := AtomicCompletionRequestFor(commit, queue.Delivery{DeliveryID: "delivery-sink", Job: commit.Job})
	if err != nil {
		t.Fatal(err)
	}
	if request.Outbox == nil || request.Outbox.TenantID != commit.TenantID || request.Outbox.AggregateID != commit.ExecutionID || request.Outbox.DedupKey != "tenant-sink|execution-sink|agent.reply" {
		t.Fatalf("request did not carry durable reply: %+v", request)
	}
	var payload ReplyOutboxPayload
	if err := json.Unmarshal(request.Outbox.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.ExecutionID != commit.ExecutionID || payload.ReplyText != commit.Result.Text {
		t.Fatalf("request reply payload=%+v", payload)
	}
}

func TestBuildReplyOutboxMessageIsStableAndBounded(t *testing.T) {
	commit := validRepositoryCommitForTest()
	commit.Result = agent.AgentResult{
		Text: "safe reply", FinishType: "stop", Usage: agent.Usage{InputTokens: 99, OutputTokens: 11},
		Events: []agent.RunnerEvent{{Sequence: 1, Type: "response", Role: "assistant", Content: "secret event content", Metadata: map[string]string{"authorization": "do-not-store"}}},
	}
	first, err := BuildReplyOutboxMessage(commit)
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildReplyOutboxMessage(commit)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != "reply-execution-sink" || first.AggregateID != commit.ExecutionID ||
		first.DedupKey != "tenant-sink|execution-sink|agent.reply" || string(first.Payload) != string(second.Payload) {
		t.Fatalf("unstable reply identity/payload: first=%+v second=%+v", first, second)
	}
	var payload ReplyOutboxPayload
	if err := json.Unmarshal(first.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.SchemaVersion != ReplyOutboxSchemaVersion || payload.TenantID != commit.TenantID ||
		payload.SessionID != commit.SessionID || payload.JobID != commit.JobID || payload.ExecutionID != commit.ExecutionID ||
		payload.ReplyText != commit.Result.Text || payload.FinishType != commit.Result.FinishType {
		t.Fatalf("reply payload identity=%+v", payload)
	}
	if string(first.Payload) == "" || containsSensitiveReplyData(first.Payload) {
		t.Fatalf("reply payload contains sensitive/provider data: %s", first.Payload)
	}
	commit.Result.Text = "changed after build"
	if string(first.Payload) != string(second.Payload) {
		t.Fatal("built payload changed after caller mutation")
	}
}

func TestBuildReplyOutboxMessageHashesOversizedDedupIdentity(t *testing.T) {
	commit := validRepositoryCommitForTest()
	commit.TenantID = strings.Repeat("t", 128)
	commit.ExecutionID = strings.Repeat("e", 128)
	commit.Job.ExecutionID = commit.ExecutionID
	commit.TenantContext.TenantID = commit.TenantID
	commit.Input.TenantContext.TenantID = commit.TenantID
	commit.Lease.TenantID = commit.TenantID
	message, err := BuildReplyOutboxMessage(commit)
	if err != nil {
		t.Fatal(err)
	}
	if len(message.DedupKey) > 256 || !strings.HasPrefix(message.DedupKey, "reply-sha256-") {
		t.Fatalf("oversized dedup identity was not bounded: %q", message.DedupKey)
	}
	again, err := BuildReplyOutboxMessage(commit)
	if err != nil || message.DedupKey != again.DedupKey {
		t.Fatalf("hashed dedup identity is not deterministic: first=%q second=%q err=%v", message.DedupKey, again.DedupKey, err)
	}
}

func containsSensitiveReplyData(payload []byte) bool {
	text := string(payload)
	for _, value := range []string{"authorization", "do-not-store", "secret event content", "input_tokens", "output_tokens"} {
		if strings.Contains(text, value) {
			return true
		}
	}
	return false
}
