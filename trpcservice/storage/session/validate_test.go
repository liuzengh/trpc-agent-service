package session

import (
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func validCommitRequest() CommitTurnRequest {
	return CommitTurnRequest{
		SessionKey: SessionKey{TenantID: "tenant", AgentAppID: "app", SessionID: "session"},
		RequestID:  "request", CommitID: "request:terminal:0", Stage: "terminal", InputSeq: 1, Fence: 1, ExpectedVersion: 0,
		Outcome:          runtime.OutcomeSucceeded,
		Events:           []BufferedEvent{{EventID: "event", EventType: "message", PayloadRef: "payload://event", EventSeq: 1}},
		StateDelta:       StateDelta{"state": "value"},
		SummaryCandidate: &SummaryCandidate{SummaryID: "summary", BaseSessionSeq: 1, LastEventID: "event", ContentRef: "summary://1", CutoffAt: time.Unix(1, 0).UTC()},
		ResultRef:        "result://request", ReplyCursor: "reply:1",
		Outbox: []OutboxEvent{{Kind: "reply", IdempotencyKey: "reply:1", PayloadRef: "result://request", EventSeq: 1, TraceParent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"}},
	}
}

func TestValidateCommitRejectsMalformedTenantAndEffects(t *testing.T) {
	valid := validCommitRequest()
	if err := ValidateCommit(valid); err != nil {
		t.Fatalf("valid commit rejected: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*CommitTurnRequest)
		want   error
	}{
		{"missing tenant", func(in *CommitTurnRequest) { in.TenantID = "" }, runtime.ErrTenantScope},
		{"missing request", func(in *CommitTurnRequest) { in.RequestID = "" }, runtime.ErrCommitConflict},
		{"zero input", func(in *CommitTurnRequest) { in.InputSeq = 0 }, runtime.ErrCommitConflict},
		{"zero fence", func(in *CommitTurnRequest) { in.Fence = 0 }, runtime.ErrCommitConflict},
		{"negative version", func(in *CommitTurnRequest) { in.ExpectedVersion = -1 }, runtime.ErrCommitConflict},
		{"invalid event", func(in *CommitTurnRequest) { in.Events[0].PayloadRef = "" }, runtime.ErrCommitConflict},
		{"invalid outbox", func(in *CommitTurnRequest) { in.Outbox[0].IdempotencyKey = "" }, runtime.ErrCommitConflict},
		{"invalid summary", func(in *CommitTurnRequest) { in.SummaryCandidate.CutoffAt = time.Time{} }, runtime.ErrCommitConflict},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			in := validCommitRequest()
			test.mutate(&in)
			if err := ValidateCommit(in); !errors.Is(err, test.want) {
				t.Fatalf("err=%v want=%v", err, test.want)
			}
		})
	}
}

func TestCommitDigestIsStableAndBindsBusinessEffects(t *testing.T) {
	first, err := CommitDigest(validCommitRequest())
	if err != nil {
		t.Fatal(err)
	}
	second, err := CommitDigest(validCommitRequest())
	if err != nil || second != first {
		t.Fatalf("same request digest=%q err=%v want=%q", second, err, first)
	}
	changed := validCommitRequest()
	changed.Outbox[0].PayloadRef = "result://other"
	digest, err := CommitDigest(changed)
	if err != nil {
		t.Fatal(err)
	}
	if digest == first {
		t.Fatal("digest did not bind outbox payload")
	}
}
