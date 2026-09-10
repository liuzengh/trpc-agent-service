package session

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// CommitDigest is persisted with a commit ID so a retry can be distinguished
// from an attempt to reuse the same ID for different business effects.
func CommitDigest(in CommitTurnRequest) (string, error) {
	type digestInput struct {
		RequestID, CommitID, Stage string
		InputSeq, Fence            uint64
		Outcome                    runtime.Outcome
		Events                     []BufferedEvent
		StateDelta                 StateDelta
		Summary                    *SummaryCandidate
		ResultRef, ReplyCursor     string
		Outbox                     []OutboxEvent
	}
	payload, err := json.Marshal(digestInput{
		RequestID: in.RequestID, CommitID: in.CommitID, Stage: in.Stage,
		InputSeq: in.InputSeq, Fence: in.Fence, Outcome: in.Outcome,
		Events: in.Events, StateDelta: in.StateDelta, Summary: in.SummaryCandidate,
		ResultRef: in.ResultRef, ReplyCursor: in.ReplyCursor, Outbox: in.Outbox,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func ValidateCommit(in CommitTurnRequest) error {
	if in.TenantID == "" || in.AgentAppID == "" || in.SessionID == "" {
		return runtime.ErrTenantScope
	}
	if in.RequestID == "" || in.CommitID == "" || in.Stage == "" || in.InputSeq < 1 || in.Fence < 1 || in.ExpectedVersion < 0 {
		return runtime.ErrCommitConflict
	}
	for _, event := range in.Events {
		if event.EventID == "" || event.EventType == "" || event.PayloadRef == "" || event.EventSeq < 1 {
			return runtime.ErrCommitConflict
		}
	}
	for _, event := range in.Outbox {
		if event.Kind == "" || event.IdempotencyKey == "" || event.PayloadRef == "" {
			return runtime.ErrCommitConflict
		}
	}
	if candidate := in.SummaryCandidate; candidate != nil {
		if candidate.SummaryID == "" || candidate.BaseSessionSeq < 1 || candidate.LastEventID == "" || candidate.CutoffAt.IsZero() || candidate.ContentRef == "" {
			return runtime.ErrCommitConflict
		}
	}
	return nil
}
