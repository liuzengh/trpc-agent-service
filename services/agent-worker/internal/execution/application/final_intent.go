package application

import (
	"time"

	codec "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	wire "github.com/liuzengh/trpc-agent-service/gen/events/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

// EncodeFinalIntent is shared by pre-Stage validation and the accepting ledger.
// It applies only the published wire contract (including the existing envelope
// size limit); it introduces no attachment count or metadata budget policy.
func EncodeFinalIntent(r domain.Run, attemptID string, generation int64, text string, attachments []domain.Attachment) ([]byte, string, error) {
	if err := domain.ValidateAttachments(attachments); err != nil {
		return nil, "", err
	}
	var items []wire.ReplyAttachment
	if len(attachments) > 0 {
		items = make([]wire.ReplyAttachment, len(attachments))
		for i, a := range attachments {
			items[i] = wire.ReplyAttachment{Name: a.Name, Version: int64(a.Version), MimeType: a.MimeType, SizeBytes: int64(a.SizeBytes), Sha256: a.SHA256}
		}
	}
	event := wire.ReplyIntent{SchemaVersion: 1, IntentID: domain.StableID("fin", r.Request.RunID), AdmissionID: r.Request.AdmissionID, RunID: r.Request.RunID, Kind: "final", Sequence: 1, Deadline: r.ReplyDeadline.UTC().Format(time.RFC3339Nano), Content: wire.FinalTextContent{Type: "text", Text: text, Attachments: items}, Execution: wire.ReplyExecution{AttemptID: attemptID, CompletionID: domain.StableID("cmp", r.Request.RunID), Generation: generation}}
	payload, err := codec.EncodeReplyIntent(event)
	if err != nil {
		return nil, "", err
	}
	digest, err := codec.ReplyIntentDigest(event)
	if err != nil {
		return nil, "", err
	}
	return payload, digest, nil
}
