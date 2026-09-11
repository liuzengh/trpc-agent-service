// Package eventadapter validates the versioned Worker event before crossing the
// Delivery application seam. Broker identity/ACK policy is a separate adapter.
package eventadapter

import (
	"context"
	"errors"
	"time"

	wire "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
)

type Acceptor interface {
	AcceptReplyIntent(context.Context, d.Intent) (d.Receipt, error)
}
type Handler struct{ acceptor Acceptor }

func NewHandler(acceptor Acceptor) (*Handler, error) {
	if acceptor == nil {
		return nil, d.ErrUnavailable
	}
	return &Handler{acceptor: acceptor}, nil
}
func (h *Handler) Handle(ctx context.Context, raw []byte) (d.Receipt, error) {
	if ctx == nil {
		return d.Receipt{}, d.ErrInvalid
	}
	event, err := wire.DecodeReplyIntent(raw)
	if err != nil {
		// Only a definite wire violation is a permanent input error. Internal
		// compiler/dependency failures must remain retryable at the broker seam.
		if errors.Is(err, wire.ErrInvalidReplyIntent) {
			return d.Receipt{}, d.ErrInvalid
		}
		return d.Receipt{}, d.ErrUnavailable
	}
	deadline, err := time.Parse(time.RFC3339Nano, event.Deadline)
	if err != nil {
		return d.Receipt{}, d.ErrInvalid
	}
	intent := d.Intent{ID: event.IntentID, AdmissionID: event.AdmissionID, RunID: event.RunID, AttemptID: event.Execution.AttemptID, CompletionID: event.Execution.CompletionID, ExecutionGeneration: event.Execution.Generation, Sequence: event.Sequence, Text: event.Content.Text, Deadline: deadline}
	for _, a := range event.Content.Attachments {
		intent.Attachments = append(intent.Attachments, d.Attachment{Name: a.Name, Version: a.Version, MIMEType: a.MimeType, SizeBytes: a.SizeBytes, SHA256: a.Sha256})
	}
	return h.acceptor.AcceptReplyIntent(ctx, intent)
}
