// media.go is the worker's half of inbound-attachment handling.
//
// The gateway already fetched the bytes and inlined them into the model message
// (see channels.buildUserContent), and the framework's session externalization
// moves those bytes into artifact storage when the session event is persisted.
// What is left is the case the user must never be left guessing about: an
// attachment the platform handed over but we could not read. The model sees a
// manifest line saying so; the user gets an explicit receipt, and the audit
// trail gets a row that explains the missing content later.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/bus"
)

// errAttachmentUnreadable is the audit error class of an inbound attachment that
// could not be fetched. It is deliberately distinct from run_error/timeout: the
// agent never saw the content, so an operator reading the audit trail must be
// able to tell "the model ignored the file" from "the file never arrived".
const errAttachmentUnreadable = "attachment_unreadable"

// mediaReceiptSuffix scopes the receipt's idempotency key to the inbound
// message, so a redelivered message never sends the receipt twice.
const mediaReceiptSuffix = ":media"

// reportUnreadableMedia tells the user, once per inbound message, which
// attachments could not be read, and records the failure in the audit trail.
// It runs before the agent turn so the user learns about the gap immediately
// rather than wondering why the agent ignored their picture.
//
// A message whose attachments all arrived is not reported: the model already
// receives the bytes and a per-attachment manifest line, and an extra "file
// received" notice on every image would be noise, not information.
func (w *Worker) reportUnreadableMedia(ctx context.Context, m *bus.Message) {
	if m == nil || len(m.Media) == 0 {
		return
	}
	failed := make([]bus.MediaRef, 0, len(m.Media))
	for _, ref := range m.Media {
		if strings.TrimSpace(ref.FetchError) != "" {
			failed = append(failed, ref)
		}
	}
	if len(failed) == 0 {
		return
	}
	slog.Warn("worker: inbound attachments unreadable",
		"tenant", m.TenantID, "session", m.SessionID, "failed", len(failed), "total", len(m.Media))
	if w.auditor != nil {
		w.auditor.Record(audit.Entry{
			TenantID:  m.TenantID,
			Channel:   m.Channel,
			UserID:    m.UserID,
			SessionID: m.SessionID,
			AgentName: m.AgentID,
			Decision:  audit.DecisionFailed,
			ErrorType: errAttachmentUnreadable,
			TraceID:   m.TraceID,
		})
	}
	if w.outbox == nil {
		return
	}
	// Durable and idempotent, exactly like the approval notice: the receipt is
	// appended before the turn and keyed on the inbound message, so a crash and
	// redelivery cannot double-send it.
	notice := replyMessage(m, mediaReceiptText(failed))
	if err := w.outbox.Append(ctx, notice, m.ID+mediaReceiptSuffix); err != nil && !errors.Is(err, bus.ErrDuplicateIdem) {
		// A missing receipt must not fail the turn: the audit row above still
		// records the failure, and the model's manifest line still tells the
		// agent the attachment is unreadable.
		slog.Warn("worker: media receipt append failed", "session", m.SessionID, "err", err)
	}
}

// mediaReceiptText renders the user-facing receipt for unreadable attachments.
func mediaReceiptText(failed []bus.MediaRef) string {
	var b strings.Builder
	b.WriteString("⚠️ 有附件未能读取，模型看不到它们的内容：\n")
	for _, ref := range failed {
		kind := ref.Kind
		if kind == "" {
			kind = "附件"
		}
		name := ref.Name
		if name == "" {
			name = "(未命名)"
		}
		fmt.Fprintf(&b, "- %s %s：%s\n", kind, name, ref.FetchError)
	}
	b.WriteString("\n可以改用文字描述内容，或重新发送一次。")
	return b.String()
}
