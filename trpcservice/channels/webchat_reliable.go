package channels

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/outbox"
)

// WebChatSender confirms webchat replies into the sent state. The browser is
// not reachable from the delivery process — the SSE connections live in the
// gateway — so "delivered" here means "the mailbox may push it". The
// gateway's mailbox flips pushed_at only after the SSE write actually
// succeeded, and un-pushed rows are re-pushed on the next connection instead
// of being lost.
type WebChatSender struct{}

// Send implements outbox.Sender.
func (s *WebChatSender) Send(context.Context, *outbox.ClaimedReply) (outbox.Outcome, error) {
	return outbox.Sent, nil
}

// webchatPollInterval is how often an open SSE connection asks for new
// replies: one indexed query per open browser per second, kept flat by
// idx_reply_outbox_webchat (tenant_id, session_pk, status, pushed_at).
const webchatPollInterval = time.Second

// WebChatMailbox serves the reliable-mode /webchat/stream: it pushes rows the
// delivery role has marked sent and marks them pushed only after the write to
// the browser succeeded. A dropped connection therefore re-pushes on the next
// open — at-least-once delivery to the browser, which is the harmless
// direction for a chat transcript (a duplicate line beats a lost reply).
type WebChatMailbox struct {
	cdp  *controlplane.DB
	poll time.Duration
}

// NewWebChatMailbox builds the mailbox over the control plane.
func NewWebChatMailbox(cdp *controlplane.DB) *WebChatMailbox {
	return &WebChatMailbox{cdp: cdp, poll: webchatPollInterval}
}

// Handler returns the SSE endpoint. The query contract matches the legacy
// page: /webchat/stream?tenant={id}&user={id}.
func (m *WebChatMailbox) Handler() http.Handler { return http.HandlerFunc(m.stream) }

func (m *WebChatMailbox) stream(w http.ResponseWriter, r *http.Request) {
	tenant := r.URL.Query().Get("tenant")
	user := r.URL.Query().Get("user")
	if tenant == "" || user == "" {
		http.Error(w, "tenant and user are required", http.StatusBadRequest)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher.Flush()

	ticker := time.NewTicker(m.poll)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			if err := m.flush(r.Context(), w, flusher, tenant, user); err != nil {
				slog.Warn("webchat mailbox: flush", "tenant", tenant, "user", user, "err", err)
			}
		}
	}
}

// flush pushes every sent-but-not-pushed reply of the user's latest session.
func (m *WebChatMailbox) flush(ctx context.Context, w io.Writer, flusher http.Flusher, tenant, user string) error {
	scope, err := m.cdp.Scope(tenant)
	if err != nil {
		return err
	}
	var sessionPK int64
	row, err := scope.QueryRow(ctx, `
		SELECT session_pk FROM sessions
		WHERE tenant_id = ? AND actor_key = ?
		ORDER BY session_pk DESC LIMIT 1`, tenant, user)
	if err != nil {
		return err
	}
	switch err := row.Scan(&sessionPK); {
	case errors.Is(err, sql.ErrNoRows):
		return nil // nothing said yet: no session, nothing to push
	case err != nil:
		return fmt.Errorf("webchat mailbox: locate session: %w", err)
	}

	rows, err := scope.Query(ctx, `
		SELECT outbox_id, text, is_done FROM reply_outbox
		WHERE tenant_id = ? AND session_pk = ? AND channel_type = 'webchat'
		  AND status = 'sent' AND pushed_at IS NULL
		ORDER BY outbox_id LIMIT 500`, tenant, sessionPK)
	if err != nil {
		return err
	}
	type pendingReply struct {
		id     int64
		text   string
		isDone bool
	}
	var batch []pendingReply
	for rows.Next() {
		var p pendingReply
		if err := rows.Scan(&p.id, &p.text, &p.isDone); err != nil {
			rows.Close()
			return fmt.Errorf("webchat mailbox: scan reply: %w", err)
		}
		batch = append(batch, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, p := range batch {
		data, err := json.Marshal(sseEvent{Text: p.text, Chunk: !p.isDone, Done: p.isDone})
		if err != nil {
			continue
		}
		if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
			return err
		}
		flusher.Flush()
		// Push-then-mark: a crash or a dropped connection between the two
		// re-pushes on the next connection (at-least-once) instead of
		// losing the reply for good.
		if _, err := scope.Exec(ctx, `
			UPDATE reply_outbox SET pushed_at = UTC_TIMESTAMP(6)
			WHERE tenant_id = ? AND outbox_id = ? AND pushed_at IS NULL`, tenant, p.id); err != nil {
			return fmt.Errorf("webchat mailbox: mark pushed: %w", err)
		}
	}
	return nil
}
