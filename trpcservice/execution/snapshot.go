package execution

import (
	"context"
	"encoding/json"
	"fmt"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

// loadCommittedEvents reads the durable history of one session, oldest first.
// The stored payload is the framework's own event.Event shape, serialized at
// commit time by the same code that reads it back here, so the round trip is
// JSON and does not need a hand-written column mapping that could drift from
// the framework's struct.
func loadCommittedEvents(ctx context.Context, tx *controlplane.TxScope, tenantID string, sessionPK int64) ([]event.Event, error) {
	rows, err := tx.Query(ctx, `
		SELECT payload
		FROM session_events
		WHERE tenant_id = ? AND session_pk = ?
		ORDER BY seq`, tenantID, sessionPK)
	if err != nil {
		return nil, fmt.Errorf("execution: read committed events: %w", err)
	}
	defer rows.Close()

	var out []event.Event
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("execution: scan event row: %w", err)
		}
		var e event.Event
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, fmt.Errorf("execution: decode event row: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// sessionForClaim rebuilds the framework session object the workspace is
// seeded from. The key must be the framework key the Runner will use for
// this execution — derived from the claim, not stored per row, because the
// claim is what is consistent across workers while a stored framework key
// would let a stale row pin a session to the wrong user space.
func sessionForClaim(c *Claim) *session.Session {
	appName, userID := FrameworkKeys(c)
	sess := session.NewSession(appName, userID, SessionID(c))
	for k, v := range c.State {
		sess.SetState(k, v)
	}
	if c.Summary != "" {
		sess.SummariesMu.Lock()
		sess.Summaries[session.SummaryFilterKeyAllContents] = &session.Summary{Summary: c.Summary}
		sess.SummariesMu.Unlock()
	}
	for i := range c.Events {
		appendLossless(sess, c.Events[i])
	}
	return sess
}

// appendLossless puts a stored event back into a session's history without
// going through any path that would mint a new identity. session.Session
// appends by value elsewhere; this mirrors that shape exactly.
func appendLossless(sess *session.Session, e event.Event) {
	sess.EventMu.Lock()
	sess.Events = append(sess.Events, e)
	sess.EventMu.Unlock()
}
