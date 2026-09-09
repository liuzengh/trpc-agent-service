package platform

import (
	"context"
	"encoding/json"
	"strings"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/summary"
)

type summaryProjection struct {
	Text           string `json:"text"`
	SourceSequence uint64 `json:"source_sequence"`
}

// ConfigureSummarizer installs the upstream summarizer before serving traffic.
// Without a model the platform persists a bounded extractive conversation view.
func (h *AdminHandler) ConfigureSummarizer(s summary.SessionSummarizer) { h.summarizer = s }

func frameworkSession(tenant, app, id string, events []SessionEvent) *session.Session {
	sess := &session.Session{ID: id, AppName: tenant + ":" + app, UserID: tenant + ":" + id, State: session.StateMap{}}
	for _, stored := range events {
		var payload map[string]string
		if json.Unmarshal(stored.Payload, &payload) != nil {
			continue
		}
		role, text := model.RoleUser, payload["input"]
		switch stored.Type {
		case "message.input":
		case "message.completed":
			role, text = model.RoleAssistant, payload["output"]
		default:
			continue
		}
		if text == "" {
			continue
		}
		sess.Events = append(sess.Events, event.Event{ID: stored.ID, Timestamp: stored.OccurredAt, Response: &model.Response{Choices: []model.Choice{{Message: model.Message{Role: role, Content: text}}}}})
	}
	return sess
}

func (h *AdminHandler) updateSessionSummary(ctx context.Context, store DataStore, options chatRunOptions) error {
	events, err := store.ListSessionEvents(ctx, options.tenant.TenantID, options.sessionID, 0)
	if err != nil {
		return err
	}
	state, err := materializeSession(options.tenant.TenantID, options.sessionID, events)
	if err != nil {
		return err
	}
	// An existing checkpoint for this request makes retries deterministic even if
	// a model would choose different words on a second call.
	key := options.requestID + ":summary"
	for _, e := range events {
		if e.IdempotencyKey == key {
			return nil
		}
	}
	sess := frameworkSession(options.tenant.TenantID, options.appID, options.sessionID, events)
	var text string
	if h.summarizer != nil {
		if !h.summarizer.ShouldSummarize(sess) {
			return nil
		}
		text, err = h.summarizer.Summarize(ctx, sess)
		if err != nil {
			return err
		}
	} else {
		start := len(sess.Events) - 8
		if start < 0 {
			start = 0
		}
		var lines []string
		for _, e := range sess.Events[start:] {
			message := e.Choices[0].Message
			content := []rune(message.Content)
			if len(content) > 256 {
				content = append(content[:256], '…')
			}
			lines = append(lines, string(message.Role)+": "+string(content))
		}
		text = strings.Join(lines, "\n")
	}
	payload, err := json.Marshal(summaryProjection{Text: text, SourceSequence: state.ProjectionSequence})
	if err != nil {
		return err
	}
	return store.AppendSessionEvent(ctx, SessionEvent{TenantID: options.tenant.TenantID, SessionID: options.sessionID, IdempotencyKey: key, Type: "summary", Payload: payload, FencingToken: options.fencingToken})
}
