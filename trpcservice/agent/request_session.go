package agent

import (
	"context"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// requestSession delegates storage to the configured framework service. Runner
// appends incoming messages before calling the model; durable retries must not
// append the same user's request on every connection failure. Runtime holds the
// existing distributed session lease across this check and the delegated write.
type requestSession struct{ session.Service }

func (s *requestSession) AppendEvent(ctx context.Context, sess *session.Session, evt *event.Event, opts ...session.Option) error {
	if sess != nil && userRequestEvent(evt) {
		sess.EventMu.RLock()
		duplicate := false
		for i := range sess.Events {
			previous := &sess.Events[i]
			if previous.RequestID == evt.RequestID && userRequestEvent(previous) {
				duplicate = true
				break
			}
		}
		sess.EventMu.RUnlock()
		if duplicate {
			return nil
		}
	}
	return s.Service.AppendEvent(ctx, sess, evt, opts...)
}

func userRequestEvent(evt *event.Event) bool {
	return evt != nil && evt.RequestID != "" && evt.Author == "user" && evt.Response != nil && len(evt.Choices) == 1 && evt.Choices[0].Message.Role == model.RoleUser
}
