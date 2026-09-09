package text

import (
	"strings"
	"unicode/utf8"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

// truncationNotice replaces the tail of an answer that does not fit in one
// reply on the channel it is going to. It is fixed text, and the same bytes are
// stored and sent: a notice composed at send time would make the record
// disagree with what the user saw.
const truncationNotice = "\n[truncated]"

// minReplyTextLimit is the smallest reply limit an adapter may declare: the
// notice plus one byte of the answer it replaced. A limit below it could not
// carry its own truncation marker, so a long answer would be silently replaced
// by the marker rather than shortened.
const minReplyTextLimit = len(truncationNotice) + 1

// replyCollector reads one execution's event stream and keeps the single final
// answer.
//
// # What counts as the answer
//
// One event does: a chat completion that is done, not partial, carries no
// error, and whose first choice is a plain assistant message. Its Content is
// the complete answer — both models this platform builds emit the accumulated
// text there, so nothing here reassembles deltas, and a partial chunk is only
// ever a prefix of a message that arrives again in full.
//
// Everything else is ignored on purpose. runner.completion is a control event
// with no content; a tool call is not done; a tool result is a tool.response
// carrying the tool role. None of them is an answer, and treating any of them
// as one would reply to a user with a fragment of the platform's own
// bookkeeping.
//
// # Why draining continues after the answer
//
// The stream is drained to the end regardless, because the Runtime lease is
// what keeps the Runner alive and a caller that stops reading strands it. An
// error after the answer therefore still arrives, and it fails the Run: the
// framework reports a failure it could not attribute to an earlier event on the
// terminal one, so a text collected before it is not an answer that was
// completed.
type replyCollector struct {
	text   string
	events int32
	failed bool
}

// observe folds one event in. It is called for every event, including the ones
// it ignores, because the count is the Run's own accounting.
func (c *replyCollector) observe(e *event.Event) {
	c.events++
	if e == nil || e.Response == nil {
		return
	}
	response := e.Response
	if response.Error != nil {
		c.failed = true
		return
	}
	if response.Object != model.ObjectTypeChatCompletion ||
		!response.Done || response.IsPartial || len(response.Choices) == 0 {
		return
	}
	message := response.Choices[0].Message
	if message.Role != model.RoleAssistant ||
		len(message.ToolCalls) > 0 || message.ToolID != "" {
		return
	}
	c.text = message.Content
}

// answer returns the collected final answer, empty when there is none. A failed
// stream has no answer even if a completion arrived before the failure.
//
// It is the model's text as it stands. Bounding it to what the channel will
// take is boundReply below, which the consumer applies before the answer is
// stored — the collector does not know which channel is going to carry it.
func (c *replyCollector) answer() string {
	if c.failed {
		return ""
	}
	return c.text
}

// boundReply makes model output storable and sendable on one channel.
//
// Two limits apply. The channel refuses a reply over limit bytes — its own
// number, declared by its adapter and checked when the consumer is built — so a
// longer answer is cut and marked rather than dropped or split: this slice
// sends one final reply per message, and a second frame would be a second
// answer. The Store refuses control characters in a body, so they are removed
// here: an answer that could not be stored would fail a Run that had in fact
// succeeded.
func boundReply(text string, limit int) string {
	cleaned := strings.Map(func(r rune) rune {
		switch {
		// The three the Store allows in a body, and the only ones a reply has
		// any use for.
		case r == '\t', r == '\n', r == '\r':
			return r
		case r < 0x20, r == 0x7f:
			return -1
		default:
			return r
		}
	}, strings.ToValidUTF8(text, ""))
	if len(cleaned) <= limit {
		return cleaned
	}
	cut := cleaned[:limit-len(truncationNotice)]
	// The cut lands anywhere, including inside a multi-byte rune. Dropping the
	// trailing partial one keeps the result valid UTF-8, which both the Store
	// and the protocol require.
	for len(cut) > 0 {
		r, size := utf8.DecodeLastRuneInString(cut)
		if r != utf8.RuneError || size > 1 {
			break
		}
		cut = cut[:len(cut)-1]
	}
	return cut + truncationNotice
}
