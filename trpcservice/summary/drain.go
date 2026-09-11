package summary

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

// drain consumes a non-streaming model call: the framework's Model interface
// is channel-shaped even when the request asks for a whole answer, so the
// summary path collects the channel instead of pretending there is a second
// interface.
func drain(ctx context.Context, m model.Model, req *model.Request) (string, error) {
	ch, err := m.GenerateContent(ctx, req)
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	for resp := range ch {
		if resp == nil {
			continue
		}
		if resp.Error != nil {
			return "", fmt.Errorf("model error: %s", resp.Error.Message)
		}
		for _, choice := range resp.Choices {
			if choice.Message.Content != "" {
				sb.WriteString(choice.Message.Content)
				continue
			}
			sb.WriteString(choice.Delta.Content)
		}
	}
	return sb.String(), nil
}

// buildPrompt renders the existing summary plus the uncovered events. The
// instruction is explicit that the reply is a summary, not an answer.
func buildPrompt(existing string, events []eventLine) string {
	var sb strings.Builder
	sb.WriteString("Summarize the conversation below. Keep durable facts (names, decisions, numbers, preferences); drop small talk. ")
	sb.WriteString("Reply with the summary text only.\n\n")
	if strings.TrimSpace(existing) != "" {
		sb.WriteString("Existing summary:\n")
		sb.WriteString(existing)
		sb.WriteString("\n\n")
	}
	sb.WriteString("New messages:\n")
	for _, e := range events {
		sb.WriteString(fmt.Sprintf("- [%s] %s\n", e.author, e.text()))
	}
	return sb.String()
}

func (e eventLine) text() string { return e.payload }

// extractText pulls readable text out of a stored framework event. The
// payload is the event's JSON; the shapes that matter here are a final
// response's message content and a streaming chunk's delta. Anything else
// degrades to the raw payload trimmed to something prompt-sized.
func extractText(payload string) string {
	var parsed struct {
		Response struct {
			Choices []struct {
				Message struct {
					Content string `json:"content"`
				} `json:"message"`
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
		} `json:"response"`
	}
	if err := json.Unmarshal([]byte(payload), &parsed); err == nil {
		for _, c := range parsed.Response.Choices {
			if c.Message.Content != "" {
				return c.Message.Content
			}
			if c.Delta.Content != "" {
				return c.Delta.Content
			}
		}
	}
	if len(payload) > 500 {
		return payload[:500]
	}
	return payload
}

func jsonUnmarshal(raw []byte, out any) error {
	if len(raw) == 0 {
		return fmt.Errorf("summary: job payload is empty")
	}
	return json.Unmarshal(raw, out)
}
