package guardrail

import (
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

// SanitizeEvent returns a copy suitable for durable journals and client
// projections. The framework keeps the original event for internal tool/model
// execution; only the externalized copy is changed.
func SanitizeEvent(input *event.Event) (*event.Event, Decision) {
	if input == nil {
		return nil, Decision{}
	}
	output := *input
	output.ID = input.ID
	output.Response = input.Response.Clone()
	if output.Response == nil {
		return &output, Decision{}
	}
	output.Response.Choices = make([]model.Choice, len(input.Response.Choices))
	decision := Decision{}
	for i, choice := range input.Response.Choices {
		output.Response.Choices[i] = choice
		var messageDecision Decision
		output.Response.Choices[i].Message, messageDecision = sanitizeMessage(choice.Message)
		decision = mergeDecision(decision, messageDecision)
		output.Response.Choices[i].Delta, messageDecision = sanitizeMessage(choice.Delta)
		decision = mergeDecision(decision, messageDecision)
	}
	if output.Response.Error != nil {
		errorValue := *output.Response.Error
		messageDecision := SanitizeOutput(errorValue.Message)
		errorValue.Message = safeDecisionText(messageDecision)
		decision = mergeDecision(decision, messageDecision)
		output.Response.Error = &errorValue
	}
	return &output, decision
}

func sanitizeMessage(input model.Message) (model.Message, Decision) {
	output := input
	decision := Decision{}
	for _, field := range []*string{&output.Content, &output.ReasoningContent} {
		fieldDecision := SanitizeOutput(*field)
		*field = safeDecisionText(fieldDecision)
		decision = mergeDecision(decision, fieldDecision)
	}
	if len(input.ToolCalls) != 0 {
		output.ToolCalls = make([]model.ToolCall, len(input.ToolCalls))
		copy(output.ToolCalls, input.ToolCalls)
		for i, call := range input.ToolCalls {
			output.ToolCalls[i].Function.Arguments = append([]byte(nil), call.Function.Arguments...)
			fieldDecision := SanitizeOutput(string(call.Function.Arguments))
			if fieldDecision.Text != string(call.Function.Arguments) {
				output.ToolCalls[i].Function.Arguments = []byte(fieldDecision.Text)
			}
			decision = mergeDecision(decision, fieldDecision)
		}
	}
	if len(input.ContentParts) != 0 {
		output.ContentParts = make([]model.ContentPart, len(input.ContentParts))
		for i, part := range input.ContentParts {
			output.ContentParts[i] = part
			if part.Text != nil {
				text := *part.Text
				fieldDecision := SanitizeOutput(text)
				text = safeDecisionText(fieldDecision)
				output.ContentParts[i].Text = &text
				decision = mergeDecision(decision, fieldDecision)
			}
			if part.ContentRef != nil {
				contentRef := *part.ContentRef
				fieldDecision := SanitizeOutput(contentRef.OriginalName)
				contentRef.OriginalName = safeDecisionText(fieldDecision)
				decision = mergeDecision(decision, fieldDecision)
				output.ContentParts[i].ContentRef = &contentRef
			}
			if part.File != nil {
				file := *part.File
				fieldDecision := SanitizeOutput(file.Name)
				file.Name = safeDecisionText(fieldDecision)
				decision = mergeDecision(decision, fieldDecision)
				file.URL = ""
				file.FileID = ""
				file.Data = nil
				output.ContentParts[i].File = &file
			}
			if part.Image != nil {
				image := *part.Image
				image.URL = ""
				image.Data = nil
				output.ContentParts[i].Image = &image
			}
			if part.Audio != nil {
				audio := *part.Audio
				audio.URL = ""
				audio.Data = nil
				output.ContentParts[i].Audio = &audio
			}
			if part.Video != nil {
				video := *part.Video
				video.URL = ""
				video.Data = nil
				output.ContentParts[i].Video = &video
			}
		}
	}
	return output, decision
}

func safeDecisionText(decision Decision) string {
	if decision.Text == "" && decision.Blocked {
		return "回复已被安全策略拦截。"
	}
	return decision.Text
}

func mergeDecision(current, next Decision) Decision {
	if next.Text == "" && !next.Redacted && !next.Blocked {
		return current
	}
	if current.RuleID == "" {
		current.RuleID = next.RuleID
		current.Reason = SafeReason(next.Reason)
	}
	current.Redacted = current.Redacted || next.Redacted
	current.Blocked = current.Blocked || next.Blocked
	return current
}
