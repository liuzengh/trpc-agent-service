package privacy

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"io"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type guardedModel struct {
	next   model.Model
	tenant config.TenantConfig
}

func WrapModel(next model.Model, tenant config.TenantConfig) model.Model {
	if tenant.Privacy.Input == "" || tenant.Privacy.Input == "off" {
		return next
	}
	return &guardedModel{next: next, tenant: tenant}
}
func (m *guardedModel) Info() model.Info { return m.next.Info() }
func (m *guardedModel) GenerateContent(ctx context.Context, req *model.Request) (<-chan *model.Response, error) {
	if req == nil {
		return nil, ErrBlocked
	}
	copyReq := *req
	copyReq.Messages = append([]model.Message(nil), req.Messages...)
	for i, original := range req.Messages {
		value := original
		apply := func(text string) (string, error) {
			clean, _, err := Apply(m.tenant.Privacy.Input, text, m.tenant)
			return clean, err
		}
		var err error
		value.Content, err = apply(value.Content)
		if err != nil {
			return nil, err
		}
		value.ReasoningContent, err = apply(value.ReasoningContent)
		if err != nil {
			return nil, err
		}
		if value.ReasoningContent != original.ReasoningContent {
			value.ReasoningSignature = ""
		}
		value.ContentParts = append([]model.ContentPart(nil), original.ContentParts...)
		for j, part := range original.ContentParts {
			// Binary/media inspection is a separate pipeline. Do not silently allow
			// an uninspected attachment when text privacy is explicitly enabled.
			if part.Type != model.ContentTypeText || part.Image != nil || part.Audio != nil || part.Video != nil || part.File != nil || part.ContentRef != nil {
				return nil, ErrBlocked
			}
			if part.Text != nil {
				text, err := apply(*part.Text)
				if err != nil {
					return nil, err
				}
				value.ContentParts[j].Text = &text
			}
		}
		value.ToolCalls = append([]model.ToolCall(nil), original.ToolCalls...)
		for j, call := range original.ToolCalls {
			if len(call.Function.Arguments) == 0 {
				continue
			}
			decoder := json.NewDecoder(bytes.NewReader(call.Function.Arguments))
			decoder.UseNumber()
			var arguments any
			if decoder.Decode(&arguments) != nil {
				return nil, ErrBlocked
			}
			var tail any
			if decoder.Decode(&tail) != io.EOF {
				return nil, ErrBlocked
			}
			arguments, err = cleanJSON(arguments, apply)
			if err != nil {
				return nil, err
			}
			encoded, err := json.Marshal(arguments)
			if err != nil {
				return nil, ErrBlocked
			}
			value.ToolCalls[j].Function.Arguments = encoded
		}
		copyReq.Messages[i] = value
	}
	return m.next.GenerateContent(ctx, &copyReq)
}
func cleanJSON(value any, apply func(string) (string, error)) (any, error) {
	switch v := value.(type) {
	case string:
		return apply(v)
	case []any:
		for i, item := range v {
			clean, err := cleanJSON(item, apply)
			if err != nil {
				return nil, err
			}
			v[i] = clean
		}
	case json.Number:
		clean, err := apply(v.String())
		if err != nil {
			return nil, err
		}
		if clean != v.String() {
			return clean, nil
		}
	case map[string]any:
		result := make(map[string]any, len(v))
		for key, item := range v {
			cleanKey, err := apply(key)
			if err != nil {
				return nil, err
			}
			if _, exists := result[cleanKey]; exists {
				return nil, ErrBlocked
			}
			clean, err := cleanJSON(item, apply)
			if err != nil {
				return nil, err
			}
			result[cleanKey] = clean
		}
		return result, nil
	}
	return value, nil
}
