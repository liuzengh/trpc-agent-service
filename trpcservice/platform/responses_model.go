package platform

import (
	"context"
	"errors"
	"strings"
	"time"

	openai "github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
	"github.com/openai/openai-go/responses"
	"github.com/openai/openai-go/shared"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type responsesModel struct {
	client openai.Client
	name   string
}

func newResponsesModel(name, baseURL, apiKey string) model.Model {
	return &responsesModel{
		client: openai.NewClient(option.WithBaseURL(baseURL), option.WithAPIKey(apiKey)),
		name:   name,
	}
}

func (m *responsesModel) Info() model.Info {
	return model.Info{Name: m.name}
}

func (m *responsesModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	if request == nil {
		return nil, errors.New("model_request_required")
	}
	if len(request.Tools) != 0 {
		return nil, errors.New("responses_model_tools_unsupported")
	}
	params := responses.ResponseNewParams{
		Model: shared.ResponsesModel(m.name),
		Input: responses.ResponseNewParamsInputUnion{OfString: openai.String(responsesInput(request.Messages))},
	}
	if instructions := responsesInstructions(request.Messages); instructions != "" {
		params.Instructions = openai.String(instructions)
	}
	if request.MaxTokens != nil {
		params.MaxOutputTokens = openai.Int(int64(*request.MaxTokens))
	}
	if request.Temperature != nil {
		params.Temperature = openai.Float(*request.Temperature)
	}
	if request.TopP != nil {
		params.TopP = openai.Float(*request.TopP)
	}
	requestOptions := make([]option.RequestOption, 0, len(request.Headers))
	for key, value := range request.Headers {
		requestOptions = append(requestOptions, option.WithHeader(key, value))
	}
	stream := m.client.Responses.NewStreaming(ctx, params, requestOptions...)
	output := make(chan *model.Response)
	go func() {
		defer close(output)
		defer stream.Close()
		send := func(response *model.Response) bool {
			select {
			case output <- response:
				return true
			case <-ctx.Done():
				return false
			}
		}
		var content string
		for stream.Next() {
			event := stream.Current()
			switch event.Type {
			case "response.output_text.delta":
				content += event.Delta.OfString
				if !send(&model.Response{
					ID: event.ItemID, Object: model.ObjectTypeChatCompletionChunk, Model: m.name,
					Choices:   []model.Choice{{Index: 0, Delta: model.Message{Role: model.RoleAssistant, Content: event.Delta.OfString}}},
					Timestamp: time.Now(), IsPartial: true,
				}) {
					return
				}
			case "response.completed":
				finishReason := "stop"
				if !send(&model.Response{
					ID: event.Response.ID, Object: model.ObjectTypeChatCompletion, Created: int64(event.Response.CreatedAt), Model: string(event.Response.Model),
					Choices:   []model.Choice{{Index: 0, Message: model.Message{Role: model.RoleAssistant, Content: content}, FinishReason: &finishReason}},
					Usage:     &model.Usage{PromptTokens: int(event.Response.Usage.InputTokens), CompletionTokens: int(event.Response.Usage.OutputTokens), TotalTokens: int(event.Response.Usage.TotalTokens)},
					Timestamp: time.Now(), Done: true,
				}) {
					return
				}
			case "error", "response.failed", "response.incomplete":
				send(responsesStreamError())
				return
			}
		}
		if err := stream.Err(); err != nil {
			send(responsesStreamError())
		}
	}()
	return output, nil
}

func responsesStreamError() *model.Response {
	return &model.Response{
		Error:     &model.ResponseError{Message: "model_provider_stream_failed", Type: model.ErrorTypeStreamError},
		Timestamp: time.Now(),
		Done:      true,
	}
}

func responsesInstructions(messages []model.Message) string {
	var instructions []string
	for _, message := range messages {
		if message.Role == model.RoleSystem && message.Content != "" {
			instructions = append(instructions, message.Content)
		}
	}
	return strings.Join(instructions, "\n\n")
}

func responsesInput(messages []model.Message) string {
	var input []string
	for _, message := range messages {
		if message.Role == model.RoleSystem || message.Content == "" {
			continue
		}
		input = append(input, message.Role.String()+": "+message.Content)
	}
	return strings.Join(input, "\n")
}
