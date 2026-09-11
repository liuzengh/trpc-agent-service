// Package testutil provides deterministic doubles for platform tests.
package testutil

import (
	"context"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

// FakeModel is a deterministic in-memory model for tests. It never performs a
// network request and returns the configured reply as one final response.
type FakeModel struct {
	reply string
}

// NewFakeModel constructs a deterministic model with the supplied final reply.
func NewFakeModel(reply string) *FakeModel {
	return &FakeModel{reply: reply}
}

// GenerateContent returns a single completed assistant response.
func (m *FakeModel) GenerateContent(ctx context.Context, _ *model.Request) (<-chan *model.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	responses := make(chan *model.Response, 1)
	responses <- &model.Response{
		ID:     "fake-response",
		Object: "chat.completion",
		Model:  m.Info().Name,
		Choices: []model.Choice{
			{
				Index:        0,
				Message:      model.NewAssistantMessage(m.reply),
				FinishReason: model.StringPtr("stop"),
			},
		},
		Done: true,
	}
	close(responses)
	return responses, nil
}

// Info identifies this test-only model.
func (m *FakeModel) Info() model.Info {
	return model.Info{Name: "fake-model", ContextWindow: 4096}
}
