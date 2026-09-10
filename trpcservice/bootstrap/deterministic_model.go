package bootstrap

import (
	"context"
	"errors"

	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

const deterministicDemoResponse = "Hello from the tRPC Agent Service demo."

// deterministicModel is an offline development model used only when the
// explicit demo bootstrap mode is enabled. It never reads credentials or
// performs network I/O.
type deterministicModel struct {
	model string
}

func (model deterministicModel) Info() trpcmodel.Info {
	return trpcmodel.Info{Name: model.model}
}

func (model deterministicModel) GenerateContent(ctx context.Context, request *trpcmodel.Request) (<-chan *trpcmodel.Response, error) {
	if ctx == nil {
		return nil, errors.New("deterministic model context is required")
	}
	if request == nil {
		return nil, errors.New("deterministic model request is required")
	}
	responses := make(chan *trpcmodel.Response, 1)
	go func() {
		defer close(responses)
		select {
		case <-ctx.Done():
			return
		default:
		}
		select {
		case responses <- &trpcmodel.Response{
			Object:  "chat.completion",
			Choices: []trpcmodel.Choice{{Message: trpcmodel.NewAssistantMessage(deterministicDemoResponse)}},
			Done:    true,
		}:
		case <-ctx.Done():
		}
	}()
	return responses, nil
}
