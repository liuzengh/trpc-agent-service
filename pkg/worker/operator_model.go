package worker

import (
	"context"
	"errors"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

// Compatible providers may return arbitrary error bodies, including request
// URLs or credentials. Preserve successful data and usage, but expose stable
// error fields to Runner, Summary, audit and telemetry.
type operatorEndpointModel struct{ base model.Model }

func (m *operatorEndpointModel) Info() model.Info { return m.base.Info() }

func (m *operatorEndpointModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	callCtx, cancel := context.WithCancel(ctx)
	input, err := m.base.GenerateContent(callCtx, request)
	if err != nil {
		cancel()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("operator model invocation failed")
	}
	if input == nil {
		cancel()
		return nil, errors.New("operator model response unavailable")
	}
	output := make(chan *model.Response, 1)
	go func() {
		defer close(output)
		defer cancel()
		forward := true
		for response := range input {
			if response != nil && response.Error != nil {
				copy := *response
				copy.Error = &model.ResponseError{Type: "model_provider_error", Message: "operator model request failed"}
				response = &copy
			}
			if forward {
				select {
				case output <- response:
				case <-ctx.Done():
					forward = false
					cancel()
				}
			}
		}
	}()
	return output, nil
}
