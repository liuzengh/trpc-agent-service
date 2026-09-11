package assembly

import (
	"context"
	"fmt"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/backendhealth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/safego"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

const modelBackendDomain = "model"

type healthModel struct {
	delegate model.Model
	registry *backendhealth.Registry
	key      backendhealth.Key
}

func observeModel(delegate model.Model, registry *backendhealth.Registry, providerID, driver string) model.Model {
	if delegate == nil || registry == nil || strings.TrimSpace(providerID) == "" || !backendhealth.ShouldProtect(driver) {
		return delegate
	}
	return &healthModel{
		delegate: delegate,
		registry: registry,
		key:      backendhealth.Key{ProfileID: providerID, Domain: modelBackendDomain, Driver: driver},
	}
}

func (m *healthModel) Info() model.Info { return m.delegate.Info() }

func (m *healthModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	permit, err := m.registry.Begin(ctx, m.key, "generate_content")
	if err != nil {
		return nil, err
	}
	responses, err := m.delegate.GenerateContent(ctx, request)
	if err != nil {
		permit.Done(err)
		return nil, err
	}
	output := make(chan *model.Response, 1)
	safego.Go("model backend health stream", func() {
		defer close(output)
		var operationErr error
		for response := range responses {
			if responseErr := modelResponseOperationError(response); responseErr != nil {
				operationErr = responseErr
			}
			select {
			case output <- response:
			case <-ctx.Done():
				permit.Done(ctx.Err())
				return
			}
		}
		permit.Done(operationErr)
	})
	return output, nil
}

func (m *healthModel) GenerateContentIter(ctx context.Context, request *model.Request) (model.Seq[*model.Response], error) {
	permit, err := m.registry.Begin(ctx, m.key, "generate_content")
	if err != nil {
		return nil, err
	}
	sequence, err := modelSequence(ctx, m.delegate, request)
	if err != nil {
		permit.Done(err)
		return nil, err
	}
	return func(yield func(*model.Response) bool) {
		var operationErr error
		sequence(func(response *model.Response) bool {
			if responseErr := modelResponseOperationError(response); responseErr != nil {
				operationErr = responseErr
			}
			return yield(response)
		})
		permit.Done(operationErr)
	}, nil
}

func modelResponseOperationError(response *model.Response) error {
	if response == nil || response.Error == nil {
		return nil
	}
	responseErr := response.Error
	code := ""
	if responseErr.Code != nil {
		code = strings.ToLower(strings.TrimSpace(*responseErr.Code))
	}
	typeName := strings.ToLower(strings.TrimSpace(responseErr.Type))
	for _, transientCode := range []string{"429", "500", "502", "503", "504", "rate_limit", "rate_limit_exceeded", "server_error", "service_unavailable"} {
		if code == transientCode {
			return fmt.Errorf("model backend service unavailable (%s): %w", code, responseErr)
		}
	}
	if strings.Contains(typeName, "rate") || strings.Contains(typeName, "stream") || strings.Contains(typeName, "server") || strings.Contains(typeName, "unavailable") || strings.Contains(typeName, "overload") {
		return fmt.Errorf("model backend service unavailable (%s): %w", typeName, responseErr)
	}
	return responseErr
}

var _ model.IterModel = (*healthModel)(nil)
