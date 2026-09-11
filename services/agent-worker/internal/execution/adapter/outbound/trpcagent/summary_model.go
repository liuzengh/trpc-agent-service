package trpcagent

import (
	"context"
	"sync"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

// summaryUsageModel accounts for summary calls that do not appear in Runner
// events. Provider error bodies never escape into the Session overlay or logs.
// Each request reports cumulative usage once, not a sum of streamed chunks.
type summaryUsageModel struct {
	model.Model
	mu        sync.Mutex
	total     Usage
	transport *modelTransport
	failure   error
}

func (m *summaryUsageModel) GenerateContent(ctx context.Context, req *model.Request) (<-chan *model.Response, error) {
	// Summary requests are non-streaming. Finish the owned HTTP call before
	// handing the SDK its response; Done may otherwise end SDK consumption
	// before usage accounting and transport cleanup finish.
	copyRequest := *req
	copyRequest.GenerationConfig.Stream = false
	responses, err := m.Model.GenerateContent(ctx, &copyRequest)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, m.modelFailure()
	}
	var collected []*model.Response
	var usage Usage
	var failed bool
	for response := range responses {
		if response != nil {
			if response.Usage != nil {
				usage = Usage{InputTokens: response.Usage.PromptTokens, OutputTokens: response.Usage.CompletionTokens, TotalTokens: response.Usage.TotalTokens}
			}
			if response.Error != nil {
				failed = true
			}
		}
		collected = append(collected, response)
	}
	m.mu.Lock()
	m.total.InputTokens += usage.InputTokens
	m.total.OutputTokens += usage.OutputTokens
	m.total.TotalTokens += usage.TotalTokens
	m.mu.Unlock()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if failed {
		return nil, m.modelFailure()
	}
	out := make(chan *model.Response, len(collected))
	for _, response := range collected {
		out <- response
	}
	close(out)
	return out, nil
}

func (m *summaryUsageModel) usage() Usage {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.total
}

func (m *summaryUsageModel) modelFailure() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failure = ErrModel
	if m.transport.retryable() {
		m.failure = ErrRetryableModel
	}
	return m.failure
}
func (m *summaryUsageModel) err() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.failure
}
