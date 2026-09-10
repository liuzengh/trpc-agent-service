// Package mockmodel provides a deterministic model for the P0 local slice.
package mockmodel

import (
	"context"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	agentmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

type Model struct {
	mu    sync.Mutex
	calls map[string]int
}

func New() *Model { return &Model{calls: make(map[string]int)} }
func (m *Model) Generate(ctx context.Context, e runtime.ExecutionEnvelope, _ profile.ExecutionProfileSnapshot) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := e.TenantID + "\x00" + e.RequestID
	m.calls[key]++
	return "mock-result://" + e.TenantID + "/" + e.RequestID, nil
}

func (m *Model) GenerateContent(ctx context.Context, request *agentmodel.Request) (<-chan *agentmodel.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	execution, ok := runtime.ExecutionContextFrom(ctx)
	if !ok {
		return nil, runtime.ErrTenantScope
	}
	m.mu.Lock()
	m.calls[execution.TenantID+"\x00"+execution.RequestID]++
	m.mu.Unlock()
	responses := make(chan *agentmodel.Response, 1)
	responses <- &agentmodel.Response{
		ID:        "mock-response:" + execution.RequestID,
		Object:    agentmodel.ObjectTypeChatCompletion,
		Model:     "mock",
		Done:      true,
		Timestamp: time.Now().UTC(),
		Choices: []agentmodel.Choice{{
			Index:   0,
			Message: agentmodel.NewAssistantMessage("mock-result://" + execution.TenantID + "/" + execution.RequestID),
		}},
	}
	close(responses)
	return responses, nil
}

func (m *Model) Info() agentmodel.Info { return agentmodel.Info{Name: "mock"} }

func (m *Model) Calls(tenantID, requestID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls[tenantID+"\x00"+requestID]
}

var _ agentmodel.Model = (*Model)(nil)
