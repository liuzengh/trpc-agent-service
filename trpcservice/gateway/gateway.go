// Package gateway provides the transport-neutral asynchronous submission boundary.
package gateway

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var (
	ErrInvalidRequest = errors.New("gateway: invalid request")
	ErrQueueRejected  = errors.New("gateway: queue rejected job")
	ErrQueueFailure   = errors.New("gateway: enqueue failed")
)

type GatewayRequest struct {
	TenantContext tenant.TenantContext
	Agent         agent.AgentSpec
	History       []agent.Message
	Input         agent.Message
	Deadline      time.Time
}

type Accepted struct {
	Accepted    bool
	JobID       string
	ExecutionID string
	RequestID   string
	TraceID     string
	ReceiptID   string
}

type Gateway struct {
	queue queue.JobQueue
}

func New(q queue.JobQueue) (*Gateway, error) {
	if q == nil {
		return nil, errors.New("gateway: queue is required")
	}
	return &Gateway{queue: q}, nil
}

func (g *Gateway) Submit(ctx context.Context, request GatewayRequest) (Accepted, error) {
	if ctx == nil {
		return Accepted{}, ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return Accepted{}, err
	}
	if err := validateRequest(request); err != nil {
		return Accepted{}, err
	}
	now := time.Now().UTC()
	jobID := uuid.NewString()
	executionID := uuid.NewString()
	tc := request.TenantContext
	history := make([]queue.MessageDTO, len(request.History))
	for i, message := range request.History {
		history[i] = messageDTO(message)
	}
	job := queue.AgentJob{
		SchemaVersion: queue.SchemaVersion,
		JobID:         jobID,
		ExecutionID:   executionID,
		Tenant:        queue.TenantContextDTOFromContext(tc),
		Agent:         queue.AgentRefDTO{TenantID: request.Agent.TenantID, AgentAppID: request.Agent.AgentAppID, Version: request.Agent.Version},
		History:       history,
		Message:       messageDTO(request.Input),
		Trace:         queue.TraceContextDTO{TraceID: tc.TraceID, RequestID: tc.RequestID, MessageID: tc.MessageID, ExecutionID: executionID},
		CreatedAt:     now,
		Deadline:      request.Deadline,
		Attempt:       1,
	}
	if err := job.ValidateAt(now, queue.DefaultJobMaxAge); err != nil {
		return Accepted{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	receipt, err := g.queue.Enqueue(ctx, job)
	if err != nil {
		return Accepted{}, fmt.Errorf("%w: %w", ErrQueueFailure, err)
	}
	if !receipt.Accepted {
		return Accepted{}, fmt.Errorf("%w: job=%s", ErrQueueRejected, jobID)
	}
	return Accepted{Accepted: true, JobID: jobID, ExecutionID: executionID, RequestID: tc.RequestID, TraceID: tc.TraceID, ReceiptID: receipt.ReceiptID}, nil
}

func validateRequest(request GatewayRequest) error {
	if err := request.TenantContext.Validate(); err != nil {
		return fmt.Errorf("%w: tenant context: %v", ErrInvalidRequest, err)
	}
	if strings.TrimSpace(request.Agent.TenantID) == "" || strings.TrimSpace(request.Agent.AgentAppID) == "" || request.Agent.Version < 1 || strings.TrimSpace(request.Agent.Name) == "" || strings.TrimSpace(request.Agent.ModelProvider) == "" {
		return fmt.Errorf("%w: agent specification is incomplete", ErrInvalidRequest)
	}
	if request.Agent.TenantID != request.TenantContext.TenantID || request.Agent.AgentAppID != request.TenantContext.AgentAppID || request.Agent.Version != request.TenantContext.ConfigVersion {
		return fmt.Errorf("%w: agent and tenant configuration differ", ErrInvalidRequest)
	}
	if request.Input.ID != request.TenantContext.MessageID || strings.TrimSpace(request.Input.Content) == "" {
		return fmt.Errorf("%w: input message does not match tenant context", ErrInvalidRequest)
	}
	if request.Deadline.IsZero() || !request.Deadline.After(time.Now()) || request.Deadline.After(time.Now().Add(queue.DefaultJobMaxAge)) {
		return fmt.Errorf("%w: deadline is outside the allowed window", ErrInvalidRequest)
	}
	for _, message := range request.History {
		if strings.TrimSpace(message.Content) == "" || message.ID == "" {
			return fmt.Errorf("%w: history message is invalid", ErrInvalidRequest)
		}
	}
	return nil
}

func messageDTO(message agent.Message) queue.MessageDTO {
	calls := make([]queue.ToolCallDTO, len(message.ToolCalls))
	for i, call := range message.ToolCalls {
		calls[i] = queue.ToolCallDTO{ID: call.ID, Name: call.Name, Arguments: call.Arguments}
	}
	return queue.MessageDTO{ID: message.ID, Role: message.Role, Content: message.Content, CreatedAt: message.CreatedAt, ToolID: message.ToolID, ToolName: message.ToolName, ToolCalls: calls}
}
