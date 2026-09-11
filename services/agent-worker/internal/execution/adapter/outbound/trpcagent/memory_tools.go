package trpcagent

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

var ErrMemoryTool = errors.New("memory tool execution failed")

// These callbacks enforce the existing published call budget and observe SDK
// errors. They do not implement tool CRUD, parse arguments, or export contents.
type memoryToolState struct {
	allowed        map[string]bool
	limit          int64
	calls          atomic.Int64
	sharedCalls    *atomic.Int64
	failed         atomic.Bool
	approvalFailed atomic.Bool
	tracer         trace.Tracer
	approval       *approvalState
}

func newMemoryToolState(names []string, limit int64, tracer trace.Tracer) *memoryToolState {
	s := &memoryToolState{allowed: make(map[string]bool), limit: limit, tracer: tracer}
	for _, name := range names {
		s.allowed[name] = true
	}
	return s
}
func (s *memoryToolState) callbacks() *tool.Callbacks {
	return &tool.Callbacks{
		BeforeTool: []tool.BeforeToolCallbackStructured{func(ctx context.Context, a *tool.BeforeToolArgs) (*tool.BeforeToolResult, error) {
			counter := &s.calls
			if s.sharedCalls != nil {
				counter = s.sharedCalls
			}
			if a == nil || !s.allowed[a.ToolName] || counter.Add(1) > s.limit {
				s.failed.Store(true)
				return nil, ErrMemoryTool
			}
			ctx, _ = telemetrytrace.Start(s.tracer, ctx, "worker.tool.call", trace.WithAttributes(attribute.String("app.tool.name", a.ToolName)))
			if s.approval != nil {
				result, err := s.approval.before(ctx, a)
				if err != nil || result != nil {
					return result, err
				}
			}
			return &tool.BeforeToolResult{Context: ctx}, nil
		}},
		AfterTool: []tool.AfterToolCallbackStructured{func(ctx context.Context, a *tool.AfterToolArgs) (*tool.AfterToolResult, error) {
			var err error
			if a == nil || a.Error != nil {
				err = ErrMemoryTool
			}
			telemetrytrace.End(trace.SpanFromContext(ctx), err)
			if s.approval != nil {
				if approvalErr := s.approval.after(ctx, a); approvalErr != nil {
					s.approvalFailed.Store(true)
					return nil, approvalErr
				}
			}
			// Preserve SDK tool-level errors for the model's correction loop.
			// Only selection/budget violations fail the whole Attempt here.
			return nil, nil
		}},
	}
}
