package assembly

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/safego"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

const modelAuditTimeout = 2 * time.Second

type auditedModel struct {
	inner  model.Model
	audits storage.AuditRecorder
}

func newAuditedModel(inner model.Model, audits storage.AuditRecorder) model.Model {
	if inner == nil || audits == nil {
		return inner
	}
	return &auditedModel{inner: inner, audits: audits}
}

func (m *auditedModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	invocation, ok := governance.InvocationFromContext(ctx)
	if !ok {
		return m.inner.GenerateContent(ctx, request)
	}
	execution := invocation.Execution
	started := time.Now().UTC()
	m.record(ctx, execution, "model.requested", "running", "", 0, started)

	responses, err := m.inner.GenerateContent(ctx, request)
	if err != nil {
		m.recordDetached(ctx, execution, "model.failed", "failed", "provider_error", time.Since(started), time.Now().UTC())
		return nil, err
	}

	forwarded := make(chan *model.Response)
	safego.Go("model audit response forwarder", func() {
		defer close(forwarded)
		for response := range responses {
			select {
			case forwarded <- response:
			case <-ctx.Done():
				m.recordDetached(ctx, execution, "model.failed", "failed", "context_canceled", time.Since(started), time.Now().UTC())
				return
			}
		}
		result, errorType := "completed", ""
		if err := ctx.Err(); err != nil {
			result = "failed"
			errorType = "context_canceled"
		}
		action := "model.completed"
		if result == "failed" {
			action = "model.failed"
		}
		m.recordDetached(ctx, execution, action, result, errorType, time.Since(started), time.Now().UTC())
	})
	return forwarded, nil
}

func (m *auditedModel) GenerateContentIter(ctx context.Context, request *model.Request) (model.Seq[*model.Response], error) {
	invocation, ok := governance.InvocationFromContext(ctx)
	if !ok {
		return modelSequence(ctx, m.inner, request)
	}
	execution := invocation.Execution
	started := time.Now().UTC()
	m.record(ctx, execution, "model.requested", "running", "", 0, started)

	responses, err := modelSequence(ctx, m.inner, request)
	if err != nil {
		m.recordDetached(ctx, execution, "model.failed", "failed", "provider_error", time.Since(started), time.Now().UTC())
		return nil, err
	}
	return func(yield func(*model.Response) bool) {
		responses(yield)
		result, errorType := "completed", ""
		if err := ctx.Err(); err != nil {
			result = "failed"
			errorType = "context_canceled"
		}
		action := "model.completed"
		if result == "failed" {
			action = "model.failed"
		}
		m.recordDetached(ctx, execution, action, result, errorType, time.Since(started), time.Now().UTC())
	}, nil
}

func (m *auditedModel) Info() model.Info { return m.inner.Info() }

func (m *auditedModel) record(ctx context.Context, execution governance.ExecutionContext, action, result, errorType string, latency time.Duration, createdAt time.Time) {
	_ = m.audits.RecordAudit(ctx, storage.AuditEvent{
		TenantID: execution.TenantID, TraceID: execution.TraceID, RequestID: execution.RequestID,
		Channel: execution.Channel, UserID: execution.UserID, SessionID: execution.SessionID,
		AgentName: execution.AgentName, Action: action, Result: result, Decision: result,
		LatencyMS: latency.Milliseconds(), ErrorType: errorType, CreatedAt: createdAt,
	})
}

func (m *auditedModel) recordDetached(parent context.Context, execution governance.ExecutionContext, action, result, errorType string, latency time.Duration, createdAt time.Time) {
	base := context.Background()
	if parent != nil {
		base = context.WithoutCancel(parent)
	}
	ctx, cancel := context.WithTimeout(base, modelAuditTimeout)
	defer cancel()
	m.record(ctx, execution, action, result, errorType, latency, createdAt)
}

var _ model.Model = (*auditedModel)(nil)
var _ model.IterModel = (*auditedModel)(nil)
