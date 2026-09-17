package gateway

import (
	"context"
	"encoding/json"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
)

// DurableRequestIDExtension preserves the service-owned durable execution ID
// when a protocol requires a different correlation ID on its wire response.
const DurableRequestIDExtension = "trpc.agent.gateway/durable-request-id/v1"

// CanonicalRunner prevents upstream protocol servers from turning request
// body/header aliases into execution identity. The invocation middleware has
// already authenticated the request and the bridge receives only those
// server-owned user/session values.
type CanonicalRunner struct{ Next runner.Runner }

func (r CanonicalRunner) Run(ctx context.Context, _ string, _ string, message model.Message, options ...agent.RunOption) (<-chan *event.Event, error) {
	if r.Next == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	trusted, ok := ServerInvocationFromContext(ctx)
	if !ok || trusted.UserID == "" || trusted.SessionID == "" || !trusted.CanRun {
		return nil, ErrUnauthenticated
	}
	events, err := r.Next.Run(ctx, trusted.UserID, trusted.SessionID, message, options...)
	if err != nil || events == nil || trusted.Protocol != "trpc-agent" {
		return events, err
	}
	protocolRequestID := agent.NewRunOptions(options...).RequestID
	if protocolRequestID == "" {
		return events, nil
	}
	out := make(chan *event.Event)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case value, ok := <-events:
				if !ok {
					return
				}
				adapted := protocolEvent(value, protocolRequestID)
				select {
				case <-ctx.Done():
					return
				case out <- adapted:
				}
			}
		}
	}()
	return out, nil
}

func protocolEvent(value *event.Event, protocolRequestID string) *event.Event {
	if value == nil {
		return nil
	}
	adapted := *value
	adapted.Extensions = make(map[string]json.RawMessage, len(value.Extensions)+1)
	for key, raw := range value.Extensions {
		adapted.Extensions[key] = append(json.RawMessage(nil), raw...)
	}
	if value.RequestID != "" && value.RequestID != protocolRequestID {
		encoded, _ := json.Marshal(value.RequestID)
		adapted.Extensions[DurableRequestIDExtension] = encoded
	}
	adapted.RequestID = protocolRequestID
	return &adapted
}

func (r CanonicalRunner) Close() error {
	if r.Next == nil {
		return runtime.ErrCapabilityUnsupported
	}
	return r.Next.Close()
}

var _ runner.Runner = CanonicalRunner{}
