package ingress

import (
	"context"
	"strings"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

type VerifiedIngress struct {
	Binding     channel.VerifiedBinding
	Events      []channel.ProviderEvent
	TraceParent string
}

// Service owns the provider-neutral ordering boundary. No ProviderEvent is
// returned until the public route, scoped verification, and promotion all
// succeed exactly once.
type Service struct {
	Adapter   channel.Adapter
	Bindings  channel.IngressBindingResolver
	Telemetry telemetry.Provider
}

func (s Service) VerifyAndDecode(ctx context.Context, request channel.CallbackRequest) (result VerifiedIngress, resultErr error) {
	if s.Adapter == nil || s.Bindings == nil {
		return VerifiedIngress{}, runtime.ErrInvariantViolation
	}
	hint, err := s.Adapter.PublicRoute(ctx, request)
	if err != nil {
		return VerifiedIngress{}, err
	}
	if hint.Channel == "" || hint.Channel != s.Adapter.ID() || hint.RouteKeyDigest == "" || hint.IngressAttemptID == "" {
		return VerifiedIngress{}, runtime.ErrVersionMismatch
	}
	traceParent := hint.TraceParent
	if traceParent == "" {
		traceParent = headerValue(request.Headers, "traceparent")
	}
	ctx, finish := telemetry.StartOperation(ctx, s.Telemetry, traceParent, telemetry.OperationChannelIngress,
		telemetry.ComponentAttribute(telemetry.ComponentChannelIngress))
	defer func() { finish(resultErr) }()
	candidate, err := s.Bindings.ResolveCandidate(ctx, hint)
	if err != nil {
		return VerifiedIngress{}, err
	}
	handle, err := s.Bindings.AcquireVerifier(ctx, candidate)
	if err != nil {
		return VerifiedIngress{}, err
	}
	defer func() { _ = handle.Close() }()
	callback, receipt, err := s.Adapter.Verify(ctx, request, handle)
	if err != nil {
		return VerifiedIngress{}, err
	}
	if callback.ProtocolIdentityDigest == "" || callback.ProtocolIdentityDigest != receipt.ProtocolIdentityDigest {
		return VerifiedIngress{}, runtime.ErrVersionMismatch
	}
	binding, err := s.Bindings.PromoteVerified(ctx, candidate, receipt)
	if err != nil {
		return VerifiedIngress{}, err
	}
	events, err := s.Adapter.Decode(ctx, callback)
	if err != nil {
		return VerifiedIngress{}, err
	}
	if len(events) == 0 {
		return VerifiedIngress{}, runtime.ErrInvalidEnvelope
	}
	for _, event := range events {
		if event.SchemaVersion != 1 || event.Channel != hint.Channel || event.ExternalAccountID == "" ||
			event.ExternalMessageID == "" || event.ExternalUserID == "" || event.OccurredAt.IsZero() {
			return VerifiedIngress{}, runtime.ErrInvalidEnvelope
		}
	}
	return VerifiedIngress{Binding: binding, Events: append([]channel.ProviderEvent(nil), events...),
		TraceParent: telemetry.EffectiveTraceParent(ctx, traceParent)}, nil
}

func headerValue(values map[string]string, key string) string {
	for name, value := range values {
		if strings.EqualFold(name, key) {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
