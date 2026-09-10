package ingress

import (
	"context"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

const maxChallengeResponseBytes = 64 << 10

// ChallengeService applies the same pre-tenant, single-use verification and
// promotion boundary as callback ingress without creating an Inbox fact.
type ChallengeService struct {
	Adapter  channel.HTTPAdapter
	Bindings channel.IngressBindingResolver
}

func (s ChallengeService) Verify(ctx context.Context, request channel.CallbackRequest) (channel.HTTPResponse, error) {
	if s.Adapter == nil || s.Bindings == nil {
		return channel.HTTPResponse{}, runtime.ErrInvariantViolation
	}
	hint, err := s.Adapter.PublicChallengeRoute(ctx, request)
	if err != nil {
		return channel.HTTPResponse{}, err
	}
	if hint.Channel == "" || hint.Channel != s.Adapter.ID() || hint.RouteKeyDigest == "" || hint.IngressAttemptID == "" {
		return channel.HTTPResponse{}, runtime.ErrVersionMismatch
	}
	candidate, err := s.Bindings.ResolveCandidate(ctx, hint)
	if err != nil {
		return channel.HTTPResponse{}, err
	}
	handle, err := s.Bindings.AcquireVerifier(ctx, candidate)
	if err != nil {
		return channel.HTTPResponse{}, err
	}
	defer func() { _ = handle.Close() }()
	response, receipt, err := s.Adapter.VerifyChallenge(ctx, request, handle)
	if err != nil {
		return channel.HTTPResponse{}, err
	}
	if receipt.ProtocolIdentityDigest == "" {
		return channel.HTTPResponse{}, runtime.ErrVersionMismatch
	}
	binding, err := s.Bindings.PromoteVerified(ctx, candidate, receipt)
	if err != nil {
		return channel.HTTPResponse{}, err
	}
	if binding.Channel != hint.Channel || binding.ExternalAccountID == "" {
		return channel.HTTPResponse{}, runtime.ErrVersionMismatch
	}
	if len(response.Body) == 0 || len(response.Body) > maxChallengeResponseBytes || response.ContentType == "" {
		return channel.HTTPResponse{}, runtime.ErrVersionMismatch
	}
	return channel.HTTPResponse{ContentType: response.ContentType, Body: append([]byte(nil), response.Body...)}, nil
}
