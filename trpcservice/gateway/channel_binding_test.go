package gateway_test

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"go.opentelemetry.io/otel/trace"
)

func TestResolveChannelBindingRouteRejectsInactiveAndMismatchedSnapshots(t *testing.T) {
	binding := testChannelBinding()
	resolver := staticPublicRouteResolver{snapshot: binding.Snapshot()}

	route, err := gateway.ResolveChannelBindingRoute(
		context.Background(),
		resolver,
		channels.ChannelWeCom,
		binding.PublicRouteID,
	)
	if err != nil {
		t.Fatalf("resolve channel binding route: %v", err)
	}
	if route.Snapshot().Binding != binding {
		t.Fatalf("snapshot = %#v, want %#v", route.Snapshot().Binding, binding)
	}

	if _, err := gateway.ResolveChannelBindingRoute(
		context.Background(),
		resolver,
		channels.ChannelFeishu,
		binding.PublicRouteID,
	); !errors.Is(err, channels.ErrBindingChannelMismatch) {
		t.Fatalf("channel mismatch error = %v, want %v", err, channels.ErrBindingChannelMismatch)
	}

	resolver.snapshot.Status = channels.BindingSuspended
	if _, err := gateway.ResolveChannelBindingRoute(
		context.Background(),
		resolver,
		channels.ChannelWeCom,
		binding.PublicRouteID,
	); !errors.Is(err, channels.ErrBindingInactive) {
		t.Fatalf("inactive binding error = %v, want %v", err, channels.ErrBindingInactive)
	}
}

func TestChannelBindingIdentityResolverCarriesTrustedScopeToGateway(t *testing.T) {
	binding := testChannelBinding()
	route, err := gateway.ResolveChannelBindingRoute(
		context.Background(),
		staticPublicRouteResolver{snapshot: binding.Snapshot()},
		binding.Channel,
		binding.PublicRouteID,
	)
	if err != nil {
		t.Fatalf("resolve channel binding route: %v", err)
	}
	channelBindingResolver, err := gateway.NewChannelBindingIdentityResolver(
		route,
		tenant.RuntimeContext{
			TenantID:           binding.TenantID,
			AppID:              binding.AppID,
			ConfigVersion:      "v1",
			Channel:            string(binding.Channel),
			BindingID:          binding.BindingID,
			SessionID:          "default",
			SessionPrincipalID: "user-1",
			UserID:             "user-1",
			TraceID:            "trace-1",
		},
	)
	if err != nil {
		t.Fatalf("new channel binding identity resolver: %v", err)
	}
	identity, err := channelBindingResolver.ResolveAdmissionIdentity(context.Background())
	if err != nil {
		t.Fatalf("resolve admission identity: %v", err)
	}
	if identity.Source != gateway.TenantSourceVerifiedChannelBinding ||
		identity.SourceID != binding.BindingID ||
		identity.PublicRouteID != binding.PublicRouteID ||
		identity.BindingRevision != binding.BindingRevision {
		t.Fatalf("channel binding admission identity = %#v", identity)
	}

	admitter := &captureAdmitter{result: gateway.AdmissionResult{
		RequestID:     "request-channel-1",
		ConfigVersion: "v1",
		TurnSeq:       1,
	}}
	_, err = gateway.New(admitter).Handle(
		context.Background(),
		gateway.Request{
			RequestID:      "request-channel-1",
			IdempotencyKey: "message-1",
			Tenant:         channelBindingResolver,
			Message:        gateway.Message{Text: "hello"},
		},
	)
	if err != nil {
		t.Fatalf("handle channel binding request: %v", err)
	}
	if admitter.request.Identity.Tenant.TenantID != binding.TenantID ||
		admitter.request.Identity.Tenant.AppID != binding.AppID {
		t.Fatalf("gateway trusted scope = %#v", admitter.request.Identity.Tenant)
	}
}

func TestChannelBindingInputIdentitySurvivesTracePropagation(t *testing.T) {
	binding := testChannelBinding()
	resolver, err := gateway.NewChannelBindingInputIdentityResolverFromBinding(
		binding.Snapshot(),
		tenant.RuntimeContext{
			TenantID:  binding.TenantID,
			AppID:     binding.AppID,
			Channel:   string(binding.Channel),
			BindingID: binding.BindingID,
			TraceID:   "request-1",
		},
	)
	if err != nil {
		t.Fatalf("new channel binding input resolver: %v", err)
	}
	input, err := channels.NewChannelInput(
		channels.ChannelInput{
			TenantID:          binding.TenantID,
			AppID:             binding.AppID,
			Channel:           binding.Channel,
			BindingID:         binding.BindingID,
			BindingRevision:   binding.BindingRevision,
			ExternalMessageID: "message-1",
			Conversation:      channels.ChannelConversation{Kind: channels.ConversationDirect},
			MessageType:       channels.MessageTypeText,
			Text:              "hello",
		},
		channels.ChannelMappingInput{
			ExternalSenderID:     "user-1",
			ProviderSenderTarget: "user-1",
		},
	)
	if err != nil {
		t.Fatalf("new channel input: %v", err)
	}
	admitter := &captureAdmitter{result: gateway.AdmissionResult{
		RequestID:     "request-1",
		ConfigVersion: "v1",
		TurnSeq:       1,
	}}
	traceContext := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1},
		SpanID:     trace.SpanID{2},
		TraceFlags: trace.FlagsSampled,
	}))
	if _, err := gateway.New(admitter).Handle(traceContext, gateway.Request{
		RequestID:      "request-1",
		IdempotencyKey: "message-1",
		Tenant:         resolver,
		ChannelInput:   &input,
	}); err != nil {
		t.Fatalf("handle channel input: %v", err)
	}
}

func TestChannelBindingIdentityResolverRejectsMismatchedRuntimeScope(t *testing.T) {
	binding := testChannelBinding()
	route, err := gateway.ResolveChannelBindingRoute(
		context.Background(),
		staticPublicRouteResolver{snapshot: binding.Snapshot()},
		binding.Channel,
		binding.PublicRouteID,
	)
	if err != nil {
		t.Fatalf("resolve channel binding route: %v", err)
	}
	_, err = gateway.NewChannelBindingIdentityResolver(
		route,
		tenant.RuntimeContext{
			TenantID:           "tenant-b",
			AppID:              binding.AppID,
			ConfigVersion:      "v1",
			Channel:            string(binding.Channel),
			BindingID:          binding.BindingID,
			SessionID:          "default",
			SessionPrincipalID: "user-1",
			UserID:             "user-1",
			TraceID:            "trace-1",
		},
	)
	if !errors.Is(err, gateway.ErrChannelBindingScopeMismatch) {
		t.Fatalf("scope mismatch error = %v, want %v", err, gateway.ErrChannelBindingScopeMismatch)
	}
}

func TestAdmissionIdentityRejectsForgedChannelBindingSource(t *testing.T) {
	binding := testChannelBinding()
	identity := gateway.AdmissionIdentity{
		Tenant: tenant.RuntimeContext{
			TenantID:           binding.TenantID,
			AppID:              binding.AppID,
			Channel:            string(binding.Channel),
			BindingID:          binding.BindingID,
			ConfigVersion:      "v1",
			SessionID:          "default",
			SessionPrincipalID: "user-1",
			UserID:             "user-1",
			TraceID:            "trace-1",
		},
		Source:          gateway.TenantSourceVerifiedChannelBinding,
		SourceID:        binding.BindingID,
		PublicRouteID:   binding.PublicRouteID,
		BindingRevision: binding.BindingRevision,
	}
	if err := identity.Validate(); err == nil {
		t.Fatal("forged channel binding identity passed validation")
	}
}

func TestAdmissionIdentityRejectsMutatedChannelBindingIdentity(t *testing.T) {
	binding := testChannelBinding()
	route, err := gateway.ResolveChannelBindingRoute(
		context.Background(),
		staticPublicRouteResolver{snapshot: binding.Snapshot()},
		binding.Channel,
		binding.PublicRouteID,
	)
	if err != nil {
		t.Fatalf("resolve channel binding route: %v", err)
	}
	channelBindingResolver, err := gateway.NewChannelBindingIdentityResolver(route, tenant.RuntimeContext{
		TenantID:           binding.TenantID,
		AppID:              binding.AppID,
		ConfigVersion:      "v1",
		Channel:            string(binding.Channel),
		BindingID:          binding.BindingID,
		SessionID:          "default",
		SessionPrincipalID: "user-1",
		UserID:             "user-1",
		TraceID:            "trace-1",
	})
	if err != nil {
		t.Fatalf("new channel binding identity resolver: %v", err)
	}
	identity, err := channelBindingResolver.ResolveAdmissionIdentity(context.Background())
	if err != nil {
		t.Fatalf("resolve admission identity: %v", err)
	}
	identity.Tenant.UserID = "user-2"
	if err := identity.Validate(); err == nil {
		t.Fatal("mutated channel binding identity passed validation")
	}
}

type staticPublicRouteResolver struct {
	snapshot channels.BindingSnapshot
}

func (r staticPublicRouteResolver) ResolveBindingByPublicRoute(
	context.Context,
	channels.Channel,
	string,
) (channels.BindingSnapshot, error) {
	return r.snapshot, nil
}

func testChannelBinding() channels.Binding {
	return channels.Binding{
		TenantID:        "tenant-a",
		AppID:           "support",
		BindingID:       "binding-channel-1",
		Channel:         channels.ChannelWeCom,
		ExternalAccount: "corp-agent-1",
		Secret: tenant.SecretRef{
			Name:    "wecom-bot-secret",
			Version: "v1",
		},
		PublicRouteID:   "route-channel-1",
		BindingRevision: 1,
		Status:          channels.BindingActive,
	}
}
