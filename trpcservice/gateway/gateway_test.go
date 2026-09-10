package gateway_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type captureAdmitter struct {
	request gateway.AdmissionRequest
	result  gateway.AdmissionResult
	err     error
}

func (a *captureAdmitter) Admit(_ context.Context, request gateway.AdmissionRequest) (gateway.AdmissionResult, error) {
	a.request = request
	if a.err != nil {
		return gateway.AdmissionResult{}, a.err
	}
	return a.result, nil
}

type staticTenantResolver struct {
	tenant       tenant.RuntimeContext
	source       gateway.TenantSource
	identity     gateway.AdmissionIdentity
	withIdentity bool
}

type tenantOnlyResolver struct {
	tenant tenant.RuntimeContext
}

type admissionRateLimiterFunc func(context.Context, gateway.AdmissionIdentity) error

func (f admissionRateLimiterFunc) Allow(ctx context.Context, identity gateway.AdmissionIdentity) error {
	return f(ctx, identity)
}

func (r tenantOnlyResolver) ResolveTenant(_ context.Context) (
	tenant.RuntimeContext,
	gateway.TenantSource,
	error,
) {
	return r.tenant, gateway.TenantSourceAuthenticatedClaims, nil
}

func (r staticTenantResolver) ResolveTenant(_ context.Context) (
	tenant.RuntimeContext,
	gateway.TenantSource,
	error,
) {
	return r.tenant, r.source, nil
}

func (r staticTenantResolver) ResolveAdmissionIdentity(
	_ context.Context,
) (gateway.AdmissionIdentity, error) {
	if !r.withIdentity {
		return gateway.AdmissionIdentity{}, errors.New("admission identity is unavailable")
	}
	return r.identity, nil
}

func TestGatewaySubmitsAtomicAdmission(t *testing.T) {
	admitter := &captureAdmitter{
		result: gateway.AdmissionResult{
			RequestID:     "request-1",
			ConfigVersion: "v2",
			TurnSeq:       7,
		},
	}
	gw := gateway.New(admitter)
	identity := validAdmissionIdentity()

	result, err := gw.Handle(context.Background(), gateway.Request{
		RequestID:      "request-1",
		IdempotencyKey: "client-key-1",
		Tenant: staticTenantResolver{
			tenant:       identity.Tenant,
			source:       identity.Source,
			identity:     identity,
			withIdentity: true,
		},
		Message: gateway.Message{Text: "hello"},
	})
	if err != nil {
		t.Fatalf("handle request: %v", err)
	}
	if result.RequestID != "request-1" || result.ConfigVersion != "v2" || result.TurnSeq != 7 {
		t.Fatalf("admission result = %#v", result)
	}
	if admitter.request.RequestID != "request-1" || admitter.request.IdempotencyKey != "client-key-1" {
		t.Fatalf("admission request = %#v", admitter.request)
	}
	if admitter.request.Identity.SourceID != identity.SourceID {
		t.Fatalf("admission source ID = %q, want %q", admitter.request.Identity.SourceID, identity.SourceID)
	}
}

func TestGatewayRejectsBeforeBackendWhenSharedAdmissionRateIsExhausted(t *testing.T) {
	admitter := &captureAdmitter{result: gateway.AdmissionResult{
		RequestID: "request-rate", ConfigVersion: "v1", TurnSeq: 1,
	}}
	gw := gateway.New(admitter)
	want := &gateway.AdmissionRateLimitError{RetryAfter: time.Second}
	gw.RateLimiter = admissionRateLimiterFunc(func(context.Context, gateway.AdmissionIdentity) error {
		return want
	})
	identity := validAdmissionIdentity()
	_, err := gw.Handle(context.Background(), gateway.Request{
		RequestID: "request-rate", IdempotencyKey: "key-rate",
		Tenant:  staticTenantResolver{tenant: identity.Tenant, source: identity.Source, identity: identity, withIdentity: true},
		Message: gateway.Message{Text: "hello"},
	})
	if !errors.Is(err, gateway.ErrAdmissionRateLimited) {
		t.Fatalf("rate-limited error = %v", err)
	}
	if admitter.request.RequestID != "" {
		t.Fatal("rate-limited request reached backend admission")
	}
}

func TestGatewayAdmissionConcurrencyIsLocalAndReleasedAfterAdmission(t *testing.T) {
	admitter := &captureAdmitter{result: gateway.AdmissionResult{
		RequestID: "request-concurrency", ConfigVersion: "v1", TurnSeq: 1,
	}}
	slots, err := gateway.NewAdmissionConcurrency(1)
	if err != nil {
		t.Fatalf("new admission concurrency: %v", err)
	}
	gw := gateway.New(admitter)
	gw.AdmissionConcurrency = slots
	rateCalls := 0
	gw.RateLimiter = admissionRateLimiterFunc(func(context.Context, gateway.AdmissionIdentity) error {
		rateCalls++
		return nil
	})
	identity := validAdmissionIdentity()
	request := gateway.Request{
		RequestID: "request-concurrency", IdempotencyKey: "key-concurrency",
		Tenant:  staticTenantResolver{tenant: identity.Tenant, source: identity.Source, identity: identity, withIdentity: true},
		Message: gateway.Message{Text: "hello"},
	}
	release, err := slots.Acquire()
	if err != nil {
		t.Fatalf("occupy admission slot: %v", err)
	}
	if _, err := gw.Handle(context.Background(), request); !errors.Is(err, gateway.ErrAdmissionConcurrencyLimit) {
		t.Fatalf("busy admission error = %v", err)
	}
	if rateCalls != 0 {
		t.Fatalf("shared rate limiter calls while local admission was full = %d, want 0", rateCalls)
	}
	release()
	if _, err := gw.Handle(context.Background(), request); err != nil {
		t.Fatalf("admission after slot release: %v", err)
	}
	if rateCalls != 1 {
		t.Fatalf("shared rate limiter calls after local admission = %d, want 1", rateCalls)
	}
}

func TestGatewayRequiresAdmitter(t *testing.T) {
	_, err := gateway.New(nil).Handle(context.Background(), gateway.Request{})
	if !errors.Is(err, gateway.ErrAdmitterRequired) {
		t.Fatalf("handle error = %v, want admitter required", err)
	}
}

func TestGatewayRequiresAdmissionIdentity(t *testing.T) {
	admitter := &captureAdmitter{
		result: gateway.AdmissionResult{
			RequestID:     "request-1",
			ConfigVersion: "v1",
			TurnSeq:       1,
		},
	}
	request := gateway.Request{
		RequestID:      "request-1",
		IdempotencyKey: "client-key-1",
		Tenant:         tenantOnlyResolver{tenant: validRuntimeContext()},
	}
	_, err := gateway.New(admitter).Handle(context.Background(), request)
	if !errors.Is(err, gateway.ErrAdmissionIdentityRequired) {
		t.Fatalf("handle error = %v, want admission identity required", err)
	}
	if admitter.request.RequestID != "" {
		t.Fatal("request reached admitter without admission identity")
	}
}

func TestGatewayRejectsInvalidAdmissionRequestBeforeBackend(t *testing.T) {
	admitter := &captureAdmitter{
		result: gateway.AdmissionResult{
			RequestID:     "request-1",
			ConfigVersion: "v1",
			TurnSeq:       1,
		},
	}
	identity := validAdmissionIdentity()
	identity.Tenant.UserID = ""
	request := gateway.Request{
		RequestID:      "request-1",
		IdempotencyKey: "client-key-1",
		Tenant: staticTenantResolver{
			tenant:       identity.Tenant,
			source:       identity.Source,
			identity:     identity,
			withIdentity: true,
		},
	}
	_, err := gateway.New(admitter).Handle(context.Background(), request)
	if err == nil {
		t.Fatal("handle request succeeded with incomplete identity")
	}
	if admitter.request.RequestID != "" {
		t.Fatal("invalid request reached admitter")
	}
}

func TestGatewayAcceptsValidatedArtifactReference(t *testing.T) {
	admitter := &captureAdmitter{
		result: gateway.AdmissionResult{
			RequestID:     "request-1",
			ConfigVersion: "v1",
			TurnSeq:       1,
		},
	}
	identity := validAdmissionIdentity()
	request := gateway.Request{
		RequestID:      "request-1",
		IdempotencyKey: "client-key-1",
		Tenant: staticTenantResolver{
			tenant:       identity.Tenant,
			source:       identity.Source,
			identity:     identity,
			withIdentity: true,
		},
		Message: gateway.Message{
			Text:         "hello",
			ArtifactRefs: []string{"artifact://file@1"},
		},
	}
	if _, err := gateway.New(admitter).Handle(context.Background(), request); err != nil {
		t.Fatalf("handle request: %v", err)
	}
	if admitter.request.RequestID == "" || len(admitter.request.Message.ArtifactRefs) != 1 {
		t.Fatalf("admission request = %#v", admitter.request)
	}
}

func TestGatewayRejectsInvalidArtifactReferenceBeforeBackend(t *testing.T) {
	admitter := &captureAdmitter{
		result: gateway.AdmissionResult{
			RequestID:     "request-1",
			ConfigVersion: "v1",
			TurnSeq:       1,
		},
	}
	identity := validAdmissionIdentity()
	request := gateway.Request{
		RequestID:      "request-1",
		IdempotencyKey: "client-key-1",
		Tenant: staticTenantResolver{
			tenant:       identity.Tenant,
			source:       identity.Source,
			identity:     identity,
			withIdentity: true,
		},
		Message: gateway.Message{
			Text:         "hello",
			ArtifactRefs: []string{"artifact://file"},
		},
	}
	if _, err := gateway.New(admitter).Handle(context.Background(), request); err == nil {
		t.Fatal("invalid artifact reference was accepted")
	} else if !errors.Is(err, gateway.ErrInvalidArtifactRef) {
		t.Fatalf("invalid artifact reference error = %v", err)
	}
	if admitter.request.RequestID != "" {
		t.Fatal("invalid artifact reference reached backend admission")
	}
}

func TestGatewayPinsChannelConfigBeforeAttachmentsAndCompensatesAdmissionFailure(t *testing.T) {
	binding := testChannelBinding()
	resolver, err := gateway.NewChannelBindingInputIdentityResolverFromBinding(
		binding.Snapshot(),
		tenant.RuntimeContext{
			TenantID: binding.TenantID, AppID: binding.AppID,
			Channel: string(binding.Channel), BindingID: binding.BindingID,
			TraceID: "request-attachment",
		},
	)
	if err != nil {
		t.Fatalf("new channel resolver: %v", err)
	}
	input, err := channels.NewChannelInput(channels.ChannelInput{
		TenantID: binding.TenantID, AppID: binding.AppID, Channel: binding.Channel,
		BindingID: binding.BindingID, BindingRevision: binding.BindingRevision,
		ExternalMessageID: "message-attachment", Conversation: channels.ChannelConversation{
			Kind: channels.ConversationDirect,
		}, MessageType: channels.MessageTypeText, Text: "hello",
	}, channels.ChannelMappingInput{
		ExternalSenderID: "user-1", ProviderSenderTarget: "user-1",
	})
	if err != nil {
		t.Fatalf("new channel input: %v", err)
	}
	admitter := &pinnerAdmitter{
		result: gateway.AdmissionResult{RequestID: "request-attachment", ConfigVersion: "v-canary", TurnSeq: 1},
		err:    errors.New("admission failed"),
	}
	cleanupCalled := false
	prepared, err := gateway.New(admitter).HandleChannel(context.Background(), gateway.Request{
		RequestID: "request-attachment", IdempotencyKey: "message-attachment",
		Tenant: resolver, ChannelInput: &input,
	}, func(_ context.Context, input channels.ChannelInput, version string) (channels.ChannelInput, func(context.Context) error, error) {
		if version != "v-canary" {
			t.Fatalf("attachment config version = %q, want v-canary", version)
		}
		input.ArtifactRefs = []string{"artifact://inbound/one@0"}
		return input, func(context.Context) error {
			cleanupCalled = true
			return nil
		}, nil
	})
	if err == nil || prepared.RequestID != "" {
		t.Fatalf("handle result=%#v err=%v, want admission failure", prepared, err)
	}
	if !cleanupCalled {
		t.Fatal("admission failure did not invoke attachment compensation")
	}
	if admitter.request.Identity.Tenant.ConfigVersion != "v-canary" || !admitter.request.Identity.ConfigVersionPinned() {
		t.Fatalf("pinned admission identity = %#v", admitter.request.Identity)
	}
	if len(admitter.request.Message.ArtifactRefs) != 1 || len(admitter.request.ChannelInput.ArtifactRefs) != 1 {
		t.Fatalf("prepared admission artifacts = %#v", admitter.request)
	}
	if admitter.failureRequest.RequestID != "request-attachment" {
		t.Fatalf("failure request = %#v", admitter.failureRequest)
	}

	prepareErr := errors.New("download attachment failed")
	prepareAdmitter := &pinnerAdmitter{}
	prepareResolver, err := gateway.NewChannelBindingInputIdentityResolverFromBinding(
		binding.Snapshot(),
		tenant.RuntimeContext{
			TenantID: binding.TenantID, AppID: binding.AppID,
			Channel: string(binding.Channel), BindingID: binding.BindingID,
			TraceID: "request-prepare-failure",
		},
	)
	if err != nil {
		t.Fatalf("new prepare failure resolver: %v", err)
	}
	_, err = gateway.New(prepareAdmitter).HandleChannel(context.Background(), gateway.Request{
		RequestID: "request-prepare-failure", IdempotencyKey: "message-attachment",
		Tenant: prepareResolver, ChannelInput: &input,
	}, func(context.Context, channels.ChannelInput, string) (channels.ChannelInput, func(context.Context) error, error) {
		return channels.ChannelInput{}, nil, prepareErr
	})
	if !errors.Is(err, prepareErr) {
		t.Fatalf("prepare failure = %v, want %v", err, prepareErr)
	}
	if prepareAdmitter.failureRequest.RequestID != "request-prepare-failure" || prepareAdmitter.request.RequestID != "" {
		t.Fatalf("prepare failure recorder=%#v admission=%#v", prepareAdmitter.failureRequest, prepareAdmitter.request)
	}
}

type pinnerAdmitter struct {
	request        gateway.AdmissionRequest
	failureRequest gateway.AdmissionRequest
	result         gateway.AdmissionResult
	err            error
}

func (a *pinnerAdmitter) RecordChannelFailure(_ context.Context, request gateway.AdmissionRequest) error {
	a.failureRequest = request
	return nil
}

func (a *pinnerAdmitter) PinChannelConfig(context.Context, gateway.AdmissionRequest) (string, error) {
	return "v-canary", nil
}

func (a *pinnerAdmitter) Admit(_ context.Context, request gateway.AdmissionRequest) (gateway.AdmissionResult, error) {
	a.request = request
	if a.err != nil {
		return gateway.AdmissionResult{}, a.err
	}
	return a.result, nil
}

func validAdmissionIdentity() gateway.AdmissionIdentity {
	return gateway.AdmissionIdentity{
		Tenant:   validRuntimeContext(),
		Source:   gateway.TenantSourceAuthenticatedClaims,
		SourceID: "credential-1",
		CredentialDigest: gateway.CredentialDigest{
			1,
		},
	}
}

func validRuntimeContext() tenant.RuntimeContext {
	return tenant.RuntimeContext{
		TenantID:           "tenant-a",
		AppID:              "support",
		ConfigVersion:      "v1",
		SessionID:          "session-1",
		SessionPrincipalID: "principal-1",
		UserID:             "user-1",
		TraceID:            "trace-1",
	}
}
