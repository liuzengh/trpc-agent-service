package httpcallback_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/httpcallback"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type adapterStub struct{}

func (*adapterStub) ID() string                { return "stub" }
func (*adapterStub) Run(context.Context) error { return nil }
func (*adapterStub) IsChallenge(request channel.CallbackRequest) bool {
	return request.Query["challenge"] == "1"
}
func (*adapterStub) PublicRoute(context.Context, channel.CallbackRequest) (channel.PublicRouteHint, error) {
	return channel.PublicRouteHint{}, nil
}
func (*adapterStub) PublicChallengeRoute(context.Context, channel.CallbackRequest) (channel.PublicRouteHint, error) {
	return channel.PublicRouteHint{}, nil
}
func (*adapterStub) Verify(context.Context, channel.CallbackRequest, channel.ScopedVerifierHandle) (channel.VerifiedCallback, channel.VerificationReceipt, error) {
	return channel.VerifiedCallback{}, channel.VerificationReceipt{}, nil
}
func (*adapterStub) VerifyChallenge(context.Context, channel.CallbackRequest, channel.ScopedVerifierHandle) (channel.HTTPResponse, channel.VerificationReceipt, error) {
	return channel.HTTPResponse{}, channel.VerificationReceipt{}, nil
}
func (*adapterStub) Decode(context.Context, channel.VerifiedCallback) ([]channel.ProviderEvent, error) {
	return nil, nil
}
func (*adapterStub) Deliver(context.Context, channel.DeliveryRequest) (channel.DeliveryResult, error) {
	return channel.DeliveryResult{}, nil
}
func (*adapterStub) Capabilities() channel.Capabilities { return channel.Capabilities{Text: true} }
func (*adapterStub) CallbackACK() channel.HTTPResponse {
	return channel.HTTPResponse{ContentType: "text/plain", Body: []byte("success")}
}

type acceptorStub struct {
	err   error
	calls int
}

func (a *acceptorStub) Accept(context.Context, channel.CallbackRequest) ([]ingress.AcceptedEvent, error) {
	a.calls++
	if a.err != nil {
		return nil, a.err
	}
	return []ingress.AcceptedEvent{{}}, nil
}

type challengeStub struct{ err error }

func (c challengeStub) Verify(context.Context, channel.CallbackRequest) (channel.HTTPResponse, error) {
	if c.err != nil {
		return channel.HTTPResponse{}, c.err
	}
	return channel.HTTPResponse{ContentType: "application/json", Body: []byte(`{"challenge":"ok"}`)}, nil
}

func TestEndpointACKsOnlySuccessfulDurableCallbacks(t *testing.T) {
	acceptor := &acceptorStub{}
	endpoint, err := httpcallback.NewEndpoint(&adapterStub{}, acceptor, challengeStub{})
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		response := httptest.NewRecorder()
		endpoint.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/callback", strings.NewReader("callback")))
		if response.Code != http.StatusOK || response.Body.String() != "success" {
			t.Fatalf("attempt=%d code=%d body=%q", attempt, response.Code, response.Body.String())
		}
	}
	if acceptor.calls != 2 {
		t.Fatalf("accept calls=%d", acceptor.calls)
	}
}

func TestEndpointServesProviderChallengeWithoutInbox(t *testing.T) {
	acceptor := &acceptorStub{}
	endpoint, _ := httpcallback.NewEndpoint(&adapterStub{}, acceptor, challengeStub{})
	response := httptest.NewRecorder()
	endpoint.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/callback?challenge=1", nil))
	if response.Code != http.StatusOK || response.Body.String() != `{"challenge":"ok"}` || acceptor.calls != 0 {
		t.Fatalf("code=%d body=%q accept calls=%d", response.Code, response.Body.String(), acceptor.calls)
	}
}

func TestEndpointBoundsBodyAndMapsFailuresWithoutLeakingDetails(t *testing.T) {
	tests := []struct {
		name         string
		method       string
		body         string
		acceptErr    error
		challenge    bool
		challengeErr error
		want         int
	}{
		{name: "method", method: http.MethodPut, want: http.StatusMethodNotAllowed},
		{name: "too large", method: http.MethodPost, body: "12345", want: http.StatusRequestEntityTooLarge},
		{name: "invalid", method: http.MethodPost, acceptErr: runtime.ErrInvalidEnvelope, want: http.StatusUnauthorized},
		{name: "collision", method: http.MethodPost, acceptErr: runtime.ErrIdempotencyCollision, want: http.StatusConflict},
		{name: "backend", method: http.MethodPost, acceptErr: runtime.ErrBackendUnavailable, want: http.StatusServiceUnavailable},
		{name: "challenge invalid", method: http.MethodGet, challenge: true, challengeErr: fmt.Errorf("secret signature detail: %w", runtime.ErrVersionMismatch), want: http.StatusUnauthorized},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			acceptor := &acceptorStub{err: test.acceptErr}
			endpoint, _ := httpcallback.NewEndpoint(&adapterStub{}, acceptor, challengeStub{err: test.challengeErr})
			endpoint.MaxBody = 4
			path := "/callback"
			if test.challenge {
				path += "?challenge=1"
			}
			response := httptest.NewRecorder()
			endpoint.ServeHTTP(response, httptest.NewRequest(test.method, path, strings.NewReader(test.body)))
			if response.Code != test.want || strings.Contains(response.Body.String(), "secret signature detail") {
				t.Fatalf("code=%d body=%q", response.Code, response.Body.String())
			}
		})
	}
}

func TestNewMuxRejectsInvalidOrDuplicateRoutes(t *testing.T) {
	endpoint, _ := httpcallback.NewEndpoint(&adapterStub{}, &acceptorStub{}, challengeStub{})
	if _, err := httpcallback.NewMux(); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("empty mux error=%v", err)
	}
	if _, err := httpcallback.NewMux(httpcallback.Route{Pattern: "callback", Endpoint: endpoint}); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("invalid route error=%v", err)
	}
	if _, err := httpcallback.NewMux(httpcallback.Route{Pattern: "/callback", Endpoint: endpoint}, httpcallback.Route{Pattern: "/callback", Endpoint: endpoint}); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("duplicate route error=%v", err)
	}
}

var _ channel.HTTPAdapter = (*adapterStub)(nil)
