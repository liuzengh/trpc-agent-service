package ingress_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type fakeAdapter struct{ badSignature bool }

func (*fakeAdapter) ID() string                { return "feishu" }
func (*fakeAdapter) Run(context.Context) error { return nil }
func (*fakeAdapter) PublicRoute(context.Context, channel.CallbackRequest) (channel.PublicRouteHint, error) {
	return channel.PublicRouteHint{Channel: "feishu", RouteKeyDigest: "route-digest-0001", IngressAttemptID: "attempt"}, nil
}
func (*fakeAdapter) IsChallenge(channel.CallbackRequest) bool { return true }
func (*fakeAdapter) PublicChallengeRoute(context.Context, channel.CallbackRequest) (channel.PublicRouteHint, error) {
	return channel.PublicRouteHint{Channel: "feishu", RouteKeyDigest: "route-digest-0001", IngressAttemptID: "challenge-attempt"}, nil
}
func (*fakeAdapter) VerifyChallenge(ctx context.Context, request channel.CallbackRequest, handle channel.ScopedVerifierHandle) (channel.HTTPResponse, channel.VerificationReceipt, error) {
	callback, receipt, err := handle.Verify(ctx, request, func(context.Context, channel.CallbackRequest, []byte) (channel.VerifiedProtocolPayload, error) {
		return channel.VerifiedProtocolPayload{Body: []byte(`{"challenge":"ok"}`), Headers: map[string]string{"content-type": "application/json"}, ProtocolIdentityDigest: "challenge-identity"}, nil
	})
	return channel.HTTPResponse{ContentType: callback.Headers["content-type"], Body: callback.Body}, receipt, err
}
func (*fakeAdapter) CallbackACK() channel.HTTPResponse {
	return channel.HTTPResponse{ContentType: "application/json", Body: []byte(`{"code":0}`)}
}
func (a *fakeAdapter) Verify(ctx context.Context, request channel.CallbackRequest, handle channel.ScopedVerifierHandle) (channel.VerifiedCallback, channel.VerificationReceipt, error) {
	return handle.Verify(ctx, request, func(_ context.Context, request channel.CallbackRequest, secret []byte) (channel.VerifiedProtocolPayload, error) {
		mac := hmac.New(sha256.New, secret)
		_, _ = mac.Write(request.Body)
		expected := hex.EncodeToString(mac.Sum(nil))
		if a.badSignature || request.Headers["signature"] != expected {
			return channel.VerifiedProtocolPayload{}, errors.New("bad signature")
		}
		return channel.VerifiedProtocolPayload{Body: append([]byte(nil), request.Body...), ProtocolIdentityDigest: "app-identity"}, nil
	})
}
func (*fakeAdapter) Decode(_ context.Context, callback channel.VerifiedCallback) ([]channel.ProviderEvent, error) {
	return []channel.ProviderEvent{{SchemaVersion: 1, Channel: "feishu", ExternalAccountID: "account", ExternalMessageID: string(callback.Body),
		ExternalUserID: "user", ExternalChatID: "chat", ConversationType: "group", MessageType: "text", Text: "hello", OccurredAt: time.Now().UTC()}}, nil
}
func (*fakeAdapter) Deliver(context.Context, channel.DeliveryRequest) (channel.DeliveryResult, error) {
	return channel.DeliveryResult{Delivered: true, ProviderMessageID: "provider-message"}, nil
}
func (*fakeAdapter) Capabilities() channel.Capabilities { return channel.Capabilities{Text: true} }

func TestServiceOrdersRouteVerifyPromoteDecode(t *testing.T) {
	resolver, _ := fixture(t)
	body := []byte("event-1")
	mac := hmac.New(sha256.New, []byte("verify-secret"))
	_, _ = mac.Write(body)
	request := channel.CallbackRequest{Body: body, Headers: map[string]string{"signature": hex.EncodeToString(mac.Sum(nil))}, ReceivedAt: time.Now().UTC()}
	result, err := (ingress.Service{Adapter: &fakeAdapter{}, Bindings: resolver}).VerifyAndDecode(context.Background(), request)
	if err != nil || result.Binding.TenantID != "tenant-a" || len(result.Events) != 1 || result.Events[0].ExternalMessageID != "event-1" {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}

func TestServiceFailsClosedBeforeTenantPromotion(t *testing.T) {
	resolver, _ := fixture(t)
	_, err := (ingress.Service{Adapter: &fakeAdapter{badSignature: true}, Bindings: resolver}).VerifyAndDecode(context.Background(),
		channel.CallbackRequest{Body: []byte("event"), Headers: map[string]string{"signature": "forged"}, ReceivedAt: time.Now().UTC()})
	if err == nil || errors.Is(err, runtime.ErrNotFound) {
		t.Fatalf("verification error=%v", err)
	}
}

func TestChallengeServiceVerifiesAndPromotesWithoutDecoding(t *testing.T) {
	resolver, _ := fixture(t)
	response, err := (ingress.ChallengeService{Adapter: &fakeAdapter{}, Bindings: resolver}).Verify(context.Background(), channel.CallbackRequest{})
	if err != nil || response.ContentType != "application/json" || string(response.Body) != `{"challenge":"ok"}` {
		t.Fatalf("response=%#v err=%v", response, err)
	}
}

var _ channel.Adapter = (*fakeAdapter)(nil)
var _ channel.HTTPAdapter = (*fakeAdapter)(nil)
