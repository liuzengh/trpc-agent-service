package webui

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/contracttest"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

func TestAdapterVerifiesAndDecodesBrowserMessage(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	body, err := json.Marshal(inboundEnvelope{SchemaVersion: 1, ExternalAccountID: "local-account",
		ExternalMessageID: "message-1", ExternalUserID: "user-1", ExternalChatID: "chat-1",
		ConversationType: "p2p", MessageType: "text", Text: "hello", OccurredAt: now})
	if err != nil {
		t.Fatal(err)
	}
	timestamp, nonce, token := "1800000000", "nonce-1", "0123456789abcdef0123456789abcdef"
	request := channel.CallbackRequest{Query: map[string]string{"route_key": "local-route"}, Body: body, ReceivedAt: now,
		Headers: map[string]string{headerTimestamp: timestamp, headerNonce: nonce, headerSignature: signatureFor(token, timestamp, nonce, body)}}
	adapter := &Adapter{Protocol: Verifier{Now: func() time.Time { return now }}}
	hint, err := adapter.PublicRoute(context.Background(), request)
	if err != nil || hint.Channel != "webui" || hint.RouteKeyDigest != RouteKeyDigest("local-route") || hint.IngressAttemptID == "" {
		t.Fatalf("hint=%#v err=%v", hint, err)
	}
	secret, _ := json.Marshal(VerificationMaterial{Token: token, ExternalAccountID: "local-account"})
	callback, receipt, err := adapter.Verify(context.Background(), request, verifierHandleStub{secret: secret, now: now})
	if err != nil || receipt.ProtocolIdentityDigest == "" {
		t.Fatalf("receipt=%#v err=%v", receipt, err)
	}
	events, err := adapter.Decode(context.Background(), callback)
	if err != nil || len(events) != 1 || events[0].Channel != "webui" || events[0].ExternalMessageID != "message-1" || events[0].Text != "hello" {
		t.Fatalf("events=%#v err=%v", events, err)
	}

	request.Headers[headerSignature] = signatureFor("wrong-token-000000000000", timestamp, nonce, body)
	if _, _, err := adapter.Verify(context.Background(), request, verifierHandleStub{secret: secret, now: now}); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("tampered signature err=%v", err)
	}
}

func TestAdapterDeliveryContract(t *testing.T) {
	contracttest.RunDelivery(t, func(t testing.TB) contracttest.DeliveryHarness {
		t.Helper()
		mailbox := NewMemoryMailbox()
		adapter := &Adapter{Mailbox: mailbox}
		content := []byte("browser reply")
		digest := sha256.Sum256(content)
		event := channel.ReplyEvent{SchemaVersion: 1, TenantID: "tenant-a", RequestID: "request-a",
			ChannelBindingID: "webui-main", ConfigVersion: 1, DeliveryKey: "request-a:reply", ContentRef: "result://request-a",
			Target: channel.DeliveryTarget{Channel: "webui", ExternalAccountID: "local-account", ExternalUserID: "user-1", ExternalChatID: "chat-1"}, Final: true}
		return contracttest.DeliveryHarness{Adapter: adapter, Event: event,
			Result: messagingResult("tenant-a", "request-a", "result://request-a", content, hex.EncodeToString(digest[:])),
			Observe: func() contracttest.DeliveryObservation {
				message, err := mailbox.GetMessageByClientRequestID(context.Background(), "tenant-a", providerClientRequestID(t, mailbox, "tenant-a"))
				if err != nil {
					return contracttest.DeliveryObservation{}
				}
				return contracttest.DeliveryObservation{Calls: 1, ClientRequestID: message.ClientRequestID, Content: content, Target: event.Target}
			},
		}
	})
}

// providerClientRequestID finds the sole message without exposing a list API
// on the production mailbox contract.
func providerClientRequestID(t testing.TB, mailbox *MemoryMailbox, tenantID string) string {
	t.Helper()
	mailbox.mu.RLock()
	defer mailbox.mu.RUnlock()
	for _, value := range mailbox.messages {
		if value.TenantID == tenantID {
			return value.ClientRequestID
		}
	}
	return ""
}

func messagingResult(tenantID, requestID, resultRef string, content []byte, digest string) messaging.ResultRecord {
	return messaging.ResultRecord{TenantID: tenantID, RequestID: requestID, ResultRef: resultRef, Content: content, ContentDigest: digest, KeyVersion: 1}
}

type verifierHandleStub struct {
	secret []byte
	now    time.Time
}

func (h verifierHandleStub) Verify(ctx context.Context, request channel.CallbackRequest, verifier channel.ProtocolVerifier) (channel.VerifiedCallback, channel.VerificationReceipt, error) {
	payload, err := verifier(ctx, request, append([]byte(nil), h.secret...))
	if err != nil {
		return channel.VerifiedCallback{}, channel.VerificationReceipt{}, err
	}
	return channel.VerifiedCallback{Body: payload.Body, Headers: payload.Headers, ReceivedAt: request.ReceivedAt,
			ProtocolIdentityDigest: payload.ProtocolIdentityDigest}, channel.VerificationReceipt{CandidateToken: "candidate", ReceiptToken: "receipt",
			Purpose: "channel_verify", ProtocolIdentityDigest: payload.ProtocolIdentityDigest, VerifiedAt: h.now}, nil
}

func (verifierHandleStub) Close() error { return nil }
