package bootstrap

import (
	"context"
	"strings"
	"testing"
	"time"

	connection "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application"
	wecomsender "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/outbound/wecomadapter"
	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/application"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
)

// This fixture supplies the Connection-owned public result port. Delivery still
// runs through its actual Provider and the actual composition bridge, so dropping
// ProviderCode at either boundary is observable as incorrect certainty/retry.
type providerEvidenceSource struct{ result connection.SendResult }

func (s providerEvidenceSource) ReserveFinal(context.Context, connection.SenderOrigin) (connection.ReservedSender, error) {
	return providerEvidenceHandle{s.result}, nil
}

type providerEvidenceHandle struct{ result connection.SendResult }

func (h providerEvidenceHandle) SendFinal(context.Context, connection.FinalCommand) connection.SendResult {
	return h.result
}
func (providerEvidenceHandle) Release()   {}
func providerEvidenceCode(n int64) *int64 { return &n }

func providerEvidenceRequest(t *testing.T) (app.SendRequest, d.Attempt) {
	t.Helper()
	now := time.Now().UTC()
	in := d.Intent{ID: "intent", AdmissionID: "admission", RunID: "run", AttemptID: "execution-attempt", CompletionID: "completion", ExecutionGeneration: 1, Sequence: 1, Text: "hello", Deadline: now.Add(time.Hour)}
	target := d.Target{TenantID: "tenant", Provider: "wecom", AccountID: "account", ManifestDigest: "sha256:" + strings.Repeat("a", 64), ConversationID: "user", SourceEventID: "message", CallbackRequestID: "request", ReceivedAt: now, Origin: &d.ReplyOrigin{InstanceID: "instance", Epoch: 1, Revision: 1, SocketGeneration: 1}}
	claim := d.Claim{Part: d.Part{ID: d.PartID(in.ID, 0), IntentID: in.ID, Index: 0, Text: in.Text, State: d.Claimed}, Intent: in, Target: target, Token: "claim", InstanceID: "instance", Owner: &d.OwnerFence{InstanceID: "instance", Epoch: 1, Revision: 1}, ExpiresAt: now.Add(time.Minute)}
	digest, err := d.RequestDigest(claim)
	if err != nil {
		t.Fatal(err)
	}
	req := app.SendRequest{Claim: claim, RequestID: target.CallbackRequestID, RequestDigest: digest}
	attempt := d.Attempt{ID: "delivery-attempt", PartID: claim.Part.ID, IntentID: in.ID, ClaimToken: claim.Token, InstanceID: claim.InstanceID, Number: 1, Owner: claim.Owner, RequestID: req.RequestID, RequestDigest: digest, EvidenceToken: "evidence", CallingUntil: now.Add(time.Minute), Intent: in, Target: target, Text: in.Text}
	return req, attempt
}

func TestConnectionDeliveryProviderEvidenceDoesNotBecomeSuccessOrRetry(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input connection.SendResult
		want  d.Result
		retry bool
	}{
		{"accepted-with-rejection-code", connection.SendResult{Certainty: connection.Accepted, ProviderCode: providerEvidenceCode(45009)}, d.Result{Certainty: d.CertaintyUnknown, ErrorClass: d.ErrorPermanent}, false},
		{"not-sent-with-zero-provider-code", connection.SendResult{Certainty: connection.NotSent, Code: connection.SendUnavailable, ProviderCode: providerEvidenceCode(0)}, d.Result{Certainty: d.CertaintyUnknown, ErrorClass: d.ErrorPermanent}, false},
		{"not-sent-with-rejection-code", connection.SendResult{Certainty: connection.NotSent, Code: connection.SendCapacity, ProviderCode: providerEvidenceCode(45009)}, d.Result{Certainty: d.CertaintyUnknown, ErrorClass: d.ErrorPermanent}, false},
		{"normal-accepted-zero", connection.SendResult{Certainty: connection.Accepted, ProviderCode: providerEvidenceCode(0)}, d.Result{Certainty: d.CertaintyAccepted}, false},
		{"normal-rejected-nonzero", connection.SendResult{Certainty: connection.Rejected, Code: "provider_rejected", ProviderCode: providerEvidenceCode(45009)}, d.Result{Certainty: d.CertaintyRejected, ErrorClass: d.ErrorPermanent}, false},
		{"normal-pre-write-unavailable", connection.SendResult{Certainty: connection.NotSent, Code: connection.SendUnavailable}, d.Result{Certainty: d.CertaintyNotSent, ErrorClass: d.ErrorTemporary}, true},
		{"unknown-never-retried", connection.SendResult{Certainty: connection.Unknown, Code: connection.SendCanceled}, d.Result{Certainty: d.CertaintyUnknown, ErrorClass: d.ErrorPermanent}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider, err := wecomsender.NewProvider(connectionDeliverySource{source: providerEvidenceSource{tc.input}})
			if err != nil {
				t.Fatal(err)
			}
			req, attempt := providerEvidenceRequest(t)
			sender, err := provider.Reserve(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			defer sender.Release()
			got := sender.SendFinal(context.Background(), attempt)
			_, retry := d.RetryDelay(got, 1, time.Now(), req.Claim.Intent.Deadline)
			if got.Validate() != nil || got != tc.want || retry != tc.retry {
				t.Fatalf("mixed evidence mapping: got=%+v retry=%t; want=%+v retry=%t", got, retry, tc.want, tc.retry)
			}
		})
	}
}
