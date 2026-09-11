package domain_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
)

func target(provider string) domain.Target {
	t := domain.Target{TenantID: "tenant-1", Provider: provider, AccountID: "account-1", ManifestDigest: "sha256:" + strings.Repeat("a", 64), ConversationID: "-10012", SourceEventID: "123", ReceivedAt: time.Date(2026, 9, 5, 1, 0, 0, 0, time.UTC)}
	if provider == "wecom" {
		t.ConversationID = "conversation-1"
		t.CallbackRequestID = "req-1"
		t.Origin = &domain.ReplyOrigin{InstanceID: "gateway-1", Epoch: 1, Revision: 1, SocketGeneration: 1}
	}
	return t
}
func intent() domain.Intent {
	return domain.Intent{ID: "intent-1", AdmissionID: "admission-1", RunID: "run-1", AttemptID: "attempt-1", CompletionID: "completion-1", ExecutionGeneration: 1, Sequence: 2, Text: "hello", Deadline: time.Date(2026, 9, 5, 1, 30, 0, 0, time.UTC)}
}

func TestPlanTextPreservesOrderedUnicodeAndRejectsUnsupportedWeCom(t *testing.T) {
	text := strings.Repeat("界", 4095) + "😀" + "end"
	parts, err := domain.PlanText(target("telegram"), text)
	if err != nil || len(parts) != 2 || len([]rune(parts[0])) != 4096 || parts[1] != "end" || strings.Join(parts, "") != text {
		t.Fatalf("parts=%d err=%v", len(parts), err)
	}
	if _, err = domain.PlanText(target("wecom"), strings.Repeat("a", 20481)); !errors.Is(err, domain.ErrUnsupported) {
		t.Fatalf("oversize WeCom = %v", err)
	}
	if parts, err = domain.PlanText(target("wecom"), strings.Repeat("a", 20480)); err != nil || len(parts) != 1 {
		t.Fatalf("WeCom boundary parts=%d err=%v", len(parts), err)
	}
	if _, err = domain.PlanText(target("telegram"), strings.Repeat("a", 65537)); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("oversize input = %v", err)
	}
}

func TestPreparedRejectsAlteredDigestAndPlan(t *testing.T) {
	in := intent()
	digest, err := domain.IntentDigest(in)
	if err != nil {
		t.Fatal(err)
	}
	p := domain.Prepared{Intent: in, Digest: digest, Target: target("telegram"), Parts: []string{"hello"}}
	if err = p.Validate(); err != nil {
		t.Fatal(err)
	}
	p.Parts[0] = "changed"
	if err = p.Validate(); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("changed plan: %v", err)
	}
	p.Parts[0] = "hello"
	p.Digest = "sha256:" + strings.Repeat("0", 64)
	if err = p.Validate(); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("changed digest: %v", err)
	}
}
func TestRetryDelayOnlyProvenNotSentAndWithinBudget(t *testing.T) {
	now := time.Date(2026, 9, 5, 1, 0, 0, 0, time.UTC)
	for _, c := range []domain.Certainty{domain.CertaintyAccepted, domain.CertaintyRejected, domain.CertaintyUnknown} {
		if _, ok := domain.RetryDelay(domain.Result{Certainty: c, ErrorClass: domain.ErrorTemporary}, 1, now, now.Add(time.Hour)); ok {
			t.Fatalf("retried %s", c)
		}
	}
	result := domain.Result{Certainty: domain.CertaintyNotSent, ErrorClass: domain.ErrorTemporary}
	for _, row := range []struct {
		attempt int64
		delay   time.Duration
		ok      bool
	}{{1, time.Second, true}, {2, 2 * time.Second, true}, {3, 0, false}, {0, 0, false}} {
		d, ok := domain.RetryDelay(result, row.attempt, now, now.Add(time.Hour))
		if d != row.delay || ok != row.ok {
			t.Fatalf("attempt=%d delay=%v ok=%v", row.attempt, d, ok)
		}
	}
	if _, ok := domain.RetryDelay(result, 1, now, now.Add(time.Second)); ok {
		t.Fatal("retry at deadline")
	}
	result.ErrorClass = domain.ErrorPermanent
	if _, ok := domain.RetryDelay(result, 1, now, now.Add(time.Hour)); ok {
		t.Fatal("permanent retry")
	}
}

func TestRequestDigestRetainsFullLocalFencePrecision(t *testing.T) {
	in := intent()
	c := domain.Claim{Intent: in, Target: target("wecom"), InstanceID: "gateway-1", Owner: &domain.OwnerFence{InstanceID: "gateway-1", Epoch: 1, Revision: 1}, Part: domain.Part{ID: domain.PartID(in.ID, 0), IntentID: in.ID, Index: 0, Text: in.Text}}
	c.Target.Origin.SocketGeneration = 9007199254740992
	first, err := domain.RequestDigest(c)
	if err != nil {
		t.Fatal(err)
	}
	c.Target.Origin.SocketGeneration++
	second, err := domain.RequestDigest(c)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("distinct full-width local socket generations had the same request digest")
	}
}

func TestDomainValidationRejectsMalformedFacts(t *testing.T) {
	for name, mutate := range map[string]func(*domain.Intent){
		"ID": func(i *domain.Intent) { i.ID = "bad id" }, "generation-zero": func(i *domain.Intent) { i.ExecutionGeneration = 0 }, "generation-overflow": func(i *domain.Intent) { i.ExecutionGeneration = 9007199254740992 }, "sequence-zero": func(i *domain.Intent) { i.Sequence = 0 }, "sequence-overflow": func(i *domain.Intent) { i.Sequence = 9007199254740992 }, "invalid-unicode": func(i *domain.Intent) { i.Text = string([]byte{0xff}) }, "empty": func(i *domain.Intent) { i.Text = "" }, "blank": func(i *domain.Intent) { i.Text = " \n" }, "nul": func(i *domain.Intent) { i.Text = "a\x00b" }, "zero-deadline": func(i *domain.Intent) { i.Deadline = time.Time{} }, "year-zero": func(i *domain.Intent) { i.Deadline = time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC) },
	} {
		t.Run(name, func(t *testing.T) {
			in := intent()
			mutate(&in)
			if !errors.Is(in.Validate(), domain.ErrInvalid) {
				t.Fatal("invalid intent accepted")
			}
			if _, err := domain.IntentDigest(in); !errors.Is(err, domain.ErrInvalid) {
				t.Fatal("invalid digest accepted")
			}
		})
	}
	cases := []domain.Target{target("unknown"), target("telegram"), target("telegram"), target("telegram"), target("wecom"), target("wecom"), target("wecom"), target("wecom")}
	cases[1].ConversationID = "0"
	cases[2].ThreadID = "01"
	cases[3].CallbackRequestID = "wrong"
	cases[4].Origin = nil
	cases[5].Origin.Epoch = 0
	cases[6].CallbackRequestID = ""
	cases[7].ReceivedAt = time.Time{}
	for _, value := range cases {
		if !errors.Is(value.Validate(), domain.ErrInvalid) {
			t.Errorf("invalid target accepted: %+v", value)
		}
	}
	if domain.PartID("invalid id", 0) != "" || domain.PartID("intent-1", -1) != "" || domain.PartID("intent-1", 65536) != "" || domain.PartID("intent-1", 0) == domain.PartID("intent-1", 1) || domain.PartID("intent-1", 0) == domain.PartID("intent-2", 0) {
		t.Fatal("invalid part identities")
	}
}

func TestResultCertaintyAndEvidenceValidation(t *testing.T) {
	valid := []domain.Result{{Certainty: domain.CertaintyAccepted}, {Certainty: domain.CertaintyAccepted, ProviderMessageID: "42"}, {Certainty: domain.CertaintyRejected, ErrorClass: domain.ErrorPermanent}, {Certainty: domain.CertaintyUnknown}, {Certainty: domain.CertaintyUnknown, ErrorClass: domain.ErrorDeadline}, {Certainty: domain.CertaintyNotSent, ErrorClass: domain.ErrorStaleOrigin}}
	for _, r := range valid {
		if err := r.Validate(); err != nil {
			t.Errorf("valid result rejected: %+v %v", r, err)
		}
	}
	invalid := []domain.Result{{}, {Certainty: domain.CertaintyAccepted, ErrorClass: domain.ErrorTemporary}, {Certainty: domain.CertaintyAccepted, ProviderMessageID: "invalid\n"}, {Certainty: domain.CertaintyUnknown, ProviderMessageID: "42"}, {Certainty: domain.CertaintyNotSent}, {Certainty: domain.CertaintyRejected}, {Certainty: domain.CertaintyNotSent, ErrorClass: "unknown-custom-error"}}
	for _, r := range invalid {
		if !errors.Is(r.Validate(), domain.ErrInvalid) {
			t.Errorf("invalid result accepted: %+v", r)
		}
	}
}

func TestClaimCallingAndObservationValidation(t *testing.T) {
	in := intent()
	c := domain.Claim{Intent: in, Target: target("telegram"), InstanceID: "gateway-1", Token: "claim-1", ExpiresAt: in.Deadline, Part: domain.Part{ID: domain.PartID(in.ID, 0), IntentID: in.ID, Index: 0, Text: in.Text}}
	digest, err := domain.RequestDigest(c)
	if err != nil {
		t.Fatal(err)
	}
	call := domain.CallingRequest{Claim: c, RequestID: "request-1", RequestDigest: digest, Timeout: time.Second}
	if err := call.Validate(); err != nil {
		t.Fatal(err)
	}
	req := domain.ClaimRequest{Provider: "telegram", AccountID: "account-1", InstanceID: "gateway-1", Limit: 1, Lease: time.Second}
	if err := req.Validate(); err != nil {
		t.Fatal(err)
	}
	req.Owner = &domain.OwnerFence{InstanceID: "gateway-1", Epoch: 1, Revision: 1}
	if !errors.Is(req.Validate(), domain.ErrInvalid) {
		t.Fatal("Telegram accepted owner")
	}
	req.Provider = "wecom"
	if err := req.Validate(); err != nil {
		t.Fatal(err)
	}
	req.Owner.InstanceID = "other"
	if !errors.Is(req.Validate(), domain.ErrInvalid) {
		t.Fatal("mismatched owner")
	}
	req.Owner.InstanceID = "gateway-1"
	req.Limit = 0
	if !errors.Is(req.Validate(), domain.ErrInvalid) {
		t.Fatal("zero limit")
	}
	obs := domain.Observation{ID: "observation-1", AttemptID: "attempt-1", EvidenceToken: "capability-1", ProviderRequestID: "request-1", RequestDigest: digest, Result: domain.Result{Certainty: domain.CertaintyAccepted}}
	if err := obs.Validate(); err != nil {
		t.Fatal(err)
	}
	obs.EvidenceToken = ""
	if !errors.Is(obs.Validate(), domain.ErrInvalid) {
		t.Fatal("unbound observation")
	}
	call.RequestDigest = "sha256:" + strings.Repeat("0", 64)
	if !errors.Is(call.Validate(), domain.ErrInvalid) {
		t.Fatal("wrong request digest")
	}
	call.RequestDigest = digest
	call.Timeout = 0
	if !errors.Is(call.Validate(), domain.ErrInvalid) {
		t.Fatal("unbounded timeout")
	}
	before := digest
	c.Token = "claim-2"
	c.ExpiresAt = c.ExpiresAt.Add(time.Hour)
	after, err := domain.RequestDigest(c)
	if err != nil || before != after {
		t.Fatal("scheduling fields entered request digest")
	}
	c.Target.ConversationID = "-999"
	after, err = domain.RequestDigest(c)
	if err != nil || before == after {
		t.Fatal("target not bound")
	}
	c.Part.Text = "changed"
	if _, err = domain.RequestDigest(c); !errors.Is(err, domain.ErrInvalid) {
		t.Fatal("arbitrary text accepted")
	}
}

func TestCallingTimeoutMatchesApplicationMinimum(t *testing.T) {
	in := intent()
	c := domain.Claim{Intent: in, Target: target("telegram"), InstanceID: "gateway-1", Token: "claim-1", ExpiresAt: in.Deadline, Part: domain.Part{ID: domain.PartID(in.ID, 0), IntentID: in.ID, Index: 0, Text: in.Text}}
	digest, err := domain.RequestDigest(c)
	if err != nil {
		t.Fatal(err)
	}
	r := domain.CallingRequest{Claim: c, RequestID: "request-1", RequestDigest: digest, Timeout: 2 * time.Millisecond}
	if err := r.Validate(); err != nil {
		t.Fatalf("valid application call/evidence budget rejected: %v", err)
	}
}
