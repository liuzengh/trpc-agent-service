package telegramadapter_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-telegram/bot"
	telegramadapter "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/outbound/telegram"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/application"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
)

func request(t *testing.T) (application.SendRequest, domain.Attempt) {
	t.Helper()
	in := domain.Intent{ID: "intent-1", AdmissionID: "admission-1", RunID: "run-1", AttemptID: "execution-attempt-1", CompletionID: "completion-1", ExecutionGeneration: 1, Sequence: 3, Text: "你好😀", Deadline: time.Now().UTC().Add(time.Minute)}
	target := domain.Target{TenantID: "tenant-1", Provider: "telegram", AccountID: "account-1", ManifestDigest: "sha256:" + strings.Repeat("a", 64), ConversationID: "-10012", ThreadID: "7", SourceMessageID: "99", SourceEventID: "123", ReceivedAt: time.Now().UTC()}
	claim := domain.Claim{Intent: in, Target: target, Part: domain.Part{ID: domain.PartID(in.ID, 0), IntentID: in.ID, Index: 0, Text: in.Text, State: domain.Claimed}, Token: "claim-1", InstanceID: "gateway-1", ExpiresAt: time.Now().UTC().Add(10 * time.Second)}
	digest, err := domain.RequestDigest(claim)
	if err != nil {
		t.Fatal(err)
	}
	req := application.SendRequest{Claim: claim, RequestID: "telegram-request-1", RequestDigest: digest}
	attempt := domain.Attempt{ID: "delivery-attempt-1", PartID: claim.Part.ID, IntentID: in.ID, ClaimToken: claim.Token, InstanceID: claim.InstanceID, Number: 1, RequestID: req.RequestID, RequestDigest: digest, EvidenceToken: "capability-1", CallingUntil: time.Now().UTC().Add(10 * time.Second), Intent: in, Target: target, Text: in.Text}
	return req, attempt
}
func configured(t *testing.T, url string) *bot.Bot {
	t.Helper()
	b, err := bot.New("100:synthetic-fixture-not-a-token", bot.WithSkipGetMe(), bot.WithServerURL(url), bot.WithErrorsHandler(func(error) {}))
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func TestReserveHasNoSideEffectAndFinalUsesFixedTarget(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/sendMessage") {
			t.Errorf("wrong method/path")
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		if r.FormValue("chat_id") != "-10012" || r.FormValue("message_thread_id") != "7" || r.FormValue("text") != "你好😀" || r.FormValue("parse_mode") != "" {
			t.Errorf("wrong fixed target/text: %v", r.Form)
		}
		var reply struct {
			MessageID int   `json:"message_id"`
			ChatID    int64 `json:"chat_id"`
			Allow     bool  `json:"allow_sending_without_reply"`
		}
		if err := json.Unmarshal([]byte(r.FormValue("reply_parameters")), &reply); err != nil || reply.MessageID != 99 || reply.ChatID != -10012 || reply.Allow {
			t.Errorf("wrong reply parameters: %+v %v", reply, err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":42,"message_thread_id":7,"chat":{"id":-10012,"type":"supergroup"}}}`))
	}))
	defer server.Close()
	accounts := map[string]*bot.Bot{"account-1": configured(t, server.URL)}
	p, err := telegramadapter.NewProvider(accounts)
	if err != nil {
		t.Fatal(err)
	}
	delete(accounts, "account-1")
	req, attempt := request(t)
	sender, err := p.Reserve(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("Reserve called Telegram")
	}
	result := sender.SendFinal(context.Background(), attempt)
	if result.Certainty != domain.CertaintyAccepted || result.ProviderMessageID != strconv.Itoa(42) || result.Validate() != nil {
		t.Fatalf("result=%+v", result)
	}
	result = sender.SendFinal(context.Background(), attempt)
	if result.Certainty != domain.CertaintyNotSent || calls.Load() != 1 {
		t.Fatalf("reused handle calls=%d result=%+v", calls.Load(), result)
	}
	sender.Release()
	sender.Release()
}

func TestTypedProviderRejectionAndUncertainResponses(t *testing.T) {
	cases := []struct {
		name, body string
		want       domain.Certainty
		class      domain.ErrorClass
	}{
		{"bad-request", `{"ok":false,"error_code":400,"description":"synthetic-sensitive-response"}`, domain.CertaintyRejected, domain.ErrorPermanent},
		{"unauthorized", `{"ok":false,"error_code":401,"description":"synthetic-sensitive-response"}`, domain.CertaintyRejected, domain.ErrorPermanent},
		{"forbidden", `{"ok":false,"error_code":403,"description":"synthetic-sensitive-response"}`, domain.CertaintyRejected, domain.ErrorPermanent},
		{"not-found", `{"ok":false,"error_code":404,"description":"synthetic-sensitive-response"}`, domain.CertaintyRejected, domain.ErrorPermanent},
		{"conflict", `{"ok":false,"error_code":409,"description":"synthetic-sensitive-response"}`, domain.CertaintyRejected, domain.ErrorPermanent},
		{"migration", `{"ok":false,"error_code":400,"description":"synthetic-sensitive-response","parameters":{"migrate_to_chat_id":-99999}}`, domain.CertaintyRejected, domain.ErrorPermanent},
		{"rate-limited", `{"ok":false,"error_code":429,"description":"synthetic-sensitive-response","parameters":{"retry_after":30}}`, domain.CertaintyRejected, domain.ErrorRateLimited},
		{"server-error", `{"ok":false,"error_code":500,"description":"synthetic-sensitive-response"}`, domain.CertaintyUnknown, domain.ErrorTemporary},
		{"malformed", `synthetic-sensitive-response`, domain.CertaintyUnknown, domain.ErrorTemporary},
		{"invalid-result", `{"ok":true,"result":"synthetic-sensitive-response"}`, domain.CertaintyUnknown, domain.ErrorTemporary},
		{"null-result", `{"ok":true,"result":null}`, domain.CertaintyUnknown, domain.ErrorPermanent},
		{"wrong-chat", `{"ok":true,"result":{"message_id":42,"message_thread_id":7,"chat":{"id":-99999,"type":"supergroup"}}}`, domain.CertaintyUnknown, domain.ErrorPermanent},
		{"wrong-topic", `{"ok":true,"result":{"message_id":42,"message_thread_id":8,"chat":{"id":-10012,"type":"supergroup"}}}`, domain.CertaintyUnknown, domain.ErrorPermanent},
		{"invalid-message", `{"ok":true,"result":{"message_id":0,"message_thread_id":7,"chat":{"id":-10012,"type":"supergroup"}}}`, domain.CertaintyUnknown, domain.ErrorPermanent},
	}
	for _, row := range cases {
		t.Run(row.name, func(t *testing.T) {
			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if err := r.ParseMultipartForm(1 << 20); err != nil {
					t.Error(err)
				}
				if r.FormValue("chat_id") != "-10012" {
					t.Error("retargeted chat")
				}
				_, _ = w.Write([]byte(row.body))
			}))
			defer server.Close()
			p, err := telegramadapter.NewProvider(map[string]*bot.Bot{"account-1": configured(t, server.URL)})
			if err != nil {
				t.Fatal(err)
			}
			req, attempt := request(t)
			sender, err := p.Reserve(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			defer sender.Release()
			result := sender.SendFinal(context.Background(), attempt)
			if result.Certainty != row.want || result.ErrorClass != row.class || result.Validate() != nil || calls.Load() != 1 {
				t.Fatalf("result=%+v calls=%d", result, calls.Load())
			}
			encoded, _ := json.Marshal(result)
			if strings.Contains(string(encoded), "synthetic-sensitive-response") || strings.Contains(string(encoded), server.URL) {
				t.Fatal("raw provider data leaked")
			}
		})
	}
}

func TestAttemptBindingAndPreSendCancellationHaveNoHTTP(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); _, _ = w.Write([]byte(`{"ok":true}`)) }))
	defer server.Close()
	p, err := telegramadapter.NewProvider(map[string]*bot.Bot{"account-1": configured(t, server.URL)})
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*domain.Attempt){
		"claim-token": func(a *domain.Attempt) { a.ClaimToken = "next-claim" }, "part": func(a *domain.Attempt) { a.PartID = "other" }, "intent-id": func(a *domain.Attempt) { a.IntentID = "other" }, "instance": func(a *domain.Attempt) { a.InstanceID = "other" }, "request-id": func(a *domain.Attempt) { a.RequestID = "other" }, "request-digest": func(a *domain.Attempt) { a.RequestDigest = "other" }, "attempt-id": func(a *domain.Attempt) { a.ID = "" }, "evidence": func(a *domain.Attempt) { a.EvidenceToken = "" }, "attempt-number": func(a *domain.Attempt) { a.Number = 0 }, "owner": func(a *domain.Attempt) { a.Owner = &domain.OwnerFence{InstanceID: "gateway-1", Epoch: 1, Revision: 1} }, "text": func(a *domain.Attempt) { a.Text = "changed" }, "target": func(a *domain.Attempt) { a.Target.ConversationID = "-999" }, "topic": func(a *domain.Attempt) { a.Target.ThreadID = "8" }, "reply": func(a *domain.Attempt) { a.Target.SourceMessageID = "100" }, "execution-generation": func(a *domain.Attempt) { a.Intent.ExecutionGeneration++ }, "missing-call-deadline": func(a *domain.Attempt) { a.CallingUntil = time.Time{} },
	} {
		t.Run(name, func(t *testing.T) {
			req, a := request(t)
			sender, err := p.Reserve(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			defer sender.Release()
			mutate(&a)
			if result := sender.SendFinal(context.Background(), a); result.Certainty != domain.CertaintyNotSent || result.ErrorClass != domain.ErrorPermanent {
				t.Fatalf("result=%+v", result)
			}
		})
	}
	t.Run("cancelled", func(t *testing.T) {
		req, a := request(t)
		sender, err := p.Reserve(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		defer sender.Release()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if result := sender.SendFinal(ctx, a); result.Certainty != domain.CertaintyNotSent || result.ErrorClass != domain.ErrorDeadline {
			t.Fatalf("result=%+v", result)
		}
	})
	t.Run("released", func(t *testing.T) {
		req, a := request(t)
		sender, err := p.Reserve(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		sender.Release()
		if result := sender.SendFinal(context.Background(), a); result.Certainty != domain.CertaintyNotSent {
			t.Fatalf("result=%+v", result)
		}
	})
	t.Run("deadline", func(t *testing.T) {
		req, a := request(t)
		sender, err := p.Reserve(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		defer sender.Release()
		a.CallingUntil = time.Now().Add(-time.Second)
		if result := sender.SendFinal(context.Background(), a); result.Certainty != domain.CertaintyNotSent || result.ErrorClass != domain.ErrorDeadline {
			t.Fatalf("result=%+v", result)
		}
	})
	if calls.Load() != 0 {
		t.Fatalf("invalid/local operations sent %d requests", calls.Load())
	}
}

func TestConcurrentReservationSendsAtMostOnce(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Error(err)
		}
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":42,"message_thread_id":7,"chat":{"id":-10012,"type":"supergroup"}}}`))
	}))
	defer server.Close()
	p, err := telegramadapter.NewProvider(map[string]*bot.Bot{"account-1": configured(t, server.URL)})
	if err != nil {
		t.Fatal(err)
	}
	req, a := request(t)
	sender, err := p.Reserve(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Release()
	results := make(chan domain.Result, 20)
	for range 20 {
		go func() { results <- sender.SendFinal(context.Background(), a) }()
	}
	accepted := 0
	for range 20 {
		r := <-results
		if r.Certainty == domain.CertaintyAccepted {
			accepted++
		} else if r.Certainty != domain.CertaintyNotSent {
			t.Errorf("unexpected %+v", r)
		}
	}
	if calls.Load() != 1 || accepted != 1 {
		t.Fatalf("calls=%d accepted=%d", calls.Load(), accepted)
	}
}

func TestInFlightCancellationAndReleaseRemainUnknown(t *testing.T) {
	for _, release := range []bool{false, true} {
		t.Run(strconv.FormatBool(release), func(t *testing.T) {
			started := make(chan struct{})
			stopServer := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := r.ParseMultipartForm(1 << 20); err != nil {
					t.Error(err)
				}
				close(started)
				select {
				case <-r.Context().Done():
				case <-stopServer:
				}
			}))
			defer server.Close()
			defer close(stopServer)
			p, err := telegramadapter.NewProvider(map[string]*bot.Bot{"account-1": configured(t, server.URL)})
			if err != nil {
				t.Fatal(err)
			}
			req, a := request(t)
			sender, err := p.Reserve(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			defer sender.Release()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan domain.Result, 1)
			go func() { done <- sender.SendFinal(ctx, a) }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("send did not start")
			}
			if release {
				sender.Release()
			} else {
				cancel()
			}
			select {
			case result := <-done:
				if result.Certainty != domain.CertaintyUnknown || result.ErrorClass != domain.ErrorDeadline {
					t.Fatalf("result=%+v", result)
				}
			case <-time.After(time.Second):
				t.Fatal("send did not cancel")
			}
			// A local cancel ends the SDK call, not a proof that a remote
			// endpoint stopped work. The fixture has an independent stop path
			// so server shutdown never relies on transport EOF timing.
		})
	}
}

func TestReserveRejectsInvalidRequestsAndUnknownAccounts(t *testing.T) {
	if _, err := telegramadapter.NewProvider(map[string]*bot.Bot{"account-1": nil}); err == nil {
		t.Fatal("nil client")
	}
	if _, err := telegramadapter.NewProvider(map[string]*bot.Bot{"bad account": configured(t, "http://127.0.0.1:1")}); err == nil {
		t.Fatal("bad account")
	}
	p, err := telegramadapter.NewProvider(nil)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := request(t)
	if _, err = p.Reserve(context.Background(), req); err != domain.ErrUnavailable {
		t.Fatalf("missing config=%v", err)
	}
	if _, err = p.Reserve(nil, req); err != domain.ErrInvalid {
		t.Fatalf("nil context=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = p.Reserve(ctx, req); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled reserve=%v", err)
	}
	req.RequestDigest = "wrong"
	if _, err = p.Reserve(context.Background(), req); err != domain.ErrInvalid {
		t.Fatalf("invalid digest=%v", err)
	}
	req, _ = request(t)
	req.Claim.Part.State = domain.Pending
	if _, err = p.Reserve(context.Background(), req); err != domain.ErrInvalid {
		t.Fatalf("not claimed=%v", err)
	}
}

func TestConnectionLossAfterRequestIsUnknown(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Error(err)
		}
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = conn.Close()
	}))
	defer server.Close()
	p, err := telegramadapter.NewProvider(map[string]*bot.Bot{"account-1": configured(t, server.URL)})
	if err != nil {
		t.Fatal(err)
	}
	req, a := request(t)
	sender, err := p.Reserve(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Release()
	result := sender.SendFinal(context.Background(), a)
	if calls.Load() != 1 || result.Certainty != domain.CertaintyUnknown || result.ErrorClass != domain.ErrorTemporary {
		t.Fatalf("calls=%d result=%+v", calls.Load(), result)
	}
}

func TestLongLogicalTextUsesOnlyPersistedPlanParts(t *testing.T) {
	text := strings.Repeat("界", 4095) + "😀" + "tail"
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Error(err)
		}
		seen = append(seen, r.FormValue("text"))
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":42,"message_thread_id":7,"chat":{"id":-10012,"type":"supergroup"}}}`))
	}))
	defer server.Close()
	p, err := telegramadapter.NewProvider(map[string]*bot.Bot{"account-1": configured(t, server.URL)})
	if err != nil {
		t.Fatal(err)
	}
	initial, _ := request(t)
	initial.Claim.Intent.Text = text
	parts, err := domain.PlanText(initial.Claim.Target, text)
	if err != nil {
		t.Fatal(err)
	}
	for index, part := range parts {
		req, a := request(t)
		req.Claim.Intent = initial.Claim.Intent
		req.Claim.Part.Index = index
		req.Claim.Part.ID = domain.PartID(req.Claim.Intent.ID, index)
		req.Claim.Part.Text = part
		req.RequestDigest, err = domain.RequestDigest(req.Claim)
		if err != nil {
			t.Fatal(err)
		}
		a.Intent = req.Claim.Intent
		a.PartID = req.Claim.Part.ID
		a.Text = part
		a.RequestDigest = req.RequestDigest
		sender, err := p.Reserve(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		result := sender.SendFinal(context.Background(), a)
		sender.Release()
		if result.Certainty != domain.CertaintyAccepted {
			t.Fatalf("part%d=%+v", index, result)
		}
	}
	if len(seen) != 2 || len([]rune(seen[0])) != 4096 || seen[1] != "tail" || strings.Join(seen, "") != text {
		t.Fatalf("unexpected part plan: %v", len(seen))
	}
	// The adapter does not split a caller-invented part; A2 must supply the exact
	// persisted plan. Scheduling the next part is the ledger's separate authority.
	initial.Claim.Part.Text = text
	initial.RequestDigest = "sha256:" + strings.Repeat("0", 64)
	if _, err := p.Reserve(context.Background(), initial); err != domain.ErrInvalid {
		t.Fatalf("oversized invented part accepted: %v", err)
	}
}
