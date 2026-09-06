package reply

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/telegram"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

// Exercise the real Sender, Telegram Adapter and MemoryJournal together. Only
// the remote Telegram API is simulated; these tests never load a real Bot token.
func TestSenderTelegramRetryLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name       string
		responses  []int
		wantStatus string
	}{
		{"retry_after_then_success", []int{429, 200}, "sent"},
		{"retry_budget_exhausted", []int{429, 429}, "dead"},
		{"permanent_error", []int{403}, "dead"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				index := int(requests.Add(1)) - 1
				if r.Method != http.MethodPost || r.URL.Path != "/botfake-token/sendMessage" {
					t.Errorf("unexpected Telegram request: %s %s", r.Method, r.URL.Path)
				}
				var body struct {
					ChatID   int64  `json:"chat_id"`
					ThreadID int64  `json:"message_thread_id"`
					Text     string `json:"text"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Errorf("decode sendMessage: %v", err)
				}
				if body.ChatID != -100 || body.ThreadID != 7 || body.Text != "test reply" {
					t.Errorf("reply target or content changed: %+v", body)
				}
				if index >= len(tc.responses) {
					t.Errorf("unexpected extra sendMessage attempt %d", index+1)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.responses[index])
				switch tc.responses[index] {
				case 200:
					_, _ = io.WriteString(w, `{"ok":true,"result":{"message_id":88}}`)
				case 429:
					_, _ = io.WriteString(w, `{"ok":false,"error_code":429,"description":"Too Many Requests","parameters":{"retry_after":1}}`)
				case 403:
					_, _ = io.WriteString(w, `{"ok":false,"error_code":403,"description":"Forbidden"}`)
				}
			}))
			t.Cleanup(server.Close)

			journal, auditWriter, requestID, newSender := telegramRetryFixture(t, server)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			first := newSender("sender-a")
			started := time.Now()
			sent, err := first.ProcessOnce(ctx)
			finished := time.Now()
			var deliveryErr *channels.DeliveryError
			if sent != 0 || !errors.As(err, &deliveryErr) || len(journal.failures) != 1 {
				t.Fatalf("first attempt: sent=%d failures=%+v err=%v", sent, journal.failures, err)
			}
			failure := journal.failures[0]
			wantDelay := time.Hour // Fallback must not override Telegram retry_after.
			if tc.responses[0] == 429 {
				wantDelay = time.Second
				if !deliveryErr.Retryable || deliveryErr.RetryAfter != wantDelay || failure.terminal {
					t.Fatalf("429 was not scheduled for retry: delivery=%+v failure=%+v", deliveryErr, failure)
				}
				assertOutboundStatus(t, journal, failure.outboundID, "pending")
			} else if deliveryErr.Retryable || !failure.terminal {
				t.Fatalf("permanent error must be terminal: delivery=%+v failure=%+v", deliveryErr, failure)
			}
			if failure.retryAt.Before(started.Add(wantDelay)) || failure.retryAt.After(finished.Add(wantDelay)) {
				t.Fatalf("retry_at=%s, want within [%s, %s]", failure.retryAt, started.Add(wantDelay), finished.Add(wantDelay))
			}

			// A different Sender must see the same persisted schedule, without
			// relying on a sleep or retry counter inside the original Sender.
			second := newSender("sender-b")
			if sent, err := second.ProcessOnce(ctx); err != nil || sent != 0 || requests.Load() != 1 {
				t.Fatalf("retried before deadline: sent=%d requests=%d err=%v", sent, requests.Load(), err)
			}
			if len(tc.responses) == 2 {
				timer := time.NewTimer(time.Until(failure.retryAt))
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-ctx.Done():
					t.Fatal("timed out waiting for retry deadline")
				}
				sent, err = second.ProcessOnce(ctx)
				if tc.wantStatus == "sent" {
					if sent != 1 || err != nil || journal.providerMessageID != "88" || len(journal.failures) != 1 {
						t.Fatalf("retry did not succeed: sent=%d receipt=%q err=%v", sent, journal.providerMessageID, err)
					}
				} else if sent != 0 || !errors.As(err, &deliveryErr) || len(journal.failures) != 2 || !journal.failures[1].terminal {
					t.Fatalf("retry budget did not stop delivery: sent=%d failures=%+v err=%v", sent, journal.failures, err)
				}
			}

			assertOutboundStatus(t, journal, failure.outboundID, tc.wantStatus)
			if len(journal.claims) != len(tc.responses) {
				t.Fatalf("claims=%+v", journal.claims)
			}
			for index, item := range journal.claims {
				if item.ID != failure.outboundID || item.RequestID != requestID || item.AttemptCount != index+1 {
					t.Fatalf("outbound identity or attempt changed: %+v", item)
				}
			}
			if sent, err := second.ProcessOnce(ctx); sent != 0 || err != nil || int(requests.Load()) != len(tc.responses) {
				t.Fatalf("terminal delivery was sent again: sent=%d requests=%d err=%v", sent, requests.Load(), err)
			}

			events := auditWriter.Events()
			if len(events) != len(tc.responses) {
				t.Fatalf("audit events=%+v", events)
			}
			for index, event := range events {
				wantDecision := "reply_failed"
				if tc.responses[index] == 200 {
					wantDecision = "reply_sent"
				}
				if event.RequestID != requestID || event.Channel != "telegram" || event.TenantID != "tutorial-tenant" ||
					event.Decision != wantDecision || event.Details["outbound_id"] != failure.outboundID {
					t.Fatalf("audit correlation lost: %+v", event)
				}
				if wantDecision == "reply_failed" && (event.ErrorType != "channel_delivery" || event.Details["terminal"] != journal.failures[index].terminal) {
					t.Fatalf("failure audit missing retry decision: %+v", event)
				}
			}
		})
	}
}

// Record state-transition arguments while retaining the real journal's claim,
// lease, next_attempt_at and terminal-state behavior. Access is synchronous with
// ProcessOnce; the HTTP handler above shares only an atomic request counter.
type recordingOutboundJournal struct {
	*gateway.MemoryJournal
	claims            []gateway.OutboundItem
	failures          []outboundFailure
	providerMessageID string
}

type outboundFailure struct {
	outboundID string
	retryAt    time.Time
	terminal   bool
}

func (j *recordingOutboundJournal) ClaimOutbound(ctx context.Context, workerID string, limit int, lease time.Duration) ([]gateway.OutboundItem, error) {
	items, err := j.MemoryJournal.ClaimOutbound(ctx, workerID, limit, lease)
	j.claims = append(j.claims, items...)
	return items, err
}

func (j *recordingOutboundJournal) MarkOutboundFailed(ctx context.Context, id, workerID string, retryAt time.Time, terminal bool, cause error) error {
	if err := j.MemoryJournal.MarkOutboundFailed(ctx, id, workerID, retryAt, terminal, cause); err != nil {
		return err
	}
	j.failures = append(j.failures, outboundFailure{id, retryAt, terminal})
	return nil
}

func (j *recordingOutboundJournal) MarkOutboundSent(ctx context.Context, id, workerID, providerMessageID string) error {
	if err := j.MemoryJournal.MarkOutboundSent(ctx, id, workerID, providerMessageID); err != nil {
		return err
	}
	j.providerMessageID = providerMessageID
	return nil
}

func assertOutboundStatus(t *testing.T, journal *recordingOutboundJournal, id, want string) {
	t.Helper()
	if status, ok := journal.OutboundStatus(id); !ok || status != want {
		t.Fatalf("outbound status=%q exists=%t want=%q", status, ok, want)
	}
}

func telegramRetryFixture(t *testing.T, server *httptest.Server) (*recordingOutboundJournal, *audit.MemoryWriter, string, func(string) *Sender) {
	t.Helper()
	data := controlplane.DefaultBootstrapData()
	binding := &data.ChannelBindings[0]
	binding.ChannelType = "telegram"
	binding.Config = json.RawMessage(fmt.Sprintf(`{"bot_token_ref":"test://bot","webhook_secret_ref":"test://webhook","api_base_url":%q}`, server.URL))
	repository := controlplane.NewMemoryRepository(data)
	journal := &recordingOutboundJournal{MemoryJournal: gateway.NewMemoryJournal()}
	auditWriter := audit.NewMemoryWriter()
	t.Cleanup(func() {
		_ = journal.Close()
		_ = repository.Close()
	})
	adapter, err := telegram.New(secret.StaticStore{"test://bot": "fake-token"}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	registry, err := channels.NewRegistry(adapter)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := runtimecontext.NewScope(binding.TenantID, binding.AppID, data.Revisions[0].ID, binding.ChannelType, binding.ID)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := journal.Accept(context.Background(), gateway.InboundRequest{
		Scope: scope, ExternalMessageID: "retry-message", UserID: "alice",
		SessionID: "retry-session", ChatType: "group", Text: "hello",
		ReplyTarget: `{"chat_id":-100,"message_thread_id":7}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	task := journal.Tasks()[0]
	if err := journal.CompleteRun(context.Background(), task, gateway.RunResult{Reply: "test reply"}); err != nil {
		t.Fatal(err)
	}
	return journal, auditWriter, accepted.RequestID, func(workerID string) *Sender {
		t.Helper()
		sender, err := New(journal, repository, registry, Options{
			WorkerID: workerID, BatchSize: 10, ClaimLease: time.Minute,
			PollInterval: time.Second, RetryDelay: time.Hour, MaxAttempts: 2,
			Audit: auditWriter,
		})
		if err != nil {
			t.Fatal(err)
		}
		return sender
	}
}
