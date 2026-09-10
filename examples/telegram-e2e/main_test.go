package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/telegram"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
)

func TestLoadConfigUsesSafeDefaults(t *testing.T) {
	values := map[string]string{"TELEGRAM_BOT_TOKEN": "receiver-token"}
	configuration, err := loadConfig(func(name string) string { return values[name] })
	if err != nil {
		t.Fatal(err)
	}
	if configuration.botToken != values["TELEGRAM_BOT_TOKEN"] || configuration.senderBotToken != "" {
		t.Fatalf("unexpected token configuration: %+v", configuration)
	}
	if configuration.testMessage == "" || !strings.HasPrefix(configuration.testMessage, "telegram-e2e-") {
		t.Fatalf("generated marker = %q", configuration.testMessage)
	}
	if configuration.runTimeout != defaultRunTimeout || configuration.pollTimeout != defaultPollTimeout {
		t.Fatalf("unexpected defaults: %+v", configuration)
	}
	if configuration.deleteWebhook || configuration.dropPendingUpdate {
		t.Fatalf("destructive webhook defaults must be false: %+v", configuration)
	}
}

func TestLoadConfigRejectsSecretBearingOrUnsafeValuesWithoutEchoingThem(t *testing.T) {
	tests := []map[string]string{
		{"TELEGRAM_BOT_TOKEN": "receiver-token", "TELEGRAM_SENDER_BOT_TOKEN": "receiver-token"},
		{"TELEGRAM_BOT_TOKEN": "receiver-token", "TELEGRAM_TEST_MESSAGE": "contains-receiver-token"},
		{"TELEGRAM_BOT_TOKEN": "receiver-token", "TELEGRAM_TIMEOUT": "not-a-duration"},
		{"TELEGRAM_BOT_TOKEN": "receiver-token", "TELEGRAM_POLL_TIMEOUT": "1s"},
		{"TELEGRAM_BOT_TOKEN": "receiver-token", "TELEGRAM_DELETE_WEBHOOK": "not-a-bool"},
	}
	for _, values := range tests {
		_, err := loadConfig(func(name string) string { return values[name] })
		if !errors.Is(err, errConfiguration) {
			t.Fatalf("values=%v error=%v, want errConfiguration", values, err)
		}
		for _, value := range values {
			if value != "" && strings.Contains(err.Error(), value) {
				t.Fatalf("error %q echoed configured value %q", err, value)
			}
		}
	}
}

func TestLoadConfigParsesExplicitSettings(t *testing.T) {
	values := map[string]string{
		"TELEGRAM_BOT_TOKEN":            "receiver-token",
		"TELEGRAM_SENDER_BOT_TOKEN":     "sender-token",
		"TELEGRAM_TEST_MESSAGE":         "telegram-e2e-marker",
		"TELEGRAM_TIMEOUT":              "45s",
		"TELEGRAM_POLL_TIMEOUT":         "3s",
		"TELEGRAM_DELETE_WEBHOOK":       "true",
		"TELEGRAM_DROP_PENDING_UPDATES": "true",
	}
	configuration, err := loadConfig(func(name string) string { return values[name] })
	if err != nil {
		t.Fatal(err)
	}
	if configuration.testMessage != values["TELEGRAM_TEST_MESSAGE"] || configuration.runTimeout != 45*time.Second || configuration.pollTimeout != 3*time.Second || !configuration.deleteWebhook || !configuration.dropPendingUpdate {
		t.Fatalf("explicit settings were not parsed: %+v", configuration)
	}
}

func TestRunTimeoutCoversBlockingPreflight(t *testing.T) {
	values := map[string]string{
		"TELEGRAM_BOT_TOKEN":    "receiver-token",
		"TELEGRAM_TIMEOUT":      "20ms",
		"TELEGRAM_POLL_TIMEOUT": "2s",
	}
	var observedContextErr error
	prepare := func(ctx context.Context, _ string, _ time.Duration, _, _ bool) (*models.User, error) {
		<-ctx.Done()
		observedContextErr = ctx.Err()
		return nil, errPreflightGetMeTimeout
	}

	err := runWithPreflight(context.Background(), func(name string) string { return values[name] }, io.Discard, prepare)
	if !errors.Is(err, errRunTimeout) {
		t.Fatalf("runWithPreflight() error = %v, want run timeout", err)
	}
	if !errors.Is(observedContextErr, context.DeadlineExceeded) {
		t.Fatalf("preflight context error = %v, want deadline exceeded", observedContextErr)
	}
}

func TestRunWithPreflightTreatsParentCancellationAsClean(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	prepare := func(ctx context.Context, _ string, _ time.Duration, _, _ bool) (*models.User, error) {
		return nil, ctx.Err()
	}
	if err := runWithPreflight(ctx, func(name string) string {
		if name == "TELEGRAM_BOT_TOKEN" {
			return "receiver-token"
		}
		return ""
	}, io.Discard, prepare); err != nil {
		t.Fatalf("runWithPreflight() error = %v, want clean cancellation", err)
	}
}

func TestClassifyManualRunResultTreatsCancellationAsClean(t *testing.T) {
	tests := []struct {
		name          string
		parentErr     error
		runContextErr error
		adapterErr    error
		want          error
	}{
		{name: "parent cancellation", parentErr: context.Canceled, runContextErr: context.Canceled, want: nil},
		{name: "run timeout", runContextErr: context.DeadlineExceeded, want: errRunTimeout},
		{name: "adapter cancellation", adapterErr: context.Canceled, want: nil},
		{name: "unexpected stop", want: errAdapterRun},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := classifyManualRunResult(test.parentErr, test.runContextErr, test.adapterErr); !errors.Is(got, test.want) {
				t.Fatalf("classifyManualRunResult() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestClassifyAutomatedSenderResultTreatsCancellationAndTimeoutAsClean(t *testing.T) {
	tests := []struct {
		name          string
		parentErr     error
		runContextErr error
		senderErr     error
		want          error
	}{
		{name: "parent cancellation", parentErr: context.Canceled, runContextErr: context.Canceled, senderErr: errSender, want: nil},
		{name: "run timeout", runContextErr: context.DeadlineExceeded, senderErr: errSender, want: errRunTimeout},
		{name: "run cancellation", runContextErr: context.Canceled, senderErr: errSender, want: nil},
		{name: "sender failure", senderErr: errSender, want: errSender},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := classifyAutomatedSenderResult(test.parentErr, test.runContextErr, test.senderErr); !errors.Is(got, test.want) {
				t.Fatalf("classifyAutomatedSenderResult() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestPrepareLongPollingHandlesWebhookSafely(t *testing.T) {
	noWebhook := &fakeWebhookClient{}
	if err := prepareLongPolling(context.Background(), noWebhook, false, false); err != nil {
		t.Fatal(err)
	}
	if noWebhook.deleted {
		t.Fatal("did not expect DeleteWebhook without a webhook")
	}

	configured := &fakeWebhookClient{info: &models.WebhookInfo{URL: "https://example.test/telegram"}}
	if err := prepareLongPolling(context.Background(), configured, false, false); !errors.Is(err, errWebhookConfigured) {
		t.Fatalf("configured webhook error = %v", err)
	}
	if configured.deleted {
		t.Fatal("must not delete webhook without explicit permission")
	}

	configured = &fakeWebhookClient{info: &models.WebhookInfo{URL: "https://example.test/telegram"}}
	if err := prepareLongPolling(context.Background(), configured, true, true); err != nil {
		t.Fatal(err)
	}
	if !configured.deleted || !configured.dropPending {
		t.Fatalf("DeleteWebhook options were not preserved: %+v", configured)
	}
}

func TestClassifyGetMeErrorRedactsProviderDetails(t *testing.T) {
	secret := "bot-secret"
	tests := []struct {
		name string
		err  error
		want error
	}{
		{name: "nil error", want: errPreflightGetMeReply},
		{name: "context timeout", err: context.DeadlineExceeded, want: errPreflightGetMeTimeout},
		{name: "api rejection", err: fmt.Errorf("provider response: %w", bot.ErrorUnauthorized), want: errPreflightGetMeAPI},
		{name: "rate limit", err: &bot.TooManyRequestsError{Message: "provider response", RetryAfter: 1}, want: errPreflightGetMeAPI},
		{name: "network error", err: &url.Error{Op: "POST", URL: "https://api.telegram.org/bot" + secret + "/getMe", Err: errors.New("dial failed")}, want: errPreflightGetMeNetwork},
		{name: "invalid response", err: errors.New("provider response could not be decoded"), want: errPreflightGetMeReply},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := classifyGetMeError(test.err)
			if !errors.Is(got, test.want) {
				t.Fatalf("classifyGetMeError(%v) = %v, want %v", test.err, got, test.want)
			}
			if strings.Contains(got.Error(), secret) {
				t.Fatalf("classification error %q leaked provider secret", got)
			}
		})
	}
}

func TestClassifyAdapterErrorRedactsProviderDetails(t *testing.T) {
	secret := "bot-secret"
	tests := []struct {
		name string
		err  error
		want error
	}{
		{name: "invalid configuration", err: fmt.Errorf("provider token %s: %w", secret, telegram.ErrInvalid), want: errAdapterConfiguration},
		{name: "identity mismatch", err: telegram.ErrBotIdentityMismatch, want: errAdapterIdentity},
		{name: "initialization", err: fmt.Errorf("provider detail: %w", telegram.ErrInitialization), want: errAdapterInitialization},
		{name: "fallback", err: errors.New("unexpected provider error"), want: errPreflight},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := classifyAdapterError(test.err)
			if !errors.Is(got, test.want) {
				t.Fatalf("classifyAdapterError(%v) = %v, want %v", test.err, got, test.want)
			}
			if strings.Contains(got.Error(), secret) {
				t.Fatalf("classification error %q leaked provider secret", got)
			}
		})
	}
}

func TestNewTrustedTargetUsesTheTrustedBoundary(t *testing.T) {
	target, err := newTrustedTarget("123456789")
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Validate(); err != nil {
		t.Fatal(err)
	}
	if target.Channel != channels.ChannelTelegram || target.ProviderAccountID != "123456789" {
		t.Fatalf("unexpected target: %+v", target)
	}
}

func TestNewTrustedTargetRejectsNonCanonicalAccountID(t *testing.T) {
	for _, value := range []string{"", "0", "+123456789", "0123456789", "bot"} {
		if _, err := newTrustedTarget(value); !errors.Is(err, errConfiguration) {
			t.Fatalf("provider account %q error = %v", value, err)
		}
	}
}

func TestDeterministicDispatcherEmitsCompleteReplyAndMarksInput(t *testing.T) {
	reply := e2eReplyFor("correlation")
	dispatcher := newDeterministicDispatcher("marker", reply)
	stream, err := dispatcher.Dispatch(context.Background(), gateway.DispatchRequest{
		Message: gateway.InboundMessage{Content: "marker"}, RequestID: "request-id",
	})
	if err != nil {
		t.Fatal(err)
	}
	var events []gateway.DispatchEvent
	for event := range stream {
		events = append(events, event)
	}
	if len(events) != 2 || events[0].Type != gateway.DispatchEventMessage || events[0].Text != reply || !events[1].Done {
		t.Fatalf("unexpected dispatch events: %+v", events)
	}
	select {
	case message := <-dispatcher.seen:
		if message.Content != "marker" {
			t.Fatalf("seen message = %+v", message)
		}
	default:
		t.Fatal("dispatcher did not mark the expected message")
	}
}

func TestDeterministicDispatcherDoesNotReplyToNonMarker(t *testing.T) {
	dispatcher := newDeterministicDispatcher("marker", e2eReplyFor("correlation"))
	stream, err := dispatcher.Dispatch(context.Background(), gateway.DispatchRequest{
		Message: gateway.InboundMessage{Content: "old-message"}, RequestID: "request-id",
	})
	if err != nil {
		t.Fatal(err)
	}
	var events []gateway.DispatchEvent
	for event := range stream {
		events = append(events, event)
	}
	if len(events) != 1 || !events[0].Done || events[0].Type != gateway.DispatchEventDone {
		t.Fatalf("unexpected non-marker events: %+v", events)
	}
	select {
	case message := <-dispatcher.seen:
		t.Fatalf("non-marker was recorded as seen: %+v", message)
	default:
	}
}

func TestExpectedAutomatedReplyRequiresCorrelationAndPrivatePeer(t *testing.T) {
	receiverID := int64(42)
	reply := e2eReplyFor("correlation")
	tests := []struct {
		name   string
		update *models.Update
		want   bool
	}{
		{name: "valid reply", update: &models.Update{Message: &models.Message{From: &models.User{ID: receiverID}, Chat: models.Chat{ID: receiverID}, Text: reply}}, want: true},
		{name: "stale correlation", update: &models.Update{Message: &models.Message{From: &models.User{ID: receiverID}, Chat: models.Chat{ID: receiverID}, Text: e2eReplyFor("stale")}}},
		{name: "unexpected chat", update: &models.Update{Message: &models.Message{From: &models.User{ID: receiverID}, Chat: models.Chat{ID: 99}, Text: reply}}},
		{name: "unexpected sender", update: &models.Update{Message: &models.Message{From: &models.User{ID: 99}, Chat: models.Chat{ID: receiverID}, Text: reply}}},
		{name: "missing message", update: &models.Update{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isExpectedAutomatedReply(test.update, receiverID, reply); got != test.want {
				t.Fatalf("isExpectedAutomatedReply() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestPreflightHTTPClientUsesPollTimeoutBudget(t *testing.T) {
	client := preflightHTTPClient(3 * time.Second)
	if client.Timeout != 8*time.Second {
		t.Fatalf("preflight HTTP timeout = %s, want 8s", client.Timeout)
	}
}

func TestReportTelegramErrorUsesStructuredFields(t *testing.T) {
	var output strings.Builder
	restoreLogger, err := configureLogger(&output)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(restoreLogger)

	reportTelegramError(telegram.ErrorEvent{Operation: telegram.ErrorOperationPolling, Err: telegram.ErrPolling})

	logged := output.String()
	if !strings.Contains(logged, "[telegram-e2e] telegram operation failed") || !strings.Contains(logged, "polling") || !strings.Contains(logged, "telegram polling failed") {
		t.Fatalf("structured Telegram error = %q", logged)
	}
}

type fakeWebhookClient struct {
	info        *models.WebhookInfo
	deleted     bool
	dropPending bool
}

func (client *fakeWebhookClient) GetWebhookInfo(context.Context) (*models.WebhookInfo, error) {
	return client.info, nil
}

func (client *fakeWebhookClient) DeleteWebhook(_ context.Context, params *bot.DeleteWebhookParams) (bool, error) {
	client.deleted = true
	client.dropPending = params != nil && params.DropPendingUpdates
	return true, nil
}
