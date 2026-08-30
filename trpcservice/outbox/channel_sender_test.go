package outbox

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	larkchannel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/lark"
	telegramchannel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/telegram"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

type sequenceHTTPDoer struct {
	mu        sync.Mutex
	responses []sequenceHTTPResponse
	keys      []string
}

type sequenceHTTPResponse struct {
	status int
	body   string
	err    error
}

func (f *sequenceHTTPDoer) Do(request *http.Request) (*http.Response, error) {
	f.mu.Lock()
	f.keys = append(f.keys, request.Header.Get("Idempotency-Key"))
	index := len(f.keys) - 1
	response := f.responses[len(f.responses)-1]
	if index < len(f.responses) {
		response = f.responses[index]
	}
	f.mu.Unlock()
	if response.err != nil {
		return nil, response.err
	}
	return &http.Response{StatusCode: response.status, Body: io.NopCloser(stringReader(response.body))}, nil
}

func stringReader(value string) io.Reader { return &fixedReader{value: []byte(value)} }

type fixedReader struct {
	value []byte
	read  int
}

func (r *fixedReader) Read(buffer []byte) (int, error) {
	if r.read == len(r.value) {
		return 0, io.EOF
	}
	n := copy(buffer, r.value[r.read:])
	r.read += n
	return n, nil
}

func (r *fixedReader) Close() error { return nil }

func channelOutboxMessage(t *testing.T, tcTenant string) storage.OutboxMessage {
	t.Helper()
	payload := channels.ReplyOutboxPayload{
		SchemaVersion: channels.ReplyOutboxSchemaVersion, Kind: channels.ReplyOutboxKind,
		TenantID: tcTenant, SessionID: "session-channel", JobID: "job-channel", ExecutionID: "execution-channel",
		RequestID: "request-channel", MessageID: "message-channel", TraceID: "trace-channel", BindingID: "binding-channel",
		Channel: "web", DestinationType: channels.DestinationTypeUser, DestinationID: "user-channel",
		ReplyText: "reply text", SenderRoutingVersion: channels.SenderRoutingVersion,
	}
	encoded, err := channels.EncodeReplyOutboxPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	return storage.OutboxMessage{
		TenantID: tcTenant, ID: "reply-" + payload.ExecutionID, Kind: channels.ReplyOutboxKind,
		AggregateID: payload.ExecutionID, DedupKey: tcTenant + "|" + payload.ExecutionID + "|agent.reply", Payload: encoded,
	}
}

func channelHTTPSender(t *testing.T, doer *sequenceHTTPDoer) *channels.HTTPSender {
	t.Helper()
	sender, err := channels.NewHTTPSender(channels.HTTPSenderConfig{
		Registry: channels.DefaultRegistry(), Client: doer,
		Endpoints: map[string]string{"web": "https://web.invalid/base"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return sender
}

func telegramOutboxMessage(t *testing.T, tcTenant string, bindingID string) storage.OutboxMessage {
	t.Helper()
	payload := channels.ReplyOutboxPayload{
		SchemaVersion: channels.ReplyOutboxSchemaVersion, Kind: channels.ReplyOutboxKind,
		TenantID: tcTenant, SessionID: "session-telegram", JobID: "job-telegram", ExecutionID: "execution-telegram",
		RequestID: "request-telegram", MessageID: "message-telegram", TraceID: "trace-telegram", BindingID: bindingID,
		Channel: telegramchannel.Channel, DestinationType: channels.DestinationTypeChat, DestinationID: "-100123456789",
		ReplyText: "reply text", SenderRoutingVersion: channels.SenderRoutingVersion,
	}
	encoded, err := channels.EncodeReplyOutboxPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	return storage.OutboxMessage{TenantID: tcTenant, ID: "reply-" + payload.ExecutionID, Kind: channels.ReplyOutboxKind, AggregateID: payload.ExecutionID, DedupKey: tcTenant + "|" + payload.ExecutionID + "|" + channels.ReplyOutboxKind, Payload: encoded}
}

func telegramSender(t *testing.T, doer *sequenceHTTPDoer, tcTenant, bindingID string) *telegramchannel.Sender {
	t.Helper()
	sender, err := telegramchannel.NewSender(telegramchannel.SenderConfig{
		Bindings: []telegramchannel.Binding{{TenantID: tcTenant, BindingID: bindingID, Channel: telegramchannel.Channel, BotTokenSecretRef: "token-ref", Enabled: true}},
		Tokens:   telegramchannel.TokenResolverFunc(func(context.Context, telegramchannel.Binding) (string, error) { return "123456:token_for_test", nil }),
		Client:   doer,
	})
	if err != nil {
		t.Fatal(err)
	}
	return sender
}

func TestChannelSenderAdaptsOutcomeWithoutDurableMutation(t *testing.T) {
	delegate := channels.SenderFunc(func(context.Context, storage.OutboxMessage) channels.SenderOutcome {
		return channels.SenderOutcome{Class: channels.OutcomePermanentFailure, Code: channels.SenderInvalidDestinationCode}
	})
	sender, err := NewChannelSender(delegate)
	if err != nil {
		t.Fatal(err)
	}
	outcome := sender.Send(context.Background(), storage.OutboxMessage{})
	if outcome.Class != OutcomePermanentFailure || outcome.Code != SenderInvalidDestinationCode {
		t.Fatalf("adapted outcome=%+v", outcome)
	}
}

func TestDispatcherTelegramDeliveredCompletesOutbox(t *testing.T) {
	tc := testTenant("tenant-telegram-dispatch")
	repository := &observingRepository{inner: storage.NewFakeRepository(), transitionCalls: make(chan observedTransition, 2)}
	message := telegramOutboxMessage(t, tc.TenantID, tc.BindingID)
	if err := repository.Enqueue(context.Background(), tc, message); err != nil {
		t.Fatal(err)
	}
	doer := &sequenceHTTPDoer{responses: []sequenceHTTPResponse{{status: http.StatusOK, body: `{"ok":true,"result":{"message_id":9}}`}}}
	sender, err := NewChannelSender(telegramSender(t, doer, tc.TenantID, tc.BindingID))
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, err := NewDispatcher(repository, sender, testConfig(tc, "telegram-dispatcher"))
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- dispatcher.Run(context.Background()) }()
	transition := waitForTransition(t, repository, "complete")
	if transition.id != message.ID || dispatcher.Stats().Completed != 1 {
		t.Fatalf("Telegram completion transition=%+v stats=%+v", transition, dispatcher.Stats())
	}
	stopCleanly(t, dispatcher)
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error=%v", err)
	}
}

func TestDispatcherTelegramFailureUsesRetryAndUnknownPolicy(t *testing.T) {
	tc := testTenant("tenant-telegram-failure")
	repository := &observingRepository{inner: storage.NewFakeRepository(), transitionCalls: make(chan observedTransition, 2)}
	message := telegramOutboxMessage(t, tc.TenantID, tc.BindingID)
	if err := repository.Enqueue(context.Background(), tc, message); err != nil {
		t.Fatal(err)
	}
	doer := &sequenceHTTPDoer{responses: []sequenceHTTPResponse{{status: http.StatusTooManyRequests, body: `{"ok":false,"error_code":429,"parameters":{"retry_after":1}}`}}}
	sender, err := NewChannelSender(telegramSender(t, doer, tc.TenantID, tc.BindingID))
	if err != nil {
		t.Fatal(err)
	}
	config := testConfig(tc, "telegram-failure-dispatcher")
	config.RetryPolicy = RetryPolicy{MaxAttempts: 2, BaseDelay: time.Hour, MaxDelay: time.Hour}
	dispatcher, err := NewDispatcher(repository, sender, config)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- dispatcher.Run(context.Background()) }()
	transition := waitForTransition(t, repository, "retry")
	if transition.code != channels.TelegramSenderRateLimitedCode || dispatcher.Stats().Completed != 0 || dispatcher.Stats().DeadLettered != 0 {
		t.Fatalf("Telegram retry transition=%+v stats=%+v", transition, dispatcher.Stats())
	}
	stopCleanly(t, dispatcher)
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error=%v", err)
	}

	repository = &observingRepository{inner: storage.NewFakeRepository(), transitionCalls: make(chan observedTransition, 2)}
	if err := repository.Enqueue(context.Background(), tc, message); err != nil {
		t.Fatal(err)
	}
	unknownDoer := &sequenceHTTPDoer{responses: []sequenceHTTPResponse{{err: errors.New("response may have been accepted")}}}
	unknownSender, err := NewChannelSender(telegramSender(t, unknownDoer, tc.TenantID, tc.BindingID))
	if err != nil {
		t.Fatal(err)
	}
	unknownDispatcher, err := NewDispatcher(repository, unknownSender, config)
	if err != nil {
		t.Fatal(err)
	}
	unknownRunDone := make(chan error, 1)
	go func() { unknownRunDone <- unknownDispatcher.Run(context.Background()) }()
	unknownTransition := waitForTransition(t, repository, "retry")
	if unknownTransition.code != DeliveryOutcomeUnknownCode || unknownDispatcher.Stats().Completed != 0 || unknownDispatcher.Stats().DeadLettered != 0 {
		t.Fatalf("Telegram unknown transition=%+v stats=%+v", unknownTransition, unknownDispatcher.Stats())
	}
	stopCleanly(t, unknownDispatcher)
	if err := <-unknownRunDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("unknown Run error=%v", err)
	}
}

func TestDispatcherLarkPermanentOutcomeMovesToDLQ(t *testing.T) {
	tc := testTenant("tenant-lark-dlq")
	repository := &observingRepository{inner: storage.NewFakeRepository(), transitionCalls: make(chan observedTransition, 2)}
	payload := channels.ReplyOutboxPayload{
		SchemaVersion: channels.ReplyOutboxSchemaVersion, Kind: channels.ReplyOutboxKind,
		TenantID: tc.TenantID, SessionID: "session-lark-dlq", JobID: "job-lark-dlq", ExecutionID: "execution-lark-dlq",
		RequestID: "request-lark-dlq", MessageID: "message-lark-dlq", TraceID: "trace-lark-dlq", BindingID: "missing-lark-binding",
		Channel: larkchannel.Channel, DestinationType: channels.DestinationTypeUser, DestinationID: "ou_lark_dlq",
		ReplyText: "reply text", SenderRoutingVersion: channels.SenderRoutingVersion,
	}
	encoded, err := channels.EncodeReplyOutboxPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	message := storage.OutboxMessage{TenantID: tc.TenantID, ID: "reply-" + payload.ExecutionID, Kind: channels.ReplyOutboxKind, AggregateID: payload.ExecutionID, DedupKey: tc.TenantID + "|" + payload.ExecutionID + "|" + channels.ReplyOutboxKind, Payload: encoded}
	if err := repository.Enqueue(context.Background(), tc, message); err != nil {
		t.Fatal(err)
	}
	larkSender, err := larkchannel.NewSender(larkchannel.SenderConfig{
		Tokens: larkchannel.TokenResolverFunc(func(context.Context, larkchannel.Binding) (string, error) { return "unused", nil }),
		Client: &sequenceHTTPDoer{responses: []sequenceHTTPResponse{{status: http.StatusOK, body: `{}`}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	sender, err := NewChannelSender(larkSender)
	if err != nil {
		t.Fatal(err)
	}
	config := testConfig(tc, "dispatcher-lark-dlq")
	config.RetryPolicy = RetryPolicy{MaxAttempts: 1, BaseDelay: 0, MaxDelay: 0}
	dispatcher, err := NewDispatcher(repository, sender, config)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- dispatcher.Run(context.Background()) }()
	transition := waitForTransition(t, repository, "dead-letter")
	if transition.code != channels.LarkSenderNotConfiguredCode {
		t.Fatalf("Lark DLQ transition=%+v", transition)
	}
	stopCleanly(t, dispatcher)
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error=%v", err)
	}
	stats := dispatcher.Stats()
	if stats.Completed != 0 || stats.DeadLettered != 1 {
		t.Fatalf("Lark DLQ stats=%+v", stats)
	}
}

func TestDispatcherChannelSenderCompletesRoutingReply(t *testing.T) {
	tc := testTenant("tenant-channel-dispatch")
	repository := &observingRepository{inner: storage.NewFakeRepository(), transitionCalls: make(chan observedTransition, 2)}
	message := channelOutboxMessage(t, tc.TenantID)
	if err := repository.Enqueue(context.Background(), tc, message); err != nil {
		t.Fatal(err)
	}
	doer := &sequenceHTTPDoer{responses: []sequenceHTTPResponse{{status: http.StatusOK, body: `{"accepted":true}`}}}
	httpSender := channelHTTPSender(t, doer)
	sender, err := NewChannelSender(httpSender)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, err := NewDispatcher(repository, sender, testConfig(tc, "channel-dispatcher"))
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- dispatcher.Run(context.Background()) }()
	transition := waitForTransition(t, repository, "complete")
	if transition.id != message.ID || dispatcher.Stats().Completed != 1 {
		t.Fatalf("routing completion transition=%+v stats=%+v", transition, dispatcher.Stats())
	}
	stopCleanly(t, dispatcher)
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error=%v", err)
	}
	doer.mu.Lock()
	keys := append([]string(nil), doer.keys...)
	doer.mu.Unlock()
	if len(keys) != 1 || keys[0] != channels.ExternalIdempotencyKey(func() channels.ReplyOutboxPayload {
		payload, err := channels.DecodeReplyOutboxPayload(message.Payload)
		if err != nil {
			t.Fatal(err)
		}
		return payload
	}()) {
		t.Fatalf("external idempotency keys=%v", keys)
	}
}

func TestDispatcherChannelSenderRetryPreservesExternalIdempotencyKey(t *testing.T) {
	tc := testTenant("tenant-channel-retry")
	repository := &observingRepository{inner: storage.NewFakeRepository(), transitionCalls: make(chan observedTransition, 4)}
	message := channelOutboxMessage(t, tc.TenantID)
	if err := repository.Enqueue(context.Background(), tc, message); err != nil {
		t.Fatal(err)
	}
	doer := &sequenceHTTPDoer{responses: []sequenceHTTPResponse{
		{status: http.StatusTooManyRequests, body: `{}`},
		{status: http.StatusOK, body: `{"accepted":true}`},
	}}
	sender, err := NewChannelSender(channelHTTPSender(t, doer))
	if err != nil {
		t.Fatal(err)
	}
	config := testConfig(tc, "channel-retry-dispatcher")
	config.RetryPolicy = RetryPolicy{MaxAttempts: 2, BaseDelay: 0, MaxDelay: 0}
	dispatcher, err := NewDispatcher(repository, sender, config)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- dispatcher.Run(context.Background()) }()
	first := waitForTransition(t, repository, "retry")
	if first.code != SenderRateLimitedCode {
		t.Fatalf("first transition=%+v", first)
	}
	waitForTransition(t, repository, "complete")
	stopCleanly(t, dispatcher)
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error=%v", err)
	}
	doer.mu.Lock()
	keys := append([]string(nil), doer.keys...)
	doer.mu.Unlock()
	if len(keys) != 2 || keys[0] == "" || keys[0] != keys[1] {
		t.Fatalf("retry changed external idempotency key: %v", keys)
	}
}

func TestChannelSenderConnectionInterruptionUsesUnknownPolicy(t *testing.T) {
	tc := testTenant("tenant-channel-unknown")
	repository := &observingRepository{inner: storage.NewFakeRepository(), transitionCalls: make(chan observedTransition, 2)}
	if err := repository.Enqueue(context.Background(), tc, channelOutboxMessage(t, tc.TenantID)); err != nil {
		t.Fatal(err)
	}
	doer := &sequenceHTTPDoer{responses: []sequenceHTTPResponse{{err: errors.New("connection interrupted Authorization Bearer secret")}}}
	sender, err := NewChannelSender(channelHTTPSender(t, doer))
	if err != nil {
		t.Fatal(err)
	}
	config := testConfig(tc, "channel-unknown-dispatcher")
	config.RetryPolicy = RetryPolicy{MaxAttempts: 2, BaseDelay: time.Hour, MaxDelay: time.Hour}
	dispatcher, err := NewDispatcher(repository, sender, config)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- dispatcher.Run(context.Background()) }()
	transition := waitForTransition(t, repository, "retry")
	if transition.code != DeliveryOutcomeUnknownCode || dispatcher.Stats().Completed != 0 || dispatcher.Stats().DeadLettered != 0 {
		t.Fatalf("unknown transition=%+v stats=%+v", transition, dispatcher.Stats())
	}
	stopCleanly(t, dispatcher)
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error=%v", err)
	}
}
