package text

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionrun"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// The attribute keys this package asserts on, spelled the way an operator would
// read them back out of a collector rather than borrowed from the constants
// that produced them.
const (
	keyStage   = "trpc.stage"
	keyOutcome = "trpc.outcome"
	keyError   = "trpc.error_type"
	keyRequest = "trpc.request_id"
	keyRun     = "trpc.run_id"
	keyOutbox  = "trpc.outbox_id"
	keyAttempt = "trpc.attempt"
)

// The values a record must never repeat, whatever channel carried them: what a
// user wrote, what the platform called their message, and what was answered.
const (
	msgIDMarker = "msgid-marker-0b7f24"
	bodyMarker  = "body-marker-hello-world"
	replyMarker = "reply-marker-answered"
)

// inMemory is a Telemetry that exports nowhere, plus the readers a test asserts
// on. A synchronous span processor keeps the records in step with the loop that
// produced them, so nothing here waits for a batch.
func inMemory(t *testing.T) (*telemetry.Telemetry, *tracetest.InMemoryExporter) {
	t.Helper()
	spans := tracetest.NewInMemoryExporter()
	observer, err := telemetry.New(
		sdktrace.NewTracerProvider(sdktrace.WithSyncer(spans)),
		sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewManualReader())),
	)
	require.NoError(t, err)
	return observer, spans
}

func newRecordingConsumer(t *testing.T, store channels.Store) (
	*Consumer, *fakeAdapter, *tracetest.InMemoryExporter,
) {
	t.Helper()
	observer, spans := inMemory(t)
	adapter := newFakeAdapter()
	consumer, err := New(Config{
		Identity:  testIdentity(),
		Adapter:   adapter,
		Store:     store,
		Runs:      &sessionrun.Service{},
		Revisions: func(context.Context, string, string, string) error { return nil },
		Telemetry: observer,
	})
	require.NoError(t, err)
	return consumer, adapter, spans
}

// stageSpans indexes what was recorded by stage, and fails a test that finds a
// span this package did not create.
func stageSpans(t *testing.T, spans *tracetest.InMemoryExporter) map[string][]attribute.Set {
	t.Helper()
	byStage := map[string][]attribute.Set{}
	for _, recorded := range spans.GetSpans() {
		attributes := attribute.NewSet(recorded.Attributes...)
		stage, ok := attributes.Value(keyStage)
		require.True(t, ok, recorded.Name)
		require.Equal(t, "channel."+stage.AsString(), recorded.Name)
		byStage[stage.AsString()] = append(byStage[stage.AsString()], attributes)
	}
	return byStage
}

// spanText returns one attribute of a recorded span.
func spanText(t *testing.T, attributes attribute.Set, key string) string {
	t.Helper()
	value, ok := attributes.Value(attribute.Key(key))
	require.Truef(t, ok, "%s was not recorded", key)
	return value.Emit()
}

// acceptStore is the accept side of a Store, and answers with the row an
// earlier delivery created.
type acceptStore struct {
	channels.Store
	stored   channels.AcceptResult
	requests []channels.AcceptRequest
	fail     int
}

func (s *acceptStore) Accept(
	_ context.Context, _ tenant.TenantContext, request channels.AcceptRequest,
) (channels.AcceptResult, error) {
	s.requests = append(s.requests, request)
	if s.fail > 0 {
		s.fail--
		return channels.AcceptResult{}, errors.New("storage unavailable")
	}
	return s.stored, nil
}

// A redelivery is recorded against the request the first delivery created. The
// ids this pass minted were never stored, and reporting them would invent a
// request that no other stage will ever mention.
func TestAcceptRecordsTheStoredRequestOnce(t *testing.T) {
	store := &acceptStore{
		stored: channels.AcceptResult{
			InboxID:   "in-first",
			RunID:     "run-first",
			RequestID: "req-first",
			Duplicate: true,
		},
		// One write whose outcome is unknown, retried with the same ids.
		fail: 1,
	}
	consumer, adapter, spans := newRecordingConsumer(t, store)
	require.NoError(t, consumer.Accept(
		context.Background(), adapter.envelope(msgIDMarker, "session-s", bodyMarker)))

	require.Len(t, store.requests, 2)
	require.Equal(t, store.requests[0].IDs, store.requests[1].IDs,
		"the retry is the same write, not a second message")
	minted := store.requests[0].IDs.RequestID

	recorded := spans.GetSpans()
	require.Len(t, recorded, 1, "one message is one record, however often the write was retried")
	accepted := stageSpans(t, spans)["accept"][0]
	require.Equal(t, "duplicate", spanText(t, accepted, keyOutcome))
	require.Equal(t, "req-first", spanText(t, accepted, keyRequest))
	require.Equal(t, "run-first", spanText(t, accepted, keyRun))
	require.Equal(t, "2", spanText(t, accepted, keyAttempt))
	require.NotContains(t, fmt.Sprintf("%+v", recorded[0]), minted)
	requireNoMarkers(t, recorded[0])
}

// A message that could not be recorded is a failed stage, and the class of the
// failure is a label rather than the error.
func TestAcceptRecordsAFailureWithoutItsCause(t *testing.T) {
	store := &acceptStore{fail: persistAttempts}
	consumer, adapter, spans := newRecordingConsumer(t, store)
	require.ErrorIs(t, consumer.Accept(
		context.Background(), adapter.envelope(msgIDMarker, "session-s", bodyMarker)),
		ErrAcceptFailed)

	recorded := spans.GetSpans()
	require.Len(t, recorded, 1)
	failed := stageSpans(t, spans)["accept"][0]
	require.Equal(t, "failed", spanText(t, failed, keyOutcome))
	require.Equal(t, string(channels.ErrorInternal), spanText(t, failed, keyError))
	require.Equal(t, fmt.Sprint(persistAttempts), spanText(t, failed, keyAttempt))
	_, hasRequest := failed.Value(keyRequest)
	require.False(t, hasRequest, "nothing was stored, so there is no request to name")
	require.NotContains(t, fmt.Sprintf("%+v", recorded[0]), "storage unavailable")
	requireNoMarkers(t, recorded[0])
}

// An envelope this consumer is not the binding for is refused before the Store
// is asked to record it, whichever of the four fields is wrong.
func TestAcceptRefusesAnEnvelopeFromAnotherBinding(t *testing.T) {
	for _, mismatch := range identityMismatches() {
		t.Run(mismatch.field, func(t *testing.T) {
			store := &acceptStore{stored: channels.AcceptResult{
				InboxID: "in-a", RunID: "run-a", RequestID: "req-a",
			}}
			consumer, adapter, spans := newRecordingConsumer(t, store)
			foreign := adapter.envelope(msgIDMarker, "session-s", bodyMarker)
			foreign.TenantID = mismatch.identity.TenantID
			foreign.AgentAppID = mismatch.identity.AgentAppID
			foreign.ChannelBindingID = mismatch.identity.BindingID
			foreign.Channel = mismatch.identity.Channel
			foreign.DeliveryTarget.Channel = mismatch.identity.Channel

			require.ErrorIs(t, consumer.Accept(context.Background(), foreign), ErrAcceptFailed)
			require.Empty(t, store.requests, "a foreign envelope is never written")
			failed := stageSpans(t, spans)["accept"][0]
			require.Equal(t, "failed", spanText(t, failed, keyOutcome))
			require.Equal(t, string(channels.ErrorPermanent), spanText(t, failed, keyError),
				"no retry will make this envelope acceptable")
		})
	}
}

// Input this consumer cannot answer is refused whole. Silently dropping the
// part it cannot read would answer a message the user did not send.
func TestAcceptRefusesInputItCannotRead(t *testing.T) {
	store := &acceptStore{}
	consumer, adapter, _ := newRecordingConsumer(t, store)
	withImage := adapter.envelope(msgIDMarker, "session-s", bodyMarker)
	withImage.Message.Attachments = []channels.AttachmentRef{{
		Kind: channels.AttachmentImage, ExternalID: "media-a",
	}}
	require.ErrorIs(t, consumer.Accept(context.Background(), withImage), ErrAcceptFailed)
	require.Empty(t, store.requests)
}

// The send record is taken around the delivery attempt, and says what happened
// to the attempt rather than what the Store will do about it.
func TestDeliverRecordsWhatTheAttemptDid(t *testing.T) {
	part, _, _ := answerPart(testIdentity(), "out-a", "run-a", 1)
	part.RequestID = "req-a"
	part.Attempt = 1
	part.Message = channels.OutboundMessage{Text: replyMarker}

	// An attempt that may already have been delivered is not attempted again,
	// and is not a send that failed either.
	risky := part
	risky.DuplicateRisk = true

	for _, expected := range []struct {
		outcome string
		part    channels.OutboxPart
		// report is what the adapter answers, for the attempts that reach it.
		report channels.DeliveryOutcome
		result channels.SendOutcome
	}{
		// An address from a connection that is gone is not the platform
		// refusing the answer.
		{"stale_target", part, channels.DeliveryTargetStale, channels.SendPermanent},
		{"skipped", risky, channels.Delivered, channels.SendUnknown},
	} {
		consumer, adapter, spans := newRecordingConsumer(t, &orderStore{})
		adapter.report = channels.DeliveryReport{Outcome: expected.report}
		result := consumer.deliver(context.Background(), expected.part)
		require.Equal(t, expected.result, result.Outcome,
			"the record must not change what the Store is told")

		recorded := spans.GetSpans()
		require.Len(t, recorded, 1)
		delivered := stageSpans(t, spans)["deliver"][0]
		require.Equal(t, expected.outcome, spanText(t, delivered, keyOutcome))
		require.Equal(t, "req-a", spanText(t, delivered, keyRequest))
		require.Equal(t, "out-a", spanText(t, delivered, keyOutbox))
		require.Equal(t, "1", spanText(t, delivered, keyAttempt))
		requireNoMarkers(t, recorded[0])
	}
}

// brokenExporter is a collector that refuses everything, in the place where the
// consumer would notice: the export runs inside the End of a stage.
type brokenExporter struct{ sdktrace.SpanExporter }

func (brokenExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error {
	return errors.New("collector refused: " + bodyMarker)
}

func (brokenExporter) Shutdown(context.Context) error { return nil }

// An export that fails is not a message that failed.
func TestAFailedExportChangesNothing(t *testing.T) {
	observer, err := telemetry.New(
		sdktrace.NewTracerProvider(sdktrace.WithSyncer(brokenExporter{})),
		sdkmetric.NewMeterProvider(),
	)
	require.NoError(t, err)
	store := &acceptStore{stored: channels.AcceptResult{
		InboxID: "in-a", RunID: "run-a", RequestID: "req-a",
	}}
	adapter := newFakeAdapter()
	consumer, err := New(Config{
		Identity:  testIdentity(),
		Adapter:   adapter,
		Store:     store,
		Runs:      &sessionrun.Service{},
		Revisions: func(context.Context, string, string, string) error { return nil },
		Telemetry: observer,
	})
	require.NoError(t, err)
	require.NoError(t, consumer.Accept(
		context.Background(), adapter.envelope(msgIDMarker, "session-s", bodyMarker)))
	require.Len(t, store.requests, 1, "a refused export is not a write to retry")
}

// A consumer without telemetry is the default, and records nothing anywhere.
func TestAConsumerWithoutTelemetryRecordsNothing(t *testing.T) {
	store := &acceptStore{stored: channels.AcceptResult{
		InboxID: "in-a", RunID: "run-a", RequestID: "req-a",
	}}
	consumer, adapter := newTestConsumer(t, store)
	require.Nil(t, consumer.stages)
	require.NoError(t, consumer.Accept(
		context.Background(), adapter.envelope(msgIDMarker, "session-s", bodyMarker)))
	require.Len(t, store.requests, 1)
}

// requireNoMarkers asserts that one whole recorded span repeats nothing this
// package protects.
func requireNoMarkers(t *testing.T, recorded tracetest.SpanStub) {
	t.Helper()
	rendered := fmt.Sprintf("%+v", recorded)
	for _, marker := range []string{msgIDMarker, bodyMarker, replyMarker} {
		require.NotContainsf(t, rendered, marker,
			"a record repeated a protected value: %s", rendered)
	}
}
