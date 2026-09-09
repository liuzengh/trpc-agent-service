package wecom

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

// What this package still asserts about telemetry is what only a real WeCom
// connection can show: three stages of one message correlated over the real
// protocol, and a refused collector changing none of it. The stage records
// themselves are the common consumer's, and are tested there.

// The attribute keys this package asserts on, spelled the way an operator would
// read them back out of a collector rather than borrowed from the constants
// that produced them.
const (
	keyStage    = "trpc.stage"
	keyOutcome  = "trpc.outcome"
	keyRequest  = "trpc.request_id"
	keyRun      = "trpc.run_id"
	keyOutbox   = "trpc.outbox_id"
	keyRevision = "trpc.revision_id"
	keyAttempt  = "trpc.attempt"
	keyEvents   = "trpc.event_count"
)

// inMemory is a Telemetry that exports nowhere, plus the readers a test asserts
// on. A synchronous span processor keeps the records in step with the loop that
// produced them, so nothing here waits for a batch.
func inMemory(t *testing.T) (*telemetry.Telemetry, *tracetest.InMemoryExporter, sdkmetric.Reader) {
	t.Helper()
	spans := tracetest.NewInMemoryExporter()
	reader := sdkmetric.NewManualReader()
	observer, err := telemetry.New(
		sdktrace.NewTracerProvider(sdktrace.WithSyncer(spans)),
		sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)),
	)
	require.NoError(t, err)
	return observer, spans, reader
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

// text returns one attribute of a recorded span.
func text(t *testing.T, attributes attribute.Set, key string) string {
	t.Helper()
	value, ok := attributes.Value(attribute.Key(key))
	require.Truef(t, ok, "%s was not recorded", key)
	return value.Emit()
}

// requireNoMarkers asserts that one whole recorded span repeats nothing this
// package protects; see the markers in mockserver_test.go.
func requireNoMarkers(t *testing.T, recorded tracetest.SpanStub) {
	t.Helper()
	requireNoMarkerText(t, fmt.Sprintf("%+v", recorded))
}

// requireNoIdentifiers asserts that a measurement carries none of the
// identifiers a span does. They are unbounded, and a metric backend keeps one
// time series per label set.
func requireNoIdentifiers(t *testing.T, reader sdkmetric.Reader) {
	t.Helper()
	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &collected))
	require.NotEmpty(t, collected.ScopeMetrics)
	for _, scope := range collected.ScopeMetrics {
		for _, recorded := range scope.Metrics {
			rendered := fmt.Sprintf("%+v", recorded)
			for _, key := range []string{keyRequest, keyRun, keyOutbox, keyRevision, keyAttempt, keyEvents} {
				require.NotContainsf(t, rendered, key,
					"%s is not a label: %s", key, rendered)
			}
			requireNoMarkerText(t, rendered)
		}
	}
}

// requireNoMarkerText is that assertion over anything already rendered.
func requireNoMarkerText(t *testing.T, rendered string) {
	t.Helper()
	for _, marker := range []string{
		secretMarker, botIDMarker, userIDMarker, msgIDMarker, bodyMarker,
		errMsgMarker, replyMarker,
	} {
		require.NotContainsf(t, rendered, marker,
			"a record repeated a protected value: %s", rendered)
	}
}

// TestIntegrationRecordsThreeStagesOfOneRequest is the correlation this slice
// promises: three independent records of one message, joined by the request id
// the Store minted, over the real protocol, the real Runner and a real
// database.
func TestIntegrationRecordsThreeStagesOfOneRequest(t *testing.T) {
	fixture := newE2E(t)
	observer, spans, reader := inMemory(t)
	fixture.observer = observer
	conn := fixture.start(t)

	conn.callback("req-1", message(msgIDMarker, bodyMarker))
	in, answer := waitReply(t, conn)
	require.Equal(t, "echo: "+bodyMarker, answer)
	conn.ack(in.Headers.ReqID, 0)

	// The same platform message id again, which the Store recognises.
	conn.callback("req-2", message(msgIDMarker, bodyMarker))
	conn.silent(500 * time.Millisecond)

	recorded := fixture.recordedRuns(t)
	require.Len(t, recorded, 1)
	part := fixture.awaitAnswer(t, recorded[0])
	require.Equal(t, channels.OutboxSent, part.Status)
	fixture.stop()

	byStage := stageSpans(t, spans)
	require.Len(t, byStage["accept"], 2)
	require.Len(t, byStage["execute"], 1, "the redelivery executed nothing")
	require.Len(t, byStage["deliver"], 1, "and was answered by nobody")

	request := recorded[0].RequestID
	require.Equal(t, "succeeded", text(t, byStage["accept"][0], keyOutcome))
	require.Equal(t, request, text(t, byStage["accept"][0], keyRequest))
	require.Equal(t, "duplicate", text(t, byStage["accept"][1], keyOutcome))
	require.Equal(t, request, text(t, byStage["accept"][1], keyRequest),
		"a redelivery is recorded against the request that was stored")

	executed := byStage["execute"][0]
	require.Equal(t, "succeeded", text(t, executed, keyOutcome))
	require.Equal(t, request, text(t, executed, keyRequest))
	require.Equal(t, recorded[0].RunID, text(t, executed, keyRun))
	require.Equal(t, recorded[0].RevisionID, text(t, executed, keyRevision),
		"the revision that answered, not the one that was published")
	require.NotEmpty(t, text(t, executed, keyEvents))

	delivered := byStage["deliver"][0]
	require.Equal(t, "succeeded", text(t, delivered, keyOutcome))
	require.Equal(t, request, text(t, delivered, keyRequest))
	require.Equal(t, part.OutboxID, text(t, delivered, keyOutbox))
	require.Equal(t, "1", text(t, delivered, keyAttempt))

	for _, span := range spans.GetSpans() {
		requireNoMarkers(t, span)
	}
	requireNoIdentifiers(t, reader)
}

// TestIntegrationARefusedCollectorChangesNothing is the acceptance for the one
// thing telemetry must never do. The collector refuses every export while a
// real message is accepted, executed and answered.
func TestIntegrationARefusedCollectorChangesNothing(t *testing.T) {
	fixture := newE2E(t)
	exports := make(chan string, 8)
	collector := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			select {
			case exports <- r.URL.Path:
			default:
			}
			http.Error(w, "the collector refused: "+bodyMarker, http.StatusBadRequest)
		}))
	defer collector.Close()
	observer, err := telemetry.Open(context.Background(),
		telemetry.Config{Enabled: true, Endpoint: collector.URL})
	require.NoError(t, err)
	fixture.observer = observer

	conn := fixture.start(t)
	conn.callback("req-1", message(msgIDMarker, bodyMarker))
	in, answer := waitReply(t, conn)
	require.Equal(t, "echo: "+bodyMarker, answer)
	conn.ack(in.Headers.ReqID, 0)

	recorded := fixture.recordedRuns(t)
	require.Len(t, recorded, 1)
	require.Equal(t, channels.OutboxSent, fixture.awaitAnswer(t, recorded[0]).Status)
	// Nothing is retried while the collector refuses: no second frame, and no
	// second execution.
	conn.silent(400 * time.Millisecond)
	fixture.stop()

	shutdown := observer.Shutdown(context.Background())
	require.NotErrorIs(t, shutdown, telemetry.ErrExportFailed,
		"what the collector said is not what the process reports")
	requireNoMarkerText(t, fmt.Sprint(shutdown))
	require.NotEmpty(t, exports, "the collector refused a real export")

	settled := fixture.recordedRuns(t)
	require.Len(t, settled, 1)
	require.Equal(t, channels.RunSucceeded, settled[0].Status)
	require.Equal(t, int32(1), settled[0].Attempt, "no Run was executed twice")
	part := fixture.answerOf(t, settled[0])
	require.Equal(t, channels.OutboxSent, part.Status)
	require.Equal(t, int32(1), part.Attempt, "a refused export is not a second send")
	require.False(t, part.DuplicateRisk)
	require.Equal(t, 1, fixture.userTurns(t))
}
