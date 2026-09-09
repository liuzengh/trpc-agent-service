package telemetry

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

// env is one process environment, as a getenv.
func env(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

// recorder is a Telemetry that keeps everything in memory, plus the two readers
// a test asserts on.
func recorder(t *testing.T) (*ChannelRecorder, *tracetest.InMemoryExporter, sdkmetric.Reader) {
	t.Helper()
	spans := tracetest.NewInMemoryExporter()
	reader := sdkmetric.NewManualReader()
	telemetry, err := New(
		sdktrace.NewTracerProvider(sdktrace.WithSyncer(spans)),
		sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)),
	)
	require.NoError(t, err)
	stages, err := telemetry.ChannelRecorder(Binding{
		TenantID:  "tenant-a",
		AppID:     "app-a",
		BindingID: "binding-a",
		Channel:   channels.ChannelWeCom,
	})
	require.NoError(t, err)
	return stages, spans, reader
}

// measured returns the label sets one collection recorded, by metric name.
func measured(t *testing.T, reader sdkmetric.Reader) map[string][]attribute.Set {
	t.Helper()
	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &collected))
	sets := map[string][]attribute.Set{}
	for _, scope := range collected.ScopeMetrics {
		for _, recorded := range scope.Metrics {
			switch data := recorded.Data.(type) {
			case metricdata.Sum[int64]:
				for _, point := range data.DataPoints {
					sets[recorded.Name] = append(sets[recorded.Name], point.Attributes)
				}
			case metricdata.Histogram[float64]:
				for _, point := range data.DataPoints {
					sets[recorded.Name] = append(sets[recorded.Name], point.Attributes)
				}
			}
		}
	}
	return sets
}

// keys returns the attribute keys of one set.
func keys(set attribute.Set) []string {
	names := make([]string, 0, set.Len())
	for _, attr := range set.ToSlice() {
		names = append(names, string(attr.Key))
	}
	return names
}

func TestTelemetryIsOffUntilItIsAskedFor(t *testing.T) {
	cfg, err := Load(env(nil))
	require.NoError(t, err)
	require.False(t, cfg.Enabled)
	require.Equal(t, "disabled", cfg.Describe())

	// Off reads nothing else, so an endpoint beside it is not configuration.
	cfg, err = Load(env(map[string]string{EndpointEnvVar: "not a url"}))
	require.NoError(t, err)
	require.False(t, cfg.Enabled)

	_, err = Load(env(map[string]string{EnabledEnvVar: "TRUE"}))
	require.ErrorIs(t, err, ErrConfig)

	cfg, err = Load(env(map[string]string{EnabledEnvVar: "true"}))
	require.NoError(t, err)
	require.Equal(t, Config{Enabled: true, Endpoint: DefaultEndpoint}, cfg)

	// The disabled process runs the same code as the enabled one: a nil
	// Telemetry, a nil recorder and a nil Span all the way down.
	telemetry, err := Open(context.Background(), Config{})
	require.NoError(t, err)
	require.Nil(t, telemetry)
	stages, err := telemetry.ChannelRecorder(Binding{})
	require.NoError(t, err)
	require.Nil(t, stages)
	ctx, span := stages.Start(context.Background(), StageAccept)
	require.Equal(t, context.Background(), ctx)
	span.End(Result{Outcome: OutcomeSucceeded})
	require.NoError(t, telemetry.Shutdown(context.Background()))
}

// A collector URL is where an operator writes a credential by accident. The
// refusal must not put it back in a startup log.
func TestARefusedEndpointIsNeverEchoed(t *testing.T) {
	const secret = "tokenfromtheoperator"
	for _, endpoint := range []string{
		"https://user:" + secret + "@collector.internal",
		"https://collector.internal/?key=" + secret,
		"https://collector.internal/#" + secret,
		"https://collector.internal/ingest/" + secret,
		"grpc://collector.internal",
		"collector.internal:4318",
	} {
		_, err := Load(env(map[string]string{
			EnabledEnvVar:  "true",
			EndpointEnvVar: endpoint,
		}))
		require.ErrorIs(t, err, ErrConfig, endpoint)
		require.NotContains(t, err.Error(), secret)
		require.NotContains(t, err.Error(), "collector.internal")
	}

	// The accepted form keeps its origin and nothing else.
	cfg, err := Load(env(map[string]string{
		EnabledEnvVar:  "true",
		EndpointEnvVar: "https://collector.internal:4318/",
	}))
	require.NoError(t, err)
	require.Equal(t, "https://collector.internal:4318", cfg.Endpoint)
}

// Turning this package on must not turn on anybody else's instrumentation. The
// upstream Runner reads the global providers, and its spans carry model input
// and tool arguments.
//
// The process error handler is the one global that is written, and only because
// the OTLP client reports a partial success through it; see
// TestPartialSuccessDoesNotLogCollectorText.
func TestOpenInstallsNoGlobalProvider(t *testing.T) {
	tracers, meters := otel.GetTracerProvider(), otel.GetMeterProvider()
	telemetry, err := Open(context.Background(),
		Config{Enabled: true, Endpoint: DefaultEndpoint})
	require.NoError(t, err)
	t.Cleanup(func() { _ = telemetry.Shutdown(context.Background()) })
	require.NotNil(t, telemetry)
	require.Same(t, tracers, otel.GetTracerProvider())
	require.Same(t, meters, otel.GetMeterProvider())
}

// One stage record carries the whitelist and nothing else, and the identifiers
// on the span are exactly what a measurement must not have.
func TestAStageRecordsTheWhitelistOnly(t *testing.T) {
	stages, spans, reader := recorder(t)
	_, span := stages.Start(context.Background(), StageExecute)
	span.End(Result{
		Outcome:    OutcomeFailed,
		ErrorType:  channels.ErrorAgentFailed,
		RequestID:  "req-1",
		RunID:      "run-1",
		OutboxID:   "out-1",
		RevisionID: "rev-1",
		Attempt:    2,
		Events:     7,
	})

	recorded := spans.GetSpans()
	require.Len(t, recorded, 1)
	require.Equal(t, "channel.execute", recorded[0].Name)
	require.ElementsMatch(t, []string{
		attrTenant, attrApp, attrBinding, attrChannel,
		attrStage, attrOutcome, attrError,
		attrRequest, attrRun, attrOutbox, attrRevision, attrAttempt, attrEvents,
	}, keys(attribute.NewSet(recorded[0].Attributes...)))
	// An error status, with the outcome as the whole of the explanation: a
	// description would be the place the backend's own words ended up.
	require.Equal(t, codes.Error, recorded[0].Status.Code)
	require.Empty(t, recorded[0].Status.Description)
	require.Empty(t, recorded[0].Events, "no error is recorded as a span event")

	labels := measured(t, reader)
	require.Len(t, labels, 2)
	for _, name := range []string{stageCountMetric, stageDurationMetric} {
		require.Len(t, labels[name], 1, name)
		require.ElementsMatch(t, []string{
			attrTenant, attrApp, attrChannel, attrStage, attrOutcome, attrError,
		}, keys(labels[name][0]), name)
	}
}

// Every value that reaches a label comes from a closed vocabulary, because a
// label is what a metric backend keeps a time series per.
func TestRecordedValuesStayInTheirVocabulary(t *testing.T) {
	stages, spans, reader := recorder(t)
	_, span := stages.Start(context.Background(), Stage("session-9d3f/../../"))
	span.End(Result{
		Outcome:   Outcome("the model said no"),
		ErrorType: channels.ErrorType("connect: dial tcp 10.0.0.4:5432"),
	})
	// A second End is the same one pass through the stage, however the caller's
	// control flow reached it twice.
	span.End(Result{Outcome: OutcomeSucceeded})

	recorded := spans.GetSpans()
	require.Len(t, recorded, 1)
	require.Equal(t, "channel.unknown", recorded[0].Name)
	attributes := attribute.NewSet(recorded[0].Attributes...)
	for key, want := range map[attribute.Key]string{
		attrStage:   string(stageUnknown),
		attrOutcome: string(OutcomeUnknown),
		attrError:   string(channels.ErrorInternal),
	} {
		value, ok := attributes.Value(key)
		require.True(t, ok, key)
		require.Equal(t, want, value.AsString())
	}

	counted := measured(t, reader)[stageCountMetric]
	require.Len(t, counted, 1, "one pass through a stage is one measurement")

	// A stage that did not fail still carries the label, so one instrument has
	// one label set.
	stages, spans, _ = recorder(t)
	_, span = stages.Start(context.Background(), StageAccept)
	span.End(Result{Outcome: OutcomeDuplicate})
	attributes = attribute.NewSet(spans.GetSpans()[0].Attributes...)
	value, ok := attributes.Value(attrError)
	require.True(t, ok)
	require.Equal(t, errorTypeNone, value.AsString())
	require.Equal(t, codes.Unset, spans.GetSpans()[0].Status.Code)
}

// failingExporter is a collector that answers with the kind of error an OTLP
// exporter returns: the endpoint, and whatever the collector said.
type failingExporter struct {
	sdktrace.SpanExporter
	sdkmetric.Exporter
	err error
}

func (e failingExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error { return e.err }

func (e failingExporter) Export(context.Context, *metricdata.ResourceMetrics) error { return e.err }

func (e failingExporter) ForceFlush(context.Context) error { return e.err }

func (e failingExporter) Shutdown(context.Context) error { return e.err }

// The SDK hands an export error to the process error handler, which logs it, so
// what the exporters may say is fixed here.
func TestExportErrorsAreReplaced(t *testing.T) {
	const secret = "Bearer tokenfromtheoperator"
	failing := failingExporter{err: fmt.Errorf(
		"traces export: https://collector.internal/v1/traces: 401 %s", secret)}
	ctx := context.Background()
	for _, err := range []error{
		safeSpanExporter{failing}.ExportSpans(ctx, nil),
		safeSpanExporter{failing}.Shutdown(ctx),
		safeMetricExporter{failing}.Export(ctx, nil),
		safeMetricExporter{failing}.ForceFlush(ctx),
		safeMetricExporter{failing}.Shutdown(ctx),
	} {
		require.ErrorIs(t, err, ErrExportFailed)
		require.NotContains(t, err.Error(), secret)
		require.NotContains(t, err.Error(), "collector.internal")
	}

	working := failingExporter{}
	require.NoError(t, safeSpanExporter{working}.ExportSpans(ctx, nil))
	require.NoError(t, safeMetricExporter{working}.Export(ctx, nil))
}

// A collector that has stopped answering must not hold the process open, and
// must not put its address into the exit status either.
func TestShutdownIsBoundedAndSaysNothing(t *testing.T) {
	t.Parallel()
	// A socket that takes the connection and answers nothing, which is the
	// outage this bound exists for: a refused connection fails immediately and
	// would prove nothing. It is a bare listener rather than an HTTP test server
	// because closing that one waits for the request still inside it.
	collector, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = collector.Close() })
	endpoint := "http://" + collector.Addr().String()

	telemetry, err := Open(context.Background(), Config{Enabled: true, Endpoint: endpoint})
	require.NoError(t, err)
	stages, err := telemetry.ChannelRecorder(Binding{
		TenantID:  "tenant-a",
		AppID:     "app-a",
		BindingID: "binding-a",
		Channel:   channels.ChannelWeCom,
	})
	require.NoError(t, err)
	_, span := stages.Start(context.Background(), StageDeliver)
	span.End(Result{Outcome: OutcomeSucceeded, RequestID: "req-1"})

	started := time.Now()
	err = telemetry.Shutdown(context.Background())
	// Whatever the flush managed, it stopped inside its bound, dropped the
	// record rather than reporting the collector's silence as a process failure,
	// and named nothing.
	require.Less(t, time.Since(started), 2*shutdownTimeout)
	require.NotErrorIs(t, err, ErrExportFailed)
	require.NotContains(t, fmt.Sprint(err), endpoint)
}

func TestARecorderNeedsAWholeBinding(t *testing.T) {
	_, err := New(nil, nil)
	require.ErrorIs(t, err, ErrConfig)

	telemetry, err := New(
		sdktrace.NewTracerProvider(),
		sdkmetric.NewMeterProvider(),
	)
	require.NoError(t, err)
	// Providers this package did not build stay the caller's to close.
	require.NoError(t, telemetry.Shutdown(context.Background()))

	for _, binding := range []Binding{
		{AppID: "app-a", BindingID: "binding-a", Channel: channels.ChannelWeCom},
		{TenantID: "tenant-a", BindingID: "binding-a", Channel: channels.ChannelWeCom},
		{TenantID: "tenant-a", AppID: "app-a", Channel: channels.ChannelWeCom},
		{TenantID: "tenant-a", AppID: "app-a", BindingID: "binding-a"},
	} {
		stages, err := telemetry.ChannelRecorder(binding)
		require.Error(t, err)
		require.Nil(t, stages)
	}
}

// The sentinels are distinct, so a caller can tell a misconfiguration from a
// collector that is down.
func TestSentinelsAreDistinct(t *testing.T) {
	require.False(t, errors.Is(ErrExportFailed, ErrConfig))
	require.False(t, errors.Is(ErrShutdownIncomplete, ErrExportFailed))
}
