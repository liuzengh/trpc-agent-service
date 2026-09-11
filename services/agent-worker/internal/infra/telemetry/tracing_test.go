package telemetry

import (
	"bytes"
	"context"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	app "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
)

func TestJSONCapturesTraceBeforeAsyncWrite(t *testing.T) {
	var logs bytes.Buffer
	r, err := New(context.Background(), "worker-test", &logs, Export{})
	if err != nil {
		t.Fatal(err)
	}
	carrier := tracecontext.Carrier{Traceparent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-00"}
	ctx, cancel := context.WithCancel(carrier.Restore(context.Background()))
	r.Observe(ctx, app.Observation{Operation: "execute", Result: "ok"})
	cancel()
	stop, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	if err = r.Shutdown(stop); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), `"trace_id":"0123456789abcdef0123456789abcdef"`) || !strings.Contains(logs.String(), `"span_id":"0123456789abcdef"`) {
		t.Fatal(logs.String())
	}
}

func TestSDKLogsAreFixedAndBounded(t *testing.T) {
	var logs bytes.Buffer
	r, err := New(context.Background(), "worker-test", &logs, Export{})
	if err != nil {
		t.Fatal(err)
	}
	r.SDKLog(context.Background(), "error")
	r.SDKLog(context.Background(), "PRIVATE")
	stop, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = r.Shutdown(stop); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "PRIVATE") || !strings.Contains(logs.String(), `"msg":"worker.sdk"`) {
		t.Fatal(logs.String())
	}
}

func TestMetricsUseSharedProcessResource(t *testing.T) {
	res := resource.NewSchemaless(attribute.String("service.version", "traced-build"), attribute.String("deployment.environment.name", "test"))
	reader := sdkmetric.NewManualReader()
	var logs bytes.Buffer
	recorder, err := newRecorderWithResource("worker-test", &logs, res, reader)
	if err != nil {
		t.Fatal(err)
	}
	defer recorder.Shutdown(context.Background())
	recorder.Observe(context.Background(), app.Observation{Operation: "execute", Result: "ok"})
	var data metricdata.ResourceMetrics
	if err = reader.Collect(context.Background(), &data); err != nil {
		t.Fatal(err)
	}
	if !data.Resource.Equal(res) {
		t.Fatal("Trace/Metrics process resource diverged")
	}
}
