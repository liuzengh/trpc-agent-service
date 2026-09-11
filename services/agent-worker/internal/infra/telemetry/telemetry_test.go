package telemetry

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	app "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	collector "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/protobuf/proto"
)

func TestObservationClosedLabelsAndJSONNoPayload(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	var logs bytes.Buffer
	r, err := newRecorder("worker-one", &logs, reader)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		r.Observe(ctx, app.Observation{Operation: "prepare", Result: "dependency", TenantID: "tenant-sensitive-identity", RunID: "run-sensitive-identity", AttemptID: "attempt-identity", Duration: time.Millisecond})
	}
	r.Observe(ctx, app.Observation{Operation: "PRIVATE_PROMPT", Result: "postgres://private-dsn:secret@host", Stage: "postgres://private-dsn:secret@host"})
	r.Observe(ctx, app.Observation{Operation: "usage", Result: "ok", InputTokens: 200, OutputTokens: 300, TotalTokens: 500})
	r.Sample(ctx, app.StorageObservation{Queued: 2, Running: 1, ReplyPending: 3, OldestRunSeconds: 9, OldestReplySeconds: 4})
	r.SampleStatus(ctx, true)
	var data metricdata.ResourceMetrics
	if err = reader.Collect(ctx, &data); err != nil {
		t.Fatal(err)
	}
	var dimensions int
	tokens := map[string]int64{}
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			switch v := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, p := range v.DataPoints {
					for _, a := range p.Attributes.ToSlice() {
						if a.Key != "operation" && a.Key != "result" && a.Key != "kind" {
							t.Fatalf("high-cardinality label %s", a.Key)
						}
						if strings.Contains(a.Value.AsString(), "sensitive") || strings.Contains(a.Value.AsString(), "PRIVATE") || strings.Contains(a.Value.AsString(), "secret") {
							t.Fatal("unbounded label value")
						}
						if m.Name == "worker.provider.tokens" && a.Key == "kind" {
							tokens[a.Value.AsString()] = p.Value
						}
					}
					if m.Name == "worker.operation.count" {
						dimensions++
					}
				}
			}
		}
	}
	if dimensions != 3 || tokens["input"] != 200 || tokens["output"] != 300 || tokens["total"] != 500 {
		t.Fatal(dimensions, tokens)
	}
	stop, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err = r.Shutdown(stop); err != nil {
		t.Fatal(err)
	}
	out := logs.String()
	if !strings.Contains(out, `"operation":"prepare"`) || !strings.Contains(out, `"run_id":"run-sensitive-identity"`) || strings.Contains(out, "PRIVATE_PROMPT") || strings.Contains(out, "private-dsn") || strings.Contains(out, "secret@host") {
		t.Fatal("JSON vocabulary or filtering failed")
	}
}
func TestOTLPExporterActualHTTPAndBoundedOutage(t *testing.T) {
	requests := make(chan *collector.ExportMetricsServiceRequest, 8)
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/v1/metrics" || req.Method != "POST" {
			t.Errorf("unexpected OTLP request")
			w.WriteHeader(400)
			return
		}
		b, _ := io.ReadAll(req.Body)
		var m collector.ExportMetricsServiceRequest
		if err := proto.Unmarshal(b, &m); err != nil {
			t.Error(err)
		}
		select {
		case requests <- &m:
		default:
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(200)
	}))
	r, err := New(context.Background(), "worker-otlp", io.Discard, Export{Endpoint: endpoint.URL + "/v1/metrics", Interval: time.Hour, Timeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	r.Observe(context.Background(), app.Observation{Operation: "execute", Result: "ok", Duration: time.Second})
	flush, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = r.provider.ForceFlush(flush); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-requests:
		if len(got.ResourceMetrics) != 1 || len(got.ResourceMetrics[0].ScopeMetrics) == 0 {
			t.Fatal("missing actual OTLP metrics")
		}
	case <-time.After(time.Second):
		t.Fatal("no exported HTTP request")
	}
	endpoint.Close()
	start := time.Now()
	r.Observe(context.Background(), app.Observation{Operation: "complete", Result: "ok"})
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("backend outage blocked business observation")
	}
	stop, done := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer done()
	_ = r.Shutdown(stop)
	if time.Since(start) > time.Second {
		t.Fatal("exporter shutdown was not bounded")
	}
	t.Log("WORKER_OTLP=PASS actual protobuf export; fixed metric labels; outage leaves observation nonblocking; bounded shutdown")
}

type blockedWriter struct {
	release chan struct{}
	entered chan struct{}
	once    sync.Once
}

func (w *blockedWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(p), nil
}
func TestBlockedStdoutDropsInsteadOfBlockingExecution(t *testing.T) {
	writer := &blockedWriter{release: make(chan struct{}), entered: make(chan struct{})}
	reader := sdkmetric.NewManualReader()
	r, err := newRecorder("worker", writer, reader)
	if err != nil {
		t.Fatal(err)
	}
	defer close(writer.release)
	r.Observe(context.Background(), app.Observation{Operation: "complete", Result: "ok"})
	select {
	case <-writer.entered:
	case <-time.After(time.Second):
		t.Fatal("writer not exercised")
	}
	done := make(chan struct{})
	go func() {
		for i := 0; i < 2000; i++ {
			r.Observe(context.Background(), app.Observation{Operation: "complete", Result: "ok"})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("blocked logging stalled observation")
	}
	if len(r.queue) != 256 {
		t.Fatal("log queue is not bounded", len(r.queue))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err = r.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("blocked stdout shutdown should honor caller deadline", err)
	}
}
func TestExportConfigurationRejectsAmbientUnsafeTargets(t *testing.T) {
	for _, endpoint := range []string{"http://collector:4318/v1/metrics", "https://user:secret@collector/v1/metrics", "https://collector/v1/metrics?token=secret", "https://collector/other", "https://collector/v1/metrics#secret", "https://collector/v1/metrics?", "https://collector/v1/metrics#"} {
		if (Export{Endpoint: endpoint, Interval: time.Second, Timeout: time.Second}).Validate() == nil {
			t.Fatal(endpoint)
		}
	}
	if (Export{}).Validate() != nil {
		t.Fatal("disabled exporter rejected")
	}
}

func TestAbsentExportDoesNotUseAmbientEndpoint(t *testing.T) {
	calls := make(chan struct{}, 1)
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls <- struct{}{}; w.WriteHeader(200) }))
	defer endpoint.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", endpoint.URL+"/v1/metrics")
	recorder, err := New(context.Background(), "worker", io.Discard, Export{})
	if err != nil {
		t.Fatal(err)
	}
	recorder.Observe(context.Background(), app.Observation{Operation: "complete", Result: "ok"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = recorder.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-calls:
		t.Fatal("absent export inherited ambient network target")
	default:
	}
}

func TestMemoryAcceptedPhaseUsesBoundedObservationLabels(t *testing.T) {
	for _, value := range []string{"memory_apply", "memory_finalize"} {
		if operation(value) != value {
			t.Fatal("Memory phase hidden as other")
		}
	}
	for _, value := range []string{"memory_apply_failed", "memory_finalize_failed"} {
		if outcome(value) != value {
			t.Fatal("Memory failure hidden as other")
		}
	}
	if operation("memory_private_content") != "other" || outcome("postgres://secret") != "other" {
		t.Fatal("unbounded Memory labels")
	}
}
