package telemetry

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	metricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

type diagnosticBuffer struct {
	sync.Mutex
	bytes.Buffer
}

func (b *diagnosticBuffer) Write(p []byte) (int, error) {
	b.Lock()
	defer b.Unlock()
	return b.Buffer.Write(p)
}

func (b *diagnosticBuffer) text() string {
	b.Lock()
	defer b.Unlock()
	return b.Buffer.String()
}

// OTLP partial success calls otel.Handle inside the HTTP client and then
// returns nil, so sanitizing Export's return value alone is insufficient.
func TestPartialSuccessDoesNotLogCollectorText(t *testing.T) {
	const marker = "collector-sensitive-marker"
	var diagnostics diagnosticBuffer
	previousOutput, previousHandler := log.Writer(), otel.GetErrorHandler()
	log.SetOutput(&diagnostics)
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) { log.Print(err) }))
	defer func() {
		otel.SetErrorHandler(previousHandler)
		log.SetOutput(previousOutput)
	}()

	seen := make(chan string, 4)
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var response proto.Message
		switch r.URL.Path {
		case "/v1/traces":
			response = &tracepb.ExportTraceServiceResponse{PartialSuccess: &tracepb.ExportTracePartialSuccess{
				RejectedSpans: 1, ErrorMessage: marker,
			}}
		case "/v1/metrics":
			response = &metricpb.ExportMetricsServiceResponse{PartialSuccess: &metricpb.ExportMetricsPartialSuccess{
				RejectedDataPoints: 1, ErrorMessage: marker,
			}}
		default:
			http.NotFound(w, r)
			return
		}
		body, err := proto.Marshal(response)
		if err != nil {
			http.Error(w, "fixture encoding failed", http.StatusInternalServerError)
			return
		}
		seen <- r.URL.Path
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(body)
	}))
	defer collector.Close()

	observer, err := Open(context.Background(), Config{Enabled: true, Endpoint: collector.URL})
	require.NoError(t, err)
	t.Cleanup(func() { _ = observer.Shutdown(context.Background()) })
	stages, err := observer.ChannelRecorder(Binding{
		TenantID: "tenant-a", AppID: "app-a", BindingID: "binding-a", Channel: channels.ChannelWeCom,
	})
	require.NoError(t, err)
	_, span := stages.Start(context.Background(), StageAccept)
	span.End(Result{Outcome: OutcomeSucceeded, RequestID: "req-partial-success"})
	require.NoError(t, observer.Shutdown(context.Background()))
	close(seen)
	var paths []string
	for path := range seen {
		paths = append(paths, path)
	}
	require.Contains(t, paths, "/v1/traces")
	require.Contains(t, paths, "/v1/metrics")
	require.NotContains(t, diagnostics.text(), marker)
}
