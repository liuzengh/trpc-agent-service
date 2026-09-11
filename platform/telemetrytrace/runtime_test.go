package telemetrytrace

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	collector "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func identity() Identity {
	return Identity{Service: "agent-worker", Instance: "worker-test", Version: "test", Environment: "test"}
}

func TestActualOTLPFilteringAndRelations(t *testing.T) {
	const secret = "CANARY private prompt token error"
	var mu sync.Mutex
	var received []*collector.ExportTraceServiceRequest
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/v1/traces" || r.Header.Get("Content-Type") != "application/x-protobuf" {
			t.Error("OTLP request shape")
		}
		if r.Header.Get("Authorization") != "" || r.Header.Get("SECRET") != "" {
			t.Error("environment header inherited")
		}
		b, _ := io.ReadAll(r.Body)
		if bytes.Contains(b, []byte(secret)) {
			t.Error("content escaped filtering")
		}
		var req collector.ExportTraceServiceRequest
		if err := proto.Unmarshal(b, &req); err != nil {
			t.Error(err)
		}
		mu.Lock()
		received = append(received, &req)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer sink.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "SECRET=private")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://127.0.0.1:1")
	cfg := testConfig(sink.URL + "/v1/traces")
	rt, err := New(context.Background(), &cfg, identity())
	if err != nil {
		t.Fatal(err)
	}
	tr := rt.Tracer("agent-worker")
	ctx, parent := tr.Start(context.Background(), "worker.run.attempt")
	sc := trace.SpanContextFromContext(ctx)
	state, _ := trace.ParseTraceState("vendor=private")
	linked := sc.WithTraceState(state)
	_, child := tr.Start(ctx, "worker.runner.run", trace.WithLinks(trace.Link{SpanContext: linked, Attributes: []attribute.KeyValue{attribute.String("secret", secret), attribute.String("app.run.id", "run-1")}}))
	child.SetAttributes(attribute.String("app.run.id", "run-1"), attribute.Int("gen_ai.usage.input_tokens", 7), attribute.String("gen_ai.input.messages", secret), attribute.String("http.url", secret))
	child.SetStatus(codes.Error, secret)
	child.AddEvent(secret, trace.WithAttributes(attribute.String("exception.message", secret)))
	child.End()
	parent.End()
	_, unknown := tr.Start(context.Background(), secret)
	unknown.End()
	flush, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = rt.ForceFlush(flush); err != nil {
		t.Fatal(err)
	}
	if err = rt.Shutdown(flush); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	var spans []*tracepb.Span
	for _, req := range received {
		for _, rs := range req.ResourceSpans {
			for _, kv := range rs.Resource.Attributes {
				if kv.Key == "service.name" && kv.Value.GetStringValue() != "agent-worker" {
					t.Fatal("resource")
				}
			}
			for _, ss := range rs.ScopeSpans {
				spans = append(spans, ss.Spans...)
			}
		}
	}
	if len(spans) != 3 {
		t.Fatalf("spans=%d", len(spans))
	}
	var p, c *tracepb.Span
	for _, s := range spans {
		switch s.Name {
		case "worker.run.attempt":
			p = s
		case "worker.runner.run":
			c = s
		case "operation":
		default:
			t.Fatal(s.Name)
		}
	}
	if p == nil || c == nil || !bytes.Equal(p.TraceId, c.TraceId) || !bytes.Equal(p.SpanId, c.ParentSpanId) {
		t.Fatal("broken parent relation")
	}
	if len(c.Events) != 0 || c.Status.Message != "" || len(c.Links) != 1 || c.Links[0].TraceState != "" || len(c.Links[0].Attributes) != 1 {
		t.Fatal("unsanitized child")
	}
	if len(c.Attributes) != 2 {
		t.Fatalf("attributes=%v", c.Attributes)
	}
	stats := rt.Stats()
	if stats.Finished != 3 || stats.Exported != 3 || stats.ExportFailures != 0 {
		t.Fatal(stats)
	}
	t.Logf("OTLP_TRACE=PASS protobuf=true spans=3 trace_id=%s parent_links=true content_filtered=true", hex.EncodeToString(p.TraceId))
}

func TestDisabledAndParentBasedSampling(t *testing.T) {
	var calls atomic.Int32
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); io.Copy(io.Discard, r.Body) }))
	defer sink.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", sink.URL)
	rt, err := New(context.Background(), nil, Identity{})
	if err != nil {
		t.Fatal(err)
	}
	_, s := rt.Tracer("agent-worker").Start(context.Background(), "worker.runner.run")
	s.End()
	if rt.Shutdown(context.Background()) != nil || calls.Load() != 0 {
		t.Fatal("disabled exporter made request")
	}
	cfg := testConfig(sink.URL + "/v1/traces")
	cfg.SamplingRatio = 1
	rt, err = New(context.Background(), &cfg, identity())
	if err != nil {
		t.Fatal(err)
	}
	parent := tracecontext.Carrier{Traceparent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-00"}
	ctx, s := rt.Tracer("agent-worker").Start(parent.Restore(context.Background()), "worker.runner.run")
	s.End()
	if tracecontext.Capture(ctx).Traceparent == "" || trace.SpanContextFromContext(ctx).IsSampled() {
		t.Fatal("unsampled context lost")
	}
	if err = rt.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("parent sampling decision ignored")
	}
}

func TestExportOutageAndQueueRemainBounded(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		io.WriteString(w, "PRIVATE_COLLECTOR_ERROR")
	}))
	defer sink.Close()
	cfg := testConfig(sink.URL + "/v1/traces")
	cfg.MaxQueueSize = 1
	cfg.MaxExportBatchSize = 1
	cfg.ExportTimeout = "50ms"
	rt, err := New(context.Background(), &cfg, identity())
	if err != nil {
		t.Fatal(err)
	}
	tr := rt.Tracer("agent-worker")
	_, s := tr.Start(context.Background(), "worker.runner.run")
	s.End()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("no export")
	}
	start := time.Now()
	for i := 0; i < 1000; i++ {
		_, s := tr.Start(context.Background(), "worker.runner.run")
		s.End()
	}
	if time.Since(start) > time.Second {
		t.Fatal("observation blocked business")
	}
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = rt.Shutdown(ctx); err != nil && strings.Contains(err.Error(), "PRIVATE") {
		t.Fatal("raw exporter error")
	}
	stats := rt.Stats()
	if stats.Finished != 1001 || stats.ExportFailures == 0 || stats.Exported != 0 {
		t.Fatal(stats)
	}
	t.Logf("OUTAGE=PASS finished=%d export_failures=%d business_nonblocking=true shutdown_bounded=true", stats.Finished, stats.ExportFailures)
}

func TestClientRedirectPartialSuccessAndResponseBound(t *testing.T) {
	var forwarded atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { forwarded.Add(1) }))
	defer target.Close()
	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
	}{
		{"redirect", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
		}},
		{"partial", func(w http.ResponseWriter, r *http.Request) {
			b, _ := proto.Marshal(&collector.ExportTraceServiceResponse{PartialSuccess: &collector.ExportTracePartialSuccess{RejectedSpans: 1, ErrorMessage: "CANARY"}})
			w.Write(b)
		}},
		{"oversized", func(w http.ResponseWriter, r *http.Request) { w.Write(bytes.Repeat([]byte("x"), 65537)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink := httptest.NewServer(tc.handler)
			defer sink.Close()
			client := newHTTPClient(sink.URL+"/v1/traces", time.Second)
			defer client.Stop(context.Background())
			if err := client.UploadTraces(context.Background(), nil); err != errExport {
				t.Fatalf("error=%v", err)
			}
		})
	}
	if forwarded.Load() != 0 {
		t.Fatal("redirect followed")
	}
}

func TestIMHandlerMakesLocalRootWithoutLeakingURL(t *testing.T) {
	cfg := testConfig("http://127.0.0.1:1/v1/traces")
	rt, err := New(context.Background(), &cfg, Identity{Service: "channel-gateway", Instance: "gw-test"})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Shutdown(context.Background())
	upstream := tracecontext.Carrier{Traceparent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"}
	req := httptest.NewRequest("POST", "http://gateway/v1/telegram/PRIVATE_PATH", nil)
	req = req.WithContext(upstream.Restore(req.Context()))
	upstream.Inject(req.Header)
	var observed trace.SpanContext
	rt.IMHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed = trace.SpanContextFromContext(r.Context())
		w.WriteHeader(204)
	})).ServeHTTP(httptest.NewRecorder(), req)
	if !observed.IsValid() || observed.TraceID().String() == "0123456789abcdef0123456789abcdef" {
		t.Fatal("inherited external root")
	}
}

func TestCallbackResponseHasExportedStatus(t *testing.T) {
	for _, tc := range []struct {
		name         string
		status, want int
	}{
		{"unavailable", 503, 503}, {"bad_request", 400, 400}, {"no_content", 204, 204}, {"implicit_ok", 0, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {

			got := make(chan *collector.ExportTraceServiceRequest, 4)
			sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				request := new(collector.ExportTraceServiceRequest)
				if err := proto.Unmarshal(body, request); err != nil {
					t.Error(err)
				}
				got <- request
				w.WriteHeader(http.StatusOK)
			}))
			defer sink.Close()
			cfg := testConfig(sink.URL + "/v1/traces")
			rt, err := New(context.Background(), &cfg, Identity{Service: "channel-gateway", Instance: "review"})
			if err != nil {
				t.Fatal(err)
			}
			defer rt.Shutdown(context.Background())
			reply := httptest.NewRecorder()
			rt.IMHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.status != 0 {
					w.WriteHeader(tc.status)
				} else {
					_, _ = w.Write([]byte("ok"))
				}
			})).ServeHTTP(reply, httptest.NewRequest("POST", "http://gateway/v1/telegram/review", nil))
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := rt.ForceFlush(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case request := <-got:
				for _, resource := range request.ResourceSpans {
					for _, scope := range resource.ScopeSpans {
						for _, span := range scope.Spans {
							if span.Name != "gateway.im.callback" {
								continue
							}
							statusAttr, errorType := int64(0), ""
							for _, kv := range span.Attributes {
								if kv.Key == "http.response.status_code" {
									statusAttr = kv.Value.GetIntValue()
								}
								if kv.Key == "error.type" {
									errorType = kv.Value.GetStringValue()
								}
							}
							t.Logf("HTTP_RESPONSE=%d SPAN_STATUS=%s HTTP_STATUS_ATTRIBUTE=%d ERROR_TYPE=%q", reply.Code, span.GetStatus().GetCode(), statusAttr, errorType)
							expectedCode := tracepb.Status_STATUS_CODE_UNSET
							expectedError := ""
							if tc.want >= 500 {
								expectedCode = tracepb.Status_STATUS_CODE_ERROR
								expectedError = "failed"
							}
							if reply.Code != tc.want || span.GetStatus().GetCode() != expectedCode || statusAttr != int64(tc.want) || errorType != expectedError {
								t.Error("failed callback lacks HTTP status and fixed failure classification")
							}
							return
						}
					}
				}
				t.Fatal("callback span missing")
			case <-ctx.Done():
				t.Fatal("export missing")
			}

		})
	}
}

func TestCallbackResponseControllerFlush(t *testing.T) {
	recorder := httptest.NewRecorder()
	response := &callbackResponse{ResponseWriter: recorder}
	if err := http.NewResponseController(response).Flush(); err != nil {
		t.Fatal(err)
	}
	if response.status != 200 || !recorder.Flushed || response.Unwrap() != recorder {
		t.Fatal("flush semantics changed")
	}
	response.WriteHeader(503)
	if response.status != 200 {
		t.Fatal("later status replaced committed response")
	}
}
