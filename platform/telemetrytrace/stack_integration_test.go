package telemetrytrace

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// This is a real Collector/Tempo transport and query gate, not a claim that
// synthetic spans execute the Worker or deliver an external IM message.
func TestCollectorTempoRoundTrip(t *testing.T) {
	endpoint, backend := os.Getenv("TRACING_STACK_OTLP_ENDPOINT"), os.Getenv("TRACING_STACK_TEMPO_URL")
	if endpoint == "" || backend == "" {
		t.Skip("explicit isolated Collector/Tempo endpoints required")
	}
	u, err := url.Parse(backend)
	if err != nil || u.Scheme != "http" || u.Hostname() != "127.0.0.1" || u.RawQuery != "" || u.User != nil || u.Path != "" {
		t.Fatal("fixture Tempo must be literal loopback")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	config := &Config{TracesEndpoint: endpoint, SamplingRatio: 1, ExportTimeout: "2s", BatchTimeout: "1s", MaxQueueSize: 128, MaxExportBatchSize: 32}
	gateway, err := New(ctx, config, Identity{Service: "channel-gateway", Instance: "trace-stack-gateway"})
	if err != nil {
		t.Fatal(err)
	}
	defer gateway.Shutdown(context.Background())
	worker, err := New(ctx, config, Identity{Service: "agent-worker", Instance: "trace-stack-worker"})
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Shutdown(context.Background())
	run := fmt.Sprintf("trace-stack-%d", time.Now().UnixNano())
	gt, wt := gateway.Tracer("gateway"), worker.Tracer("worker")
	gctx, root := gt.Start(ctx, "gateway.im.callback", trace.WithAttributes(attribute.String("app.run.id", run)))
	creationCtx, creation := gt.Start(gctx, "create execution.run-requested.v1", trace.WithSpanKind(trace.SpanKindProducer))
	carrier := tracecontext.Capture(creationCtx)
	creation.End()
	wctx, attempt := Resume(wt, ctx, carrier, "worker.run.attempt", trace.WithLinks(trace.Link{SpanContext: creation.SpanContext(), Attributes: []attribute.KeyValue{attribute.String("raw", "STACK_SECRET_CANARY")}}))
	_, session := wt.Start(wctx, "worker.session.commit", trace.WithAttributes(attribute.String("app.run.id", run), attribute.String("prompt", "STACK_SECRET_CANARY"), attribute.String("app.outcome", "accepted")))
	session.AddEvent("STACK_SECRET_CANARY")
	session.SetStatus(codes.Error, "STACK_SECRET_CANARY")
	session.End()
	attempt.End()
	root.End()
	if err = gateway.ForceFlush(ctx); err != nil {
		t.Fatal(err)
	}
	if err = worker.ForceFlush(ctx); err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	traceID := root.SpanContext().TraceID().String()
	traceURL := backend + "/api/traces/" + traceID
	var raw []byte
	var document map[string]any
	for ctx.Err() == nil {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, traceURL, nil)
		req.Header.Set("Accept", "application/json")
		res, e := client.Do(req)
		if e == nil {
			body, re := io.ReadAll(io.LimitReader(res.Body, 1<<20))
			res.Body.Close()
			if re == nil && res.StatusCode == 200 {
				raw = body
				if json.Unmarshal(raw, &document) == nil && strings.Contains(string(raw), "worker.session.commit") && strings.Contains(string(raw), "gateway.im.callback") {
					break
				}
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(200 * time.Millisecond):
		}
	}
	if len(raw) == 0 || document == nil {
		t.Fatal("Tempo trace lookup did not complete", ctx.Err())
	}
	if strings.Contains(string(raw), "STACK_SECRET_CANARY") {
		t.Fatal("source/Collector filtering leaked canary")
	}
	// Tempo serializes protobuf IDs as base64. Also accept hex for API versions
	// using their canonical trace representation; never accept handwritten attrs.
	id := func(x any) string {
		v, _ := x.(string)
		if b, e := base64.StdEncoding.DecodeString(v); e == nil && (len(b) == 8 || len(b) == 16) {
			return hex.EncodeToString(b)
		}
		return v
	}
	spans := map[string]map[string]any{}
	services := map[string]bool{}
	var visit func(any)
	visit = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			if name, ok := x["name"].(string); ok {
				if _, exists := x["spanId"]; exists {
					spans[name] = x
				}
			}
			if x["key"] == "service.name" {
				if v, ok := x["value"].(map[string]any); ok {
					if n, ok := v["stringValue"].(string); ok {
						services[n] = true
					}
				}
			}
			for _, child := range x {
				visit(child)
			}
		case []any:
			for _, child := range x {
				visit(child)
			}
		}
	}
	visit(document)
	for name, expected := range map[string]string{"gateway.im.callback": "", "create execution.run-requested.v1": root.SpanContext().SpanID().String(), "worker.run.attempt": creation.SpanContext().SpanID().String(), "worker.session.commit": attempt.SpanContext().SpanID().String()} {
		s := spans[name]
		if s == nil || id(s["traceId"]) != traceID {
			t.Fatal("missing span or wrong actual trace ID", name)
		}
		if expected != "" && id(s["parentSpanId"]) != expected {
			t.Fatal("actual parent mismatch", name)
		}
	}
	if len(spans) != 4 || !services["channel-gateway"] || !services["agent-worker"] {
		t.Fatal("resource/span coverage")
	}
	links, ok := spans["worker.run.attempt"]["links"].([]any)
	if !ok || len(links) != 1 {
		t.Fatal("creation link lost")
	}
	link := links[0].(map[string]any)
	if id(link["spanId"]) != creation.SpanContext().SpanID().String() {
		t.Fatal("link identity changed")
	}
	query := backend + "/api/search?q=" + url.QueryEscape(`{ span.app.run.id = "`+run+`" }`)
	found := false
	for ctx.Err() == nil {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, query, nil)
		res, e := client.Do(req)
		if e == nil {
			body, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
			res.Body.Close()
			if res.StatusCode == 200 && strings.Contains(string(body), traceID) {
				found = true
				break
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(200 * time.Millisecond):
		}
	}
	if !found {
		t.Fatal("run ID search did not find trace")
	}
	if gateway.Stats().Exported != 2 || worker.Stats().Exported != 2 {
		t.Fatal("export confirmation mismatch")
	}
	if dir := os.Getenv("TRACING_STACK_EVIDENCE_DIR"); dir != "" {
		if err = os.WriteFile(dir+"/stack-trace.json", raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("COLLECTOR_TEMPO=PASS trace_id=%s run_id=%s spans=4 services=2 parentage=true link=true canary_absent=true search=true", traceID, run)
}
