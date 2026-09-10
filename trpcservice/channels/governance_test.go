package channels

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"go.opentelemetry.io/otel"
	metricsdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/guardrail"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// useTracer installs a real (exporter-less) tracer provider so spans carry
// valid trace ids, mirroring a deployment with tracing enabled. Note that the
// otel global provider delegates permanently once set, so tests must not
// assert the absence of trace ids after this ran.
func useTracer(t *testing.T) {
	t.Helper()
	otel.SetTracerProvider(tracesdk.NewTracerProvider())
}

// recordAdapter captures outbound deliveries so governance tests can assert
// what the channel would have received. dispatch is called synchronously.
type recordAdapter struct {
	out []*OutboundMessage
}

func newRecordAdapter() *recordAdapter { return &recordAdapter{} }

func (r *recordAdapter) Type() Type { return TypeWebChat }

func (r *recordAdapter) Callback(http.ResponseWriter, *http.Request) ([]*InboundMessage, error) {
	return nil, nil
}

func (r *recordAdapter) Send(_ context.Context, msg *OutboundMessage) error {
	r.out = append(r.out, msg)
	return nil
}

func (r *recordAdapter) sends() []*OutboundMessage { return r.out }

// testConfig builds a minimal valid config with one tenant; the api key is
// fake but never used because the guarded paths stop before the model call.
func testConfig() *config.Config {
	return &config.Config{
		DefaultTenant: "demo",
		Tenants: map[string]*tenant.Context{
			"demo": {
				ID:    "demo",
				Model: tenant.ModelConfig{Name: "m", APIKey: "sk-test"},
			},
		},
	}
}

// sumPoints collects the int64 counter totals per metric name from a manual
// reader, mirroring the assertion style of the metrics package tests.
func sumPoints(t *testing.T, reader *metricsdk.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	sums := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if d, ok := m.Data.(metricdata.Sum[int64]); ok {
				var total int64
				for _, dp := range d.DataPoints {
					total += dp.Value
				}
				sums[m.Name] = total
			}
		}
	}
	return sums
}

// TestDispatchInputGuardrail drives one blocked message through the full
// governance path: rejection reply on the adapter, inbound + block lines on
// the audit trail, and the tenant-labelled counters — all before any model
// call happens.
func TestDispatchInputGuardrail(t *testing.T) {
	useTracer(t)
	auditPath := filepath.Join(t.TempDir(), "audit.jsonl")
	aud, err := audit.New(auditPath)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	t.Cleanup(func() { aud.Close() })

	reader := metricsdk.NewManualReader()
	provider := metricsdk.NewMeterProvider(metricsdk.WithReader(reader))
	rec, err := metrics.NewRecorder(provider.Meter("test"))
	if err != nil {
		t.Fatalf("recorder: %v", err)
	}

	reg, err := agent.NewRegistry(testConfig(), inmemory.NewSessionService())
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	pol := tenant.Guardrails{BlockedKeywords: []string{"forbidden"}}
	gw := NewGateway(reg).WithGovernance(Governance{
		PolicyFor: func(string) tenant.Guardrails { return pol },
		Audit:     aud,
		Metrics:   rec,
	})

	ad := newRecordAdapter()
	in := &InboundMessage{TenantID: "demo", Channel: TypeWebChat, UserID: "u1", MsgID: "m1", Text: "say forbidden now"}
	gw.dispatch(context.Background(), ad, in)

	sends := ad.sends()
	if len(sends) != 1 || !sends[0].Done || sends[0].Text != guardrail.RejectionText {
		t.Fatalf("sends = %+v, want one done rejection", sends)
	}

	raw, err := os.ReadFile(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	var lines []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte("\n")) {
		var m map[string]any
		if err := json.Unmarshal(line, &m); err != nil {
			t.Fatalf("audit line not JSON: %v (%s)", err, line)
		}
		lines = append(lines, m)
	}
	if len(lines) != 3 {
		t.Fatalf("audit lines = %d, want 3 (inbound + block + the reply that carried it): %s", len(lines), raw)
	}
	if lines[0]["event"] != audit.EventInbound || lines[0]["decision"] != audit.DecisionAllow {
		t.Fatalf("first audit line = %v", lines[0])
	}
	blk := lines[1]
	if blk["event"] != audit.EventGuardrailBlock || blk["decision"] != audit.DecisionBlock ||
		blk["stage"] != "input" || blk["rule"] != guardrail.RuleKeyword ||
		blk["tenant_id"] != "demo" || blk["session_id"] != "demo:webchat:u1" {
		t.Fatalf("block audit line = %v", blk)
	}
	if blk["trace_id"] == nil || blk["trace_id"] == "" {
		t.Fatal("audit lines must carry the dispatch trace id")
	}
	if lines[0]["trace_id"] != blk["trace_id"] {
		t.Fatal("inbound and block must share one trace id")
	}
	if tid, _ := blk["trace_id"].(string); len(tid) != 32 {
		t.Fatalf("trace_id = %q, want a 32 hex char id", tid)
	}
	// A rejection is a reply like any other: the user was told something, so the
	// trail has to say what. This row did not exist on any guarded path before
	// reply() became the single recording point, which is why the drill counts
	// came back inbound > reply.
	rp := lines[2]
	if rp["event"] != audit.EventReply || rp["decision"] != audit.DecisionBlock ||
		rp["trace_id"] != blk["trace_id"] || rp["session_id"] != "demo:webchat:u1" {
		t.Fatalf("reply audit line = %v, want the block decision on the same trace", rp)
	}
	if _, ok := rp["error_type"]; ok {
		t.Fatalf("a delivered rejection carries error_type %v, want none: the send worked", rp["error_type"])
	}

	sums := sumPoints(t, reader)
	if sums["trpcservice.guardrail.blocks"] != 1 {
		t.Fatalf("guardrail.blocks = %d, want 1", sums["trpcservice.guardrail.blocks"])
	}
	if sums["trpcservice.messages"] != 1 {
		t.Fatalf("messages = %d, want 1", sums["trpcservice.messages"])
	}
}

// TestDispatchUnknownTenantNoGovernance keeps the unwired gateway honest:
// zero-value governance must not panic, and an unknown tenant still gets a
// clear error reply.
func TestDispatchUnknownTenantNoGovernance(t *testing.T) {
	reg, err := agent.NewRegistry(testConfig(), inmemory.NewSessionService())
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	gw := NewGateway(reg) // no WithGovernance: zero value everywhere

	ad := newRecordAdapter()
	in := &InboundMessage{TenantID: "ghost", Channel: TypeWebChat, UserID: "u1", Text: "hello"}
	gw.dispatch(context.Background(), ad, in)

	sends := ad.sends()
	if len(sends) != 1 || !sends[0].Done {
		t.Fatalf("sends = %+v, want one done error", sends)
	}
	if want := `unknown tenant "ghost"`; sends[0].Text != want {
		t.Fatalf("text = %q, want %q", sends[0].Text, want)
	}
}

// TestPolicyZeroValue covers the helper directly: an unwired gateway must
// resolve to the empty (permit-all) policy.
func TestPolicyZeroValue(t *testing.T) {
	gw := NewGateway(nil)
	if pol := gw.policy("demo"); pol.MaxInputBytes != 0 || len(pol.BlockedKeywords) != 0 {
		t.Fatalf("policy = %+v, want zero value", pol)
	}
}

// TestDispatchLengthRule covers the second input rule and an unwired metric
// recorder: the length rejection must reply and audit without panicking.
func TestDispatchLengthRule(t *testing.T) {
	auditPath := filepath.Join(t.TempDir(), "audit.jsonl")
	aud, err := audit.New(auditPath)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	t.Cleanup(func() { aud.Close() })

	reg, err := agent.NewRegistry(testConfig(), inmemory.NewSessionService())
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	gw := NewGateway(reg).WithGovernance(Governance{
		PolicyFor: func(string) tenant.Guardrails {
			return tenant.Guardrails{MaxInputBytes: 4}
		},
		Audit: aud, // Metrics deliberately nil
	})

	ad := newRecordAdapter()
	gw.dispatch(context.Background(), ad, &InboundMessage{
		TenantID: "demo", Channel: TypeWebChat, UserID: "u1", Text: "far too long",
	})

	sends := ad.sends()
	if len(sends) != 1 || sends[0].Text != guardrail.RejectionText {
		t.Fatalf("sends = %+v, want one rejection", sends)
	}
	raw, err := os.ReadFile(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(`"rule":"length"`)) {
		t.Fatalf("length rule not recorded:\n%s", raw)
	}
}

// TestTraceIDOfInvalidContext pins the default-config semantic: with no
// recording span in the context (the noop provider) the audit trail leaves
// trace_id out instead of writing an all-zero id. A valid propagated span
// context must be rendered verbatim.
func TestTraceIDOfInvalidContext(t *testing.T) {
	if got := traceIDOf(context.Background()); got != "" {
		t.Fatalf("traceIDOf(background) = %q, want empty", got)
	}

	want := "4823ad34949742e64e17672a5a1424e5"
	tid, err := trace.TraceIDFromHex(want)
	if err != nil {
		t.Fatal(err)
	}
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid}))
	if got := traceIDOf(ctx); got != want {
		t.Fatalf("traceIDOf(propagated) = %q, want %q", got, want)
	}
}
