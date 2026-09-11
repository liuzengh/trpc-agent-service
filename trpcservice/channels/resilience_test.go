package channels

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	metricsdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/guardrail"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// configPointingAt builds a one-tenant config whose model calls hit baseURL, so
// a test controls the upstream instead of depending on the internet.
func configPointingAt(baseURL string) *config.Config {
	return &config.Config{
		DefaultTenant: "demo",
		Tenants: map[string]*tenant.Context{
			"demo": {
				ID:    "demo",
				Model: tenant.ModelConfig{Name: "m", APIKey: "sk-test", BaseURL: baseURL},
			},
		},
	}
}

// stallModel is a hung upstream: it reads the request, then writes nothing.
// It returns as soon as the client cancels, which is what lets a test tell
// "the dispatch abandoned the call" from "the dispatch cancelled it" — the
// abandoned variant would leave the handler (and httptest's Close) blocked for
// the whole delay.
//
// Draining the body is load-bearing, not decoration: net/http only starts the
// background read that detects a departed client once the request body has been
// consumed (server.go: registerOnHitEOF(..., startBackgroundRead)), so a handler
// that stalls before reading it would never see r.Context() cancelled no matter
// what the client does. Reading the request and then stalling is also what a
// real hung upstream looks like.
type stallModel struct {
	srv       *httptest.Server
	hits      atomic.Int32
	cancelled atomic.Int32
}

func newStallModel(t *testing.T, delay time.Duration) *stallModel {
	t.Helper()
	m := &stallModel{}
	m.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.hits.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			m.cancelled.Add(1)
		}
	}))
	t.Cleanup(m.srv.Close)
	return m
}

// waitCancelled polls until the upstream saw its request cancelled, so the
// assertion does not race the client's cleanup goroutines.
func (m *stallModel) waitCancelled(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if m.cancelled.Load() > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("the upstream request was never cancelled: the dispatch abandoned it instead")
}

// newAudit opens a throwaway trail and returns it with a reader over the lines
// written so far.
func newAudit(t *testing.T) (*audit.Logger, func() []map[string]any) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	aud, err := audit.New(path)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	t.Cleanup(func() { aud.Close() })
	return aud, func() []map[string]any {
		t.Helper()
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var lines []map[string]any
		for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte("\n")) {
			if len(line) == 0 {
				continue
			}
			var m map[string]any
			if err := json.Unmarshal(line, &m); err != nil {
				t.Fatalf("audit line is not JSON: %v (%s)", err, line)
			}
			lines = append(lines, m)
		}
		return lines
	}
}

// newMeter returns a recorder over a manual reader so each test can assert its
// own counters.
func newMeter(t *testing.T) (*metrics.Recorder, *metricsdk.ManualReader) {
	t.Helper()
	reader := metricsdk.NewManualReader()
	rec, err := metrics.NewRecorder(metricsdk.NewMeterProvider(metricsdk.WithReader(reader)).Meter("test"))
	if err != nil {
		t.Fatalf("recorder: %v", err)
	}
	return rec, reader
}

// resultsByLabel splits the message counter by its result label, which is how
// the platform tells an ok reply from a throttled or failed one. sumPoints in
// governance_test.go deliberately ignores labels; the resilience paths need
// them because they share the instrument.
func resultsByLabel(t *testing.T, reader *metricsdk.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "trpcservice.messages" {
				continue
			}
			d, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range d.DataPoints {
				result, _ := dp.Attributes.Value(attribute.Key("result"))
				out[result.AsString()] += dp.Value
			}
		}
	}
	return out
}

// sseClient reads what a browser on the WebChat stream would see.
type sseClient struct {
	body *bufio.Reader
}

// openSSE starts the real SSE endpoint and returns once the stream is
// registered: the handler registers its channel before flushing the response
// headers, so a returned response means Send can now find it. That ordering is
// the whole synchronisation — no sleeping, no polling, no flake. The client
// timeout is the safety net that turns "the reply never arrived" into a failure
// instead of a hung test.
func openSSE(t *testing.T, wc *WebChat, tenantID, userID string) *sseClient {
	t.Helper()
	srv := httptest.NewServer(wc.Routes()["/webchat/stream"])
	t.Cleanup(srv.Close)
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Get(
		srv.URL + "?tenant=" + tenantID + "&user=" + userID)
	if err != nil {
		t.Fatalf("open sse stream: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("sse status = %d, want 200", resp.StatusCode)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return &sseClient{body: bufio.NewReader(resp.Body)}
}

// next returns the payload of the next event, skipping blank separators.
func (c *sseClient) next(t *testing.T) sseEvent {
	t.Helper()
	for {
		line, err := c.body.ReadString('\n')
		if err != nil {
			t.Fatalf("read sse event: %v (did any reply reach the user?)", err)
		}
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var ev sseEvent
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev); err != nil {
			t.Fatalf("sse payload %q: %v", line, err)
		}
		return ev
	}
}

// strictAdapter mirrors WebChat.Send's select on ctx.Done() — deterministically.
// The real one cannot be used as a regression guard: it races a buffered,
// actively drained channel against the deadline, and Go picks randomly between
// two ready select cases, so a reply sent on an expired context gets through
// roughly half the time. Measured under mutation (reply sent on the dispatch
// context): the SSE assertion caught it 3 times out of 5.
type strictAdapter struct {
	recordAdapter
	// The context is snapshotted inside Send rather than kept: the caller's
	// defer cancel() fires before the test gets to look at it, which would
	// report "context canceled" for a reply that was delivered just fine.
	gotErr      error
	gotBudget   time.Duration
	hadDeadline bool
}

func newStrictAdapter() *strictAdapter { return &strictAdapter{} }

func (s *strictAdapter) Send(ctx context.Context, msg *OutboundMessage) error {
	s.gotErr = ctx.Err()
	deadline, ok := ctx.Deadline()
	s.hadDeadline, s.gotBudget = ok, time.Until(deadline)
	if s.gotErr != nil {
		return s.gotErr
	}
	return s.recordAdapter.Send(ctx, msg)
}

// TestReplySurvivesAnExpiredDeadline is the deterministic guard for the one
// failure mode that is invisible in production until it matters: the terminal
// reply must be sent on a context that is no longer bound by the dispatch
// deadline. Without it the timeout notice — the message whose only job is to
// tell the user something timed out — is the one most likely to be dropped.
func TestReplySurvivesAnExpiredDeadline(t *testing.T) {
	gw := NewGateway(nil)
	ctx, cancel := context.WithTimeout(context.Background(), -time.Second)
	defer cancel()
	if ctx.Err() == nil {
		t.Fatal("the dispatch context must already be expired for this to mean anything")
	}

	ad := newStrictAdapter()
	// decision=error is what the timeout path actually passes (failModel); the
	// point of this test is the context, not the row, but the argument has to be
	// the real one or the test stops describing production.
	gw.reply(ctx, ad, &InboundMessage{TenantID: "demo", Channel: TypeWebChat, UserID: "u1"},
		TimeoutText, audit.DecisionError)

	sends := ad.sends()
	if len(sends) != 1 || !sends[0].Done || sends[0].Text != TimeoutText {
		t.Fatalf("sends = %+v, want the timeout notice delivered", sends)
	}
	if ad.gotErr != nil {
		t.Fatalf("Send saw a context carrying %v, want one detached from the deadline", ad.gotErr)
	}
	// Detached does not mean unbounded: a reply must still have its own ceiling.
	if !ad.hadDeadline || ad.gotBudget <= 0 || ad.gotBudget > replyTimeout {
		t.Fatalf("Send's budget = %v (deadline %v), want a fresh %v ceiling",
			ad.gotBudget, ad.hadDeadline, replyTimeout)
	}
}

// deadAdapter is the last hop refusing to carry the message: a browser that
// hung up, a chat API that revoked the token, a send queue that filled up.
type deadAdapter struct {
	recordAdapter
	err error
}

func (d *deadAdapter) Send(_ context.Context, _ *OutboundMessage) error { return d.err }

// TestReplyRecordsAFailedSend pins both halves of the hole the fault drills
// walked into. The row used to be written by the caller and only on the success
// path, so:
//
//  1. every failure path told the user something the trail never mentioned —
//     the counts after a run of drills were 52 inbound / 47 model_call / 45
//     reply, and the missing rows were precisely the answers nobody got;
//  2. a Send that errored on the success path was still recorded as
//     decision=ok, i.e. as a reply that got through.
//
// "We tried to say X" is not "we said X", and error_type=send is the axis that
// says so: unlike timeout/agent/runner the platform did its job and the last
// hop failed, so the user is sitting there with nothing.
func TestReplyRecordsAFailedSend(t *testing.T) {
	aud, lines := newAudit(t)
	gw := NewGateway(nil).WithGovernance(Governance{Audit: aud}) // Metrics deliberately nil

	in := &InboundMessage{TenantID: "demo", Channel: TypeWebChat, UserID: "u1", MsgID: "m1"}
	gw.reply(context.Background(), newRecordAdapter(), in, "", audit.DecisionOK)
	gw.reply(context.Background(), &deadAdapter{err: errors.New("channel closed")}, in,
		TimeoutText, audit.DecisionError)

	rows := lines()
	if len(rows) != 2 {
		t.Fatalf("got %d audit rows, want one per reply: the refused one is the row that matters most", len(rows))
	}
	for i, want := range []string{audit.DecisionOK, audit.DecisionError} {
		if got := rows[i]["event"]; got != audit.EventReply {
			t.Fatalf("row %d: event = %v, want %q", i, got, audit.EventReply)
		}
		if got := rows[i]["decision"]; got != want {
			t.Fatalf("row %d: decision = %v, want %q", i, got, want)
		}
		// The row must still say who was left without an answer.
		for _, k := range []string{"tenant_id", "channel", "user_id", "session_id"} {
			if got, _ := rows[i][k].(string); got == "" {
				t.Fatalf("row %d: %s is empty, want the recipient identified", i, k)
			}
		}
	}
	if _, ok := rows[0]["error_type"]; ok {
		t.Fatalf("delivered reply carries error_type %v, want none", rows[0]["error_type"])
	}
	if got := rows[1]["error_type"]; got != errorTypeSend {
		t.Fatalf("refused reply: error_type = %v, want %q", got, errorTypeSend)
	}
	if got, _ := rows[1]["detail"].(string); !strings.Contains(got, "channel closed") {
		t.Fatalf("refused reply: detail = %q, want the adapter's own words", got)
	}
}

// TestFailureWordingKeepsTheDetailOutOfTheReply is the guard on the half of
// failModel that faces the user. The drill on the deployed stack (D4, Redis
// stopped mid-run) showed a visitor receiving
//
//	runner error: check session exists: ... dial tcp: lookup redis on
//	127.0.0.11:53: no such host
//
// — internal service name, embedded DNS address and the whole chain, on an
// endpoint that needs no credentials. Nothing caught it because every existing
// failure test asserted either the timeout wording or the audit row, never what
// the non-timeout paths actually said out loud.
//
// One path is enough to pin the decision: failModel picks the wording itself
// now, so runner and agent cannot diverge, and the timeout branch is covered by
// TestDispatchTimeoutReachesTheUser. What must hold on both sides is that the
// detail survives — into the audit row, where the operator looks.
func TestFailureWordingKeepsTheDetailOutOfTheReply(t *testing.T) {
	useTracer(t)
	aud, lines := newAudit(t)
	rec, _ := newMeter(t)

	// Port 1 refuses instantly, so this is a real upstream failure rather than a
	// budget running out — the case whose wording used to be assembled inline.
	reg, err := agent.NewRegistry(configPointingAt("http://127.0.0.1:1"), inmemory.NewSessionService())
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	gw := NewGateway(reg).WithGovernance(Governance{
		LimitsFor: func() config.AgentConfig { return config.AgentConfig{MessageTimeout: time.Minute} },
		Audit:     aud,
		Metrics:   rec,
	})

	ad := newRecordAdapter()
	gw.dispatch(context.Background(), ad, &InboundMessage{
		TenantID: "demo", Channel: TypeWebChat, UserID: "u1", MsgID: "m1", Text: "hello",
	})

	sends := ad.sends()
	if len(sends) != 1 || !sends[0].Done {
		t.Fatalf("sends = %+v, want exactly one terminal message", sends)
	}
	if sends[0].Text != FailureText {
		t.Fatalf("reply = %q, want %q", sends[0].Text, FailureText)
	}
	for _, leak := range []string{"error:", "127.0.0.1", "refused", "dial", "POST"} {
		if strings.Contains(sends[0].Text, leak) {
			t.Fatalf("reply %q leaks %q: the raw chain belongs in the audit row, not in front of the user",
				sends[0].Text, leak)
		}
	}

	var call map[string]any
	for _, l := range lines() {
		if l["event"] == audit.EventModelCall {
			call = l
		}
	}
	if call == nil {
		t.Fatal("no model_call row: the failure was not recorded at all")
	}
	if call["decision"] != audit.DecisionError {
		t.Fatalf("decision = %v, want %q", call["decision"], audit.DecisionError)
	}
	if call["error_type"] == errorTypeTimeout {
		t.Fatalf("error_type = %q, want the upstream failure bucket: a refused connection is not a budget",
			errorTypeTimeout)
	}
	// The whole point of not telling the user: the operator still gets it.
	detail, _ := call["detail"].(string)
	if !strings.Contains(detail, "127.0.0.1") {
		t.Fatalf("detail = %q, want the verbatim upstream error the reply withheld", detail)
	}
}

// TestDispatchTimeoutReachesTheUser is drill D1 in a single process: a model
// that never answers is cut off at message_timeout, the trail says "timeout"
// rather than lumping it in with real model errors, and the user actually
// receives the friendly wording. That last part is the one that silently breaks:
// WebChat.Send selects on ctx.Done(), so a reply sent on the expired dispatch
// context would fail and the user would see nothing at all. The deterministic
// guard for that invariant is TestReplySurvivesAnExpiredDeadline; this one is
// the end-to-end proof that a real browser connection does get the notice.
func TestDispatchTimeoutReachesTheUser(t *testing.T) {
	useTracer(t)
	upstream := newStallModel(t, 5*time.Second)
	aud, lines := newAudit(t)
	rec, reader := newMeter(t)

	reg, err := agent.NewRegistry(configPointingAt(upstream.srv.URL), inmemory.NewSessionService())
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	gw := NewGateway(reg).WithGovernance(Governance{
		LimitsFor: func() config.AgentConfig {
			return config.AgentConfig{MessageTimeout: 300 * time.Millisecond}
		},
		Audit:   aud,
		Metrics: rec,
	})

	wc := NewWebChat()
	sse := openSSE(t, wc, "demo", "u1")

	start := time.Now()
	gw.dispatch(context.Background(), wc, &InboundMessage{
		TenantID: "demo", Channel: TypeWebChat, UserID: "u1", MsgID: "m1", Text: "hello",
	})
	elapsed := time.Since(start)

	if elapsed > 3*time.Second {
		t.Fatalf("dispatch took %v, want it cut off near the 300ms budget", elapsed)
	}
	if upstream.hits.Load() == 0 {
		t.Fatal("the model was never called, so no timeout was exercised")
	}
	upstream.waitCancelled(t)

	ev := sse.next(t)
	if !ev.Done || ev.Chunk || ev.Text != TimeoutText {
		t.Fatalf("sse event = %+v, want one done message carrying TimeoutText", ev)
	}

	var inbound, call map[string]any
	for _, l := range lines() {
		switch l["event"] {
		case audit.EventInbound:
			inbound = l
		case audit.EventModelCall:
			call = l
		}
	}
	if inbound == nil || call == nil {
		t.Fatalf("audit lines = %v, want an inbound and a model_call", lines())
	}
	if call["error_type"] != errorTypeTimeout {
		t.Fatalf("error_type = %v, want %q", call["error_type"], errorTypeTimeout)
	}
	if call["decision"] != audit.DecisionError {
		t.Fatalf("decision = %v, want %q", call["decision"], audit.DecisionError)
	}
	if ms, _ := call["latency_ms"].(float64); ms <= 0 {
		t.Fatalf("latency_ms = %v, want the slow failure recorded", call["latency_ms"])
	}
	if call["trace_id"] != inbound["trace_id"] {
		t.Fatal("the timeout line must share the dispatch trace id")
	}
	if results := resultsByLabel(t, reader); results[resultError] != 1 || results[resultThrottled] != 0 {
		t.Fatalf("messages by result = %v, want exactly one error", results)
	}
}

// TestDispatchOverQuotaIsRejected is the admission half of drill D7: with the
// tenant's single slot already taken, the next message is refused at once
// instead of queueing, never reaches the model, and still leaves a trail the
// operator can alert on.
func TestDispatchOverQuotaIsRejected(t *testing.T) {
	useTracer(t)
	upstream := newStallModel(t, 5*time.Second)
	aud, lines := newAudit(t)
	rec, reader := newMeter(t)

	reg, err := agent.NewRegistry(configPointingAt(upstream.srv.URL), inmemory.NewSessionService())
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	gw := NewGateway(reg).WithGovernance(Governance{
		LimitsFor: func() config.AgentConfig {
			return config.AgentConfig{MessageTimeout: time.Minute, MaxConcurrencyPerTenant: 1}
		},
		Audit:   aud,
		Metrics: rec,
	})

	// One message of this tenant is already in flight.
	hold, ok := gw.quota.acquire("demo", 1)
	if !ok {
		t.Fatal("the first slot must be free")
	}
	defer hold()

	ad := newRecordAdapter()
	gw.dispatch(context.Background(), ad, &InboundMessage{
		TenantID: "demo", Channel: TypeWebChat, UserID: "u1", MsgID: "m1", Text: "hello",
	})

	sends := ad.sends()
	if len(sends) != 1 || !sends[0].Done || sends[0].Text != ThrottleText {
		t.Fatalf("sends = %+v, want one done throttle notice", sends)
	}
	if upstream.hits.Load() != 0 {
		t.Fatal("a throttled message must not reach the model")
	}

	recs := lines()
	if len(recs) != 3 {
		t.Fatalf("audit lines = %d, want 3 (inbound + throttled + the reply that carried it): %v", len(recs), recs)
	}
	if recs[0]["event"] != audit.EventInbound {
		t.Fatalf("first audit line = %v, want the arrival recorded before the decision", recs[0])
	}
	th := recs[1]
	if th["event"] != audit.EventThrottled || th["decision"] != audit.DecisionBlock ||
		th["stage"] != stageAdmission || th["rule"] != ruleConcurrency ||
		th["tenant_id"] != "demo" || th["session_id"] != "demo:webchat:u1" {
		t.Fatalf("throttled audit line = %v", th)
	}
	if th["trace_id"] != recs[0]["trace_id"] {
		t.Fatal("the throttle line must share the dispatch trace id")
	}
	if !strings.Contains(th["detail"].(string), "max_concurrency_per_tenant=1") {
		t.Fatalf("detail = %v, want the cap the operator configured", th["detail"])
	}
	// ThrottleText reached the user, so the trail owes a row saying so. This is
	// the admission outcome D7 asserts on: without it a drill can only see the
	// counter and cannot tell a rejection the user read from one that vanished.
	rp := recs[2]
	if rp["event"] != audit.EventReply || rp["decision"] != audit.DecisionBlock ||
		rp["trace_id"] != th["trace_id"] {
		t.Fatalf("reply audit line = %v, want the block decision on the same trace", rp)
	}
	if _, ok := rp["error_type"]; ok {
		t.Fatalf("a delivered throttle notice carries error_type %v, want none: the send worked", rp["error_type"])
	}
	if results := resultsByLabel(t, reader); results[resultThrottled] != 1 || results[resultError] != 0 {
		t.Fatalf("messages by result = %v, want exactly one throttled", results)
	}
}

// TestLimitsAreRereadPerDispatch pins the update mechanism: LimitsFor is
// consulted on every message rather than captured at wiring time, so flipping
// the cap changes the behaviour of the very next dispatch. PUT /admin/settings
// is the HTTP writer this enables; the admin package covers that side.
func TestLimitsAreRereadPerDispatch(t *testing.T) {
	reg, err := agent.NewRegistry(configPointingAt("http://127.0.0.1:1"), inmemory.NewSessionService())
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	var mu sync.Mutex
	lim := config.AgentConfig{MessageTimeout: time.Minute} // 0 concurrency: unlimited
	gw := NewGateway(reg).WithGovernance(Governance{
		PolicyFor: func(string) tenant.Guardrails {
			return tenant.Guardrails{BlockedKeywords: []string{"forbidden"}}
		},
		LimitsFor: func() config.AgentConfig {
			mu.Lock()
			defer mu.Unlock()
			return lim
		},
	})
	setLimit := func(n int) {
		mu.Lock()
		lim.MaxConcurrencyPerTenant = n
		mu.Unlock()
	}

	in := &InboundMessage{TenantID: "demo", Channel: TypeWebChat, UserID: "u1", Text: "say forbidden"}
	lastText := func(ad *recordAdapter) string {
		t.Helper()
		sends := ad.sends()
		if len(sends) == 0 {
			t.Fatal("no reply at all")
		}
		return sends[len(sends)-1].Text
	}

	// One message in flight, but the cap is still "unlimited": admitted, and
	// stopped later by the guardrail rather than by admission.
	hold, ok := gw.quota.acquire("demo", 1)
	if !ok {
		t.Fatal("the first slot must be free")
	}
	defer hold()

	ad := newRecordAdapter()
	gw.dispatch(context.Background(), ad, in)
	if got := lastText(ad); got != guardrail.RejectionText {
		t.Fatalf("unlimited: reply = %q, want the guardrail rejection", got)
	}

	setLimit(1)
	ad = newRecordAdapter()
	gw.dispatch(context.Background(), ad, in)
	if got := lastText(ad); got != ThrottleText {
		t.Fatalf("capped: reply = %q, want %q", got, ThrottleText)
	}

	hold() // the in-flight message finishes, freeing the tenant's only slot
	ad = newRecordAdapter()
	gw.dispatch(context.Background(), ad, in)
	if got := lastText(ad); got != guardrail.RejectionText {
		t.Fatalf("after release: reply = %q, want the guardrail rejection (slot leaked?)", got)
	}
	if n := len(gw.quota.inflt); n != 0 {
		t.Fatalf("inflight tenants = %v, want none: every dispatch must hand its slot back", gw.quota.inflt)
	}
}

// TestThrottleHappensBeforeTheSessionLock pins the ordering of the two gates.
// If admission ran after the lock, an over-cap message would queue behind the
// in-flight one instead of being refused — and since a burst is exactly a pile
// of messages waiting on that lock, the cap would never bind when it matters.
func TestThrottleHappensBeforeTheSessionLock(t *testing.T) {
	reg, err := agent.NewRegistry(configPointingAt("http://127.0.0.1:1"), inmemory.NewSessionService())
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	gw := NewGateway(reg).WithGovernance(Governance{
		LimitsFor: func() config.AgentConfig {
			return config.AgentConfig{MessageTimeout: time.Minute, MaxConcurrencyPerTenant: 1}
		},
	})

	in := &InboundMessage{TenantID: "demo", Channel: TypeWebChat, UserID: "u1", Text: "hello"}
	unlock := gw.serial.lock(in.SessionID()) // an earlier message of the same session ...
	defer unlock()
	release, ok := gw.quota.acquire("demo", 1) // ... holding the tenant's only slot
	if !ok {
		t.Fatal("the first slot must be free")
	}
	defer release()

	done := make(chan struct{})
	go func() {
		defer close(done)
		ad := newRecordAdapter()
		gw.dispatch(context.Background(), ad, in)
		if sends := ad.sends(); len(sends) != 1 || sends[0].Text != ThrottleText {
			t.Errorf("sends = %+v, want the throttle notice", sends)
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatch queued on the session lock instead of refusing at admission")
	}
}

// TestBudgetCoversTheSessionLockWait pins the other ordering decision: the
// deadline is taken before the lock, so queueing counts against the budget.
// Moved after the lock, message_timeout would only start once the wait ended —
// a bound that begins after an unbounded wait is not a bound.
func TestBudgetCoversTheSessionLockWait(t *testing.T) {
	useTracer(t)
	upstream := newStallModel(t, 5*time.Second)
	aud, lines := newAudit(t)
	reg, err := agent.NewRegistry(configPointingAt(upstream.srv.URL), inmemory.NewSessionService())
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	gw := NewGateway(reg).WithGovernance(Governance{
		LimitsFor: func() config.AgentConfig {
			return config.AgentConfig{MessageTimeout: 30 * time.Millisecond}
		},
		Audit: aud,
	})

	in := &InboundMessage{TenantID: "demo", Channel: TypeWebChat, UserID: "u1", Text: "hello"}
	unlock := gw.serial.lock(in.SessionID())

	done := make(chan *recordAdapter)
	go func() {
		ad := newRecordAdapter()
		gw.dispatch(context.Background(), ad, in)
		done <- ad
	}()

	time.Sleep(150 * time.Millisecond) // five budgets spent, still queued on the lock
	unlock()
	ad := <-done

	// The budget went on waiting, so the model must never be reached. Had the
	// deadline been taken after the lock, this dispatch would have gone on to
	// call the upstream with a fresh 30ms of its own.
	if hits := upstream.hits.Load(); hits != 0 {
		t.Fatalf("the model was called %d time(s): the budget did not cover the lock wait", hits)
	}
	if sends := ad.sends(); len(sends) != 1 || sends[0].Text != TimeoutText {
		t.Fatalf("sends = %+v, want one TimeoutText", sends)
	}
	var call map[string]any
	for _, l := range lines() {
		if l["event"] == audit.EventModelCall {
			call = l
		}
	}
	if call == nil || call["error_type"] != errorTypeTimeout {
		t.Fatalf("model_call line = %v, want error_type %q", call, errorTypeTimeout)
	}
}

// TestLimitsDefaults pins the unwired and misconfigured cases: a zero-value
// Governance must reproduce the pre-quota behaviour, and a non-positive timeout
// coming out of LimitsFor must be corrected rather than trusted, because
// WithTimeout with a non-positive duration hands out an already-expired context
// and would fail every single message.
func TestLimitsDefaults(t *testing.T) {
	if got := NewGateway(nil).limits(); got.MessageTimeout != config.DefaultMessageTimeout ||
		got.MaxConcurrencyPerTenant != 0 {
		t.Fatalf("unwired limits = %+v, want the defaults", got)
	}

	gw := NewGateway(nil).WithGovernance(Governance{
		LimitsFor: func() config.AgentConfig { return config.AgentConfig{} },
	})
	if got := gw.limits(); got.MessageTimeout != config.DefaultMessageTimeout {
		t.Fatalf("zero limits = %+v, want the timeout corrected to the default", got)
	}

	gw = NewGateway(nil).WithGovernance(Governance{
		LimitsFor: func() config.AgentConfig {
			return config.AgentConfig{MessageTimeout: 5 * time.Second, MaxConcurrencyPerTenant: 3}
		},
	})
	if got := gw.limits(); got.MessageTimeout != 5*time.Second || got.MaxConcurrencyPerTenant != 3 {
		t.Fatalf("configured limits = %+v, want them passed through verbatim", got)
	}
}

// TestTenantQuota covers the semaphore directly: per-tenant accounting, refusal
// at the cap, an idempotent release, and no residue once every slot is back —
// a leaked count would throttle a tenant forever after one bad message.
func TestTenantQuota(t *testing.T) {
	q := newTenantQuota()

	release, ok := q.acquire("demo", 0)
	if !ok {
		t.Fatal("a zero cap means unlimited")
	}
	release() // must be a no-op, not a panic
	if _, again := q.acquire("demo", -1); !again {
		t.Fatal("a negative cap means unlimited")
	}
	if len(q.inflt) != 0 {
		t.Fatalf("unlimited acquire must not bookkeep, inflt = %v", q.inflt)
	}

	first, ok := q.acquire("demo", 2)
	if !ok {
		t.Fatal("the first slot must be free")
	}
	second, ok := q.acquire("demo", 2)
	if !ok {
		t.Fatal("the second slot must be free")
	}
	if _, ok := q.acquire("demo", 2); ok {
		t.Fatal("the third slot must be refused")
	}
	if _, ok := q.acquire("other", 2); !ok {
		t.Fatal("another tenant must not be throttled by demo's load")
	}

	first()
	first() // idempotent: a double release must not free someone else's slot
	if _, ok := q.acquire("demo", 2); !ok {
		t.Fatal("the slot freed by the first release must be reusable")
	}
	second()
	if q.inflt["demo"] != 1 {
		t.Fatalf("demo inflight = %d, want 1 (a double release leaked a slot)", q.inflt["demo"])
	}
}
