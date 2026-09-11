package tracecontext

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"
)

const parent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"

func TestCarrierValidationAndHeaders(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		headers               map[string][]string
		wantParent, wantState string
	}{
		{"valid", map[string][]string{"TraceParent": {parent}, "TRACEState": {"vendor=value"}}, parent, "vendor=value"},
		{"unsampled", map[string][]string{"traceparent": {strings.TrimSuffix(parent, "01") + "00"}}, strings.TrimSuffix(parent, "01") + "00", ""},
		{"absent", nil, "", ""},
		{"duplicate", map[string][]string{"traceparent": {parent}, "Traceparent": {parent}}, "", ""},
		{"invalid-id", map[string][]string{"traceparent": {strings.Replace(parent, "0123456789abcdef0123456789abcdef", strings.Repeat("0", 32), 1)}}, "", ""},
		{"oversized", map[string][]string{"traceparent": {strings.Repeat("x", 513)}}, "", ""},
		{"oversized-state", map[string][]string{"traceparent": {parent}, "tracestate": {"a=" + strings.Repeat("x", 513)}}, parent, ""},
		{"invalid-state", map[string][]string{"traceparent": {parent}, "tracestate": {"not a valid state"}}, parent, ""},
		{"crlf", map[string][]string{"traceparent": {parent + "\r\nSECRET"}}, "", ""},
		{"state-list", map[string][]string{"traceparent": {parent}, "tracestate": {"a=one", "b=two"}}, parent, "a=one,b=two"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := FromHeaders(tc.headers)
			if c.Traceparent != tc.wantParent || c.Tracestate != tc.wantState {
				t.Fatalf("%+v", c)
			}
			h := map[string][]string{"Nats-Msg-Id": {"event-1"}, "Traceparent": {"old"}, "TraceState": {"old"}}
			c.Inject(h)
			if h["Nats-Msg-Id"][0] != "event-1" || FromHeaders(h) != c {
				t.Fatalf("%v", h)
			}
		})
	}
	var state []string
	for i := 0; i < 33; i++ {
		state = append(state, string(rune('a'+i%26))+string(rune('a'+i/26))+"=x")
	}
	if c := (Carrier{parent, strings.Join(state, ",")}).Normalize(); c.Traceparent != parent || c.Tracestate != "" {
		t.Fatal(c)
	}
}

func TestRestorePreservesTaskLifetimeAndUnsampledContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	restored := (Carrier{parent, "vendor=value"}).Restore(ctx)
	if Capture(restored) != (Carrier{parent, "vendor=value"}) {
		t.Fatal(Capture(restored))
	}
	if !trace.SpanContextFromContext(restored).IsRemote() {
		t.Fatal("expected remote context")
	}
	want, _ := ctx.Deadline()
	got, _ := restored.Deadline()
	if got != want {
		t.Fatal("deadline replaced")
	}
	cancel()
	if restored.Err() != context.Canceled {
		t.Fatal("lost cancellation")
	}
	if (Carrier{}).Restore(ctx) != ctx {
		t.Fatal("invalid carrier replaced task context")
	}
}

func FuzzCarrier(f *testing.F) {
	f.Add(parent, "vendor=value")
	f.Add("", "")
	f.Add("bad", "secret\r\n")
	f.Fuzz(func(t *testing.T, p, s string) {
		c := (Carrier{p, s}).Normalize()
		if len(c.Traceparent) > MaxFieldBytes || len(c.Tracestate) > MaxFieldBytes || c.Normalize() != c {
			t.Fatal("noncanonical")
		}
		if c.Traceparent != "" && !trace.SpanContextFromContext(c.Restore(context.Background())).IsValid() {
			t.Fatal("invalid restored context")
		}
	})
}
