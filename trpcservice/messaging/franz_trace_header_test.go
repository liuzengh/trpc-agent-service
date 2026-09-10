package messaging

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestTraceHeadersRoundTripAndDoNotOverrideEnvelope(t *testing.T) {
	const traceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	headers := traceHeaders(traceParent)
	if got := traceParentFromHeaders(headers); got != traceParent {
		t.Fatalf("traceParentFromHeaders() = %q, want %q", got, traceParent)
	}
	if headers := traceHeaders(""); headers != nil {
		t.Fatalf("traceHeaders(empty) = %#v, want nil", headers)
	}
	if got := traceParentFromHeaders([]kgo.RecordHeader{{Key: "other", Value: []byte("ignored")}}); got != "" {
		t.Fatalf("traceParentFromHeaders(non-trace) = %q, want empty", got)
	}
}
