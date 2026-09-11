package telemetrytrace

import (
	"go.opentelemetry.io/otel/attribute"
	"testing"
)

func TestDeliveryCertaintyFilter(t *testing.T) {
	for _, value := range []string{"ACCEPTED", "NOT_SENT", "UNKNOWN", "REJECTED"} {
		t.Run(value, func(t *testing.T) {
			got := allowedAttributes([]attribute.KeyValue{attribute.String("app.outcome", value)})
			if len(got) != 1 || got[0].Value.AsString() != value {
				t.Fatalf("durable delivery certainty lost: %s", value)
			}
		})
	}
	for _, value := range []attribute.KeyValue{attribute.String("app.outcome", "REJECTED private provider detail"), attribute.Int("app.outcome", 429), attribute.String("provider.response", "private")} {
		if got := allowedAttributes([]attribute.KeyValue{value}); len(got) != 0 {
			t.Fatal("raw provider detail or wrong attribute type retained")
		}
	}
}
