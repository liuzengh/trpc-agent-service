package telemetry

import (
	"go.opentelemetry.io/otel/attribute"
	"testing"
)

func TestMetricsResourceHasPrivateIndependentInstanceIdentity(t *testing.T) {
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "secret=canary,host.name=private-host")
	a, b := metricsResource("service"), metricsResource("service")
	one, _ := a.Set().Value(attribute.Key("service.instance.id"))
	two, _ := b.Set().Value(attribute.Key("service.instance.id"))
	if one.AsString() == "" || one.AsString() == two.AsString() {
		t.Fatal("nodes would share cumulative series")
	}
	for _, kv := range a.Attributes() {
		switch string(kv.Key) {
		case "service.name", "service.instance.id", "telemetry.sdk.language", "telemetry.sdk.name":
		default:
			t.Fatal("private resource attribute exported")
		}
	}
}
