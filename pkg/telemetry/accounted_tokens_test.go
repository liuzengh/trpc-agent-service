package telemetry

import (
	"testing"

	dto "github.com/prometheus/client_model/go"
)

func TestAccountedTokensDoNotClaimPromptCompletionBreakdown(t *testing.T) {
	label := MetricTenantLabel("token-accounting-test")
	model := MetricModelLabel("token-accounting-test", "model")
	read := func(kind string) float64 {
		var value dto.Metric
		if err := tokenConsumption.WithLabelValues(label, model, kind).Write(&value); err != nil {
			t.Fatal(err)
		}
		return value.GetCounter().GetValue()
	}
	beforeTotal, beforePrompt, beforeCompletion := read("accounted"), read("prompt"), read("completion")
	NewCollectorWithAuditSink(nil).RecordAccountedTokens("token-accounting-test", "model", 30)
	if read("accounted")-beforeTotal != 30 || read("prompt") != beforePrompt || read("completion") != beforeCompletion {
		t.Fatal("ledger amount was lost or attributed to a provider usage breakdown")
	}
}
