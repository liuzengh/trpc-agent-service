package telemetry

import (
	"strings"

	"go.opentelemetry.io/otel/attribute"
	sdk "go.opentelemetry.io/otel/sdk/metric"
)

// Native framework metrics include user/session IDs by default. Keep only
// routing/model/tool dimensions, never prompts, arguments or raw errors.
func frameworkMetricView(instrument sdk.Instrument) (sdk.Stream, bool) {
	if !strings.HasPrefix(instrument.Scope.Name, "trpc_agent_go.") {
		return sdk.Stream{}, false
	}
	return sdk.Stream{Name: instrument.Name, AttributeFilter: func(kv attribute.KeyValue) bool {
		switch string(kv.Key) {
		case "trpc_go_agent.app.name", "gen_ai.operation.name", "gen_ai.system", "gen_ai.request.model", "gen_ai.response.model", "gen_ai.provider.name", "gen_ai.agent.name", "gen_ai.tool.name", "gen_ai.token.type":
			return true
		default:
			return false
		}
	}}, true
}
