package telemetry

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"go.opentelemetry.io/otel/metric/noop"
	sdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	frameworkmetric "trpc.group/trpc-go/trpc-agent-go/telemetry/metric"
)

type metricToolModel struct{ calls atomic.Int64 }

func (*metricToolModel) Info() model.Info { return model.Info{Name: "metric-fixture"} }
func (m *metricToolModel) GenerateContent(_ context.Context, req *model.Request) (<-chan *model.Response, error) {
	reply := model.NewAssistantMessage("secret-result")
	hasTool := false
	for _, m := range req.Messages {
		hasTool = hasTool || m.Role == model.RoleTool
	}
	if !hasTool && m.calls.Add(1) == 1 {
		reply.ToolCalls = []model.ToolCall{{ID: "call", Type: "function", Function: model.FunctionDefinitionParam{Name: "echo", Arguments: []byte(`{"message":"secret-argument"}`)}}}
	}
	ch := make(chan *model.Response, 1)
	ch <- &model.Response{Done: true, Choices: []model.Choice{{Message: reply}}}
	close(ch)
	return ch, nil
}
func TestFrameworkModelAndToolMetricsWithPrivateIDsRemoved(t *testing.T) {
	reader := sdk.NewManualReader()
	provider := sdk.NewMeterProvider(sdk.WithReader(reader), sdk.WithView(frameworkMetricView))
	if err := frameworkmetric.InitMeterProvider(provider); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = frameworkmetric.InitMeterProvider(noop.NewMeterProvider())
		_ = provider.Shutdown(context.Background())
	}()
	catalog := platformtool.DefaultCatalog()
	tools, err := catalog.Resolve([]string{"echo"})
	if err != nil {
		t.Fatal(err)
	}
	r := runner.NewRunner("t/tenant/a/app", llmagent.New("metrics-agent", llmagent.WithModel(&metricToolModel{}), llmagent.WithTools(tools)))
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	events, err := r.Run(ctx, "secret-user", "secret-session", model.NewUserMessage("secret-prompt"))
	if err != nil {
		t.Fatal(err)
	}
	for range events {
	}
	var result metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &result); err != nil {
		t.Fatal(err)
	}
	counts := map[string]uint64{}
	for _, scope := range result.ScopeMetrics {
		for _, m := range scope.Metrics {
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok || m.Name != "gen_ai.client.operation.duration" {
				continue
			}
			for _, point := range hist.DataPoints {
				counts[scope.Scope.Name] += point.Count
				for _, kv := range point.Attributes.ToSlice() {
					if strings.Contains(kv.Value.AsString(), "secret-") {
						t.Fatal("private value in native metric")
					}
				}
			}
		}
	}
	if counts["trpc_agent_go.internal.chat"] < 2 || counts["trpc_agent_go.internal.execute_tool"] != 1 {
		t.Fatalf("framework metric wiring missing: %v", counts)
	}
}
