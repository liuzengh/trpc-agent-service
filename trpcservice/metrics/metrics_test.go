package metrics

import (
	"context"
	"strings"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
)

func TestValidateLabelsRejectsHighCardinality(t *testing.T) {
	if err := ValidateLabels(map[string]string{"component": "gateway", "status": "ok"}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateLabels(map[string]string{"session_id": "sensitive"}); err == nil {
		t.Fatal("session_id must not be a metric label")
	}
	if err := ValidateLabels(map[string]string{"operation": "session-123"}); err == nil {
		t.Fatal("operation values must use the stable allowlist")
	}
	if err := ValidateLabels(map[string]string{"provider": "request-1234567890123456"}); err == nil {
		t.Fatal("provider values must reject high-cardinality identifiers")
	}
}

func TestCatalogNoopAcceptsAllowedLabels(t *testing.T) {
	catalog := New(observability.NewNoopProvider())
	labels := map[string]string{"component": "gateway", "operation": observability.OperationGatewayDispatch, "status": "ok"}
	ctx := context.Background()
	if err := catalog.Request(ctx, labels); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Duration(ctx, 1.2, labels); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Active(ctx, 1, labels); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Lease(ctx, -1, labels); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Retry(ctx, labels); err != nil {
		t.Fatal(err)
	}
	if err := catalog.State(ctx, 1, 0, labels); err != nil {
		t.Fatal(err)
	}
}

func TestCatalogUsageUsesBoundedDimensions(t *testing.T) {
	catalog := New(observability.NewNoopProvider())
	if err := catalog.Usage(context.Background(), audit.UsageTotal{ModelCostMinor: 1}, map[string]string{"component": "model"}); err == nil {
		t.Fatal("cost without currency must be rejected")
	}
	if err := catalog.Cost(context.Background(), 1, map[string]string{"component": "model"}); err == nil {
		t.Fatal("direct cost without currency must be rejected")
	}
	total := audit.UsageTotal{TenantID: "tenant-a", AppID: "app-a", Channel: "telegram", Provider: "openai", Model: "gpt-family", Currency: "USD", ModelCostMinor: 3, ToolCostMinor: 2}
	if err := catalog.Usage(context.Background(), total, map[string]string{"component": "model"}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Usage(context.Background(), total, map[string]string{"session_id": "sensitive"}); err == nil {
		t.Fatal("high-cardinality labels must be rejected")
	}
}

func TestCatalogUsageDoesNotMutateLabelsAndRecordsIssue79Signals(t *testing.T) {
	catalog := New(observability.NewNoopProvider())
	labels := map[string]string{"component": "model"}
	total := audit.UsageTotal{Channel: "telegram", Provider: "openai", Currency: "USD", InputTokens: 4, OutputTokens: 3, ModelCostMinor: 2}
	if err := catalog.Usage(context.Background(), total, labels); err != nil {
		t.Fatal(err)
	}
	if len(labels) != 1 || labels["channel"] != "" || labels["provider"] != "" {
		t.Fatalf("usage mutated labels: %#v", labels)
	}
	if err := catalog.Tokens(context.Background(), 1, map[string]string{"component": "model", "provider": "openai"}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Cost(context.Background(), 1, map[string]string{"component": "model", "provider": "openai", "currency": "usd"}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.BackendDuration(context.Background(), 2, map[string]string{"component": "storage", "status": "success"}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Delivery(context.Background(), map[string]string{"component": "channel", "channel": "telegram", "status": "dead_letter"}); err != nil {
		t.Fatal(err)
	}
}

func TestNormalizeLabelsMapsProviderModelChannelAndCurrency(t *testing.T) {
	labels := NormalizeLabels(map[string]string{"provider": "postgresql", "channel": "WeChat_Work", "model_family": "claude-3", "currency": "CAD"})
	if labels["provider"] != "postgres" || labels["channel"] != "wecom" || labels["model_family"] != "claude" || labels["currency"] != "cad" {
		t.Fatalf("normalized labels = %#v", labels)
	}
	if err := ValidateLabels(labels); err != nil {
		t.Fatal(err)
	}
	for _, code := range []string{"CAD", "AUD"} {
		if got := NormalizeLabels(map[string]string{"currency": code})["currency"]; got != strings.ToLower(code) {
			t.Fatalf("currency %s normalized to %q", code, got)
		}
	}
	if err := ValidateLabels(map[string]string{"currency": "zzz"}); err == nil {
		t.Fatal("unrecognized currency must be rejected")
	}
}

func TestZeroCatalogIsSafe(t *testing.T) {
	var catalog Catalog
	ctx := context.Background()
	if err := catalog.Request(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Duration(ctx, 1, nil); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Tokens(ctx, 1, nil); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Cost(ctx, 1, nil); err != nil {
		t.Fatal(err)
	}
	if err := catalog.BackendDuration(ctx, 1, nil); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Delivery(ctx, nil); err != nil {
		t.Fatal(err)
	}
}

func TestCatalogNilProviderFallsBackToNoop(t *testing.T) {
	if err := New(nil).Request(context.Background(), map[string]string{"component": "gateway", "status": "ok"}); err != nil {
		t.Fatal(err)
	}
}
