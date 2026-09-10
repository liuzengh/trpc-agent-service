package telemetry_test

import (
	"context"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

func TestNoopProviderIsConcurrentAndPreservesContext(t *testing.T) {
	provider := telemetry.Noop()
	if telemetry.Enabled(provider) {
		t.Fatal("no-op provider reported enabled")
	}
	key := struct{}{}
	ctx := context.WithValue(context.Background(), key, "kept")
	returned, span := provider.StartSpan(ctx, telemetry.OperationAuditExport,
		telemetry.ComponentAttribute(telemetry.ComponentAuditRelay))
	if returned.Value(key) != "kept" || span == nil {
		t.Fatal("no-op provider did not preserve context")
	}
	var group sync.WaitGroup
	for index := 0; index < 100; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			span.End(nil)
			provider.Counter(telemetry.MetricAuditExportTotal).Add(ctx, 1)
			provider.Histogram(telemetry.MetricAuditExportDuration).Record(ctx, 0.1)
			provider.Logger(telemetry.ComponentAuditRelay).Info(ctx, telemetry.LogAuditRelayDegraded)
		}()
	}
	group.Wait()
	if err := provider.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := provider.Shutdown(ctx); err != nil {
		t.Fatal("shutdown is not idempotent:", err)
	}
}
