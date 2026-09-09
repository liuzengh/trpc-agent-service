package metrics

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/control"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type metricRepo struct {
	audits  atomic.Int64
	metrics atomic.Int64
}

func (*metricRepo) Ready(context.Context) error                                        { return nil }
func (*metricRepo) InitializeTenants(context.Context, []tenant.Tenant, []string) error { return nil }
func (*metricRepo) GetPolicy(context.Context, string) (control.TenantPolicy, error) {
	return control.TenantPolicy{}, nil
}
func (*metricRepo) PutPolicy(context.Context, control.TenantPolicy, int64) (control.TenantPolicy, error) {
	return control.TenantPolicy{}, nil
}
func (*metricRepo) GetPlacement(context.Context, string) (control.TenantPlacement, error) {
	return control.TenantPlacement{}, nil
}
func (*metricRepo) PutPlacement(context.Context, control.TenantPlacement, int64) (control.TenantPlacement, error) {
	return control.TenantPlacement{}, nil
}
func (*metricRepo) RegisterNode(context.Context, control.NodeRecord) error { return nil }
func (*metricRepo) HeartbeatNode(context.Context, string, string, control.NodeState, int, time.Time) error {
	return nil
}
func (*metricRepo) GetNode(context.Context, string) (control.NodeRecord, error) {
	return control.NodeRecord{}, nil
}
func (*metricRepo) ListNodes(context.Context) ([]control.NodeRecord, error) { return nil, nil }
func (*metricRepo) CreateAssignment(context.Context, control.NodeAssignment) (control.NodeAssignment, bool, error) {
	return control.NodeAssignment{}, false, nil
}
func (*metricRepo) GetAssignment(context.Context, string) (control.NodeAssignment, error) {
	return control.NodeAssignment{}, nil
}
func (*metricRepo) ListAssignments(context.Context) ([]control.NodeAssignment, error) {
	return nil, nil
}
func (*metricRepo) PutAssignment(context.Context, control.NodeAssignment, int64) (control.NodeAssignment, error) {
	return control.NodeAssignment{}, nil
}
func (r *metricRepo) AppendAudit(context.Context, control.AuditRecord) error {
	r.audits.Add(1)
	return nil
}
func (*metricRepo) QueryAudit(context.Context, control.AuditQuery) ([]control.AuditRecord, string, error) {
	return nil, "", nil
}
func (r *metricRepo) AppendMetric(context.Context, control.MetricEvent) error {
	r.metrics.Add(1)
	return nil
}
func (*metricRepo) QueryMetrics(context.Context, control.MetricQuery) ([]control.MetricEvent, string, error) {
	return nil, "", nil
}
func (*metricRepo) SetTenantDegraded(context.Context, string, string) error { return nil }
func (*metricRepo) ClearTenantDegraded(context.Context, string) error       { return nil }
func (*metricRepo) CreateConfirmation(context.Context, control.Confirmation, time.Duration) error {
	return nil
}
func (*metricRepo) ApproveConfirmation(context.Context, string, string, string, string) (control.Confirmation, error) {
	return control.Confirmation{}, nil
}
func (*metricRepo) ConsumeConfirmation(context.Context, control.Confirmation) error { return nil }
func (*metricRepo) AcquireLease(context.Context, string, string, time.Duration) (bool, error) {
	return true, nil
}
func (*metricRepo) RenewLease(context.Context, string, string, time.Duration) (bool, error) {
	return true, nil
}
func (*metricRepo) ReleaseLease(context.Context, string, string) error { return nil }
func (*metricRepo) Close() error                                       { return nil }

func TestAsyncSinksAreFailOpenAndDrainOnClose(t *testing.T) {
	repo := &metricRepo{}
	audit := NewAuditSink(repo, 2)
	metric := NewMetricSink(repo, 2)
	audit.Emit(context.Background(), control.AuditRecord{ID: "audit-a", TenantID: "tenant-a", EventType: "test", Sequence: 0, OccurredAt: time.Now()})
	metric.Record(context.Background(), control.MetricEvent{ID: "metric-a", TenantID: "tenant-a", Name: "requests", Count: 1, OccurredAt: time.Now()})
	_ = audit.Close()
	_ = metric.Close()
	if repo.audits.Load() != 1 || repo.metrics.Load() != 1 {
		t.Fatalf("audit=%d metrics=%d", repo.audits.Load(), repo.metrics.Load())
	}
}
