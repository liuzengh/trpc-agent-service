package budget

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

type usageTestModel struct {
	calls atomic.Int32
}

func (m *usageTestModel) Info() model.Info { return model.Info{Name: "usage-test"} }

func (m *usageTestModel) GenerateContent(context.Context, *model.Request) (<-chan *model.Response, error) {
	m.calls.Add(1)
	responses := make(chan *model.Response, 1)
	responses <- &model.Response{
		Model: "usage-test", Done: true,
		Usage: &model.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
	}
	close(responses)
	return responses, nil
}

func TestTrackedModelReservesAndSettlesEachActualCall(t *testing.T) {
	ledger := NewMemory()
	underlying := &usageTestModel{}
	modelImpl := WrapModel(underlying, ModelSpec{
		TenantID: "tenant-a", AppNamespace: "tenant-a/app", ModelName: "usage-test",
		MaxTokens: 100, InputPrice: 1, OutputPrice: 2, MonthlyLimitUnits: 100000,
	}, ledger)
	run := NewRun(RunMetadata{
		TenantID: "tenant-a", AppNamespace: "tenant-a/app", RequestID: "request",
		RunID: "run-1", MonthlyLimitUnits: 100000, Ledger: ledger,
	})
	ctx := WithRun(context.Background(), run)
	for i := 0; i < 2; i++ {
		responses, err := modelImpl.GenerateContent(ctx, model.NewRequest([]model.Message{model.NewUserMessage("hello")}))
		if err != nil {
			t.Fatal(err)
		}
		for range responses {
		}
	}
	if underlying.calls.Load() != 2 {
		t.Fatalf("provider calls = %d, want 2", underlying.calls.Load())
	}
	summary := run.Summary()
	if summary.RecordedCalls != 2 || summary.PromptTokens != 20 || summary.CompletionTokens != 10 || summary.UnknownCalls != 0 {
		t.Fatalf("usage summary = %+v", summary)
	}
	snapshot := ledger.Snapshot("tenant-a", summaryPeriod())
	if snapshot.SettledUnits != CostUnits(20, 10, 1, 2) || snapshot.ReservedUnits != 0 {
		t.Fatalf("ledger snapshot = %+v", snapshot)
	}
}

func TestTrackedModelMarksMissingUsageUnknownWithoutRefund(t *testing.T) {
	ledger := NewMemory()
	underlying := &usageTestModelNoUsage{}
	modelImpl := WrapModel(underlying, ModelSpec{
		TenantID: "tenant-a", AppNamespace: "tenant-a/app", ModelName: "usage-test",
		MaxTokens: 100, OutputPrice: 1, MonthlyLimitUnits: 100000,
	}, ledger)
	run := NewRun(RunMetadata{
		TenantID: "tenant-a", AppNamespace: "tenant-a/app", RequestID: "request",
		RunID: "run-unknown", MonthlyLimitUnits: 100000, Ledger: ledger,
	})
	responses, err := modelImpl.GenerateContent(WithRun(context.Background(), run), model.NewRequest([]model.Message{model.NewUserMessage("hello")}))
	if err != nil {
		t.Fatal(err)
	}
	for range responses {
	}
	if summary := run.Summary(); summary.UnknownCalls != 1 || summary.UnknownUnits == 0 {
		t.Fatalf("unknown usage summary = %+v", summary)
	}
	snapshot := ledger.Snapshot("tenant-a", summaryPeriod())
	if snapshot.UnknownUnits != run.Summary().UnknownUnits || snapshot.ReservedUnits != 0 {
		t.Fatalf("unknown ledger snapshot = %+v", snapshot)
	}
	later := testReserve("tenant-a", "later", 100000, 100000)
	if _, err := ledger.Reserve(context.Background(), later); err == nil {
		t.Fatal("unknown cost was automatically refunded")
	}
}

func TestTrackedModelDoesNotAckWhenUsageLedgerCannotTransition(t *testing.T) {
	ledger := &usageUnavailableLedger{Memory: NewMemory()}
	underlying := &usageTestModel{}
	modelImpl := WrapModel(underlying, ModelSpec{
		TenantID: "tenant-a", AppNamespace: "tenant-a/app", ModelName: "usage-test",
		MaxTokens: 100, OutputPrice: 1, MonthlyLimitUnits: 100000,
	}, ledger)
	run := NewRun(RunMetadata{
		TenantID: "tenant-a", AppNamespace: "tenant-a/app", RequestID: "request",
		RunID: "run-ledger-down", MonthlyLimitUnits: 100000, Ledger: ledger,
	})
	responses, err := modelImpl.GenerateContent(WithRun(context.Background(), run), model.NewRequest(nil))
	if err != nil {
		t.Fatal(err)
	}
	var got []*model.Response
	for response := range responses {
		got = append(got, response)
	}
	if len(got) != 1 || got[0].Error == nil || got[0].Error.Type != ErrorTypeLedgerUnavailable {
		t.Fatalf("ledger failure response = %+v", got)
	}
	if got[0].Done != true || got[0].Choices != nil {
		t.Fatalf("ledger failure was not terminal: %+v", got[0])
	}
	if snapshot := ledger.Snapshot("tenant-a", time.Now().UTC()); snapshot.ReservedUnits == 0 {
		t.Fatal("ledger failure released a possibly sent provider call")
	}
}

type usageUnavailableLedger struct{ *Memory }

func (*usageUnavailableLedger) Settle(context.Context, SettlementRequest) error {
	return ErrLedgerUnavailable
}

func (*usageUnavailableLedger) MarkUnknown(context.Context, UnknownRequest) error {
	return ErrLedgerUnavailable
}

type usageTestModelNoUsage struct{}

func (*usageTestModelNoUsage) Info() model.Info { return model.Info{Name: "usage-no-usage"} }

func (*usageTestModelNoUsage) GenerateContent(context.Context, *model.Request) (<-chan *model.Response, error) {
	responses := make(chan *model.Response, 1)
	responses <- &model.Response{Model: "usage-no-usage", Done: true}
	close(responses)
	return responses, nil
}

func summaryPeriod() time.Time { return time.Now().UTC() }
