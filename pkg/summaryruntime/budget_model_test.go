package summaryruntime

import (
	"context"
	"errors"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/governance"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type fixedSummaryModel struct {
	responses []*model.Response
	err       error
}

func (m fixedSummaryModel) GenerateContent(context.Context, *model.Request) (<-chan *model.Response, error) {
	if m.err != nil {
		return nil, m.err
	}
	ch := make(chan *model.Response, len(m.responses))
	for _, response := range m.responses {
		ch <- response
	}
	close(ch)
	return ch, nil
}

func (fixedSummaryModel) Info() model.Info { return model.Info{Name: "summary-model"} }

type recordingTokenBudget struct {
	reservation governance.TokenReservation
	reserved    int
	dispatched  int
	settled     []int64
	settleErr   error
}

func (b *recordingTokenBudget) ReserveTokenBudget(context.Context) (governance.TokenReservation, error) {
	b.reserved++
	return b.reservation, nil
}

func (b *recordingTokenBudget) DispatchTokenBudget(context.Context, governance.TokenReservation) error {
	b.dispatched++
	return nil
}

func (b *recordingTokenBudget) SettleTokenBudget(_ context.Context, _ governance.TokenReservation, actual int64) error {
	b.settled = append(b.settled, actual)
	return b.settleErr
}

func TestBudgetedModelSettlesProviderUsage(t *testing.T) {
	budget := &recordingTokenBudget{reservation: governance.TokenReservation{ID: "reservation-1", Reserved: 100}}
	base := fixedSummaryModel{responses: []*model.Response{
		{ID: "response-1", Usage: &model.Usage{TotalTokens: 7}},
	}}
	wrapped, err := NewBudgetedModel(base, budget)
	if err != nil {
		t.Fatal(err)
	}
	responses, err := wrapped.GenerateContent(context.Background(), &model.Request{})
	if err != nil {
		t.Fatal(err)
	}
	var count int
	for range responses {
		count++
	}
	if count != 1 || budget.reserved != 1 || budget.dispatched != 1 || len(budget.settled) != 1 || budget.settled[0] != 7 {
		t.Fatalf("count=%d reserved=%d dispatched=%d settled=%v", count, budget.reserved, budget.dispatched, budget.settled)
	}
}

func TestBudgetedModelChargesReservationWhenUsageIsMissing(t *testing.T) {
	budget := &recordingTokenBudget{reservation: governance.TokenReservation{ID: "reservation-1", Reserved: 100}}
	wrapped, err := NewBudgetedModel(fixedSummaryModel{responses: []*model.Response{{ID: "response-1"}}}, budget)
	if err != nil {
		t.Fatal(err)
	}
	responses, err := wrapped.GenerateContent(context.Background(), &model.Request{})
	if err != nil {
		t.Fatal(err)
	}
	for range responses {
	}
	if len(budget.settled) != 1 || budget.settled[0] != 100 {
		t.Fatalf("settled=%v", budget.settled)
	}
}

func TestBudgetedModelSurfacesSettlementFailureWithoutSecretDetails(t *testing.T) {
	budget := &recordingTokenBudget{
		reservation: governance.TokenReservation{ID: "reservation-1", Reserved: 100},
		settleErr:   errors.New("redis://user:secret@internal should not escape"),
	}
	wrapped, err := NewBudgetedModel(fixedSummaryModel{responses: []*model.Response{{ID: "response-1"}}}, budget)
	if err != nil {
		t.Fatal(err)
	}
	responses, err := wrapped.GenerateContent(context.Background(), &model.Request{})
	if err != nil {
		t.Fatal(err)
	}
	var last *model.Response
	for response := range responses {
		last = response
	}
	if last == nil || last.Error == nil || last.Error.Message != "summary token accounting failed" {
		t.Fatalf("last response=%#v", last)
	}
}
