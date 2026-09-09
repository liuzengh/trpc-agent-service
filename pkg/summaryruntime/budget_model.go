// Package summaryruntime composes durable Summary jobs with tenant-routed
// Session storage, immutable Agent versions and governed model execution.
package summaryruntime

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/governance"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type tokenBudget interface {
	ReserveTokenBudget(context.Context) (governance.TokenReservation, error)
	DispatchTokenBudget(context.Context, governance.TokenReservation) error
	SettleTokenBudget(context.Context, governance.TokenReservation, int64) error
}

type budgetedModel struct {
	base   model.Model
	budget tokenBudget
}

// NewBudgetedModel reserves and settles the same tenant token ledger used by
// foreground Agent execution. Missing provider usage is charged at the full
// reservation so auxiliary summaries cannot bypass the hard daily limit.
func NewBudgetedModel(base model.Model, budget tokenBudget) (model.Model, error) {
	if nilValue(base) || nilValue(budget) {
		return nil, fmt.Errorf("summary model and token budget are required")
	}
	return &budgetedModel{base: base, budget: budget}, nil
}

func (m *budgetedModel) Info() model.Info { return m.base.Info() }

func (m *budgetedModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	reservation, err := m.budget.ReserveTokenBudget(ctx)
	if err != nil {
		return nil, fmt.Errorf("reserve summary token budget: %w", err)
	}
	if reservation.ID == "" && reservation.Reserved == 0 {
		return m.base.GenerateContent(ctx, request)
	}
	if err := m.budget.DispatchTokenBudget(ctx, reservation); err != nil {
		return nil, fmt.Errorf("dispatch summary token budget: %w", err)
	}
	input, err := m.base.GenerateContent(ctx, request)
	if err != nil {
		if settleErr := m.settle(ctx, reservation, 0); settleErr != nil {
			return nil, errors.Join(err, errors.New("summary token accounting failed"))
		}
		return nil, err
	}
	if input == nil {
		if settleErr := m.settle(ctx, reservation, reservation.Reserved); settleErr != nil {
			return nil, errors.New("summary token accounting failed")
		}
		return nil, fmt.Errorf("summary model returned a nil response stream")
	}
	out := make(chan *model.Response, 1)
	go m.relay(ctx, reservation, input, out)
	return out, nil
}

func (m *budgetedModel) relay(
	ctx context.Context,
	reservation governance.TokenReservation,
	input <-chan *model.Response,
	out chan<- *model.Response,
) {
	defer close(out)
	usageByResponse := make(map[string]int64)
	usageReliable := true
	sawPositiveUsage := false
	forward := true
	for response := range input {
		if response != nil && response.Usage != nil {
			tokens := response.Usage.TotalTokens
			if tokens <= 0 && response.Usage.PromptTokens >= 0 && response.Usage.CompletionTokens >= 0 {
				tokens = response.Usage.PromptTokens + response.Usage.CompletionTokens
			}
			if tokens < 0 || response.ID == "" {
				usageReliable = false
			} else if tokens > 0 {
				sawPositiveUsage = true
				value := int64(tokens)
				if value > usageByResponse[response.ID] {
					usageByResponse[response.ID] = value
				}
			}
		}
		if forward {
			select {
			case out <- response:
			case <-ctx.Done():
				forward = false
			}
		}
	}
	actual := reservation.Reserved
	if usageReliable && sawPositiveUsage {
		actual = 0
		for _, value := range usageByResponse {
			if value > math.MaxInt64-actual {
				actual = reservation.Reserved
				usageReliable = false
				break
			}
			actual += value
		}
	}
	if err := m.settle(ctx, reservation, actual); err != nil && forward {
		failure := &model.Response{
			Done: true,
			Error: &model.ResponseError{
				Type:    "summary_accounting_error",
				Message: "summary token accounting failed",
			},
		}
		select {
		case out <- failure:
		case <-ctx.Done():
		}
	}
}

func (m *budgetedModel) settle(ctx context.Context, reservation governance.TokenReservation, actual int64) error {
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return m.budget.SettleTokenBudget(settleCtx, reservation, actual)
}

func nilValue(value any) bool {
	if value == nil {
		return true
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}
