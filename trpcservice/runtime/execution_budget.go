package runtime

import (
	"context"
	"fmt"
	"sync"
)

// ExecutionBudget is a durable execution limit. Zero means that a limit is
// unset. It is copied into the ExecutionEnvelope at dispatch time so a queued
// run never observes a later revision edit or rollback.
type ExecutionBudget struct {
	MaxLLMCalls             int `json:"max_llm_calls,omitempty"`
	MaxToolCalls            int `json:"max_tool_calls,omitempty"`
	MaxParallelTools        int `json:"max_parallel_tools,omitempty"`
	ExecutionTimeoutSeconds int `json:"execution_timeout_seconds,omitempty"`
}

func (b ExecutionBudget) Validate() error {
	if b.MaxLLMCalls < 0 || b.MaxToolCalls < 0 || b.MaxParallelTools < 0 || b.ExecutionTimeoutSeconds < 0 ||
		b.MaxLLMCalls > 10000 || b.MaxToolCalls > 10000 || b.MaxParallelTools > 1000 || b.ExecutionTimeoutSeconds > 86400 {
		return ErrInvalidEnvelope
	}
	return nil
}

// BudgetExceededError identifies the circuit breaker which rejected work.
// It intentionally contains no request or provider data, so it is safe to
// use as an audit reason code.
type BudgetExceededError struct{ Limit string }

func (e *BudgetExceededError) Error() string        { return "execution budget exceeded: " + e.Limit }
func (e *BudgetExceededError) Is(target error) bool { return target == ErrExecutionBudgetExceeded }

type executionBudgetKey struct{}

type budgetController struct {
	budget ExecutionBudget
	mu     sync.Mutex
	llm    int
	tools  int
	active int
	breach error
}

// WithExecutionBudget attaches a per-run, in-memory circuit breaker. It is
// deliberately not a ledger: its counters die with the execution attempt.
func WithExecutionBudget(ctx context.Context, budget ExecutionBudget) context.Context {
	return context.WithValue(ctx, executionBudgetKey{}, &budgetController{budget: budget})
}

func ConsumeLLMCall(ctx context.Context) error {
	controller := executionBudgetFromContext(ctx)
	if controller == nil {
		return nil
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.budget.MaxLLMCalls > 0 && controller.llm >= controller.budget.MaxLLMCalls {
		return controller.reject("max_llm_calls")
	}
	controller.llm++
	return nil
}

// BeginToolCall reserves one local call slot and returns an idempotent release
// function for its parallelism slot. A rejected call is not counted.
func BeginToolCall(ctx context.Context) (func(), error) {
	controller := executionBudgetFromContext(ctx)
	if controller == nil {
		return func() {}, nil
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.budget.MaxToolCalls > 0 && controller.tools >= controller.budget.MaxToolCalls {
		return nil, controller.reject("max_tool_calls")
	}
	if controller.budget.MaxParallelTools > 0 && controller.active >= controller.budget.MaxParallelTools {
		return nil, controller.reject("max_parallel_tools")
	}
	controller.tools++
	controller.active++
	var once sync.Once
	return func() {
		once.Do(func() {
			controller.mu.Lock()
			controller.active--
			controller.mu.Unlock()
		})
	}, nil
}

func ExecutionBudgetViolation(ctx context.Context) error {
	controller := executionBudgetFromContext(ctx)
	if controller == nil {
		return nil
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return controller.breach
}

func ExecutionBudgetReason(err error) string {
	if value, ok := err.(*BudgetExceededError); ok {
		return value.Limit
	}
	return "execution_timeout_seconds"
}

func executionBudgetFromContext(ctx context.Context) *budgetController {
	if ctx == nil {
		return nil
	}
	controller, _ := ctx.Value(executionBudgetKey{}).(*budgetController)
	return controller
}

func (c *budgetController) reject(limit string) error {
	if c.breach == nil {
		c.breach = &BudgetExceededError{Limit: limit}
	}
	return c.breach
}

var _ error = (*BudgetExceededError)(nil)

func (b ExecutionBudget) String() string {
	return fmt.Sprintf("llm=%d tools=%d parallel_tools=%d timeout_seconds=%d", b.MaxLLMCalls, b.MaxToolCalls, b.MaxParallelTools, b.ExecutionTimeoutSeconds)
}
