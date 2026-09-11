package budget

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	frameworkagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

const RuntimeStateKey = "trpcservice.budget.usage_run"

type ModelSpec struct {
	TenantID          string
	AppNamespace      string
	SessionID         string
	DedupKey          string
	RequestID         string
	RunID             string
	ModelName         string
	MaxTokens         int
	InputPrice        float64
	OutputPrice       float64
	MonthlyLimitUnits int64
}

type RunMetadata struct {
	TenantID          string
	AppNamespace      string
	SessionID         string
	DedupKey          string
	RequestID         string
	RunID             string
	MonthlyLimitUnits int64
	Ledger            Ledger
	Observer          func(UsageRecord)
}

type UsageRecord struct {
	TenantID         string
	CallID           string
	ModelName        string
	State            State
	EstimatedUnits   int64
	ActualUnits      int64
	PromptTokens     int
	CompletionTokens int
}

type Summary struct {
	PromptTokens     int
	CompletionTokens int
	SettledUnits     int64
	UnknownUnits     int64
	UnknownCalls     int
	RecordedCalls    int
}

type Run struct {
	metadata RunMetadata
	mu       sync.Mutex
	nextCall int
	active   map[string]callHandle
	summary  Summary
}

type callHandle struct {
	request ReserveRequest
	model   string
}

func NewRun(metadata RunMetadata) *Run {
	if metadata.RunID == "" {
		metadata.RunID = uuid.NewString()
	}
	return &Run{metadata: metadata, active: make(map[string]callHandle)}
}

func WithRun(ctx context.Context, run *Run) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, runContextKey{}, run)
}

func RunFromContext(ctx context.Context) (*Run, bool) {
	if ctx == nil {
		return nil, false
	}
	if run, ok := ctx.Value(runContextKey{}).(*Run); ok && run != nil {
		return run, true
	}
	if run, ok := frameworkagent.GetRuntimeStateValueFromContext[*Run](ctx, RuntimeStateKey); ok && run != nil {
		return run, true
	}
	return nil, false
}

type runContextKey struct{}

func NewRunFromSpec(spec ModelSpec, ledger Ledger) *Run {
	return NewRun(RunMetadata{
		TenantID: spec.TenantID, AppNamespace: spec.AppNamespace, SessionID: spec.SessionID,
		DedupKey: spec.DedupKey, RequestID: spec.RequestID, RunID: spec.RunID,
		MonthlyLimitUnits: spec.MonthlyLimitUnits, Ledger: ledger,
	})
}

func (r *Run) reserve(ctx context.Context, spec ModelSpec, request *model.Request) (callHandle, error) {
	if r == nil || r.metadata.Ledger == nil {
		return callHandle{}, ErrLedgerUnavailable
	}
	r.mu.Lock()
	r.nextCall++
	callNo := r.nextCall
	r.mu.Unlock()
	maxTokens := spec.MaxTokens
	if request != nil && request.GenerationConfig.MaxTokens != nil && *request.GenerationConfig.MaxTokens > 0 {
		maxTokens = *request.GenerationConfig.MaxTokens
	}
	if maxTokens < 0 {
		maxTokens = 0
	}
	promptTokens := estimatePromptTokens(request)
	inputPriceUnits := USDUnits(spec.InputPrice)
	outputPriceUnits := USDUnits(spec.OutputPrice)
	estimateUnits := CostUnitsAtPriceUnits(promptTokens, maxTokens, inputPriceUnits, outputPriceUnits)
	callID := r.metadata.RunID + ":" + strconv.Itoa(callNo)
	reserve := ReserveRequest{
		TenantID: r.metadata.TenantID, AppNamespace: r.metadata.AppNamespace,
		SessionID: r.metadata.SessionID, DedupKey: r.metadata.DedupKey,
		RequestID: r.metadata.RequestID, RunID: r.metadata.RunID, CallID: callID,
		CallNo: callNo, ModelName: spec.ModelName, BillingPeriod: time.Now().UTC(),
		StartedAt: time.Now().UTC(), MonthlyLimitUnits: r.metadata.MonthlyLimitUnits,
		EstimatedUnits: estimateUnits, EstimatedPrompt: promptTokens, EstimatedOutput: maxTokens,
		InputPricePerMillionUnits: inputPriceUnits, OutputPricePerMillionUnits: outputPriceUnits,
	}
	reservation, err := r.metadata.Ledger.Reserve(ctx, reserve)
	if err != nil {
		return callHandle{}, err
	}
	handle := callHandle{request: reserve, model: spec.ModelName}
	r.mu.Lock()
	r.active[callID] = handle
	r.mu.Unlock()
	_ = reservation
	return handle, nil
}

func (r *Run) settle(ctx context.Context, handle callHandle, response *model.Response, spec ModelSpec) error {
	if response == nil || response.Error != nil || response.Usage == nil {
		return r.unknown(ctx, handle, responseErrorType(response))
	}
	actual := CostUnitsAtPriceUnits(response.Usage.PromptTokens, response.Usage.CompletionTokens,
		handle.request.InputPricePerMillionUnits, handle.request.OutputPricePerMillionUnits)
	err := r.metadata.Ledger.Settle(ctx, SettlementRequest{
		TenantID: handle.request.TenantID, CallID: handle.request.CallID,
		PromptTokens: response.Usage.PromptTokens, CompletionTokens: response.Usage.CompletionTokens,
		ActualUnits: actual, SettledAt: time.Now().UTC(),
	})
	if err != nil {
		// A provider response exists, so a failed settlement must never release
		// the reservation. Preserve the estimate as unknown when possible.
		unknownErr := r.unknown(ctx, handle, "settlement_failed")
		if unknownErr != nil {
			return fmt.Errorf("settle model usage: %w", err)
		}
		return nil
	}
	r.mu.Lock()
	delete(r.active, handle.request.CallID)
	r.summary.PromptTokens += response.Usage.PromptTokens
	r.summary.CompletionTokens += response.Usage.CompletionTokens
	r.summary.SettledUnits += actual
	r.summary.RecordedCalls++
	r.mu.Unlock()
	r.notify(UsageRecord{TenantID: handle.request.TenantID, CallID: handle.request.CallID,
		ModelName: handle.model, State: StateSettled, EstimatedUnits: handle.request.EstimatedUnits,
		ActualUnits: actual, PromptTokens: response.Usage.PromptTokens,
		CompletionTokens: response.Usage.CompletionTokens})
	return nil
}

func (r *Run) unknown(ctx context.Context, handle callHandle, reason string) error {
	err := r.metadata.Ledger.MarkUnknown(ctx, UnknownRequest{
		TenantID: handle.request.TenantID, CallID: handle.request.CallID,
		ErrorType: reason, MarkedAt: time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	r.mu.Lock()
	delete(r.active, handle.request.CallID)
	r.summary.UnknownUnits += handle.request.EstimatedUnits
	r.summary.UnknownCalls++
	r.summary.RecordedCalls++
	r.mu.Unlock()
	r.notify(UsageRecord{TenantID: handle.request.TenantID, CallID: handle.request.CallID,
		ModelName: handle.model, State: StateUnknown, EstimatedUnits: handle.request.EstimatedUnits})
	return nil
}

func (r *Run) notify(record UsageRecord) {
	if r.metadata.Observer != nil {
		r.metadata.Observer(record)
	}
}

func (r *Run) Summary() Summary {
	if r == nil {
		return Summary{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.summary
}

func estimatePromptTokens(request *model.Request) int {
	if request == nil {
		return 0
	}
	runes := 0
	for _, message := range request.Messages {
		runes += len([]rune(message.Content))
		for _, part := range message.ContentParts {
			if part.Text != nil {
				runes += len([]rune(*part.Text))
			}
		}
	}
	return (runes + 3) / 4
}

func responseErrorType(response *model.Response) string {
	if response == nil || response.Error == nil {
		return "provider_outcome_unknown"
	}
	if response.Error.Type != "" {
		return response.Error.Type
	}
	return "provider_api_error"
}

type trackedModel struct {
	next   model.Model
	spec   ModelSpec
	ledger Ledger
}

// WrapModel instruments one concrete provider model. It intentionally does
// not wrap the outer failover object: each candidate must get its own
// reservation and settlement when failover makes a real second call.
func WrapModel(next model.Model, spec ModelSpec, ledger Ledger) model.Model {
	if next == nil || ledger == nil {
		return next
	}
	return &trackedModel{next: next, spec: spec, ledger: ledger}
}

func (m *trackedModel) Info() model.Info { return m.next.Info() }

func (m *trackedModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	run, ok := RunFromContext(ctx)
	if !ok || run == nil {
		run = NewRunFromSpec(m.spec, m.ledger)
	}
	handle, err := run.reserve(ctx, m.spec, request)
	if err != nil {
		return nil, err
	}
	responses, err := m.next.GenerateContent(ctx, request)
	if err != nil {
		_ = run.unknown(context.Background(), handle, "provider_call_error")
		return nil, err
	}
	if responses == nil {
		_ = run.unknown(context.Background(), handle, "provider_nil_response")
		return nil, errors.New("budget: provider returned nil response channel")
	}
	out := make(chan *model.Response, 1)
	go func() {
		defer close(out)
		var terminal *model.Response
		var usageResponse *model.Response
		for response := range responses {
			if response != nil {
				if response.Usage != nil {
					usageResponse = response
				}
				if response.Done {
					terminal = response
				}
			}
			if response != nil && (response.Done || response.Usage != nil) {
				// Hold the terminal/usage-bearing response until accounting has
				// completed. Intermediate stream chunks can still be forwarded,
				// but a provider result must never look successful before its
				// durable settlement is known.
				continue
			}
			select {
			case out <- response:
			case <-ctx.Done():
				_ = run.unknown(context.Background(), handle, "stream_interrupted")
				return
			}
		}
		final := terminal
		settlementResponse := terminal
		if final == nil {
			final = usageResponse
		}
		if settlementResponse == nil {
			settlementResponse = usageResponse
		}
		if final == nil || settlementResponse == nil {
			_ = run.unknown(context.Background(), handle, "stream_interrupted")
			return
		}
		if err := run.settle(context.Background(), handle, settlementResponse, m.spec); err != nil {
			// The durable reservation remains in place; surface a typed terminal
			// response so the worker retries/fails closed instead of acknowledging
			// a provider result without a committed usage transition.
			ledgerError := &model.Response{Model: final.Model, Done: true, Error: &model.ResponseError{
				Type: ErrorTypeLedgerUnavailable, Message: "model usage ledger unavailable",
			}}
			select {
			case out <- ledgerError:
			case <-ctx.Done():
			}
			return
		}
		select {
		case out <- final:
		case <-ctx.Done():
			return
		}
	}()
	return out, nil
}

// Compile-time assertions keep accidental interface drift visible when the
// upstream model package changes.
var _ model.Model = (*trackedModel)(nil)

func (r Summary) CostUSD() float64 { return UnitsUSD(r.SettledUnits) }

func ValidateRun(run *Run) error {
	if run == nil || run.metadata.Ledger == nil {
		return ErrLedgerUnavailable
	}
	if strings.TrimSpace(run.metadata.TenantID) == "" {
		return ErrInvalidRequest
	}
	return nil
}
