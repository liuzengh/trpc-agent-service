// Package modelops accounts for each provider invocation, including background
// summaries and Memory extraction. It deliberately exposes only model.Model:
// an optional iterator fast path must not bypass accounting.
package modelops

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type Options struct {
	TenantID, AppID, Purpose               string
	PromptPerMillion, CompletionPerMillion float64
	MaxPromptTokens, MaxCompletionTokens   int
	Timeout                                time.Duration
}
type Model struct {
	base      model.Model
	guard     *tenant.Guard
	options   Options
	usage     *metrics.Recorder
	operation *metrics.Operation
}

func New(base model.Model, guard *tenant.Guard, opts Options) (*Model, error) {
	if base == nil || guard == nil || opts.TenantID == "" || opts.AppID == "" {
		return nil, errors.New("model accounting scope required")
	}
	if opts.Purpose != "chat" && opts.Purpose != "summary" && opts.Purpose != "memory" {
		return nil, errors.New("invalid model accounting purpose")
	}
	if opts.MaxPromptTokens == 0 {
		opts.MaxPromptTokens = 131072
	}
	if opts.MaxCompletionTokens == 0 {
		opts.MaxCompletionTokens = 4096
	}
	if opts.Timeout == 0 {
		opts.Timeout = 90 * time.Second
	}
	if opts.MaxPromptTokens < 1 || opts.MaxPromptTokens > 1_000_000 || opts.MaxCompletionTokens < 1 || opts.MaxCompletionTokens > 131072 || opts.Timeout < time.Millisecond || opts.Timeout > 5*time.Minute || !priceValid(opts.PromptPerMillion) || !priceValid(opts.CompletionPerMillion) {
		return nil, errors.New("invalid per-call model limits or prices")
	}
	recorder, err := metrics.New()
	if err != nil {
		return nil, err
	}
	return &Model{base: base, guard: guard, options: opts, usage: recorder, operation: metrics.NewOperation("model")}, nil
}
func priceValid(p float64) bool   { return p >= 0 && p <= 1000000 && !math.IsNaN(p) && !math.IsInf(p, 0) }
func (m *Model) Info() model.Info { return m.base.Info() }
func (m *Model) cost(p, c int64) float64 {
	// Round up to nanodollars: never introduce negative accounting adjustments
	// through tiny floating point residues.
	return math.Ceil((float64(p)*m.options.PromptPerMillion+float64(c)*m.options.CompletionPerMillion)*1000) / 1e9
}
func (m *Model) GenerateContent(parent context.Context, request *model.Request) (<-chan *model.Response, error) {
	if request == nil {
		return nil, errors.New("model request required")
	}
	started := time.Now()
	ctx, cancel := context.WithTimeout(parent, m.options.Timeout)
	ctx, availability := ObserveAvailability(ctx)
	copyRequest := *request
	output := m.options.MaxCompletionTokens
	if request.MaxTokens != nil && *request.MaxTokens > 0 && *request.MaxTokens < output {
		output = *request.MaxTokens
	}
	copyRequest.MaxTokens = &output
	// Byte count is deliberately conservative, not a provider tokenizer. Tool
	// schemas are excluded from Request JSON, so include them explicitly.
	encoded, err := json.Marshal(request)
	if err != nil {
		cancel()
		return nil, errors.New("cannot size model request")
	}
	prompt := len(encoded) + 256
	for _, tool := range request.Tools {
		decl, err := json.Marshal(tool.Declaration())
		if err != nil {
			cancel()
			return nil, errors.New("cannot size model tool schema")
		}
		prompt += len(decl) + 64
	}
	if prompt > m.options.MaxPromptTokens {
		cancel()
		return nil, errors.New("model prompt exceeds per-call bound")
	}
	r, err := m.guard.ReserveModel(ctx, m.options.TenantID, int64(prompt), int64(output), m.cost(int64(prompt), int64(output)))
	if err != nil {
		cancel()
		m.finish(parent, started, err)
		return nil, err
	}
	responses, err := m.base.GenerateContent(ctx, &copyRequest)
	if err != nil || responses == nil {
		cancel()
		if err == nil {
			err = errors.New("provider returned no response stream")
		}
		// Sending may have succeeded remotely even when opening the stream fails.
		p, c, cost := r.Prompt, r.Completion, r.Cost
		if availability.DefinitelyNotSent() {
			p, c, cost = 0, 0, 0
		}
		settleErr := m.settle(parent, r, p, c, cost, !availability.DefinitelyNotSent())
		m.finish(parent, started, err)
		return nil, errors.Join(errors.New("model provider call failed"), settleErr)
	}
	out := make(chan *model.Response, 1)
	go func() {
		defer close(out)
		defer cancel()
		var p, c int64
		var haveUsage bool
		var callErr error
	loop:
		for {
			select {
			case <-ctx.Done():
				callErr = context.Cause(ctx)
				break loop
			case response, ok := <-responses:
				if !ok {
					break loop
				}
				if response == nil {
					continue
				}
				if response.Error != nil {
					callErr = errors.New("model response failed")
				}
				if u := response.Usage; u != nil && u.PromptTokens >= 0 && u.CompletionTokens >= 0 && (u.PromptTokens > 0 || u.CompletionTokens > 0 || u.TotalTokens > 0) {
					haveUsage = true
					p = max(p, int64(u.PromptTokens))
					c = max(c, int64(u.CompletionTokens), int64(u.TotalTokens-u.PromptTokens))
				}
				select {
				case out <- response:
				case <-ctx.Done():
					callErr = context.Cause(ctx)
					break loop
				}
			}
		}
		if availability.DefinitelyNotSent() && !haveUsage {
			p, c = 0, 0
		} else if callErr != nil || !haveUsage {
			p = max(p, r.Prompt)
			c = max(c, r.Completion)
		}
		settleErr := m.settle(parent, r, p, c, m.cost(p, c), (callErr != nil || !haveUsage) && !availability.DefinitelyNotSent())
		m.finish(parent, started, errors.Join(callErr, settleErr))
		if settleErr != nil || callErr != nil {
			terminal := &model.Response{Done: true, Error: &model.ResponseError{Type: "model_accounting_error", Message: "model call or usage settlement failed"}}
			// The provider timeout belongs to this wrapper, not the Runner's
			// parent context. Never race the terminal error against an already
			// canceled provider context and accidentally report partial success.
			select {
			case out <- terminal:
			default:
				select {
				case <-out:
				default:
				} // discard at most one unread partial
				out <- terminal // only this goroutine produces; a slot is now free
			}
		}
	}()
	return out, nil
}
func (m *Model) settle(ctx context.Context, r tenant.Reservation, p, c int64, cost float64, estimated bool) error {
	finish, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	first, err := m.guard.SettleModel(finish, r, p, c, cost)
	if first || err != nil {
		m.usage.RecordSettlement(finish, m.options.TenantID, m.options.Purpose, estimated, p > r.Prompt || c > r.Completion || cost > r.Cost, err)
	}
	if err == nil && first {
		m.usage.RecordUsage(finish, m.options.TenantID, int(p), int(c), cost)
	}
	return err
}
func (m *Model) finish(ctx context.Context, started time.Time, err error) {
	m.operation.Finish(ctx, started, m.options.TenantID, m.options.AppID, m.options.Purpose, m.Info().Name, err)
}
