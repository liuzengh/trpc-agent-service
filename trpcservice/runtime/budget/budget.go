// Package budget owns the durable token and monetary budget ledger used by
// Gateway execution. Reservation and settlement are intentionally separate so
// concurrent executions cannot oversubscribe one tenant's monthly limits.
package budget

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

var (
	// ErrInvalid reports malformed budget input.
	ErrInvalid = errors.New("invalid budget")
	// ErrExceeded reports a token or spend limit that cannot admit more usage.
	ErrExceeded = errors.New("budget exceeded")
	// ErrNotFound reports a settlement/release for an unknown reservation.
	ErrNotFound = errors.New("budget reservation not found")
	// ErrConflict reports an idempotency key reused with different data or an
	// invalid state transition.
	ErrConflict = errors.New("budget reservation conflict")
	// ErrUnavailable reports a configured budget without a backing ledger.
	ErrUnavailable = errors.New("budget ledger unavailable")
	// ErrCostUnavailable reports a spend budget without model pricing.
	ErrCostUnavailable = errors.New("model cost unavailable")
)

const (
	// InputCostOption and OutputCostOption are provider-profile option keys.
	// They are deliberately explicit so cost calculation never depends on a
	// provider-specific SDK response shape.
	InputCostOption = "input_cost_minor_per_million"
	// OutputCostOption is the output-token price in minor currency units per million tokens.
	OutputCostOption = "output_cost_minor_per_million"
	// DefaultInputTokensEstimate bounds prompt usage when no provider estimate is available.
	DefaultInputTokensEstimate = int64(1024)
	// DefaultOutputTokensEstimate bounds completion usage when no revision limit is set.
	DefaultOutputTokensEstimate = int64(4096)
	maxReservationIDRunes       = 256
)

// ReservationState is the durable lifecycle of a budget reservation.
type ReservationState string

const (
	// ReservationStateReserved means estimated capacity is held for execution.
	ReservationStateReserved ReservationState = "reserved"
	// ReservationStateSettled means actual usage has been committed.
	ReservationStateSettled ReservationState = "settled"
	// ReservationStateReleased means held capacity was returned without usage.
	ReservationStateReleased ReservationState = "released"
	// ReservationStateDisabled means no tenant budget was configured.
	ReservationStateDisabled ReservationState = "disabled"
)

// Limits is a tenant's fixed monthly budget snapshot.
type Limits struct {
	TokenBudget     *int64
	SpendLimitMinor *int64
	Currency        string
}

// LimitsForTenant projects the validated tenant configuration into a
// defensive budget snapshot.
func LimitsForTenant(value tenant.Tenant) Limits {
	return Limits{
		TokenBudget: cloneInt64(value.MonthlyTokenBudget), SpendLimitMinor: cloneInt64(value.MonthlySpendLimitMinor),
		Currency: value.BillingCurrency,
	}
}

func (limits Limits) enabled() bool {
	return limits.TokenBudget != nil || limits.SpendLimitMinor != nil
}

// Estimate is the conservative amount reserved before model execution.
type Estimate struct {
	InputTokens  int64
	OutputTokens int64
	SpendMinor   int64
}

func (estimate Estimate) validate() error {
	if estimate.InputTokens < 0 || estimate.OutputTokens < 0 || estimate.SpendMinor < 0 || estimate.InputTokens > math.MaxInt64-estimate.OutputTokens {
		return ErrInvalid
	}
	return nil
}

// Tokens returns the total token reservation represented by the estimate.
func (estimate Estimate) Tokens() int64 {
	if estimate.InputTokens > math.MaxInt64-estimate.OutputTokens {
		return math.MaxInt64
	}
	return estimate.InputTokens + estimate.OutputTokens
}

// Usage is the provider-reported usage settled after execution.
type Usage struct {
	InputTokens  int64
	OutputTokens int64
	SpendMinor   int64
}

func (usage Usage) validate() error {
	if usage.InputTokens < 0 || usage.OutputTokens < 0 || usage.SpendMinor < 0 || usage.InputTokens > math.MaxInt64-usage.OutputTokens {
		return ErrInvalid
	}
	return nil
}

// Tokens returns the total token usage represented by the sample.
func (usage Usage) Tokens() int64 {
	if usage.InputTokens > math.MaxInt64-usage.OutputTokens {
		return math.MaxInt64
	}
	return usage.InputTokens + usage.OutputTokens
}

// Reservation is the immutable result of a reserve/settle/release operation.
type Reservation struct {
	TenantID            string
	ReservationID       string
	PeriodStart         time.Time
	Limits              Limits
	EstimatedTokens     int64
	EstimatedSpendMinor int64
	ActualTokens        int64
	ActualSpendMinor    int64
	State               ReservationState
}

// ReserveInput is the store-facing atomic admission request.
type ReserveInput struct {
	TenantID      string
	ReservationID string
	PeriodStart   time.Time
	Limits        Limits
	Estimate      Estimate
}

// Store is the cross-node budget ledger contract. Implementations must make
// Reserve atomic with the limit check and make Settle/Release idempotent.
type Store interface {
	Reserve(context.Context, ReserveInput) (Reservation, error)
	Settle(context.Context, string, string, Usage) (Reservation, error)
	Release(context.Context, string, string) (Reservation, error)
}

// Controller adapts tenant snapshots to the durable Store contract.
type Controller struct {
	store Store
	now   func() time.Time
}

// NewController creates a budget controller. A nil store is valid only when
// every tenant has no budget limit; limited tenants fail closed with
// ErrUnavailable.
func NewController(store Store) *Controller {
	return &Controller{store: store, now: time.Now}
}

// Reserve atomically admits one execution for the tenant's current UTC month.
func (controller *Controller) Reserve(ctx context.Context, value tenant.Tenant, reservationID string, estimate Estimate) (Reservation, error) {
	if ctx == nil {
		return Reservation{}, fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return Reservation{}, err
	}
	if err := validateReservationID(reservationID); err != nil {
		return Reservation{}, err
	}
	if err := estimate.validate(); err != nil {
		return Reservation{}, err
	}
	limits := LimitsForTenant(value)
	if !limits.enabled() {
		return Reservation{TenantID: value.TenantID, ReservationID: reservationID, PeriodStart: monthStart(controller.clock()), State: ReservationStateDisabled}, nil
	}
	if controller == nil || controller.store == nil {
		return Reservation{}, ErrUnavailable
	}
	if err := limits.Validate(); err != nil {
		return Reservation{}, err
	}
	reservation, err := controller.store.Reserve(ctx, ReserveInput{
		TenantID: value.TenantID, ReservationID: reservationID, PeriodStart: monthStart(controller.clock()), Limits: limits, Estimate: estimate,
	})
	if err != nil {
		return Reservation{}, err
	}
	return reservation, nil
}

// Settle commits actual usage and releases the original reservation from the
// in-flight counter. A second identical call is safe and returns the settled
// row from the store.
func (controller *Controller) Settle(ctx context.Context, reservation Reservation, usage Usage) (Reservation, error) {
	if reservation.State == ReservationStateDisabled {
		return reservation, nil
	}
	if controller == nil || controller.store == nil {
		return Reservation{}, ErrUnavailable
	}
	if err := usage.validate(); err != nil {
		return Reservation{}, err
	}
	return controller.store.Settle(ctx, reservation.TenantID, reservation.ReservationID, usage)
}

// Release returns an unused reservation to the monthly available capacity.
func (controller *Controller) Release(ctx context.Context, reservation Reservation) (Reservation, error) {
	if reservation.State == ReservationStateDisabled {
		return reservation, nil
	}
	if controller == nil || controller.store == nil {
		return Reservation{}, ErrUnavailable
	}
	return controller.store.Release(ctx, reservation.TenantID, reservation.ReservationID)
}

func (controller *Controller) clock() time.Time {
	if controller == nil || controller.now == nil {
		return time.Now().UTC()
	}
	return controller.now().UTC()
}

// Validate checks the monetary/token limit snapshot before a store uses it.
// A spend limit always carries a normalized uppercase currency.
func (limits Limits) Validate() error {
	if limits.TokenBudget != nil && *limits.TokenBudget < 0 || limits.SpendLimitMinor != nil && *limits.SpendLimitMinor < 0 {
		return fmt.Errorf("%w: limits cannot be negative", ErrInvalid)
	}
	if limits.SpendLimitMinor != nil && !validCurrency(limits.Currency) {
		return fmt.Errorf("%w: spend limit requires currency", ErrInvalid)
	}
	return nil
}

func validateReservationID(value string) error {
	if value == "" || len([]rune(value)) > maxReservationIDRunes || strings.TrimSpace(value) != value || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return fmt.Errorf("%w: reservation id is invalid", ErrInvalid)
	}
	return nil
}

func monthStart(value time.Time) time.Time {
	value = value.UTC()
	return time.Date(value.Year(), value.Month(), 1, 0, 0, 0, 0, time.UTC)
}

// Pricing is the provider-neutral per-million-token price in minor currency
// units. Both rates are required when a spend budget is enabled.
type Pricing struct {
	InputMinorPerMillion  int64
	OutputMinorPerMillion int64
	Currency              string
	Configured            bool
}

// ParsePricing reads the reserved model-profile options. Missing rates are
// allowed when there is no monetary budget, but a caller can require them with
// RequirePricing.
func ParsePricing(options map[string]string, currency string) (Pricing, error) {
	pricing := Pricing{Currency: strings.TrimSpace(currency)}
	input, hasInput, err := parseRate(options, InputCostOption)
	if err != nil {
		return Pricing{}, err
	}
	output, hasOutput, err := parseRate(options, OutputCostOption)
	if err != nil {
		return Pricing{}, err
	}
	if hasInput != hasOutput {
		return Pricing{}, fmt.Errorf("%w: both input and output rates are required", ErrCostUnavailable)
	}
	if hasInput {
		pricing.InputMinorPerMillion, pricing.OutputMinorPerMillion = input, output
		pricing.Configured = true
		if !validCurrency(pricing.Currency) {
			return Pricing{}, fmt.Errorf("%w: pricing currency is invalid", ErrInvalid)
		}
	}
	return pricing, nil
}

// RequirePricing fails when a monetary limit cannot be settled in a known
// currency and rate.
func (pricing Pricing) RequirePricing() error {
	if !pricing.Configured || pricing.InputMinorPerMillion < 0 || pricing.OutputMinorPerMillion < 0 || !validCurrency(pricing.Currency) {
		return fmt.Errorf("%w: pricing is incomplete", ErrCostUnavailable)
	}
	return nil
}

// Cost computes a rounded-up minor-unit charge for actual model usage.
func (pricing Pricing) Cost(usage Usage) (int64, error) {
	if err := usage.validate(); err != nil {
		return 0, err
	}
	if pricing.InputMinorPerMillion < 0 || pricing.OutputMinorPerMillion < 0 {
		return 0, fmt.Errorf("%w: negative model rate", ErrInvalid)
	}
	input, err := ceilMillionCost(usage.InputTokens, pricing.InputMinorPerMillion)
	if err != nil {
		return 0, err
	}
	output, err := ceilMillionCost(usage.OutputTokens, pricing.OutputMinorPerMillion)
	if err != nil {
		return 0, err
	}
	if input > math.MaxInt64-output {
		return 0, fmt.Errorf("%w: model cost overflow", ErrInvalid)
	}
	return input + output, nil
}

// EstimateExecution creates a bounded conservative reservation for one Agent
// execution. Chain callers pass the total child-call ceiling.
func EstimateExecution(maxLLMCalls, maxOutputTokens int, pricing Pricing) (Estimate, error) {
	if maxLLMCalls < 1 || maxOutputTokens < 1 {
		return Estimate{}, fmt.Errorf("%w: execution limits must be positive", ErrInvalid)
	}
	inputPerCall := DefaultInputTokensEstimate
	if int64(maxLLMCalls) > math.MaxInt64/inputPerCall {
		return Estimate{}, fmt.Errorf("%w: token estimate overflow", ErrInvalid)
	}
	input := int64(maxLLMCalls) * inputPerCall
	outputPerCall := int64(maxOutputTokens)
	if int64(maxLLMCalls) > math.MaxInt64/outputPerCall {
		return Estimate{}, fmt.Errorf("%w: token estimate overflow", ErrInvalid)
	}
	output := int64(maxLLMCalls) * outputPerCall
	spend, err := pricing.Cost(Usage{InputTokens: input, OutputTokens: output})
	if err != nil {
		return Estimate{}, err
	}
	return Estimate{InputTokens: input, OutputTokens: output, SpendMinor: spend}, nil
}

func parseRate(options map[string]string, key string) (int64, bool, error) {
	value, ok := options[key]
	if !ok || strings.TrimSpace(value) == "" {
		return 0, false, nil
	}
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || parsed < 0 {
		return 0, false, fmt.Errorf("%w: %s must be a non-negative integer", ErrInvalid, key)
	}
	return parsed, true, nil
}

func ceilMillionCost(tokens, rate int64) (int64, error) {
	if tokens < 0 || rate < 0 {
		return 0, ErrInvalid
	}
	if tokens == 0 || rate == 0 {
		return 0, nil
	}
	const million = int64(1_000_000)
	whole, remainder := tokens/million, tokens%million
	if whole > math.MaxInt64/rate {
		return 0, fmt.Errorf("%w: model cost overflow", ErrInvalid)
	}
	cost := whole * rate
	if remainder == 0 {
		return cost, nil
	}
	if remainder > math.MaxInt64/rate {
		return 0, fmt.Errorf("%w: model cost overflow", ErrInvalid)
	}
	partial := remainder * rate
	partial, partialRemainder := partial/million, partial%million
	if partialRemainder != 0 {
		if partial == math.MaxInt64 {
			return 0, fmt.Errorf("%w: model cost overflow", ErrInvalid)
		}
		partial++
	}
	if cost > math.MaxInt64-partial {
		return 0, fmt.Errorf("%w: model cost overflow", ErrInvalid)
	}
	return cost + partial, nil
}

func validCurrency(value string) bool {
	if len(value) != 3 {
		return false
	}
	for _, runeValue := range value {
		if runeValue < 'A' || runeValue > 'Z' {
			return false
		}
	}
	return true
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}
