// Package budget contains the durable, tenant-scoped model usage ledger and
// the in-memory implementation used by local/demo deployments.
package budget

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

const CostScale int64 = 100_000_000 // one unit is 1e-8 USD

type State string

const (
	StateReserved State = "reserved"
	StateSettled  State = "settled"
	StateUnknown  State = "unknown"
	StateReleased State = "released"
)

var (
	ErrBudgetExceeded    = errors.New("budget: tenant monthly model budget exceeded")
	ErrLedgerUnavailable = errors.New("budget: persistent ledger unavailable")
	ErrConflict          = errors.New("budget: usage call conflicts with existing record")
	ErrNotFound          = errors.New("budget: usage call not found")
	ErrInvalidRequest    = errors.New("budget: invalid usage request")
)

// ErrorTypeLedgerUnavailable is carried through the model response stream
// when the provider result was received but its durable usage transition
// could not be committed. The worker maps it back to ErrLedgerUnavailable so
// a successful provider response is never acknowledged without accounting.
const ErrorTypeLedgerUnavailable = "budget_ledger_unavailable"

// Ledger is the accounting boundary used by every actual provider call.
// Implementations must make Reserve's monthly limit check and reservation
// insertion one atomic operation, and all state transitions idempotent for a
// given call ID.
type Ledger interface {
	Reserve(context.Context, ReserveRequest) (Reservation, error)
	Settle(context.Context, SettlementRequest) error
	MarkUnknown(context.Context, UnknownRequest) error
	Release(context.Context, string) error
	Close() error
}

type ReserveRequest struct {
	TenantID          string
	AppNamespace      string
	SessionID         string
	DedupKey          string
	RequestID         string
	RunID             string
	CallID            string
	CallNo            int
	ModelName         string
	BillingPeriod     time.Time
	StartedAt         time.Time
	MonthlyLimitUnits int64
	EstimatedUnits    int64
	EstimatedPrompt   int
	EstimatedOutput   int
	// Price snapshots are fixed-point USD units per million tokens. Keeping
	// them on the call row makes the reservation auditable even if tenant
	// configuration changes before a retry or a late settlement.
	InputPricePerMillionUnits  int64
	OutputPricePerMillionUnits int64
}

type Reservation struct {
	TenantID       string
	CallID         string
	BillingPeriod  time.Time
	EstimatedUnits int64
	State          State
	Existing       bool
}

type SettlementRequest struct {
	TenantID         string
	CallID           string
	PromptTokens     int
	CompletionTokens int
	ActualUnits      int64
	SettledAt        time.Time
}

type UnknownRequest struct {
	TenantID  string
	CallID    string
	ErrorType string
	MarkedAt  time.Time
}

type PeriodSnapshot struct {
	TenantID      string
	BillingPeriod time.Time
	LimitUnits    int64
	ReservedUnits int64
	SettledUnits  int64
	UnknownUnits  int64
}

func USDUnits(usd float64) int64 {
	if usd <= 0 || math.IsNaN(usd) || math.IsInf(usd, 0) {
		return 0
	}
	if usd >= float64(math.MaxInt64)/float64(CostScale) {
		return math.MaxInt64
	}
	return int64(math.Round(usd * float64(CostScale)))
}

func UnitsUSD(units int64) float64 {
	return float64(units) / float64(CostScale)
}

func CostUnits(promptTokens, completionTokens int, inputPrice, outputPrice float64) int64 {
	return CostUnitsAtPriceUnits(promptTokens, completionTokens, USDUnits(inputPrice), USDUnits(outputPrice))
}

// CostUnitsAtPriceUnits computes a call charge without floating-point
// accumulation. The price arguments are 1e-8 USD units per million tokens.
func CostUnitsAtPriceUnits(promptTokens, completionTokens int, inputPriceUnits, outputPriceUnits int64) int64 {
	if promptTokens < 0 {
		promptTokens = 0
	}
	if completionTokens < 0 {
		completionTokens = 0
	}
	if inputPriceUnits < 0 {
		inputPriceUnits = 0
	}
	if outputPriceUnits < 0 {
		outputPriceUnits = 0
	}
	total := saturatingAdd(
		saturatingMultiply(inputPriceUnits, promptTokens),
		saturatingMultiply(outputPriceUnits, completionTokens),
	)
	if total == math.MaxInt64 || total > math.MaxInt64-500_000 {
		return math.MaxInt64
	}
	return (total + 500_000) / 1_000_000
}

func saturatingMultiply(value int64, count int) int64 {
	if value <= 0 || count <= 0 {
		return 0
	}
	if value > math.MaxInt64/int64(count) {
		return math.MaxInt64
	}
	return value * int64(count)
}

func saturatingAdd(left, right int64) int64 {
	if left == math.MaxInt64 || right == math.MaxInt64 || left > math.MaxInt64-right {
		return math.MaxInt64
	}
	return left + right
}

func PeriodStart(value time.Time) time.Time {
	if value.IsZero() {
		value = time.Now().UTC()
	}
	value = value.UTC()
	return time.Date(value.Year(), value.Month(), 1, 0, 0, 0, 0, time.UTC)
}

type memoryPeriod struct {
	limit, reserved, settled, unknown int64
	initialized                       bool
}

type memoryCall struct {
	request                        ReserveRequest
	state                          State
	actual                         int64
	promptTokens, completionTokens int
}

type Memory struct {
	mu      sync.Mutex
	periods map[string]memoryPeriod
	calls   map[string]memoryCall
}

func NewMemory() *Memory {
	return &Memory{periods: make(map[string]memoryPeriod), calls: make(map[string]memoryCall)}
}

func periodKey(tenantID string, period time.Time) string {
	return tenantID + "\x1f" + PeriodStart(period).Format("2006-01")
}

func validateReserve(req ReserveRequest) error {
	if req.TenantID == "" || req.CallID == "" || req.RunID == "" || req.CallNo <= 0 {
		return ErrInvalidRequest
	}
	if req.EstimatedUnits < 0 || req.MonthlyLimitUnits < 0 || req.EstimatedPrompt < 0 ||
		req.EstimatedOutput < 0 || req.InputPricePerMillionUnits < 0 ||
		req.OutputPricePerMillionUnits < 0 {
		return ErrInvalidRequest
	}
	return nil
}

func sameReserve(a, b ReserveRequest) bool {
	return a.TenantID == b.TenantID && a.AppNamespace == b.AppNamespace &&
		a.SessionID == b.SessionID && a.DedupKey == b.DedupKey &&
		a.RequestID == b.RequestID && a.RunID == b.RunID && a.CallID == b.CallID &&
		a.CallNo == b.CallNo && a.ModelName == b.ModelName &&
		PeriodStart(a.BillingPeriod).Equal(PeriodStart(b.BillingPeriod)) &&
		a.EstimatedUnits == b.EstimatedUnits &&
		a.EstimatedPrompt == b.EstimatedPrompt && a.EstimatedOutput == b.EstimatedOutput &&
		a.InputPricePerMillionUnits == b.InputPricePerMillionUnits &&
		a.OutputPricePerMillionUnits == b.OutputPricePerMillionUnits
}

func (m *Memory) Reserve(_ context.Context, req ReserveRequest) (Reservation, error) {
	if err := validateReserve(req); err != nil {
		return Reservation{}, err
	}
	period := PeriodStart(req.BillingPeriod)
	key := periodKey(req.TenantID, period)
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.calls[req.CallID]; ok {
		if !sameReserve(existing.request, req) {
			return Reservation{}, ErrConflict
		}
		return Reservation{TenantID: req.TenantID, CallID: req.CallID, BillingPeriod: period,
			EstimatedUnits: existing.request.EstimatedUnits, State: existing.state, Existing: true}, nil
	}
	p := m.periods[key]
	// The tenant configuration is the source of the current monthly limit;
	// keep the period aggregate's limit in sync while holding the ledger lock.
	// Existing settled/unknown usage is never removed when a limit changes.
	p.limit = req.MonthlyLimitUnits
	p.initialized = true
	if p.limit > 0 && exceeds(p.settled+p.unknown+p.reserved, req.EstimatedUnits, p.limit) {
		return Reservation{}, ErrBudgetExceeded
	}
	p.reserved += req.EstimatedUnits
	m.periods[key] = p
	req.BillingPeriod = period
	m.calls[req.CallID] = memoryCall{request: req, state: StateReserved}
	return Reservation{TenantID: req.TenantID, CallID: req.CallID, BillingPeriod: period,
		EstimatedUnits: req.EstimatedUnits, State: StateReserved}, nil
}

func exceeds(used, add, limit int64) bool {
	if add < 0 || used < 0 || limit <= 0 {
		return false
	}
	if used > math.MaxInt64-add {
		return true
	}
	return used+add > limit
}

func (m *Memory) Settle(_ context.Context, req SettlementRequest) error {
	if req.CallID == "" || req.TenantID == "" || req.PromptTokens < 0 || req.CompletionTokens < 0 || req.ActualUnits < 0 {
		return ErrInvalidRequest
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	call, ok := m.calls[req.CallID]
	if !ok || call.request.TenantID != req.TenantID {
		return ErrNotFound
	}
	if call.state == StateSettled {
		if call.actual == req.ActualUnits && call.promptTokens == req.PromptTokens &&
			call.completionTokens == req.CompletionTokens {
			return nil
		}
		return ErrConflict
	}
	if call.state != StateReserved {
		return ErrConflict
	}
	key := periodKey(call.request.TenantID, call.request.BillingPeriod)
	p := m.periods[key]
	p.reserved -= call.request.EstimatedUnits
	if p.reserved < 0 {
		p.reserved = 0
	}
	p.settled += req.ActualUnits
	m.periods[key] = p
	call.state, call.actual = StateSettled, req.ActualUnits
	call.promptTokens, call.completionTokens = req.PromptTokens, req.CompletionTokens
	m.calls[req.CallID] = call
	return nil
}

func (m *Memory) MarkUnknown(_ context.Context, req UnknownRequest) error {
	if req.CallID == "" || req.TenantID == "" {
		return ErrInvalidRequest
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	call, ok := m.calls[req.CallID]
	if !ok || call.request.TenantID != req.TenantID {
		return ErrNotFound
	}
	if call.state == StateUnknown {
		return nil
	}
	if call.state != StateReserved {
		return ErrConflict
	}
	key := periodKey(call.request.TenantID, call.request.BillingPeriod)
	p := m.periods[key]
	p.reserved -= call.request.EstimatedUnits
	if p.reserved < 0 {
		p.reserved = 0
	}
	p.unknown += call.request.EstimatedUnits
	m.periods[key] = p
	call.state = StateUnknown
	m.calls[req.CallID] = call
	return nil
}

func (m *Memory) Release(_ context.Context, callID string) error {
	if callID == "" {
		return ErrInvalidRequest
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	call, ok := m.calls[callID]
	if !ok {
		return ErrNotFound
	}
	if call.state == StateReleased {
		return nil
	}
	if call.state != StateReserved {
		return ErrConflict
	}
	key := periodKey(call.request.TenantID, call.request.BillingPeriod)
	p := m.periods[key]
	p.reserved -= call.request.EstimatedUnits
	if p.reserved < 0 {
		p.reserved = 0
	}
	m.periods[key] = p
	call.state = StateReleased
	m.calls[callID] = call
	return nil
}

func (m *Memory) Snapshot(tenantID string, period time.Time) PeriodSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.periods[periodKey(tenantID, period)]
	return PeriodSnapshot{TenantID: tenantID, BillingPeriod: PeriodStart(period), LimitUnits: p.limit,
		ReservedUnits: p.reserved, SettledUnits: p.settled, UnknownUnits: p.unknown}
}

func (*Memory) Close() error { return nil }

func invalidLedgerError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", ErrLedgerUnavailable, err)
}
