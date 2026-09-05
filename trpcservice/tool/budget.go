package tool

import (
	"context"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type BudgetLimits struct {
	Requests       int64
	InputBytes     int64
	OutputBytes    int64
	Duration       time.Duration
	Concurrent     int64
	ReservationTTL time.Duration
}

type budgetScope struct {
	tenantID  string
	agentID   string
	bindingID string
	sessionID string
	requestID string
	messageID string
}

type budgetState struct {
	requests      int64
	inputBytes    int64
	outputBytes   int64
	duration      time.Duration
	reserved      int64
	reservedInput int64
}

type BudgetManager struct {
	mu           sync.Mutex
	limits       BudgetLimits
	states       map[budgetScope]budgetState
	reservations map[uint64]*BudgetReservation
	nextID       uint64
	now          func() time.Time
}

// BudgetReservation is an opaque server-owned reservation. Its internal ID
// never appears in a GovernanceError, log, metric, or audit record.
type BudgetReservation struct {
	manager   *BudgetManager
	scope     budgetScope
	id        uint64
	inputByte int64
	createdAt time.Time
	done      bool
}

type BudgetSnapshot struct {
	Requests    int64
	InputBytes  int64
	OutputBytes int64
	Duration    time.Duration
	Concurrent  int64
}

func NewBudgetManager(limits BudgetLimits) (*BudgetManager, error) {
	if limits.Requests <= 0 || limits.InputBytes <= 0 || limits.OutputBytes <= 0 || limits.Duration <= 0 || limits.Concurrent <= 0 {
		return nil, safeError(CategoryInvalidInput, "invalid_budget_limits")
	}
	if limits.ReservationTTL <= 0 {
		limits.ReservationTTL = time.Minute
	}
	return &BudgetManager{limits: limits, states: make(map[budgetScope]budgetState), reservations: make(map[uint64]*BudgetReservation), now: func() time.Time { return time.Now().UTC() }}, nil
}

func (m *BudgetManager) Reserve(ctx context.Context, tc tenant.TenantContext, inputBytes int) (*BudgetReservation, error) {
	if m == nil || ctx == nil {
		return nil, safeError(CategoryPolicyUnavailable, "budget_unavailable")
	}
	if err := ctx.Err(); err != nil {
		return nil, safeError(categoryForContext(err), "budget_context")
	}
	if err := tc.Validate(); err != nil || inputBytes < 0 || int64(inputBytes) > m.limits.InputBytes {
		return nil, safeError(CategoryInvalidContext, "invalid_budget_scope")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reclaimLocked(m.now())
	scope := makeBudgetScope(tc)
	state := m.states[scope]
	if state.requests+state.reserved >= m.limits.Requests || state.reserved >= m.limits.Concurrent || state.inputBytes+state.reservedInput+int64(inputBytes) > m.limits.InputBytes {
		return nil, safeError(CategoryBudgetExceeded, "budget_exceeded")
	}
	m.nextID++
	reservation := &BudgetReservation{manager: m, scope: scope, id: m.nextID, inputByte: int64(inputBytes), createdAt: m.now()}
	state.reserved++
	state.reservedInput += int64(inputBytes)
	m.states[scope] = state
	m.reservations[reservation.id] = reservation
	return reservation, nil
}

func (r *BudgetReservation) Commit(outputBytes int, elapsed time.Duration) error {
	if r == nil || r.manager == nil {
		return safeError(CategoryPolicyUnavailable, "budget_unavailable")
	}
	if outputBytes < 0 || elapsed < 0 {
		return safeError(CategoryInvalidInput, "invalid_budget_measurement")
	}
	m := r.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	if r.done || m.reservations[r.id] != r {
		return safeError(CategoryUnknown, "budget_reservation_closed")
	}
	state := m.states[r.scope]
	state.reserved--
	state.reservedInput -= r.inputByte
	state.requests++
	state.inputBytes += r.inputByte
	state.outputBytes += int64(outputBytes)
	state.duration += elapsed
	r.done = true
	delete(m.reservations, r.id)
	m.states[r.scope] = state
	if state.requests > m.limits.Requests || state.inputBytes > m.limits.InputBytes || state.outputBytes > m.limits.OutputBytes || state.duration > m.limits.Duration {
		return safeError(CategoryBudgetExceeded, "budget_exceeded")
	}
	return nil
}

func (r *BudgetReservation) Release() error {
	if r == nil || r.manager == nil {
		return safeError(CategoryPolicyUnavailable, "budget_unavailable")
	}
	m := r.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	if r.done {
		return nil
	}
	if m.reservations[r.id] != r {
		return safeError(CategoryUnknown, "budget_reservation_closed")
	}
	state := m.states[r.scope]
	state.reserved--
	state.reservedInput -= r.inputByte
	r.done = true
	delete(m.reservations, r.id)
	m.states[r.scope] = state
	return nil
}

func (m *BudgetManager) Snapshot(ctx context.Context, tc tenant.TenantContext) (BudgetSnapshot, error) {
	if m == nil || ctx == nil {
		return BudgetSnapshot{}, safeError(CategoryPolicyUnavailable, "budget_unavailable")
	}
	if err := ctx.Err(); err != nil {
		return BudgetSnapshot{}, safeError(categoryForContext(err), "budget_context")
	}
	if err := tc.Validate(); err != nil {
		return BudgetSnapshot{}, safeError(CategoryInvalidContext, "invalid_budget_scope")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.states[makeBudgetScope(tc)]
	return BudgetSnapshot{Requests: state.requests, InputBytes: state.inputBytes, OutputBytes: state.outputBytes, Duration: state.duration, Concurrent: state.reserved}, nil
}

// Reclaim releases reservations whose bounded TTL elapsed. It is safe to call
// during shutdown or cancellation and never accepts a caller-supplied owner.
func (m *BudgetManager) Reclaim(now time.Time) int {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.reclaimLocked(now)
}

func (m *BudgetManager) reclaimLocked(now time.Time) int {
	if now.IsZero() {
		now = m.now()
	}
	removed := 0
	for id, reservation := range m.reservations {
		if now.Sub(reservation.createdAt) < m.limits.ReservationTTL {
			continue
		}
		state := m.states[reservation.scope]
		state.reserved--
		state.reservedInput -= reservation.inputByte
		m.states[reservation.scope] = state
		reservation.done = true
		delete(m.reservations, id)
		removed++
	}
	return removed
}

func makeBudgetScope(tc tenant.TenantContext) budgetScope {
	return budgetScope{tenantID: tc.TenantID, agentID: tc.AgentAppID, bindingID: tc.BindingID, sessionID: tc.SessionID, requestID: tc.RequestID, messageID: tc.MessageID}
}
