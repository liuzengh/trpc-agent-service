package governance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

// BudgetTracker tracks token and cost usage using Redis as the backend store.
type BudgetTracker struct {
	redis                    *redis.Client
	tenant                   *tenant.Tenant
	sessionSlotTTL           time.Duration
	sessionSlotRenewInterval time.Duration
}

const (
	tokenReservationLease  = 10 * time.Minute
	budgetDataRetention    = 48 * time.Hour
	defaultSessionSlotTTL  = 5 * time.Minute
	sessionSlotRenewalPart = 3
	budgetOperationTimeout = 5 * time.Second
)

const maxBudgetExactInteger = int64(1<<53 - 1)

var (
	// ErrBudgetSettlementConflict means a reservation was already completed
	// with a different outcome. Callers must not retry the model invocation.
	ErrBudgetSettlementConflict = errors.New("token budget reservation already completed with a different outcome")
	// ErrBudgetReservationExceeded means provider-reported usage exceeded the
	// amount reserved. The full reported usage has already been recorded.
	ErrBudgetReservationExceeded = errors.New("provider token usage exceeded the reservation")
	// ErrBudgetReservationInvalid means the reservation is unknown or does not
	// belong to the supplied tracker. Treat this as a fail-closed accounting error.
	ErrBudgetReservationInvalid = errors.New("token budget reservation is invalid")
	// ErrSessionSlotLost means a tenant concurrency lease expired, was removed,
	// or could not be renewed. The associated invocation must be cancelled.
	ErrSessionSlotLost = errors.New("tenant concurrency slot no longer held")
)

// SessionSlot is a self-renewing tenant concurrency lease. Done closes when
// the lease is released or ownership is lost. Callers must treat a non-nil Err
// as terminal and must not return a successful invocation result.
type SessionSlot interface {
	Done() <-chan struct{}
	Err() error
	Release(context.Context) error
}

type sessionSlotLease struct {
	tracker *BudgetTracker
	token   string
	ttl     time.Duration
	cancel  context.CancelFunc
	done    chan struct{}

	mu      sync.Mutex
	lostErr error
}

// TokenReservation is an opaque claim on a tenant's daily token budget.
// ExpiresAt is expressed in the Redis server's wall clock and is therefore
// useful for audit only. The process-local execution deadline is deliberately
// separate: it is never used as authorization, which always remains Redis's
// responsibility at DispatchTokenBudget.
type TokenReservation struct {
	ID            string
	Reserved      int64
	ExpiresAt     time.Time
	day           string
	localDeadline time.Time
}

// ExecutionDeadline returns a conservative, process-local deadline captured
// before the successful Redis reservation call. It uses Go's monotonic clock
// when available and must not be treated as an authorization expiry. A zero
// result means the reservation came from a legacy/custom BudgetController.
func (r TokenReservation) ExecutionDeadline() (time.Time, bool) {
	return r.localDeadline, !r.localDeadline.IsZero()
}

func budgetDay(now time.Time) string {
	return now.UTC().Format("2006-01-02")
}

func budgetDayBounds(now time.Time) (day string, start, end time.Time) {
	now = now.UTC()
	start = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	return budgetDay(now), start, start.AddDate(0, 0, 1)
}

func parseReservationResult(value interface{}) (int64, time.Time, error) {
	values, ok := value.([]interface{})
	if !ok || len(values) != 2 {
		return 0, time.Time{}, fmt.Errorf("unexpected Redis reservation result")
	}
	status, err := redisResultInt64(values[0])
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("invalid Redis reservation status: %w", err)
	}
	milliseconds, err := redisResultInt64(values[1])
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("invalid Redis reservation time: %w", err)
	}
	return status, time.UnixMilli(milliseconds).UTC(), nil
}

func redisResultInt64(value interface{}) (int64, error) {
	switch value := value.(type) {
	case int64:
		return value, nil
	case int:
		return int64(value), nil
	case string:
		return strconv.ParseInt(value, 10, 64)
	case []byte:
		return strconv.ParseInt(string(value), 10, 64)
	default:
		return 0, fmt.Errorf("unexpected type %T", value)
	}
}

// redisTime is the sole production authority for a tenant's daily budget
// partition. A local process clock may be skewed or deliberately adjusted;
// using it would let separate Worker replicas reserve against different days.
func (b *BudgetTracker) redisTime(ctx context.Context) (time.Time, error) {
	if b == nil || b.redis == nil {
		return time.Time{}, fmt.Errorf("budget store unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	now, err := b.redis.Time(ctx).Result()
	if err != nil {
		return time.Time{}, fmt.Errorf("read Redis budget clock: %w", err)
	}
	return now.UTC(), nil
}

func (b *BudgetTracker) tokenBudgetKeys(day string) []string {
	prefix := b.budgetKeyPrefix() + ":" + day
	return []string{
		b.dailyTokenKey(day),
		prefix + ":token-reservations:active",
		prefix + ":token-reservations:amounts",
		prefix + ":token-reservations:pending",
		prefix + ":token-reservations:states",
	}
}

func (b *BudgetTracker) budgetKeyPrefix() string {
	digest := sha256.Sum256([]byte(b.tenant.ID))
	return "budget:{" + hex.EncodeToString(digest[:]) + "}"
}

func (b *BudgetTracker) dailyTokenKey(day string) string {
	return b.budgetKeyPrefix() + ":" + day + ":tokens"
}

func (b *BudgetTracker) dailyCostKey(day string) string {
	return b.budgetKeyPrefix() + ":" + day + ":cost"
}

const reserveTokenBudgetScript = `
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local day_start = tonumber(ARGV[1])
local day_end = tonumber(ARGV[2])
if now < day_start or now >= day_end then return {-4, now} end

local lease = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])
local budget = tonumber(ARGV[5])
local reservation_id = ARGV[6]
local retention = tonumber(ARGV[7])
if not lease or lease <= 0 or not requested or requested <= 0 or
   not budget or budget <= 0 or not retention or retention <= 0 then
  return {-2, now}
end

local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now)
local reclaimed = 0

-- Validate every touched record before changing any state. Redis scripts are
-- atomic, but returning after an earlier write would still commit that write.
for _, id in ipairs(expired) do
  local amount = redis.call('HGET', KEYS[3], id)
  local state = redis.call('HGET', KEYS[5], id)
  local numeric_amount = tonumber(amount)
  if not amount or not state or not numeric_amount or numeric_amount <= 0 then return {-2, now} end
  if state == 'reserved' then
    reclaimed = reclaimed + numeric_amount
  elseif state == 'dispatched' then
    -- Its provider outcome is unknown, so it remains charged against pending.
  elseif state ~= 'uncertain' then
    return {-2, now}
  end
end
local pending = tonumber(redis.call('GET', KEYS[4]) or '0')
if not pending or pending < 0 or reclaimed > pending then return {-2, now} end
pending = pending - reclaimed
if redis.call('HEXISTS', KEYS[3], reservation_id) == 1 then return {-2, now} end
local used = tonumber(redis.call('GET', KEYS[1]) or '0')
if not used or used < 0 then return {-2, now} end

-- All validation succeeded. Apply expiration reconciliation as one coherent
-- state transition before deciding whether capacity remains for a new claim.
for _, id in ipairs(expired) do
  local state = redis.call('HGET', KEYS[5], id)
  if state == 'reserved' then
    redis.call('HSET', KEYS[5], id, 'released:expired')
  elseif state == 'dispatched' then
    redis.call('HSET', KEYS[5], id, 'uncertain')
  end
end
if #expired > 0 then redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now) end
if pending == 0 then redis.call('DEL', KEYS[4]) else redis.call('SET', KEYS[4], pending) end

if requested > budget or used > budget - requested or pending > budget - requested - used then
  return {0, now}
end
pending = pending + requested
redis.call('HSET', KEYS[3], reservation_id, requested)
redis.call('HSET', KEYS[5], reservation_id, 'reserved')
redis.call('ZADD', KEYS[2], now + lease, reservation_id)
redis.call('SET', KEYS[4], pending)
for i = 1, 5 do redis.call('PEXPIRE', KEYS[i], retention) end
return {1, now}`

// ReserveTokenBudget atomically reserves the tenant's configured per-request
// ceiling. Only expired reservations which were never dispatched to a model
// are reclaimed. Dispatched work becomes uncertain and keeps its hold until a
// settlement arrives or the UTC daily ledger expires.
func (b *BudgetTracker) ReserveTokenBudget(ctx context.Context) (TokenReservation, error) {
	if b == nil || b.tenant == nil {
		return TokenReservation{}, fmt.Errorf("budget tracker is not configured")
	}
	requested := b.tenant.Budget.MaxTokensPerRequest
	limit := b.tenant.Budget.MaxTokensPerDay
	if requested == 0 && limit == 0 {
		return TokenReservation{}, nil
	}
	if requested <= 0 || limit <= 0 || requested > limit ||
		requested > maxBudgetExactInteger || limit > maxBudgetExactInteger {
		return TokenReservation{}, fmt.Errorf("hard token budget is not safely configured")
	}
	if b.redis == nil {
		return TokenReservation{}, fmt.Errorf("budget store unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// Redis TIME is read once to select the daily hash-tagged key set. The Lua
	// script reads it again and returns -4 when midnight passed in between; that
	// retry prevents a reservation being committed to the wrong UTC day.
	for attempt := 0; attempt < 2; attempt++ {
		now, err := b.redisTime(ctx)
		if err != nil {
			return TokenReservation{}, err
		}
		day, dayStart, dayEnd := budgetDayBounds(now)
		id := uuid.NewString()
		localStart := time.Now()
		value, err := b.redis.Eval(
			ctx,
			reserveTokenBudgetScript,
			b.tokenBudgetKeys(day),
			dayStart.UnixMilli(),
			dayEnd.UnixMilli(),
			tokenReservationLease.Milliseconds(),
			requested,
			limit,
			id,
			budgetDataRetention.Milliseconds(),
		).Result()
		if err != nil {
			return TokenReservation{}, fmt.Errorf("reserve token budget: %w", err)
		}
		status, redisNow, err := parseReservationResult(value)
		if err != nil {
			return TokenReservation{}, fmt.Errorf("reserve token budget: %w", err)
		}
		switch status {
		case 1:
			return TokenReservation{
				ID:            id,
				Reserved:      requested,
				ExpiresAt:     redisNow.Add(tokenReservationLease),
				day:           day,
				localDeadline: localStart.Add(tokenReservationLease),
			}, nil
		case 0:
			return TokenReservation{}, ErrBudgetExceeded
		case -4:
			continue
		default:
			return TokenReservation{}, fmt.Errorf("token budget state is inconsistent")
		}
	}
	return TokenReservation{}, fmt.Errorf("reserve token budget: Redis UTC day changed repeatedly")
}

const dispatchTokenBudgetScript = `
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local state = redis.call('HGET', KEYS[5], ARGV[1])
local reserved = redis.call('HGET', KEYS[3], ARGV[1])
if not state or not reserved or reserved ~= ARGV[2] then return -3 end
if state ~= 'reserved' then return -1 end
local expiry = redis.call('ZSCORE', KEYS[2], ARGV[1])
if not expiry or tonumber(expiry) <= now then return 0 end
redis.call('HSET', KEYS[5], ARGV[1], 'dispatched')
redis.call('PEXPIRE', KEYS[5], ARGV[3])
return 1`

// DispatchTokenBudget irreversibly marks that provider work may have started.
// A dispatched hold is never automatically reclaimed merely because its lease
// elapsed; a crashed or partitioned caller has an unknown billing outcome.
// Dispatch is single-use: a repeated call is not a second authorization.
func (b *BudgetTracker) DispatchTokenBudget(ctx context.Context, reservation TokenReservation) error {
	if reservation.ID == "" && reservation.Reserved == 0 {
		return nil
	}
	if b == nil || b.redis == nil || b.tenant == nil {
		return fmt.Errorf("budget store unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if reservation.ID == "" || reservation.Reserved <= 0 || reservation.day == "" {
		return ErrBudgetReservationInvalid
	}
	status, err := b.redis.Eval(
		ctx,
		dispatchTokenBudgetScript,
		b.tokenBudgetKeys(reservation.day),
		reservation.ID,
		reservation.Reserved,
		budgetDataRetention.Milliseconds(),
	).Int()
	if err != nil {
		return fmt.Errorf("dispatch token budget: %w", err)
	}
	switch status {
	case 1:
		return nil
	case 0:
		return fmt.Errorf("%w: reservation expired before dispatch", ErrBudgetReservationInvalid)
	case -1:
		return ErrBudgetSettlementConflict
	case -3:
		return ErrBudgetReservationInvalid
	default:
		return fmt.Errorf("token budget state is inconsistent")
	}
}

const settleTokenBudgetScript = `
local state = redis.call('HGET', KEYS[5], ARGV[1])
local reserved = redis.call('HGET', KEYS[3], ARGV[1])
if not state or not reserved or reserved ~= ARGV[2] then return -3 end
if string.sub(state, 1, 8) == 'settled:' then
	local receipt = string.sub(state, 9)
	local separator = string.find(receipt, ':', 1, true)
	if not separator or string.sub(receipt, 1, separator - 1) ~= ARGV[3] then return -1 end
	return tonumber(string.sub(receipt, separator + 1))
end
if state ~= 'dispatched' and state ~= 'uncertain' then return -1 end
local pending = tonumber(redis.call('GET', KEYS[4]) or '0')
local amount = tonumber(reserved)
if pending < amount then return -2 end
pending = pending - amount
redis.call('ZREM', KEYS[2], ARGV[1])
if pending == 0 then redis.call('DEL', KEYS[4]) else redis.call('SET', KEYS[4], pending) end
local used = tonumber(redis.call('INCRBY', KEYS[1], ARGV[3]))
local outcome = 1
if tonumber(ARGV[3]) > tonumber(reserved) then
	outcome = 2
elseif used > tonumber(ARGV[5]) or pending > tonumber(ARGV[5]) - used then
	outcome = 3
end
redis.call('HSET', KEYS[5], ARGV[1], 'settled:' .. ARGV[3] .. ':' .. outcome)
for i = 1, 5 do redis.call('PEXPIRE', KEYS[i], ARGV[4]) end
return outcome`

// SettleTokenBudget converts one reservation into provider-reported usage.
// Settlement is idempotent for the same usage and records an overrun before
// returning ErrBudgetReservationExceeded.
func (b *BudgetTracker) SettleTokenBudget(ctx context.Context, reservation TokenReservation, actual int64) error {
	if reservation.ID == "" && reservation.Reserved == 0 {
		return nil
	}
	if b == nil || b.redis == nil || b.tenant == nil {
		return fmt.Errorf("budget store unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if reservation.ID == "" || reservation.Reserved <= 0 || reservation.day == "" || actual < 0 || actual > maxBudgetExactInteger {
		return ErrBudgetReservationInvalid
	}
	status, err := b.redis.Eval(
		ctx,
		settleTokenBudgetScript,
		b.tokenBudgetKeys(reservation.day),
		reservation.ID,
		reservation.Reserved,
		actual,
		budgetDataRetention.Milliseconds(),
		b.tenant.Budget.MaxTokensPerDay,
	).Int()
	if err != nil {
		return fmt.Errorf("settle token budget: %w", err)
	}
	switch status {
	case 1:
		return nil
	case 2:
		return ErrBudgetReservationExceeded
	case 3:
		return ErrBudgetExceeded
	case -1:
		return ErrBudgetSettlementConflict
	case -3:
		return ErrBudgetReservationInvalid
	default:
		return fmt.Errorf("token budget state is inconsistent")
	}
}

const releaseTokenBudgetScript = `
local state = redis.call('HGET', KEYS[5], ARGV[1])
local reserved = redis.call('HGET', KEYS[3], ARGV[1])
if not state or not reserved or reserved ~= ARGV[2] then return -3 end
if string.sub(state, 1, 8) == 'released' then return 1 end
if state ~= 'reserved' then return -1 end
local pending = tonumber(redis.call('GET', KEYS[4]) or '0')
local amount = tonumber(reserved)
if pending < amount then return -2 end
pending = pending - amount
redis.call('ZREM', KEYS[2], ARGV[1])
if pending == 0 then redis.call('DEL', KEYS[4]) else redis.call('SET', KEYS[4], pending) end
redis.call('HSET', KEYS[5], ARGV[1], 'released')
for i = 2, 5 do redis.call('PEXPIRE', KEYS[i], ARGV[3]) end
return 1`

// ReleaseTokenBudget returns a reservation only when the model invocation did
// not start. Releasing an already-settled reservation is a conflict.
func (b *BudgetTracker) ReleaseTokenBudget(ctx context.Context, reservation TokenReservation) error {
	if reservation.ID == "" && reservation.Reserved == 0 {
		return nil
	}
	if b == nil || b.redis == nil || b.tenant == nil {
		return fmt.Errorf("budget store unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if reservation.ID == "" || reservation.Reserved <= 0 || reservation.day == "" {
		return ErrBudgetReservationInvalid
	}
	status, err := b.redis.Eval(
		ctx,
		releaseTokenBudgetScript,
		b.tokenBudgetKeys(reservation.day),
		reservation.ID,
		reservation.Reserved,
		budgetDataRetention.Milliseconds(),
	).Int()
	if err != nil {
		return fmt.Errorf("release token budget: %w", err)
	}
	switch status {
	case 1:
		return nil
	case -1:
		return ErrBudgetSettlementConflict
	case -3:
		return ErrBudgetReservationInvalid
	default:
		return fmt.Errorf("token budget state is inconsistent")
	}
}

// AcquireSessionSlot reserves one tenant-wide invocation slot and renews the
// opaque owner token until Release. A sorted set provides crash recovery:
// abandoned entries expire by score and are removed by the next acquisition.
func (b *BudgetTracker) AcquireSessionSlot(ctx context.Context) (SessionSlot, error) {
	if b == nil || b.tenant == nil {
		return nil, fmt.Errorf("budget tracker is not configured")
	}
	limit := b.tenant.Budget.MaxConcurrentSessions
	if limit <= 0 {
		return nil, nil
	}
	if b.redis == nil {
		return nil, fmt.Errorf("budget store unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	lease := b.sessionSlotTTL
	if lease <= 0 {
		lease = defaultSessionSlotTTL
	}
	token := uuid.NewString()
	key := b.sessionSlotKey()
	const script = `
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[2]) then return 0 end
redis.call('ZADD', KEYS[1], now + tonumber(ARGV[1]), ARGV[3])
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return 1`
	allowed, err := b.redis.Eval(
		ctx,
		script,
		[]string{key},
		lease.Milliseconds(),
		limit,
		token,
		(2 * lease).Milliseconds(),
	).Int()
	if err != nil {
		return nil, fmt.Errorf("reserve tenant concurrency slot: %w", err)
	}
	if allowed != 1 {
		return nil, ErrBudgetExceeded
	}

	renewCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	slot := &sessionSlotLease{
		tracker: b,
		token:   token,
		ttl:     lease,
		cancel:  cancel,
		done:    make(chan struct{}),
	}
	go slot.renewLoop(renewCtx, b.sessionSlotRenewInterval)
	return slot, nil
}

func (b *BudgetTracker) sessionSlotKey() string {
	return b.budgetKeyPrefix() + ":active-sessions"
}

const renewSessionSlotScript = `
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local expiry = redis.call('ZSCORE', KEYS[1], ARGV[1])
if not expiry or tonumber(expiry) <= now then return 0 end
redis.call('ZADD', KEYS[1], 'XX', now + tonumber(ARGV[2]), ARGV[1])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 1`

func (b *BudgetTracker) renewSessionSlot(ctx context.Context, token string, ttl time.Duration) error {
	status, err := b.redis.Eval(
		ctx,
		renewSessionSlotScript,
		[]string{b.sessionSlotKey()},
		token,
		ttl.Milliseconds(),
		(2 * ttl).Milliseconds(),
	).Int()
	if err != nil {
		return fmt.Errorf("renew tenant concurrency slot: %w", err)
	}
	if status != 1 {
		return ErrSessionSlotLost
	}
	return nil
}

func (s *sessionSlotLease) renewLoop(ctx context.Context, interval time.Duration) {
	defer close(s.done)
	if interval <= 0 || interval >= s.ttl {
		interval = s.ttl / sessionSlotRenewalPart
	}
	if interval <= 0 {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			opCtx, cancel := context.WithTimeout(ctx, budgetOperationTimeout)
			err := s.tracker.renewSessionSlot(opCtx, s.token, s.ttl)
			cancel()
			if err != nil {
				s.mu.Lock()
				if s.lostErr == nil {
					s.lostErr = fmt.Errorf("%w: %v", ErrSessionSlotLost, err)
				}
				s.mu.Unlock()
				return
			}
		}
	}
}

func (s *sessionSlotLease) Done() <-chan struct{} { return s.done }

func (s *sessionSlotLease) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lostErr
}

func (s *sessionSlotLease) Release(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.cancel()
	waitCtx, cancelWait := context.WithTimeout(context.WithoutCancel(ctx), budgetOperationTimeout)
	defer cancelWait()
	select {
	case <-s.done:
	case <-waitCtx.Done():
		return waitCtx.Err()
	}
	if err := s.Err(); err != nil {
		return err
	}
	const script = `
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local expiry = redis.call('ZSCORE', KEYS[1], ARGV[1])
if not expiry then return 0 end
redis.call('ZREM', KEYS[1], ARGV[1])
if tonumber(expiry) <= now then return 0 end
return 1`
	releaseCtx, cancelRelease := context.WithTimeout(context.WithoutCancel(ctx), budgetOperationTimeout)
	defer cancelRelease()
	removed, err := s.tracker.redis.Eval(
		releaseCtx,
		script,
		[]string{s.tracker.sessionSlotKey()},
		s.token,
	).Int()
	if err != nil {
		return fmt.Errorf("release tenant concurrency slot: %w", err)
	}
	if removed != 1 {
		return ErrSessionSlotLost
	}
	return nil
}

// NewBudgetTracker creates a new BudgetTracker with Redis backend.
func NewBudgetTracker(redisClient *redis.Client, t *tenant.Tenant) *BudgetTracker {
	var snapshot *tenant.Tenant
	if t != nil {
		copy := *t
		snapshot = &copy
	}
	return &BudgetTracker{
		redis:  redisClient,
		tenant: snapshot,
	}
}

// CheckBudget checks if the tenant has exceeded their budget limits.
// Returns nil if within budget, ErrBudgetExceeded if over limit.
// Fails closed when a budget is configured: allowing unmetered model calls on
// a Redis outage defeats the budget's security and cost-control purpose.
func (b *BudgetTracker) CheckBudget(ctx context.Context) error {
	if b == nil || b.tenant == nil || b.redis == nil {
		return fmt.Errorf("budget store unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if b.tenant.Budget.MaxTokensPerDay < 0 || b.tenant.Budget.MaxCostPerDay < 0 ||
		math.IsNaN(b.tenant.Budget.MaxCostPerDay) || math.IsInf(b.tenant.Budget.MaxCostPerDay, 0) {
		return fmt.Errorf("invalid budget limits")
	}
	now, err := b.redisTime(ctx)
	if err != nil {
		return fmt.Errorf("budget store unavailable: %w", err)
	}
	day := budgetDay(now)
	// Check token budget
	if b.tenant.Budget.MaxTokensPerDay > 0 {
		tokens, err := b.dailyTokenUsage(ctx, day)
		if err != nil {
			return fmt.Errorf("budget store unavailable: %w", err)
		}
		if tokens >= b.tenant.Budget.MaxTokensPerDay {
			return ErrBudgetExceeded
		}
	}

	// Check cost budget
	if b.tenant.Budget.MaxCostPerDay > 0 {
		cost, err := b.dailyCostUsage(ctx, day)
		if err != nil {
			return fmt.Errorf("budget store unavailable: %w", err)
		}
		if cost >= b.tenant.Budget.MaxCostPerDay {
			return ErrBudgetExceeded
		}
	}

	return nil
}

// getDailyTokenUsage retrieves the token usage for today from Redis.
func (b *BudgetTracker) getDailyTokenUsage(ctx context.Context) (int64, error) {
	if b == nil || b.redis == nil || b.tenant == nil {
		return 0, fmt.Errorf("budget store unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	now, err := b.redisTime(ctx)
	if err != nil {
		return 0, err
	}
	return b.dailyTokenUsage(ctx, budgetDay(now))
}

func (b *BudgetTracker) dailyTokenUsage(ctx context.Context, day string) (int64, error) {
	key := b.dailyTokenKey(day)
	val, err := b.redis.Get(ctx, key).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return val, err
}

// getDailyCostUsage retrieves the cost usage for today from Redis.
func (b *BudgetTracker) getDailyCostUsage(ctx context.Context) (float64, error) {
	if b == nil || b.redis == nil || b.tenant == nil {
		return 0, fmt.Errorf("budget store unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	now, err := b.redisTime(ctx)
	if err != nil {
		return 0, err
	}
	return b.dailyCostUsage(ctx, budgetDay(now))
}

func (b *BudgetTracker) dailyCostUsage(ctx context.Context, day string) (float64, error) {
	key := b.dailyCostKey(day)
	val, err := b.redis.Get(ctx, key).Float64()
	if err == redis.Nil {
		return 0, nil
	}
	return val, err
}

// RecordUsage records token and cost usage for the current day.
func (b *BudgetTracker) RecordUsage(ctx context.Context, tokens int64, cost float64) error {
	if b == nil || b.redis == nil || b.tenant == nil {
		return fmt.Errorf("budget store unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if tokens < 0 || tokens > maxBudgetExactInteger || cost < 0 ||
		math.IsNaN(cost) || math.IsInf(cost, 0) {
		return fmt.Errorf("invalid usage values")
	}
	now, err := b.redisTime(ctx)
	if err != nil {
		return err
	}
	dateKey := budgetDay(now)
	_, err = b.redis.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		if tokens > 0 {
			key := b.dailyTokenKey(dateKey)
			pipe.IncrBy(ctx, key, tokens)
			pipe.Expire(ctx, key, budgetDataRetention)
		}
		if cost > 0 {
			key := b.dailyCostKey(dateKey)
			pipe.IncrByFloat(ctx, key, cost)
			pipe.Expire(ctx, key, budgetDataRetention)
		}
		return nil
	})
	return err
}
