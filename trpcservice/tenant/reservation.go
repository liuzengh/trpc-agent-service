package tenant

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Reservation debits the conservative bound immediately. Until settlement or
// after a crash it continues to occupy the budget; it is never auto-refunded.
// Accounting stays on the UTC date at which the provider call started.
type Reservation struct {
	ID         string
	TenantID   string
	Day        string
	Prompt     int64
	Completion int64
	Cost       float64
}
type reservationState struct {
	Reservation
	Settled          bool
	ActualPrompt     int64
	ActualCompletion int64
	ActualCost       float64
}

const budgetTTL = 7 * 24 * 60 * 60

var reserveModel = redis.NewScript(`
local p=tonumber(redis.call('GET',KEYS[2]) or '0')
local c=tonumber(redis.call('GET',KEYS[3]) or '0')
local cost=tonumber(redis.call('GET',KEYS[4]) or '0')
if (tonumber(ARGV[4])>0 and p+tonumber(ARGV[1])>tonumber(ARGV[4])) or
 (tonumber(ARGV[5])>0 and c+tonumber(ARGV[2])>tonumber(ARGV[5])) or
 (tonumber(ARGV[6])>0 and cost+tonumber(ARGV[3])>tonumber(ARGV[6])) then return 0 end
if redis.call('EXISTS',KEYS[1])==1 then return -1 end
redis.call('SET',KEYS[1],ARGV[7],'EX',ARGV[8])
redis.call('INCRBY',KEYS[2],ARGV[1]); redis.call('EXPIRE',KEYS[2],ARGV[8])
redis.call('INCRBY',KEYS[3],ARGV[2]); redis.call('EXPIRE',KEYS[3],ARGV[8])
redis.call('INCRBYFLOAT',KEYS[4],ARGV[3]); redis.call('EXPIRE',KEYS[4],ARGV[8])
return 1`)
var settleModel = redis.NewScript(`
local raw=redis.call('GET',KEYS[1]); if not raw then return -1 end
local r=cjson.decode(raw)
for i=2,4 do if redis.call('EXISTS',KEYS[i])==0 then return -1 end end
if r.Settled then
 if r.ActualPrompt==tonumber(ARGV[1]) and r.ActualCompletion==tonumber(ARGV[2]) and r.ActualCostString==ARGV[3] then return 0 end
 return -1
end
redis.call('INCRBY',KEYS[2],tonumber(ARGV[1])-r.Prompt)
redis.call('INCRBY',KEYS[3],tonumber(ARGV[2])-r.Completion)
redis.call('INCRBYFLOAT',KEYS[4],tonumber(ARGV[3])-r.Cost)
r.Settled=true; r.ActualPrompt=tonumber(ARGV[1]); r.ActualCompletion=tonumber(ARGV[2]); r.ActualCost=tonumber(ARGV[3])
r.ActualCostString=ARGV[3]
redis.call('SET',KEYS[1],cjson.encode(r),'EX',ARGV[4])
for i=2,4 do redis.call('EXPIRE',KEYS[i],ARGV[4]) end
return 1`)

func validUsage(prompt, completion int64, cost float64) bool {
	return prompt >= 0 && completion >= 0 && prompt <= 1_000_000_000 && completion <= 1_000_000_000 && cost >= 0 && cost <= 1_000_000 && !math.IsNaN(cost) && !math.IsInf(cost, 0)
}

func (g *Guard) ReserveModel(ctx context.Context, tenantID string, prompt, completion int64, cost float64) (Reservation, error) {
	if !validUsage(prompt, completion, cost) {
		return Reservation{}, errors.New("invalid model reservation")
	}
	policy, err := g.policy(ctx, tenantID)
	if err != nil {
		return Reservation{}, err
	}
	if policy.DailyCostUSD > 0 && cost == 0 && (prompt > 0 || completion > 0) {
		return Reservation{}, ErrPricingRequired
	}
	r := Reservation{ID: uuid.NewString(), TenantID: tenantID, Day: time.Now().UTC().Format("20060102"), Prompt: prompt, Completion: completion, Cost: cost}
	state := reservationState{Reservation: r}
	if g.redis != nil {
		encoded, _ := json.Marshal(state)
		n, err := reserveModel.Run(ctx, g.redis, g.reservationKeys(r), prompt, completion, cost, policy.DailyPromptTokens, policy.DailyCompletionTokens, policy.DailyCostUSD, string(encoded), budgetTTL).Int()
		if err != nil {
			return Reservation{}, errors.New("model budget reservation unavailable")
		}
		if n != 1 {
			return Reservation{}, ErrBudgetExceeded
		}
		return r, nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	entry := g.local[tenantID]
	if entry == nil {
		entry = &localQuota{}
		g.local[tenantID] = entry
	}
	if entry.day != r.Day {
		entry.day, entry.prompt, entry.completion, entry.cost = r.Day, 0, 0, 0
	}
	if (policy.DailyPromptTokens > 0 && entry.prompt+prompt > policy.DailyPromptTokens) || (policy.DailyCompletionTokens > 0 && entry.completion+completion > policy.DailyCompletionTokens) || (policy.DailyCostUSD > 0 && entry.cost+cost > policy.DailyCostUSD) {
		return Reservation{}, ErrBudgetExceeded
	}
	entry.prompt += prompt
	entry.completion += completion
	entry.cost += cost
	if g.reservations == nil {
		g.reservations = map[string]reservationState{}
	}
	for id, old := range g.reservations {
		if old.Day < time.Now().UTC().AddDate(0, 0, -7).Format("20060102") {
			delete(g.reservations, id)
		}
	}
	g.reservations[r.ID] = state
	return r, nil
}

// SettleModel is idempotent for identical usage and refuses conflicting replay.
// Unknown provider outcomes must be charged at least the reserved amount.
func (g *Guard) SettleModel(ctx context.Context, r Reservation, prompt, completion int64, cost float64) (bool, error) {
	if !validUsage(prompt, completion, cost) {
		return false, errors.New("invalid model settlement")
	}
	if g.redis != nil {
		n, err := settleModel.Run(ctx, g.redis, g.reservationKeys(r), prompt, completion, cost, budgetTTL).Int()
		if err != nil || n < 0 {
			return false, errors.New("model budget settlement unavailable or conflicting")
		}
		return n == 1, nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	s, ok := g.reservations[r.ID]
	if !ok || s.Reservation != r {
		return false, errors.New("model reservation not found")
	}
	if s.Settled {
		if s.ActualPrompt != prompt || s.ActualCompletion != completion || s.ActualCost != cost {
			return false, errors.New("model settlement conflict")
		}
		return false, nil
	}
	entry := g.local[r.TenantID]
	if entry != nil && entry.day == r.Day {
		entry.prompt += prompt - r.Prompt
		entry.completion += completion - r.Completion
		entry.cost += cost - r.Cost
	}
	s.Settled = true
	s.ActualPrompt = prompt
	s.ActualCompletion = completion
	s.ActualCost = cost
	g.reservations[r.ID] = s
	return true, nil
}
func (g *Guard) reservationKeys(r Reservation) []string {
	base := g.prefix + ":usage:" + r.TenantID + ":" + r.Day
	return []string{base + ":call:" + r.ID, base + ":prompt", base + ":completion", base + ":cost"}
}
