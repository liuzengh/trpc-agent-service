package tenant

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var ErrRunLeaseLost = errors.New("tenant concurrency lease lost")

const runLeaseTTL = 30 * time.Second

// Redis TIME is authoritative across nodes with skewed wall clocks. Members
// are unique attempts, not worker names or request IDs reused after a crash.
var acquireRunLease = redis.NewScript(`
local t=redis.call('TIME');local now=tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000)
redis.call('ZREMRANGEBYSCORE',KEYS[1],'-inf',now)
if redis.call('ZCARD',KEYS[1])>=tonumber(ARGV[2]) then return 0 end
redis.call('ZADD',KEYS[1],now+tonumber(ARGV[3]),ARGV[1])
redis.call('PEXPIRE',KEYS[1],tonumber(ARGV[3])*2)
return 1`)
var renewRunLease = redis.NewScript(`
local t=redis.call('TIME');local now=tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000)
local untilAt=redis.call('ZSCORE',KEYS[1],ARGV[1])
if not untilAt or tonumber(untilAt)<=now then return 0 end
redis.call('ZADD',KEYS[1],now+tonumber(ARGV[2]),ARGV[1]);redis.call('PEXPIRE',KEYS[1],tonumber(ARGV[2])*2)
return 1`)

type RunLease struct {
	guard   *Guard
	ctx     context.Context
	cancel  context.CancelCauseFunc
	key, id string
	done    chan struct{}
	once    sync.Once
	ttl     time.Duration
}

func (l *RunLease) Context() context.Context { return l.ctx }

func (g *Guard) AcquireRunLease(ctx context.Context, tenantID string) (*RunLease, error) {
	return g.acquireRunLease(ctx, tenantID, runLeaseTTL)
}
func (g *Guard) acquireRunLease(ctx context.Context, tenantID string, ttl time.Duration) (*RunLease, error) {
	policy, err := g.policy(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	if err = g.checkBudget(ctx, tenantID, policy); err != nil {
		return nil, err
	}
	l := &RunLease{guard: g, key: g.prefix + ":concurrent:v2:" + tenantID, id: uuid.NewString(), done: make(chan struct{}), ttl: ttl}
	l.ctx, l.cancel = context.WithCancelCause(ctx)
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		l.cancel(ErrRunLeaseLost)
		return nil, ErrRunLeaseLost
	}
	if policy.ConcurrentRuns > 0 {
		if g.redis != nil {
			n, err := acquireRunLease.Run(ctx, g.redis, []string{l.key}, l.id, policy.ConcurrentRuns, ttl.Milliseconds()).Int()
			if err != nil {
				l.cancel(err)
				return nil, errors.New("tenant concurrency backend unavailable")
			}
			if n != 1 {
				l.cancel(ErrConcurrencyLimited)
				return nil, ErrConcurrencyLimited
			}
		} else {
			members := g.runMembers[l.key]
			if members == nil {
				members = map[string]time.Time{}
				g.runMembers[l.key] = members
			}
			now := time.Now()
			for id, until := range members {
				if !until.After(now) {
					delete(members, id)
				}
			}
			if len(members) >= policy.ConcurrentRuns {
				l.cancel(ErrConcurrencyLimited)
				return nil, ErrConcurrencyLimited
			}
			members[l.id] = now.Add(ttl)
		}
	} else {
		l.key = ""
	}
	g.runLeases[l] = struct{}{}
	go l.keepAlive()
	return l, nil
}
func (l *RunLease) renew(ctx context.Context) error {
	if l.key == "" {
		return nil
	}
	if l.guard.redis != nil {
		n, err := renewRunLease.Run(ctx, l.guard.redis, []string{l.key}, l.id, l.ttl.Milliseconds()).Int()
		if err != nil || n != 1 {
			return ErrRunLeaseLost
		}
		return nil
	}
	l.guard.mu.Lock()
	defer l.guard.mu.Unlock()
	members := l.guard.runMembers[l.key]
	if !members[l.id].After(time.Now()) {
		return ErrRunLeaseLost
	}
	members[l.id] = time.Now().Add(l.ttl)
	return nil
}
func (l *RunLease) keepAlive() {
	defer close(l.done)
	if l.key == "" {
		<-l.ctx.Done()
		return
	}
	ticker := time.NewTicker(l.ttl / 3)
	defer ticker.Stop()
	for {
		select {
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(l.ctx, min(2*time.Second, l.ttl/3))
			err := l.renew(ctx)
			cancel()
			if err != nil {
				l.cancel(err)
				return
			}
		}
	}
}

// Release removes this exact owner only. Canceling the execution context stops
// renewal but does not prematurely free a slot while cleanup is still running.
func (l *RunLease) Release() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		l.cancel(context.Canceled)
		<-l.done
		if l.key != "" && l.guard.redis != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = l.guard.redis.ZRem(ctx, l.key, l.id).Err()
			cancel()
		}
		l.guard.mu.Lock()
		defer l.guard.mu.Unlock()
		if members := l.guard.runMembers[l.key]; members != nil {
			delete(members, l.id)
		}
		delete(l.guard.runLeases, l)
	})
}
