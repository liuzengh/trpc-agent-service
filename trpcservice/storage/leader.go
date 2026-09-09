package storage

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

// LeaderLock is a platform-level single-holder lock (key
// lock:leader:{name}). Exactly-one-of behavior across replicas (all wecomws
// WebSocket connections, the senders-ws consumer group) hangs off it: while
// held, a watchdog renews the lease every ttl/3; on takeover by another
// replica the lost channel closes and the holder must stop all leader
// behavior immediately — its connections were kicked by the platform anyway
// (one connection per bot), and the new leader re-campaigns them.
type LeaderLock struct {
	rdb *redis.Client
}

// NewLeaderLock creates a LeaderLock on an established Redis client.
func NewLeaderLock(rdb *redis.Client) *LeaderLock {
	return &LeaderLock{rdb: rdb}
}

func leaderKey(name string) string { return "lock:leader:" + name }

// Acquire blocks until leadership is won or ctx is done, retrying the SetNX
// roughly every second with jitter. While held, a watchdog renews the lease
// every ttl/3: a Redis hiccup is retried on the next tick (the TTL has slack
// for one or two misses), but once renew failures span the full TTL the
// lease has provably lapsed and lost closes, so the holder steps down
// instead of running alongside a new leader. lost also closes when another
// holder took over after expiry. release is idempotent and frees the key
// only if we still own it.
func (l *LeaderLock) Acquire(ctx context.Context, name, owner string, ttl time.Duration) (release func(), lost <-chan struct{}, err error) {
	// A non-positive ttl would both lease a key that never expires (SetNX 0)
	// and panic the watchdog's ticker on ttl/3; refuse it instead — the
	// caller campaigns again after fixing its config.
	if ttl <= 0 {
		return nil, nil, fmt.Errorf("leader %s: ttl must be positive, got %s", name, ttl)
	}
	key := leaderKey(name)
	for {
		ok, aerr := l.rdb.SetNX(ctx, key, owner, ttl).Result()
		if aerr == nil && ok {
			break
		}
		if aerr != nil && ctx.Err() == nil {
			plog.Warnf("leader %s acquire: %v", name, aerr)
		}
		// ~1s retry with ±50% jitter so competing replicas do not poll in
		// lockstep; a canceled ctx exits on the select below.
		//nolint:gosec // G404: backoff jitter needs no cryptographic randomness
		delay := time.Second/2 + time.Duration(rand.Int64N(int64(time.Second)))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, nil, ctx.Err()
		case <-timer.C:
		}
	}
	plog.Infof("leader %s acquired (owner %s, ttl %s)", name, owner, ttl)

	lostC := make(chan struct{})
	done := make(chan struct{})
	var once sync.Once
	stop := func() { once.Do(func() { close(done) }) }

	go func() {
		ticker := time.NewTicker(ttl / 3)
		defer ticker.Stop()
		lastRenewOK := time.Now()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				ok, err := extendKey(ctx, l.rdb, key, owner, ttl)
				switch {
				case err == nil && ok:
					lastRenewOK = time.Now()
				case err != nil:
					// Transient Redis failure: keep renewing — the lease has
					// slack for a missed tick or two, and declaring loss here
					// would drop leadership on a blip while the key is still
					// ours. But once failures span the full TTL the lease has
					// provably lapsed and another replica may already hold it:
					// step down instead of running alongside a second leader.
					if time.Since(lastRenewOK) >= ttl {
						plog.Warnf("leader %s lost (owner %s): renew failing for %s (>= ttl %s), stepping down",
							name, owner, time.Since(lastRenewOK).Truncate(time.Millisecond), ttl)
						close(lostC)
						return
					}
					plog.Warnf("leader %s renew failed (retrying): %v", name, err)
				case !ok:
					plog.Warnf("leader %s lost (owner %s): taken over by another holder", name, owner)
					close(lostC)
					return
				}
			}
		}
	}()

	return func() {
		stop()
		// The caller releases at shutdown, often after canceling the Acquire
		// ctx, so the release runs on its own short-lived context.
		rctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := releaseKey(rctx, l.rdb, key, owner); err != nil {
			plog.Warnf("leader %s release (owner %s): %v", name, owner, err)
		}
	}, lostC, nil
}
