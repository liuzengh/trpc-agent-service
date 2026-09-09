// Package redis coordinates session run leases across Workers through Redis.
//
// It is the multi-Worker counterpart of the in-memory reference in the parent
// package and is held to the same contract by the shared conformance suite.
// Everything the parent package says about what a lease is and is not applies
// here unchanged: this is a cooperative lease, the fence is an observation
// handle rather than an admission token, and a Worker that has lost its lease
// is not prevented from writing to the Session.
//
// # Keys
//
// A lease scope becomes two keys:
//
//	<prefix>:{<digest>}:lock    the owner token, with a PX expiry
//	<prefix>:{<digest>}:fence   a counter that only ever goes up
//
// The digest is [sessionlease.KeyDigest], so no tenant, principal or session
// identifier reaches the keyspace: an operator reading SCAN output, MONITOR or
// the slow log sees opaque hex. The braces are a Redis Cluster hash tag and
// they wrap the digest in both names, so the two keys always land in the same
// slot — the scripts below touch both in one call, which a cluster only allows
// within a slot.
//
// # Retention
//
// Releasing a lease deletes the lock and leaves the fence. That is deliberate:
// a fence that could be deleted and start again at 1 would not be monotonic,
// and two Workers comparing tokens across that gap would order themselves
// wrongly. The cost is that fence keys accumulate, one small integer per
// Session ever run, and are never collected. This is a known limitation. Fence
// keys have no expiry today; reclaiming them needs a scheme that cannot let a
// fence go backwards while any Worker still holds a token, and none is
// implemented.
//
// # Deployment
//
// A single Redis instance is the deployment this has been verified against.
// Under failover the lock key can be lost with the replica that never received
// it and the fence counter can go backwards, so mutual exclusion and fence
// monotonicity are not claimed across a failover.
package redis

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionlease"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// DefaultKeyPrefix namespaces lease keys inside a Redis instance that may be
// shared with the Session store and with other services.
const DefaultKeyPrefix = "trpc-service:sessionlease"

const maxKeyPrefixLen = 64

// keyPrefixPattern matches the characters Redis key namespaces conventionally
// use. Braces are excluded because this package adds the cluster hash tag
// itself, and a prefix carrying its own tag would move the two keys of one
// lease into different slots.
var keyPrefixPattern = regexp.MustCompile(`^[A-Za-z0-9_.:-]+$`)

// acquireScript takes the lock only when no live lock exists, and advances the
// fence only when it took it. Doing both in one script is what makes "the
// winner is the one whose fence advanced" true: a client-side EXISTS followed
// by a SET would let two Workers both observe an empty key.
//
// KEYS: lock, fence. ARGV: owner token, ttl in milliseconds.
var acquireScript = goredis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
  return {0, 0}
end
local fence = redis.call('INCR', KEYS[2])
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return {1, fence}
`)

// renewScript extends the lock only for the holder that still owns it, so a
// holder whose lock expired and was taken over cannot renew it back.
//
// KEYS: lock. ARGV: owner token, ttl in milliseconds.
var renewScript = goredis.NewScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return 0
end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
`)

// releaseScript deletes the lock only for the holder that still owns it, and
// never touches the fence.
//
// KEYS: lock. ARGV: owner token.
var releaseScript = goredis.NewScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return 0
end
return redis.call('DEL', KEYS[1])
`)

// Options configures a [Coordinator].
type Options struct {
	// KeyPrefix namespaces this service's lease keys. Empty means
	// [DefaultKeyPrefix].
	KeyPrefix string
	// Lease is the lease timing. The zero value means the package defaults.
	Lease sessionlease.Config
}

// Coordinator is the Redis implementation of [sessionlease.Coordinator].
type Coordinator struct {
	client goredis.UniversalClient
	prefix string
	life   *sessionlease.Lifetime
}

var _ sessionlease.Coordinator = (*Coordinator)(nil)

// New returns a coordinator over client.
//
// The client is borrowed, not owned: [Coordinator.Close] never closes it,
// because the same client may be shared with whoever built it. The owner must
// close the coordinator first and the client second, so a lease that is still
// being released has a usable connection. That order is only worth anything
// because Close waits: it returns once no renewal and no acquisition of this
// coordinator's is still on the wire, which is exactly when closing the client
// underneath it becomes safe.
//
// # What the client decides, and what it does not
//
// Taking any [goredis.UniversalClient] is what makes this an extension point,
// and it is also the limit of what this constructor can promise. The timing
// this package controls is expressed as contexts, and a go-redis client only
// turns a context deadline into a socket read deadline when it was built with
// ContextTimeoutEnabled. A client built without it, and with its ReadTimeout
// disabled rather than merely long, leaves an in-flight command unbounded — and
// since Close waits for in-flight commands, such a client can hold a shutdown
// open for as long as its backend stays silent. That is a real property of the
// client the caller chose; it is not something this package can bound on the
// caller's behalf, and it is not claimed to be. The process in cmd/trpc-service
// builds its own client with ContextTimeoutEnabled set for exactly this reason.
//
// Mutual exclusion does not depend on any of that. A reply that arrives too
// late to be worth anything is refused rather than handed out, on every client,
// because [sessionlease.Lifetime.HandOut] checks the elapsed time after the
// backend has answered instead of trusting the deadline to have been honoured.
func New(client goredis.UniversalClient, opts Options) (*Coordinator, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: nil redis client", sessionlease.ErrInvalidConfig)
	}
	prefix := opts.KeyPrefix
	if prefix == "" {
		prefix = DefaultKeyPrefix
	}
	if len(prefix) > maxKeyPrefixLen {
		return nil, fmt.Errorf("%w: key prefix is %d characters (max %d)",
			sessionlease.ErrInvalidConfig, len(prefix), maxKeyPrefixLen)
	}
	if !keyPrefixPattern.MatchString(prefix) {
		return nil, fmt.Errorf(
			"%w: invalid key prefix %q (letters, digits, '_', '.', ':' and '-' only)",
			sessionlease.ErrInvalidConfig, prefix)
	}
	if err := opts.Lease.Validate(); err != nil {
		return nil, err
	}
	// Keep Redis PX expiry and local lease deadlines on the same duration.
	if ttl := opts.Lease.WithDefaults().TTL; ttl%time.Millisecond != 0 {
		return nil, fmt.Errorf("%w: ttl %s must be a whole number of milliseconds",
			sessionlease.ErrInvalidConfig, ttl)
	}
	return &Coordinator{
		client: client,
		prefix: prefix,
		life:   sessionlease.NewLifetime(opts.Lease),
	}, nil
}

// Acquire implements [sessionlease.Coordinator].
func (c *Coordinator) Acquire(ctx context.Context, key sessiondir.Key) (sessionlease.Lease, error) {
	if c == nil || c.client == nil {
		return nil, fmt.Errorf("%w: nil coordinator", tenant.ErrInvalidArgument)
	}
	if err := sessionlease.ValidateAcquire(ctx, key); err != nil {
		return nil, err
	}

	if err := c.life.Begin(); err != nil {
		return nil, err
	}
	defer c.life.End()

	digest := sessionlease.KeyDigest(key)
	lockKey, fenceKey := c.lockKey(digest), c.fenceKey(digest)
	owner := uuid.NewString()
	ttlMS := c.life.Config().TTL.Milliseconds()

	// Under the caller's context and the coordinator's both, and bounded by the
	// budget the resulting lease would have had. How much of that reaches a
	// script already on the wire is the client's decision rather than this
	// package's — see [sessionlease.Lifetime.Call] — so a Close can still have to
	// wait one out. What does not depend on the client is that a reply arriving
	// after the budget cannot become a lease: HandOut re-checks the elapsed time
	// below.
	callCtx, cancel := c.life.Call(ctx)
	defer cancel()

	// Read before the script goes out. Redis dates the lock from the moment it
	// runs the SET, which is no later than this and possibly much earlier than
	// the reply; anchoring to the reply would have this process outlive its own
	// lock by however long the reply took.
	acquiredAt := time.Now()
	reply, err := acquireScript.Run(callCtx, c.client,
		[]string{lockKey, fenceKey}, owner, ttlMS).Result()
	if err != nil {
		if interrupted := c.life.Interrupted(ctx, err); interrupted != nil {
			return nil, interrupted
		}
		return nil, classify("acquire", err)
	}
	acquired, fence, err := parseAcquire(reply)
	if err != nil {
		// Fail closed. The script may or may not have taken the lock; the
		// caller must not run either way, and a lock this process cannot
		// recognise is left for the TTL to clear.
		return nil, err
	}
	if !acquired {
		return nil, sessionlease.ErrSessionBusy
	}

	held := &holder{
		client: c.client,
		lock:   lockKey,
		owner:  owner,
		ttlMS:  ttlMS,
	}
	return c.life.HandOut(ctx, fence, acquiredAt, held)
}

// Close implements [sessionlease.Coordinator]. It stops renewing, leaves every
// lock to expire by TTL, waits for whatever it just cancelled to come back off
// the wire, and does not close the borrowed client.
func (c *Coordinator) Close() error {
	if c == nil {
		return nil
	}
	return c.life.Close()
}

func (c *Coordinator) lockKey(digest string) string {
	return c.prefix + ":{" + digest + "}:lock"
}

func (c *Coordinator) fenceKey(digest string) string {
	return c.prefix + ":{" + digest + "}:fence"
}

// holder is one acquisition's view of Redis.
type holder struct {
	client goredis.UniversalClient
	lock   string
	owner  string
	ttlMS  int64
}

var _ sessionlease.Holder = (*holder)(nil)

// Renew implements [sessionlease.Holder].
func (h *holder) Renew(ctx context.Context) (bool, error) {
	reply, err := renewScript.Run(ctx, h.client, []string{h.lock}, h.owner, h.ttlMS).Int64()
	if err != nil {
		return false, classify("renew", err)
	}
	switch reply {
	case 1:
		return true, nil
	case 0:
		// Definitive: this holder no longer owns the lock.
		return false, nil
	default:
		return false, fmt.Errorf("%w: renew script returned %d",
			sessionlease.ErrUnavailable, reply)
	}
}

// Release implements [sessionlease.Holder]. A reply of 0 means another holder
// owns the lock now, and deleting nothing is exactly the right outcome.
func (h *holder) Release(ctx context.Context) error {
	if _, err := releaseScript.Run(ctx, h.client, []string{h.lock}, h.owner).Int64(); err != nil {
		return classify("release", err)
	}
	return nil
}

// parseAcquire reads the {taken, fence} reply. Anything it does not recognise
// is [sessionlease.ErrUnavailable]: a reply this build cannot interpret may
// come from a Redis running a different version of the script, and guessing
// would be the one mistake this package exists to avoid.
func parseAcquire(reply any) (bool, uint64, error) {
	malformed := func() (bool, uint64, error) {
		return false, 0, fmt.Errorf("%w: acquire script returned %T",
			sessionlease.ErrUnavailable, reply)
	}
	fields, ok := reply.([]any)
	if !ok || len(fields) != 2 {
		return malformed()
	}
	taken, ok := fields[0].(int64)
	if !ok {
		return malformed()
	}
	fence, ok := fields[1].(int64)
	if !ok {
		return malformed()
	}
	switch taken {
	case 0:
		return false, 0, nil
	case 1:
		if fence <= 0 {
			return false, 0, fmt.Errorf("%w: acquire script returned fence %d",
				sessionlease.ErrUnavailable, fence)
		}
		return true, uint64(fence), nil
	default:
		return malformed()
	}
}

// classify turns a Redis failure into this package's vocabulary. A context
// error stays itself so callers can still tell "the client went away" from
// "coordination is down"; everything else is unavailable, which the HTTP layer
// answers with 503.
//
// The cause is kept in the chain. This package is handed a client, never a
// connection URL, so it has no credentials to leak; the process that owns the
// URL scrubs its own construction and shutdown errors.
func classify(op string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: redis %s: %w", sessionlease.ErrUnavailable, op, err)
}
