package coordination

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// ProjectionError distinguishes "another writer already put a newer version
// here" from a genuine Redis failure, so a caller can log the former as the
// normal outcome of two commits racing and the latter as something broken.
var ErrStaleProjection = errors.New("coordination: a newer projection already exists")

// projectScript stores payload only when version is strictly greater than
// what is stored, and reports what happened, in one atomic round trip. A
// read-then-write here would have a window in which two projections with the
// same version both believe they won; Redis evaluates this script as one
// operation, which is the whole reason it is a script and not two commands.
const projectScript = `
local current = redis.call("get", KEYS[1])
local incoming = tonumber(ARGV[1])
if current then
  local cur = tonumber(string.match(current, '"version":(%d+)'))
  if cur and cur >= incoming then
    return 0
  end
end
redis.call("set", KEYS[1], ARGV[2])
return 1
`

// readScript returns the raw stored projection, or nil. It is a separate
// command from the write path on purpose: reading never needs the compare,
// and folding both into one script would make a cache miss indistinguishable
// from a stale-version rejection at the call site.
const readScript = `return redis.call("get", KEYS[1])`

// ProjectedSession is the committed session state a projection carries.
// Events live in MySQL for good; the projection exists so a read path can
// answer "what is this conversation's committed state right now" without a
// database round trip, and is allowed to be behind, never ahead.
type ProjectedSession struct {
	TenantID  string            `json:"tenant_id"`
	SessionPK int64             `json:"session_pk"`
	Version   uint64            `json:"version"`
	State     map[string][]byte `json:"state"`
	Summary   string            `json:"summary,omitempty"`
}

// SessionProjection stores committed session state in Redis under a version
// that only ever increases. It is a cache in the strict sense: losing every
// key here loses nothing, since MySQL has the same data.
type SessionProjection struct {
	redis  *Redis
	ttlSec int
}

// NewSessionProjection wires a projection store over the same Redis client
// the coordinator already uses, under its own key namespace.
func NewSessionProjection(r *Redis, ttlSeconds int) *SessionProjection {
	if ttlSeconds <= 0 {
		ttlSeconds = 3600
	}
	return &SessionProjection{redis: r, ttlSec: ttlSeconds}
}

// Project stores s if — and only if — nothing newer is already there. A
// rejected write is not an error to raise on; it means something ahead of
// this already landed, which is the correct end state for a cache.
func (p *SessionProjection) Project(ctx context.Context, s ProjectedSession) (bool, error) {
	payload, err := json.Marshal(s)
	if err != nil {
		return false, fmt.Errorf("coordination: encode projection: %w", err)
	}
	res, err := p.redis.client.Eval(ctx, projectScript,
		[]string{p.key(s.TenantID, s.SessionPK)}, s.Version, string(payload)).Result()
	if err != nil {
		return false, fmt.Errorf("coordination: project session: %w", err)
	}
	code, ok := toInt(res)
	if !ok {
		return false, fmt.Errorf("coordination: unexpected projection reply %T %v", res, res)
	}
	written := code == 1
	if written {
		// The TTL is set separately from the script: keeping the compare and
		// the expiry in one Lua call would need the TTL as a script argument
		// and buy nothing, and a Redis that dies between the two leaves a
		// never-expiring key that is still correct data, merely sticky.
		if err := p.redis.client.Expire(ctx, p.key(s.TenantID, s.SessionPK), secondsDuration(p.ttlSec)).Err(); err != nil {
			return written, fmt.Errorf("coordination: set projection ttl: %w", err)
		}
	}
	return written, nil
}

// Read returns the stored projection, if any.
func (p *SessionProjection) Read(ctx context.Context, tenantID string, sessionPK int64) (ProjectedSession, bool, error) {
	res, err := p.redis.client.Eval(ctx, readScript, []string{p.key(tenantID, sessionPK)}).Result()
	if errors.Is(err, redis.Nil) {
		return ProjectedSession{}, false, nil
	}
	if err != nil {
		return ProjectedSession{}, false, fmt.Errorf("coordination: read projection: %w", err)
	}
	raw, ok := res.(string)
	if !ok || raw == "" {
		return ProjectedSession{}, false, nil
	}
	var s ProjectedSession
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		return ProjectedSession{}, false, fmt.Errorf("coordination: decode projection: %w", err)
	}
	return s, true, nil
}

// Forget removes a projection, e.g. after a session's tenant is deleted.
func (p *SessionProjection) Forget(ctx context.Context, tenantID string, sessionPK int64) error {
	return p.redis.client.Del(ctx, p.key(tenantID, sessionPK)).Err()
}

func (p *SessionProjection) key(tenantID string, sessionPK int64) string {
	return p.redis.prefix + "project:" + tenantID + ":" + strconv.FormatInt(sessionPK, 10)
}

func toInt(v any) (int, bool) {
	switch n := v.(type) {
	case int64:
		return int(n), true
	case string:
		parsed, err := strconv.Atoi(n)
		return parsed, err == nil
	}
	return 0, false
}

func secondsDuration(n int) time.Duration {
	return time.Duration(n) * time.Second
}
