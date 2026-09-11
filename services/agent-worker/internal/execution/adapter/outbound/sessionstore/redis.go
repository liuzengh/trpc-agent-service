package sessionstore

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	redisclient "github.com/redis/go-redis/v9"
	"net"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type RedisTarget struct {
	Host           string
	Port           uint16
	Database       int
	Username       string
	TLS            bool
	MaxConcurrency int
}

type Redis struct {
	client   *redisclient.Client
	capacity int
}

// OpenRedis connects only to the fixed standalone target with a dedicated ACL
// principal. Keys never expire; server durability/eviction are deployment policy.
func OpenRedis(ctx context.Context, t RedisTarget, password string, capacity int) (*Redis, error) {
	if capacity <= 0 {
		return nil, ErrCapacity
	}
	if t.Host == "" || len(t.Host) > 253 || !utf8.ValidString(t.Host) || strings.ContainsAny(t.Host, ",/\\ \t\r\n\x00@?#%[]") || (strings.Contains(t.Host, ":") && net.ParseIP(t.Host) == nil) || t.Port == 0 || t.Database < 0 || t.Database > 15 || t.Username != "session_runtime" || t.MaxConcurrency < 0 || strings.TrimSpace(password) == "" || !utf8.ValidString(password) || strings.ContainsAny(password, "\x00\r\n") {
		return nil, ErrIdentity
	}
	opts := &redisclient.Options{Addr: net.JoinHostPort(t.Host, strconv.Itoa(int(t.Port))), Username: t.Username, Password: password, DB: t.Database, MaxRetries: -1, ContextTimeoutEnabled: true, DialTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, PoolTimeout: 5 * time.Second, DisableIdentity: true}
	if t.MaxConcurrency > 0 {
		opts.PoolSize = t.MaxConcurrency
		opts.MaxActiveConns = t.MaxConcurrency
	}
	if t.TLS {
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: t.Host}
	}
	c := redisclient.NewClient(opts)
	if err := c.Ping(ctx).Err(); err != nil {
		c.Close()
		return nil, redisFailure(err)
	}
	return &Redis{client: c, capacity: capacity}, nil
}
func (s *Redis) Close() { _ = s.client.Close() }
func redisFailure(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return errRedisUnavailable
}

var errRedisUnavailable = errors.New("session Redis unavailable")

func redisKey(tenant, session, ref string) string {
	t := sha256.Sum256([]byte(tenant))
	s := sha256.Sum256([]byte(session))
	return "runtime_session:{" + hex.EncodeToString(t[:]) + "}:" + hex.EncodeToString(s[:]) + ":" + ref
}

// Candidate bytes are immutable. All validation precedes the sole write; no
// command after SET can fail and leave a partially completed transaction.
const redisPutScript = `
local typ=redis.call('TYPE',KEYS[1]).ok
if typ~='none' and typ~='string' then return 'corrupt' end
if redis.call('PTTL',KEYS[1])>=0 then return 'corrupt' end
local old=redis.call('GET',KEYS[1])
if old then
 if old==ARGV[1] then return 'ok' end
 return 'conflict'
end
redis.call('SET',KEYS[1],ARGV[1])
return 'ok'
`
const redisLoadScript = `
local typ=redis.call('TYPE',KEYS[1]).ok
if typ=='none' then return {'missing'} end
if typ~='string' or redis.call('PTTL',KEYS[1])>=0 then return {'corrupt'} end
return {'ok',redis.call('GET',KEYS[1])}
`

func (s *Redis) Put(ctx context.Context, c Candidate) (Head, error) {
	if err := ctx.Err(); err != nil {
		return Head{}, err
	}
	body, head, err := c.Encode(s.capacity)
	if err != nil {
		return Head{}, err
	}
	result, err := s.client.Eval(ctx, redisPutScript, []string{redisKey(c.Identity.TenantID, c.Identity.SessionID, head.Ref)}, body).Text()
	if err != nil {
		return Head{}, redisFailure(err)
	}
	switch result {
	case "ok":
		return head, nil
	case "conflict":
		return Head{}, ErrConflict
	default:
		return Head{}, ErrCorrupt
	}
}
func (s *Redis) Load(ctx context.Context, tenant, session string, head Head) (Candidate, error) {
	if err := ctx.Err(); err != nil {
		return Candidate{}, err
	}
	if tenant == "" || session == "" || len(tenant) > 256 || len(session) > 256 {
		return Candidate{}, ErrIdentity
	}
	if err := head.Validate(); err != nil {
		return Candidate{}, err
	}
	if head.Ref == "" {
		return Candidate{}, ErrNotFound
	}
	result, err := s.client.Eval(ctx, redisLoadScript, []string{redisKey(tenant, session, head.Ref)}).StringSlice()
	if err != nil {
		return Candidate{}, redisFailure(err)
	}
	if len(result) == 1 && result[0] == "missing" {
		return Candidate{}, ErrNotFound
	}
	if len(result) != 2 || result[0] != "ok" {
		return Candidate{}, ErrCorrupt
	}
	return Decode([]byte(result[1]), tenant, session, head, s.capacity)
}

var _ Store = (*Redis)(nil)
