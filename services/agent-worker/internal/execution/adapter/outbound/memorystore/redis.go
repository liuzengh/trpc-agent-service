package memorystore

import (
	"context"
	"crypto/tls"
	"encoding/json"
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
	if t.Host == "" || len(t.Host) > 253 || !utf8.ValidString(t.Host) || strings.ContainsAny(t.Host, ",/\\ \t\r\n\x00@?#%[]") || (strings.Contains(t.Host, ":") && net.ParseIP(t.Host) == nil) || t.Port == 0 || t.Database < 0 || t.Database > 15 || t.Username != "memory_runtime" || t.MaxConcurrency < 0 || strings.TrimSpace(password) == "" || !utf8.ValidString(password) || strings.ContainsAny(password, "\x00\r\n") {
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
	return ErrUnavailable
}

// The hash tag places this tenant's head/receipt/attempt keys in one Redis slot.
// SHA256 array encoding prevents separator/braces injection or scope collisions.
func redisKey(scope Scope, kind string, ids ...string) string {
	tenant := strings.TrimPrefix(digest([]byte(scope.TenantID)), "sha256:")
	body, _ := json.Marshal(ids)
	return "runtime_memory:{" + tenant + "}:" + kind + ":" + strings.TrimPrefix(digest(body), "sha256:")
}

type redisRecord struct {
	Revision     string `json:"revision"`
	CompletionID string `json:"completion_id"`
	RunID        string `json:"run_id"`
	AttemptID    string `json:"attempt_id"`
	Digest       string `json:"digest"`
	Body         string `json:"body"`
}

func (s *Redis) decodeRecord(raw string, scope Scope) (redisRecord, Snapshot, error) {
	var r redisRecord
	if len(raw) > 8192 && (len(raw)-8192+1)/2 > s.capacity {
		return r, Snapshot{}, ErrCapacity
	}
	if json.Unmarshal([]byte(raw), &r) != nil {
		return r, Snapshot{}, ErrCorrupt
	}
	revision, err := strconv.ParseUint(r.Revision, 10, 63)
	if err != nil || revision == 0 || strconv.FormatUint(revision, 10) != r.Revision || !validID(r.CompletionID) || !validID(r.RunID) || !validID(r.AttemptID) {
		return r, Snapshot{}, ErrCorrupt
	}
	c, err := decode([]byte(r.Body), scope, r.Digest, s.capacity)
	if err != nil {
		return r, Snapshot{}, err
	}
	if c.BaseRevision+1 != revision {
		return r, Snapshot{}, ErrCorrupt
	}
	canonical, _ := json.Marshal(r)
	if string(canonical) != raw {
		return r, Snapshot{}, ErrCorrupt
	}
	return r, Snapshot{Revision: revision, Entries: c.Entries}, nil
}
func (s *Redis) Load(ctx context.Context, scope Scope) (Snapshot, error) {
	if !scope.valid() {
		return Snapshot{}, ErrIdentity
	}
	raw, err := s.client.Get(ctx, redisKey(scope, "head", scope.ID)).Result()
	if errors.Is(err, redisclient.Nil) {
		return Snapshot{Entries: nil}, nil
	}
	if err != nil {
		return Snapshot{}, redisFailure(err)
	}
	_, out, err := s.decodeRecord(raw, scope)
	return out, err
}

// Validation and comparisons precede the sole MSET. Redis Lua errors do not
// roll back writes, so no command follows MSET and no separate write is used.
// Exact decimal revisions are Go-validated; Lua never converts them to doubles.
const redisApplyScript = `
for i=1,3 do
 local typ=redis.call('TYPE',KEYS[i]).ok
 if typ~='none' and typ~='string' then return {'corrupt'} end
 if redis.call('PTTL',KEYS[i])>=0 then return {'corrupt'} end
end
local receipt=redis.call('GET',KEYS[2])
local attempt=redis.call('GET',KEYS[3])
if receipt then
 if receipt~=ARGV[2] then return {'conflict'} end
 if attempt~=receipt then return {'corrupt'} end
 return {'ok',receipt}
end
if attempt then return {'conflict'} end
local head=redis.call('GET',KEYS[1])
if (head or '')~=ARGV[1] then return {'conflict'} end
redis.call('MSET',KEYS[1],ARGV[2],KEYS[2],ARGV[2],KEYS[3],ARGV[2])
return {'ok',ARGV[2]}
`

func (s *Redis) ApplyAccepted(ctx context.Context, a Accepted, c Candidate) (Snapshot, error) {
	if !validID(a.CompletionID) || !validID(a.RunID) || !validID(a.AttemptID) {
		return Snapshot{}, ErrIdentity
	}
	body, err := c.encode()
	if err != nil {
		return Snapshot{}, err
	}
	if len(body) > s.capacity {
		return Snapshot{}, ErrCapacity
	}
	if digest(body) != a.CandidateDigest {
		return Snapshot{}, ErrIdentity
	}
	record := redisRecord{Revision: strconv.FormatUint(c.BaseRevision+1, 10), CompletionID: a.CompletionID, RunID: a.RunID, AttemptID: a.AttemptID, Digest: a.CandidateDigest, Body: string(body)}
	encoded, _ := json.Marshal(record)
	keys := []string{redisKey(c.Scope, "head", c.Scope.ID), redisKey(c.Scope, "receipt", a.CompletionID), redisKey(c.Scope, "attempt", a.RunID, a.AttemptID)}
	// MGET avoids a torn preflight. The script compares the complete previous head
	// again; concurrent writers either replay exactly or get a CAS conflict.
	values, err := s.client.MGet(ctx, keys...).Result()
	if err != nil {
		return Snapshot{}, redisFailure(err)
	}
	old := ""
	if values[1] == nil {
		if values[0] != nil {
			var ok bool
			old, ok = values[0].(string)
			if !ok {
				return Snapshot{}, ErrCorrupt
			}
			_, snap, e := s.decodeRecord(old, c.Scope)
			if e != nil {
				return Snapshot{}, e
			}
			if snap.Revision != c.BaseRevision {
				return Snapshot{}, ErrConflict
			}
		} else if c.BaseRevision != 0 {
			return Snapshot{}, ErrConflict
		}
	}
	result, err := s.client.Eval(ctx, redisApplyScript, keys, old, string(encoded)).Slice()
	if err != nil {
		return Snapshot{}, redisFailure(err)
	}
	if len(result) == 0 {
		return Snapshot{}, ErrCorrupt
	}
	switch result[0] {
	case "conflict":
		return Snapshot{}, ErrConflict
	case "corrupt":
		return Snapshot{}, ErrCorrupt
	case "ok":
		if len(result) != 2 {
			return Snapshot{}, ErrCorrupt
		}
		raw, ok := result[1].(string)
		if !ok || raw != string(encoded) {
			return Snapshot{}, ErrCorrupt
		}
		_, out, e := s.decodeRecord(raw, c.Scope)
		return out, e
	default:
		return Snapshot{}, ErrCorrupt
	}
}
