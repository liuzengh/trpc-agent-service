// Package redis implements Redis-backed session leases and monotonic fences.
package redis

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	redisclient "github.com/redis/go-redis/v9"
)

var acquireScript = redisclient.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
  return {0, '0'}
end
redis.call('INCR', KEYS[2])
local fence = redis.call('GET', KEYS[2])
local value = ARGV[1] .. '|' .. ARGV[2] .. '|' .. fence
redis.call('PSETEX', KEYS[1], ARGV[3], value)
return {1, fence}
`)

var renewScript = redisclient.NewScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return 0
end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
`)

var releaseScript = redisclient.NewScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return 0
end
return redis.call('DEL', KEYS[1])
`)

var ensureFenceScript = redisclient.NewScript(`
local function normalize(value)
  local normalized = string.gsub(value, '^0+', '')
  if normalized == '' then return '0' end
  return normalized
end
local function decimal_less(left, right)
  left = normalize(left)
  right = normalize(right)
  if string.len(left) ~= string.len(right) then
    return string.len(left) < string.len(right)
  end
  return left < right
end
local current = redis.call('GET', KEYS[1]) or '0'
local minimum = ARGV[1]
if decimal_less(current, minimum) then
  redis.call('SET', KEYS[1], minimum)
end
return 1
`)

type Manager struct {
	client      redisclient.UniversalClient
	environment string
}

func New(client redisclient.UniversalClient, environment string) (*Manager, error) {
	if client == nil || !validSegment(environment) {
		return nil, runtime.ErrInvalidEnvelope
	}
	return &Manager{client: client, environment: environment}, nil
}

func (m *Manager) Acquire(ctx context.Context, key coordination.SessionKey, workerID string, ttl time.Duration) (coordination.Lease, error) {
	if err := validate(key, workerID, ttl); err != nil {
		return coordination.Lease{}, err
	}
	leaseID, err := newLeaseID()
	if err != nil {
		return coordination.Lease{}, err
	}
	leaseKey, fenceKey := m.keys(key)
	result, err := acquireScript.Run(ctx, m.client, []string{leaseKey, fenceKey}, workerID, leaseID, ttl.Milliseconds()).Slice()
	if err != nil {
		return coordination.Lease{}, err
	}
	if len(result) != 2 || asInt64(result[0]) != 1 {
		return coordination.Lease{}, runtime.ErrVersionConflict
	}
	fence, err := strconv.ParseUint(fmt.Sprint(result[1]), 10, 64)
	if err != nil || fence == 0 {
		return coordination.Lease{}, runtime.ErrInvariantViolation
	}
	return coordination.Lease{Session: key, WorkerID: workerID, LeaseID: leaseID, Fence: fence, ExpiresAt: time.Now().Add(ttl)}, nil
}

func (m *Manager) Renew(ctx context.Context, lease coordination.Lease, ttl time.Duration) (coordination.Lease, error) {
	if err := validate(lease.Session, lease.WorkerID, ttl); err != nil || lease.LeaseID == "" || lease.Fence == 0 {
		if err != nil {
			return coordination.Lease{}, err
		}
		return coordination.Lease{}, runtime.ErrLeaseLost
	}
	leaseKey, _ := m.keys(lease.Session)
	matched, err := renewScript.Run(ctx, m.client, []string{leaseKey}, leaseValue(lease), ttl.Milliseconds()).Int64()
	if err != nil {
		return coordination.Lease{}, err
	}
	if matched != 1 {
		return coordination.Lease{}, runtime.ErrLeaseLost
	}
	lease.ExpiresAt = time.Now().Add(ttl)
	return lease, nil
}

func (m *Manager) Release(ctx context.Context, lease coordination.Lease) error {
	if validateKey(lease.Session) != nil || lease.WorkerID == "" || lease.LeaseID == "" || lease.Fence == 0 {
		return runtime.ErrLeaseLost
	}
	leaseKey, _ := m.keys(lease.Session)
	matched, err := releaseScript.Run(ctx, m.client, []string{leaseKey}, leaseValue(lease)).Int64()
	if err != nil {
		return err
	}
	if matched != 1 {
		return runtime.ErrLeaseLost
	}
	return nil
}

func (m *Manager) EnsureFenceAtLeast(ctx context.Context, key coordination.SessionKey, minimum uint64) error {
	if err := validateKey(key); err != nil {
		return err
	}
	// Redis INCR is signed 64-bit, so keep one value available for the next
	// successful acquisition after calibration.
	if minimum >= math.MaxInt64 {
		return runtime.ErrInvariantViolation
	}
	_, fenceKey := m.keys(key)
	return ensureFenceScript.Run(ctx, m.client, []string{fenceKey}, minimum).Err()
}

func (m *Manager) keys(key coordination.SessionKey) (string, string) {
	digest := sha256.Sum256([]byte(key.TenantID + "\x00" + key.AgentAppID + "\x00" + key.SessionID))
	tag := hex.EncodeToString(digest[:])
	prefix := fmt.Sprintf("trpc:%s:{%s}", m.environment, tag)
	return prefix + ":lease", prefix + ":fence"
}

func leaseValue(lease coordination.Lease) string {
	return lease.WorkerID + "|" + lease.LeaseID + "|" + strconv.FormatUint(lease.Fence, 10)
}

func validate(key coordination.SessionKey, workerID string, ttl time.Duration) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if workerID == "" || strings.Contains(workerID, "|") || ttl <= 0 || ttl.Milliseconds() < 1 {
		return runtime.ErrInvalidEnvelope
	}
	return nil
}

func validateKey(key coordination.SessionKey) error {
	if key.TenantID == "" || key.AgentAppID == "" || key.SessionID == "" {
		return runtime.ErrInvalidEnvelope
	}
	return nil
}

func newLeaseID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

func asInt64(value any) int64 {
	switch typed := value.(type) {
	case int64:
		return typed
	case string:
		parsed, _ := strconv.ParseInt(typed, 10, 64)
		return parsed
	case []byte:
		parsed, _ := strconv.ParseInt(string(typed), 10, 64)
		return parsed
	default:
		return 0
	}
}

func validSegment(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '-' || char == '_' {
			continue
		}
		return false
	}
	return true
}

var _ coordination.LeaseManager = (*Manager)(nil)
