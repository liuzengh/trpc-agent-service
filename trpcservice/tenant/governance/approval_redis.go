package governance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/redis/go-redis/v9"
)

var consumeRedisApproval = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`)

// RedisApprovalStore shares approvals across workers. Redis owns expiration
// time and each token occupies one key, so consuming also works on a cluster.
// Redis availability/persistence policy determines approval retention; failed
// or ambiguous requests always deny execution. The caller owns the client.
type RedisApprovalStore struct {
	client   redis.UniversalClient
	prefix   string
	newNonce func() (string, error)
}

var _ ApprovalBackend = (*RedisApprovalStore)(nil)

func NewRedisApprovalStore(client redis.UniversalClient, prefix string) *RedisApprovalStore {
	return &RedisApprovalStore{client: client, prefix: prefix, newNonce: newApprovalNonce}
}

func (s *RedisApprovalStore) IssueApproval(ctx context.Context, scope string, ttl time.Duration) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if s == nil || s.client == nil || scope == "" || ttl < time.Millisecond {
		return "", ErrApprovalUnavailable
	}
	for attempt := 0; attempt < 3; attempt++ {
		nonce, err := s.newNonce()
		if err != nil {
			return "", ErrApprovalUnavailable
		}
		// Explicit PX preserves millisecond expiration for all TTLs. NX never
		// overwrites an existing approval, including on a nonce collision.
		result, err := s.client.Do(ctx, "SET", s.key(nonce), approvalHash(scope), "NX", "PX", ttl.Milliseconds()).Text()
		if err == redis.Nil {
			continue
		}
		if err != nil || result != "OK" {
			return "", approvalBackendError(ctx)
		}
		return nonce, nil
	}
	return "", ErrApprovalUnavailable
}

func (s *RedisApprovalStore) ConsumeApproval(ctx context.Context, nonce, scope string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if s == nil || s.client == nil {
		return false, ErrApprovalUnavailable
	}
	if nonce == "" || scope == "" {
		return false, nil
	}
	consumed, err := consumeRedisApproval.Run(ctx, s.client, []string{s.key(nonce)}, approvalHash(scope)).Int64()
	if err != nil {
		// A lost response after DEL may have consumed the nonce. Never retry
		// the tool or convert this uncertainty into an authorization grant.
		return false, approvalBackendError(ctx)
	}
	return consumed == 1, nil
}

func (s *RedisApprovalStore) key(nonce string) string {
	return s.prefix + ":approval:" + approvalHash(nonce)
}

func approvalHash(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func approvalBackendError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Redis errors may contain credentials, addresses, or command contents.
	return ErrApprovalUnavailable
}
