package redis

import (
	"context"
	"fmt"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"strconv"
	"time"
)

const claimScript = `local k=KEYS[1]; local fenceSeq=KEYS[2]; local attemptSeq=KEYS[3]; local owner=ARGV[1]; local ttl=tonumber(ARGV[2]); local epoch=ARGV[3]; local now=redis.call('TIME')[1]; local v=redis.call('HGETALL',k); local h={}; for i=1,#v,2 do h[v[i]]=v[i+1] end; if h.status=='completed' and (not h.epoch or h.epoch==epoch) then return {3,h.owner or '',h.attempt or '0',h.fence or '0',h.response or '',h.claimed or '0',h.expires or '0',h.epoch or epoch} end; if h.owner and h.expires and tonumber(h.expires)>tonumber(now) and (not h.epoch or h.epoch==epoch) then return {2,h.owner,h.attempt,h.fence,h.response or '',h.claimed,h.expires,h.epoch or epoch} end; local attempt=redis.call('INCR',attemptSeq); local fence=redis.call('INCR',fenceSeq); local exp=tonumber(now)+ttl; redis.call('HSET',k,'status','acquired','owner',owner,'attempt',attempt,'fence',fence,'epoch',epoch,'claimed',now,'expires',exp); redis.call('EXPIRE',k,ttl); return {1,owner,attempt,fence,'',now,exp,epoch}`
const claimWriteScript = `local k=KEYS[1]; local owner=ARGV[1]; local fence=ARGV[2]; local status=ARGV[3]; local response=ARGV[4]; local now=redis.call('TIME')[1]; local cur=redis.call('HMGET',k,'owner','fence','expires'); if cur[1]~=owner or cur[2]~=fence or not cur[3] or tonumber(cur[3])<=tonumber(now) then return 0 end; redis.call('HSET',k,'status',status,'response',response); return 1`

func (b *Backend) Claim(ctx context.Context, tc tenant.TenantContext, id string, ttl time.Duration) (storage.Claim, error) {
	return b.claim(ctx, tc, storage.DedupKey{TenantID: tc.TenantID, Channel: tc.Channel, BindingID: tc.BindingID, ExternalMessageID: id}, ttl, "claim")
}

func (b *Backend) claim(ctx context.Context, tc tenant.TenantContext, k storage.DedupKey, ttl time.Duration, owner string) (storage.Claim, error) {
	return b.claimWithEpoch(ctx, tc, k, ttl, owner, 1)
}

func (b *Backend) claimWithEpoch(ctx context.Context, tc tenant.TenantContext, k storage.DedupKey, ttl time.Duration, owner string, epoch storage.Epoch) (storage.Claim, error) {
	if err := validateContext(ctx, tc, owner); err != nil {
		return storage.Claim{}, err
	}
	if err := storageKeyTenant(tc, k); err != nil {
		return storage.Claim{}, err
	}
	if ttl <= 0 {
		ttl = b.claimTTL
	}
	if ttl <= 0 || owner == "" {
		return storage.Claim{}, fmt.Errorf("%w: claim owner and ttl are required", storage.ErrInvalidArgument)
	}
	dedup, e := dedupKey(b.prefix, k.TenantID, k.Channel, k.BindingID, k.ExternalMessageID)
	if e != nil {
		return storage.Claim{}, e
	}
	seq, _ := key(b.prefix, "fence", "claim", tc.TenantID, tc.Channel, tc.BindingID, k.ExternalMessageID)
	attemptSeq, _ := key(b.prefix, "attempt", "claim", tc.TenantID, tc.Channel, tc.BindingID, k.ExternalMessageID)
	r, err := b.client.Eval(ctx, claimScript, []string{dedup, seq, attemptSeq}, owner, int(ttl.Seconds()), strconv.FormatUint(uint64(epoch), 10)).Result()
	if err != nil {
		return storage.Claim{}, mapRedisErr(err)
	}
	a, ok := r.([]interface{})
	if !ok || len(a) < 8 {
		return storage.Claim{}, fmt.Errorf("%w: claim result", ErrRedisUnavailable)
	}
	code, _ := resultInt(a[0])
	attempt, _ := strconv.Atoi(fmt.Sprint(a[2]))
	fence, _ := strconv.ParseUint(fmt.Sprint(a[3]), 10, 64)
	status := storage.ClaimInFlight
	if code == 1 {
		status = storage.ClaimAcquired
	}
	if code == 3 {
		status = storage.ClaimCompleted
	}
	claimed, _ := strconv.ParseInt(fmt.Sprint(a[5]), 10, 64)
	exp, _ := strconv.ParseInt(fmt.Sprint(a[6]), 10, 64)
	claimEpoch, _ := strconv.ParseUint(fmt.Sprint(a[7]), 10, 64)
	return storage.Claim{Key: k, Status: status, OwnerID: fmt.Sprint(a[1]), Attempt: attempt, FenceToken: fence, ResponseRef: fmt.Sprint(a[4]), ClaimedAt: time.Unix(claimed, 0).UTC(), ExpiresAt: time.Unix(exp, 0).UTC(), Backend: storage.BackendRedis, Epoch: storage.Epoch(claimEpoch)}, nil
}

func storageKeyTenant(tc tenant.TenantContext, k storage.DedupKey) error {
	if err := storage.ValidateDedupKey(k); err != nil {
		return err
	}
	if tc.TenantID != k.TenantID || tc.Channel != k.Channel || tc.BindingID != k.BindingID {
		return storage.ErrTenantMismatch
	}
	return nil
}
func (b *Backend) Complete(ctx context.Context, tc tenant.TenantContext, k storage.DedupKey, response string, fence uint64) error {
	return b.writeClaim(ctx, tc, k, "completed", response, fence)
}
func (b *Backend) Fail(ctx context.Context, tc tenant.TenantContext, k storage.DedupKey, fence uint64, retryable bool) error {
	status := "completed"
	if retryable {
		status = "acquired"
	}
	return b.writeClaim(ctx, tc, k, status, "", fence)
}
func (b *Backend) writeClaim(ctx context.Context, tc tenant.TenantContext, k storage.DedupKey, status, response string, fence uint64) error {
	return b.writeClaimOwner(ctx, tc, k, status, response, "claim", fence)
}

func (b *Backend) writeClaimOwner(ctx context.Context, tc tenant.TenantContext, k storage.DedupKey, status, response, owner string, fence uint64) error {
	if err := validateContext(ctx, tc, owner); err != nil {
		return err
	}
	key, _ := dedupKey(b.prefix, k.TenantID, k.Channel, k.BindingID, k.ExternalMessageID)
	r, err := b.client.Eval(ctx, claimWriteScript, []string{key}, owner, strconv.FormatUint(fence, 10), status, response).Int()
	if err != nil {
		return mapRedisErr(err)
	}
	if r != 1 {
		return fmt.Errorf("%w: %v", storage.ErrFenceRejected, ErrFenceRejected)
	}
	return nil
}
