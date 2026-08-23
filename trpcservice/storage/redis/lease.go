package redis

import (
	"context"
	"fmt"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"strconv"
	"time"
)

type LeaseKind string

const (
	LeaseSession LeaseKind = "session"
	LeaseClaim   LeaseKind = "claim"
)

type Lease struct {
	Kind                          LeaseKind
	TenantID, ResourceID, OwnerID string
	FenceToken                    uint64
	ExpiresAt                     time.Time
}
type LeaseManager interface {
	Acquire(context.Context, tenant.TenantContext, LeaseKind, string, string, time.Duration) (Lease, error)
	Renew(context.Context, Lease, time.Duration) (Lease, error)
	Release(context.Context, Lease) error
	Validate(context.Context, Lease) error
}

const leaseAcquire = `local k=KEYS[1]; local seq=KEYS[2]; local owner=ARGV[1]; local ttl=tonumber(ARGV[2]); local now=redis.call('TIME')[1]; local cur=redis.call('HMGET',k,'owner','fence','expires'); if cur[1] and cur[3] and tonumber(cur[3])>tonumber(now) then return {0,cur[1],cur[2],cur[3]} end; local f=redis.call('INCR',seq); local exp=tonumber(now)+ttl; redis.call('HSET',k,'owner',owner,'fence',f,'expires',exp); redis.call('EXPIRE',k,ttl); return {1,owner,f,exp}`
const leaseRenew = `local k=KEYS[1]; local now=redis.call('TIME')[1]; local c=redis.call('HMGET',k,'owner','fence','expires'); if c[1]~=ARGV[1] or c[2]~=ARGV[2] or not c[3] or tonumber(c[3])<=tonumber(now) then return 0 end; local exp=tonumber(now)+tonumber(ARGV[3]); redis.call('HSET',k,'expires',exp); redis.call('EXPIRE',k,tonumber(ARGV[3])); return exp`
const leaseRelease = `local c=redis.call('HMGET',KEYS[1],'owner','fence'); if c[1]~=ARGV[1] or c[2]~=ARGV[2] then return 0 end; redis.call('DEL',KEYS[1]); return 1`

func (b *Backend) Acquire(ctx context.Context, tc tenant.TenantContext, kind LeaseKind, id, owner string, ttl time.Duration) (Lease, error) {
	if err := validateContext(ctx, tc, owner); err != nil {
		return Lease{}, err
	}
	if id == "" {
		return Lease{}, fmt.Errorf("%w: resource", ErrInvalidKey)
	}
	k, _ := sessionLeaseKey(b.prefix, tc.TenantID, string(kind)+"-"+id)
	seq, _ := fenceKey(b.prefix, string(kind), tc.TenantID, id)
	if ttl <= 0 {
		ttl = b.leaseTTL
	}
	r, e := b.client.Eval(ctx, leaseAcquire, []string{k, seq}, owner, int(ttl.Seconds())).Result()
	if e != nil {
		return Lease{}, mapRedisErr(e)
	}
	a := r.([]interface{})
	f, _ := strconv.ParseUint(fmt.Sprint(a[2]), 10, 64)
	exp, _ := strconv.ParseInt(fmt.Sprint(a[3]), 10, 64)
	if fmt.Sprint(a[0]) != "1" {
		return Lease{}, ErrAlreadyClaimed
	}
	return Lease{Kind: kind, TenantID: tc.TenantID, ResourceID: id, OwnerID: owner, FenceToken: f, ExpiresAt: time.Unix(exp, 0).UTC()}, nil
}
func (b *Backend) Renew(ctx context.Context, l Lease, ttl time.Duration) (Lease, error) {
	k, _ := sessionLeaseKey(b.prefix, l.TenantID, string(l.Kind)+"-"+l.ResourceID)
	r, e := b.client.Eval(ctx, leaseRenew, []string{k}, l.OwnerID, strconv.FormatUint(l.FenceToken, 10), int(ttl.Seconds())).Int64()
	if e != nil {
		return l, mapRedisErr(e)
	}
	if r == 0 {
		return l, ErrLeaseLost
	}
	l.ExpiresAt = time.Unix(r, 0).UTC()
	return l, nil
}
func (b *Backend) Release(ctx context.Context, l Lease) error {
	k, _ := sessionLeaseKey(b.prefix, l.TenantID, string(l.Kind)+"-"+l.ResourceID)
	r, e := b.client.Eval(ctx, leaseRelease, []string{k}, l.OwnerID, strconv.FormatUint(l.FenceToken, 10)).Int()
	if e != nil {
		return mapRedisErr(e)
	}
	if r == 0 {
		return ErrFenceRejected
	}
	return nil
}
func (b *Backend) Validate(ctx context.Context, l Lease) error {
	_, e := b.Renew(ctx, l, time.Second)
	if e == nil {
		return nil
	}
	return e
}

var _ storage.Lease = storage.Lease{}
