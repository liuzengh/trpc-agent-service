package httpadapter

import (
	"sync"
	"time"
)

type loginRateLimiter struct {
	mu      sync.Mutex
	limit   int
	window  time.Duration
	now     func() time.Time
	buckets map[string]loginRateBucket
}

type loginRateBucket struct {
	startedAt time.Time
	attempts  int
}

func newLoginRateLimiter(limit int, window time.Duration) *loginRateLimiter {
	return &loginRateLimiter{
		limit:   limit,
		window:  window,
		now:     time.Now,
		buckets: make(map[string]loginRateBucket),
	}
}

func (l *loginRateLimiter) allow(key string) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	bucket, found := l.buckets[key]
	if !found || !now.Before(bucket.startedAt.Add(l.window)) {
		l.buckets[key] = loginRateBucket{startedAt: now, attempts: 1}
		l.removeExpired(now)
		return true, 0
	}
	if bucket.attempts >= l.limit {
		return false, bucket.startedAt.Add(l.window).Sub(now)
	}
	bucket.attempts++
	l.buckets[key] = bucket
	return true, 0
}

func (l *loginRateLimiter) removeExpired(now time.Time) {
	if len(l.buckets) < 1024 {
		return
	}
	for key, bucket := range l.buckets {
		if !now.Before(bucket.startedAt.Add(l.window)) {
			delete(l.buckets, key)
		}
	}
}
