package sessionstore

import (
	"context"
	"errors"
	redisclient "github.com/redis/go-redis/v9"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
)

func redisFixture(t *testing.T) (*Redis, *redisclient.Client) {
	t.Helper()
	addr := os.Getenv("WORKER_SESSION_REDIS_ADDR")
	if addr == "" {
		t.Skip("isolated Redis fixture required")
	}
	host, port, _ := net.SplitHostPort(addr)
	p, _ := strconv.Atoi(port)
	s, err := OpenRedis(context.Background(), RedisTarget{Host: host, Port: uint16(p), Username: "session_runtime", MaxConcurrency: 2}, os.Getenv("WORKER_SESSION_REDIS_PASSWORD"), 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	a := redisclient.NewClient(&redisclient.Options{Addr: addr, Username: "fixture_admin", Password: os.Getenv("WORKER_SESSION_REDIS_ADMIN_PASSWORD")})
	t.Cleanup(func() { a.Close() })
	return s, a
}
func redisCandidate() Candidate {
	return Candidate{Identity: Identity{TenantID: "tenant", SessionID: "session", RunID: "run", AttemptID: "attempt"}, ContentVersion: ContentVersion, Snapshot: []byte(`{"events":[],"summaries":{"root":{"summary":"remember"}}}`)}
}
func TestRedisSessionContract(t *testing.T) {
	s, a := redisFixture(t)
	ctx := context.Background()
	c := redisCandidate()
	h, err := s.Put(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Load(ctx, "tenant", "session", h)
	if err != nil || string(got.Snapshot) != string(c.Snapshot) {
		t.Fatalf("load %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			actual, e := s.Put(ctx, c)
			if e != nil || actual != h {
				t.Errorf("replay %v", e)
			}
		}()
	}
	wg.Wait()
	changed := c
	changed.Snapshot = []byte("{}")
	if _, err = s.Put(ctx, changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict %v", err)
	}
	for _, scope := range [][2]string{{"other", "session"}, {"tenant", "other"}} {
		if _, err = s.Load(ctx, scope[0], scope[1], h); !errors.Is(err, ErrNotFound) {
			t.Fatalf("scope %v", err)
		}
	}
	// Malicious copied bytes at a foreign scope cannot pass identity validation.
	body, _, _ := c.Encode(4096)
	foreign := redisKey("other", "session", h.Ref)
	if err = a.Set(ctx, foreign, body, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Load(ctx, "other", "session", h); !errors.Is(err, ErrIdentity) {
		t.Fatalf("copied identity %v", err)
	}
	bad := h
	bad.Digest = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	if _, err = s.Load(ctx, "tenant", "session", bad); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("digest %v", err)
	}
	missing := c
	missing.Identity.AttemptID = "missing"
	_, mh, _ := missing.Encode(4096)
	if _, err = s.Load(ctx, "tenant", "session", mh); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing parent %v", err)
	}
	wrong := redisKey("tenant", "session", mh.Ref)
	if err = a.LPush(ctx, wrong, "wrongtype").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Load(ctx, "tenant", "session", mh); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("wrong type read %v", err)
	}
	if _, err = s.Put(ctx, missing); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("wrong type put %v", err)
	}
	if n := a.LLen(ctx, wrong).Val(); n != 1 {
		t.Fatal("wrong type mutated")
	}
	ttlCandidate := c
	ttlCandidate.Identity.AttemptID = "ttl"
	ttlBody, ttlHead, _ := ttlCandidate.Encode(4096)
	ttlKey := redisKey("tenant", "session", ttlHead.Ref)
	if err = a.Set(ctx, ttlKey, ttlBody, 60*1000000000).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Load(ctx, "tenant", "session", ttlHead); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("TTL read %v", err)
	}
	if _, err = s.Put(ctx, ttlCandidate); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("TTL put %v", err)
	}
	if err = a.Del(ctx, ttlKey).Err(); err != nil {
		t.Fatal(err)
	}
	denied := c
	denied.Identity.AttemptID = "denied"
	_, dh, _ := denied.Encode(4096)
	if err = a.Do(ctx, "ACL", "SETUSER", "session_runtime", "-set").Err(); err != nil {
		t.Fatal(err)
	}
	_, err = s.Put(ctx, denied)
	restore := a.Do(ctx, "ACL", "SETUSER", "session_runtime", "+set").Err()
	if err == nil || restore != nil {
		t.Fatalf("ACL %v restore %v", err, restore)
	}
	if a.Exists(ctx, redisKey("tenant", "session", dh.Ref)).Val() != 0 {
		t.Fatal("ACL failure wrote")
	}
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = s.Put(cancelCtx, c); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err = s.Load(cancelCtx, "tenant", "session", h); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	small := &Redis{client: s.client, capacity: 1}
	if _, err = small.Put(ctx, c); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	if _, err = small.Load(ctx, "tenant", "session", h); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	for _, key := range a.Keys(ctx, "runtime_session:*").Val() {
		if a.PTTL(ctx, key).Val() != -1 {
			t.Fatalf("TTL %s", key)
		}
	}
}
func TestRedisSessionAfterRestart(t *testing.T) {
	if os.Getenv("WORKER_SESSION_REDIS_VERIFY_RESTART") != "1" {
		t.Skip("restart phase required")
	}
	s, _ := redisFixture(t)
	c := redisCandidate()
	_, h, _ := c.Encode(4096)
	got, err := s.Load(context.Background(), "tenant", "session", h)
	if err != nil || string(got.Snapshot) != string(c.Snapshot) {
		t.Fatalf("durable summary snapshot %v", err)
	}
	if actual, err := s.Put(context.Background(), c); err != nil || actual != h {
		t.Fatalf("durable replay %v", err)
	}
}
