package permissions

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
	redis "github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestRedisACLIntegration(t *testing.T) {
	if os.Getenv("TEST_PERMISSIONS_DOCKER") != "1" {
		t.Skip("TEST_PERMISSIONS_DOCKER not enabled")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	tag := fmt.Sprintf("permissions-%x", time.Now().UnixNano())
	cmd := exec.CommandContext(ctx, "docker", "run", "-d", "--pull=never", "--label", "trpc-agent.permissions-test="+tag, "--tmpfs", "/data:rw", "-p", "127.0.0.1::6379", "redis:7-alpine", "redis-server", "--save", "", "--appendonly", "no")
	out, err := cmd.Output()
	if err != nil {
		t.Fatal("start isolated Redis container")
	}
	id := strings.TrimSpace(string(out))
	defer func() {
		label, err := exec.Command("docker", "inspect", "--format", `{{index .Config.Labels "trpc-agent.permissions-test"}}`, id).Output()
		if err != nil || strings.TrimSpace(string(label)) != tag {
			t.Error("cannot verify test container ownership")
			return
		}
		if err := exec.Command("docker", "rm", "-f", id).Run(); err != nil {
			t.Error("cleanup isolated Redis")
		}
	}()
	port, err := exec.CommandContext(ctx, "docker", "port", id, "6379/tcp").Output()
	if err != nil {
		t.Fatal("resolve isolated port")
	}
	addr := strings.TrimSpace(string(port))
	root := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = root.Close() }()
	for i := 0; i < 30; i++ {
		if root.Ping(ctx).Err() == nil {
			break
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal("Redis startup timeout")
		}
	}
	policy, _ := Redis("trpc", "policy-test")
	// The fixture alone enables users with a synthetic password on loopback.
	// go-redis does not send AUTH for a username with an empty password.
	for _, line := range strings.Split(strings.TrimSpace(policy), "\n") {
		fields := strings.Fields(line)
		args := []any{"ACL", "SETUSER", fields[1]}
		var selector []string
		for _, part := range fields[2:] {
			if strings.HasPrefix(part, "(") {
				selector = []string{part}
				continue
			}
			if selector != nil {
				selector = append(selector, part)
				if strings.HasSuffix(part, ")") {
					args = append(args, strings.Join(selector, " "))
					selector = nil
				}
				continue
			}
			args = append(args, part)
		}
		args = append(args, "on", ">fixture-only")
		if err := root.Do(ctx, args...).Err(); err != nil {
			t.Fatal("ACL selector rejected: ", err)
		}
	}
	client := func(role string) *redis.Client {
		c := redis.NewClient(&redis.Options{Addr: addr, Username: "trpc_" + role, Password: "fixture-only"})
		t.Cleanup(func() { _ = c.Close() })
		return c
	}
	gw, w, relay, sender := client("gateway"), client("worker"), client("relay"), client("sender")
	if err := gw.Set(ctx, "policy-test:quota:rate:t:u:m", 1, 0).Err(); err != nil {
		t.Fatal(err)
	}
	for _, fn := range []func() error{func() error { return gw.Get(ctx, "other:secret").Err() }, func() error { return gw.FlushDB(ctx).Err() }, func() error { return gw.Keys(ctx, "*").Err() }, func() error { return gw.Set(ctx, "policy-test:coord:session:x:lock", "x", 0).Err() }, func() error { return relay.Get(ctx, "policy-test:quota:rate:t:u:m").Err() }, func() error { return sender.Set(ctx, "policy-test:quota:rate:t:u:m", 1, 0).Err() }, func() error { return w.Eval(ctx, `return redis.call('GET','other:secret')`, nil).Err() }} {
		if err := fn(); err == nil || !strings.Contains(strings.ToUpper(err.Error()), "PERM") {
			t.Fatalf("expected ACL denial: %v", err)
		}
	}
	endpoint := func(role string) string {
		u := url.URL{Scheme: "redis", Host: addr, User: url.UserPassword("trpc_"+role, "fixture-only"), Path: "/0"}
		return u.String()
	}
	coord, err := coordination.NewRedisCoordinator(coordination.RedisOptions{URL: endpoint("gateway"), KeyPrefix: "policy-test:channel-poll", LeaseTTL: time.Second, RenewInterval: 200 * time.Millisecond, RetryInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = coord.Close() }()
	lease, err := coord.Acquire(ctx, coordination.Key{AppName: "poll", UserID: "binding", SessionID: "group"})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	idem, err := idempotency.New(ctx, config.IdempotencyConfig{Backend: config.IdempotencyBackendRedis, RedisURL: endpoint("worker"), RedisPrefix: "policy-test", ProcessingTTL: time.Second, CompletedTTL: time.Minute, RenewInterval: 200 * time.Millisecond, PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = idem.Close() }()
	idemKey := idempotency.Key{AppName: "app", UserID: "user", SessionID: "session", MessageID: "message", ChannelBindingID: "binding"}
	begin, err := idem.Begin(ctx, idemKey, "fingerprint")
	if err != nil {
		t.Fatal("idempotency claim under ACL: ", err)
	}
	if err := begin.Attempt.Complete(ctx, idempotency.Result{Reply: "ok"}); err != nil {
		t.Fatal(err)
	}
	q, err := workqueue.NewRedisQueue(ctx, workqueue.RedisOptions{URL: endpoint("relay"), KeyPrefix: "policy-test", Stream: "agent-runs", Group: "workers", BlockTimeout: time.Millisecond, ClaimMinIdle: time.Second, MaxLen: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Close() }()
	if err := q.Publish(ctx, workqueue.AgentTask{Scope: runtimecontext.TutorialScope(), RequestID: "test", MessageID: "test", Text: "test"}); err != nil {
		t.Fatal("publish under relay ACL: ", err)
	}
	workerQueue, err := workqueue.NewRedisQueue(ctx, workqueue.RedisOptions{URL: endpoint("worker"), KeyPrefix: "policy-test", Stream: "agent-runs", Group: "workers", BlockTimeout: time.Millisecond, ClaimMinIdle: time.Second, MaxLen: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = workerQueue.Close() }()
	delivery, err := workerQueue.Receive(ctx)
	if err != nil {
		t.Fatal("consume under worker ACL: ", err)
	}
	// Exercise heartbeat XCLAIM/XPENDING under the real Redis ACL, not just
	// a fast ACK which might finish before the first renewal.
	time.Sleep(1200 * time.Millisecond)
	if delivery.(workqueue.LeasedDelivery).Context().Err() != nil {
		t.Fatal("delivery could not renew under worker ACL")
	}
	if err := delivery.Ack(ctx); err != nil {
		t.Fatal(err)
	}
	svc, err := storage.NewSessionService(ctx, config.SessionConfig{Backend: config.SessionBackendRedis, RedisURL: endpoint("worker"), RedisKeyPrefix: "policy-test", TTL: time.Minute})
	if err != nil {
		t.Fatal("session ACL: ", err)
	}
	defer func() { _ = svc.Close() }()
	key := session.Key{AppName: "t/tenant/a/app", UserID: "user", SessionID: "session"}
	sess, err := svc.CreateSession(ctx, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.AppendEvent(ctx, sess, &event.Event{ID: "test-event"}); err != nil {
		t.Fatal("append under ACL: ", err)
	}
	if _, err := svc.GetSession(ctx, key); err != nil {
		t.Fatal(err)
	}
}
