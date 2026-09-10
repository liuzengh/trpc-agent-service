package memorydriver

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/redis/go-redis/v9"
	agentmemory "trpc.group/trpc-go/trpc-agent-go/memory"
)

func TestDigestIsOrderIndependentAndTenantScoped(t *testing.T) {
	now := time.Unix(1, 0).UTC()
	first := Image{Entry: agentmemory.Entry{ID: "one", AppName: "tenant-a/app", UserID: "u", Memory: &agentmemory.Memory{Memory: "one"}, CreatedAt: now, UpdatedAt: now}}
	second := Image{Entry: agentmemory.Entry{ID: "two", AppName: "tenant-a/app", UserID: "u", Memory: &agentmemory.Memory{Memory: "two"}, CreatedAt: now, UpdatedAt: now}}
	left, err := Digest([]Image{first, second})
	if err != nil {
		t.Fatal(err)
	}
	right, err := Digest([]Image{second, first})
	if err != nil || left != right || len(left) != 64 {
		t.Fatalf("digest = %q / %q / %v", left, right, err)
	}
	invalid := first
	invalid.Entry.AppName = "wrong"
	if _, err := Digest([]Image{invalid}); err != runtime.ErrTenantScope {
		t.Fatalf("invalid app digest error = %v, want tenant scope", err)
	}
}

func TestRedisSourceSkipsOtherTenantEntries(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	now := time.Unix(2, 0).UTC()
	local := agentmemory.Entry{ID: "local", AppName: "tenant-a/app", UserID: "u", Memory: &agentmemory.Memory{Memory: "local"}, CreatedAt: now, UpdatedAt: now}
	foreign := agentmemory.Entry{ID: "foreign", AppName: "tenant-b/app", UserID: "u", Memory: &agentmemory.Memory{Memory: "foreign"}, CreatedAt: now, UpdatedAt: now}
	localJSON, _ := json.Marshal(local)
	foreignJSON, _ := json.Marshal(foreign)
	if err := client.HSet(context.Background(), "trpc-memory:mem:shared", map[string]string{"local": string(localJSON), "foreign": string(foreignJSON)}).Err(); err != nil {
		t.Fatal(err)
	}
	images, _, err := (RedisSource{Client: client, KeyPrefix: "trpc-memory"}).ExportTenant(context.Background(), "tenant-a")
	if err != nil || len(images) != 1 || images[0].Entry.ID != "local" {
		t.Fatalf("images=%#v err=%v", images, err)
	}
}

func TestRedisTargetApplyUserReplacesOnlyOneUserImage(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()
	now := time.Unix(3, 0).UTC()
	key := UserKey{TenantID: "tenant-a", AppName: "tenant-a/app", UserID: "u"}
	otherKey := redisMemoryKey("trpc-memory", "tenant-a/app", "other")
	if err := client.HSet(ctx, otherKey, "other", `{"untouched":true}`).Err(); err != nil {
		t.Fatal(err)
	}
	image := Image{Entry: agentmemory.Entry{ID: "one", AppName: key.AppName, UserID: key.UserID, Memory: &agentmemory.Memory{Memory: "one"}, CreatedAt: now, UpdatedAt: now}}
	target := RedisTarget{Client: client, KeyPrefix: "trpc-memory"}
	digest, err := target.ApplyUser(ctx, key, []Image{image})
	if err != nil {
		t.Fatal(err)
	}
	want, _ := Digest([]Image{image})
	if digest != want {
		t.Fatalf("digest=%q want=%q", digest, want)
	}
	values, err := client.HGetAll(ctx, redisMemoryKey("trpc-memory", key.AppName, key.UserID)).Result()
	if err != nil || len(values) != 1 {
		t.Fatalf("user values=%#v err=%v", values, err)
	}
	if count, err := client.HLen(ctx, otherKey).Result(); err != nil || count != 1 {
		t.Fatalf("other user modified: count=%d err=%v", count, err)
	}
	if _, err := target.ApplyUser(ctx, key, nil); err != nil {
		t.Fatal(err)
	}
	if exists, err := client.Exists(ctx, redisMemoryKey("trpc-memory", key.AppName, key.UserID)).Result(); err != nil || exists != 0 {
		t.Fatalf("user key exists=%d err=%v", exists, err)
	}
}
