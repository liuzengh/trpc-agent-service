package testinfra

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func TestRealRedisNetworkPartitionQuarantine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	lab := NewDockerLab(t)
	lab.Start(ctx)
	lab.WaitHealthy(ctx)
	opts, err := redis.ParseURL(lab.RedisURL(ctx))
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(opts)
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	lab.DisconnectRedis(ctx)
	failureCtx, failureCancel := context.WithTimeout(ctx, 5*time.Second)
	err = client.Ping(failureCtx).Err()
	failureCancel()
	if err == nil {
		t.Fatal("expected real proxy connection failure after Redis network disconnect")
	}
	if err := lab.RedisManagementPing(ctx); err != nil {
		t.Fatalf("Redis stopped unexpectedly during network partition: %v", err)
	}
	lab.ReconnectRedis(ctx)
	lab.WaitHealthy(ctx)
	client.Close()
	client = redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("proxy did not recover after network reconnect: %v", err)
	}
}

func TestRealRedisServiceRestartAndRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	lab := NewDockerLab(t)
	lab.Start(ctx)
	lab.WaitHealthy(ctx)
	opts, err := redis.ParseURL(lab.RedisURL(ctx))
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(opts)
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	lab.StopRedis(ctx)
	failureCtx, failureCancel := context.WithTimeout(ctx, 5*time.Second)
	err = client.Ping(failureCtx).Err()
	failureCancel()
	if err == nil {
		t.Fatal("expected real proxy connection failure after Redis stop")
	}
	lab.RestartRedis(ctx)
	lab.WaitHealthy(ctx)
	client.Close()
	client = redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("proxy did not recover after Redis restart: %v", err)
	}
}

func TestRealPostgresServiceRestartAndRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	lab := NewDockerLab(t)
	lab.Start(ctx)
	lab.WaitHealthy(ctx)
	pool, err := pgxpool.New(ctx, lab.PostgresURL(ctx))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	lab.StopPostgres(ctx)
	failureCtx, failureCancel := context.WithTimeout(ctx, 5*time.Second)
	err = pool.Ping(failureCtx)
	failureCancel()
	if err == nil {
		t.Fatal("expected PostgreSQL connection failure after stop")
	}
	lab.RestartPostgres(ctx)
	lab.WaitHealthy(ctx)
	if err := pool.Ping(ctx); err == nil {
		t.Fatal("expected existing pgxpool to retain the stopped container endpoint")
	}
	pool.Close()
	recovered, err := pgxpool.New(ctx, lab.PostgresURL(ctx))
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if err := recovered.Ping(ctx); err != nil {
		t.Fatalf("explicitly recreated pgxpool did not recover: %v", err)
	}
}

func TestIsolatedFaultRecoveryLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	lab := NewDockerLab(t)
	lab.Start(ctx)
	lab.WaitHealthy(ctx)
	redisID, postgresID := lab.ContainerIDs()
	if redisID == "" || postgresID == "" || lab.ProxyID() == "" || lab.NetworkID() == "" {
		t.Fatal("harness did not record isolated resource IDs")
	}
	proxyURL := lab.RedisURL(ctx)
	client, err := redis.ParseURL(proxyURL)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(client)
	defer rdb.Close()
	var pingErr error
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
		pingErr = rdb.Ping(ctx).Err()
		if pingErr == nil {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if pingErr != nil {
		t.Fatalf("proxy ping before partition: %v; proxy logs:\n%s", pingErr, lab.LogsSummary(ctx, lab.ProxyID()))
	}
	if err := lab.RedisManagementPing(ctx); err != nil {
		t.Fatalf("management ping before partition: %v", err)
	}
	lab.DisconnectRedis(ctx)
	partitionCtx, partitionCancel := context.WithTimeout(ctx, 5*time.Second)
	err = rdb.Ping(partitionCtx).Err()
	partitionCancel()
	if err == nil {
		t.Fatal("proxy client unexpectedly reached Redis after network disconnect")
	}
	if err := lab.RedisManagementPing(ctx); err != nil {
		t.Fatalf("Redis management path failed during network partition: %v", err)
	}
	lab.ReconnectRedis(ctx)
	lab.WaitHealthy(ctx)
	_ = rdb.Close()
	rdb = redis.NewClient(client)
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("proxy ping after reconnect: %v", err)
	}
	lab.StopRedis(ctx)
	lab.RestartRedis(ctx)
	lab.StopPostgres(ctx)
	lab.RestartPostgres(ctx)
	lab.WaitHealthy(ctx)
	if logs := lab.LogsSummary(ctx, redisID); len(logs) > 8192 {
		t.Fatal("Redis log summary is unbounded")
	}
}
