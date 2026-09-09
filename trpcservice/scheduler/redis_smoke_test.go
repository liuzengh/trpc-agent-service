package scheduler

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/control"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestPhase6Redis7MultiNodeSmoke(t *testing.T) {
	rawURL := strings.TrimSpace(os.Getenv("PHASE6_REDIS_SMOKE_URL"))
	if rawURL == "" {
		t.Skip("PHASE6_REDIS_SMOKE_URL is not set")
	}
	endpoint, messagingURL := redisURLsForSmoke(t, rawURL, 0)
	_, controlURL := redisURLsForSmoke(t, rawURL, 1)
	suffix := fmt.Sprint(time.Now().UnixNano())
	messagingPrefix := "phase6-smoke:messaging:" + suffix
	controlPrefix := "phase6-smoke:control:" + suffix
	claimPrefix := "phase6-smoke:claim:" + suffix
	defer cleanupRedisPrefix(t, messagingURL, messagingPrefix)
	defer cleanupRedisPrefix(t, messagingURL, claimPrefix)
	defer cleanupRedisPrefix(t, controlURL, controlPrefix)

	controlConfig := config.ControlPlaneConfig{
		RedisEndpoint: endpoint, RedisURL: controlURL, LogicalDB: 1, KeyPrefix: controlPrefix,
		PolicyCacheTTL: 5 * time.Minute, AuditRetention: time.Hour, MetricRetention: time.Hour,
		NodeHeartbeatInterval: 50 * time.Millisecond, NodeOfflineAfter: 400 * time.Millisecond,
		NodeAssignmentWaitBackoff: 25 * time.Millisecond, TraceDigestV2Enabled: true,
		NodeAssignmentEnabled: true, DevelopmentAllowSharedRedis: true,
	}
	rawRepository, err := control.NewRedisRepository(controlConfig)
	if err != nil {
		t.Fatal(err)
	}
	repository := control.NewInitializingRepository(rawRepository, []tenant.Tenant{{ID: "tenant-a", Enabled: true}}, []string{})
	defer repository.Close()
	if err := repository.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	store, err := messaging.NewStore(smokeMessagingConfig(messagingURL, messagingPrefix))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}

	controllers := make(map[string]*Controller, 2)
	for _, nodeID := range []string{"node-a", "node-b"} {
		controller, createErr := NewController(repository, store, &controlConfig, nodeID, "phase6-smoke")
		if createErr != nil {
			t.Fatal(createErr)
		}
		if startErr := controller.Start(context.Background()); startErr != nil {
			t.Fatal(startErr)
		}
		controllers[nodeID] = controller
		defer controller.Close()
	}
	waitForSmoke(t, 3*time.Second, func() bool {
		return controllers["node-a"].Ready(context.Background()) == nil && controllers["node-b"].Ready(context.Background()) == nil
	}, "both nodes to register")

	scheduling, err := New(repository, store, true)
	if err != nil {
		t.Fatal(err)
	}
	assignmentsByNode := make(map[string]control.NodeAssignment)
	for index := 0; index < 32 && len(assignmentsByNode) < 2; index++ {
		task := phase6SmokeTask(index)
		if _, created, submitErr := scheduling.Submit(context.Background(), task); submitErr != nil || !created {
			t.Fatalf("Submit task %d = (created=%t, err=%v)", index, created, submitErr)
		}
		assignment, getErr := repository.GetAssignment(context.Background(), task.InboxID())
		if getErr != nil {
			t.Fatal(getErr)
		}
		assignmentsByNode[assignment.NodeID] = assignment
	}
	if len(assignmentsByNode) != 2 {
		t.Fatalf("rendezvous scheduling did not use both nodes: %#v", assignmentsByNode)
	}

	offlineNode := "node-a"
	assignment := assignmentsByNode[offlineNode]
	if err := controllers[offlineNode].Close(); err != nil {
		t.Fatal(err)
	}
	reconciler := NewReconciler(repository, store)
	if err := reconciler.ReconcileOnce(context.Background(), 64); err != nil {
		t.Fatal(err)
	}
	reassigned, err := repository.GetAssignment(context.Background(), assignment.InboxID)
	if err != nil {
		t.Fatal(err)
	}
	if reassigned.NodeID != "node-b" || reassigned.Revision <= assignment.Revision {
		t.Fatalf("offline shared reassignment = %#v", reassigned)
	}

	firstLeader := NewReconciler(repository, store)
	secondLeader := NewReconciler(repository, store)
	if err := firstLeader.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer firstLeader.Close()
	if err := secondLeader.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer secondLeader.Close()
	waitForSmoke(t, 3*time.Second, func() bool { return firstLeader.IsLeader() != secondLeader.IsLeader() }, "one reconciler leader")
	leader, follower := firstLeader, secondLeader
	if secondLeader.IsLeader() {
		leader, follower = secondLeader, firstLeader
	}
	if err := leader.Close(); err != nil {
		t.Fatal(err)
	}
	waitForSmoke(t, 4*time.Second, follower.IsLeader, "reconciler leader takeover")

	claimStore, err := messaging.NewStore(smokeMessagingConfig(messagingURL, claimPrefix))
	if err != nil {
		t.Fatal(err)
	}
	defer claimStore.Close()
	if err := claimStore.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	claimTask := phase6SmokeTask(1000)
	claimTask.DigestVersion = 0
	claimTask.TraceParent = ""
	claimTask.PayloadDigest = claimTask.CanonicalDigest()
	if _, _, err := claimStore.Submit(context.Background(), claimTask); err != nil {
		t.Fatal(err)
	}
	if _, err := claimStore.ReadTask(context.Background(), "worker-old", time.Second); err != nil {
		t.Fatal(err)
	}
	time.Sleep(250 * time.Millisecond)
	claimed, err := claimStore.ClaimStale(context.Background(), "worker-new", 4)
	if err != nil || len(claimed) != 1 || claimed[0].Task.TaskID != claimTask.TaskID {
		t.Fatalf("XAUTOCLAIM = (%#v, %v)", claimed, err)
	}
	if err := claimStore.Recover(context.Background(), claimed[0], "worker-new"); err != nil {
		t.Fatal(err)
	}
	recovered, err := claimStore.ReadTask(context.Background(), "worker-new", time.Second)
	if err != nil || recovered.Task.TaskID != claimTask.TaskID {
		t.Fatalf("recovered consumer delivery = (%#v, %v)", recovered, err)
	}
}

func smokeMessagingConfig(redisURL, prefix string) config.MessagingConfig {
	return config.MessagingConfig{
		RedisURL: redisURL, KeyPrefix: prefix, SessionFencing: "legacy",
		LeaseDuration: 150 * time.Millisecond, HeartbeatInterval: 40 * time.Millisecond,
		InitialBackoff: 20 * time.Millisecond, MaxBackoff: 100 * time.Millisecond, MaxAttempts: 3,
		InboxRetention: time.Minute, ReplyWaitTimeout: time.Second,
	}
}

func phase6SmokeTask(index int) message.ExecutionTask {
	task := testTask(fmt.Sprintf("phase6-task-%d", index), fmt.Sprintf("phase6-message-%d", index))
	task.TraceID = fmt.Sprintf("%032x", index+1)
	task.TraceParent = "00-" + task.TraceID + "-0123456789abcdef-01"
	task.DigestVersion = 2
	task.PayloadDigest = task.CanonicalDigest()
	return task
}

func redisURLsForSmoke(t *testing.T, raw string, db int) (string, string) {
	t.Helper()
	normalized, err := config.NormalizeRedisURL(raw)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(normalized)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = fmt.Sprintf("/%d", db)
	redisURL := parsed.String()
	parsed.Path = ""
	return parsed.String(), redisURL
}

func cleanupRedisPrefix(t *testing.T, redisURL, prefix string) {
	t.Helper()
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Errorf("parse cleanup Redis URL: %v", err)
		return
	}
	client := redis.NewClient(options)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var cursor uint64
	for {
		keys, next, scanErr := client.Scan(ctx, cursor, prefix+"*", 100).Result()
		if scanErr != nil {
			t.Errorf("scan smoke keys: %v", scanErr)
			return
		}
		if len(keys) > 0 {
			if err := client.Del(ctx, keys...).Err(); err != nil {
				t.Errorf("delete smoke keys: %v", err)
				return
			}
		}
		cursor = next
		if cursor == 0 {
			return
		}
	}
}

func waitForSmoke(t *testing.T, timeout time.Duration, ready func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}
