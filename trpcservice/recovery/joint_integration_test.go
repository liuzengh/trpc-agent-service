package recovery_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
	redis "github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/memory"
)

// This test owns every process/container and does not load a dotenv file or
// accept application DSNs. HTTP model/embedding responses are synthetic; the
// CLI roles, Runner, databases, queue, scoped tools and recovery are real.
func TestIsolatedTwoTenantTwoWorkerWorkflow(t *testing.T) {
	if os.Getenv("TEST_RECOVERY_DOCKER") != "1" {
		t.Skip("isolated Docker suite disabled")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	_, pgAddr := isolatedPostgres(t, ctx)
	redisAddr := jointContainer(t, ctx, "redis:7-alpine", "6379", "/data")
	qAddr := jointContainer(t, ctx, "qdrant/qdrant:v1.15.4", "6334", "/qdrant/storage")
	dsn := "postgres://drill@" + pgAddr + "/source?sslmode=disable"
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(db)
	jointEventually(t, ctx, func() bool { return db.PingContext(ctx) == nil })
	if err := database.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	var held atomic.Int32
	var modelCalls atomic.Int64
	blocked := make(chan struct{}, 1)
	stopModel := make(chan struct{})
	modelServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Model    string `json:"model"`
			Messages []struct {
				Role    string          `json:"role"`
				Content json.RawMessage `json:"content"`
			} `json:"messages"`
		}
		if json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&request) != nil {
			w.WriteHeader(400)
			return
		}
		if r.Header.Get("Authorization") != "Bearer key-"+request.Model {
			t.Error("tenant model credential crossed scopes")
			w.WriteHeader(401)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/embeddings" {
			_, _ = io.WriteString(w, `{"data":[{"index":0,"embedding":[1,0,0]}],"usage":{"prompt_tokens":3,"total_tokens":3}}`)
			return
		}
		if r.URL.Path != "/v1/chat/completions" || len(request.Messages) == 0 {
			w.WriteHeader(404)
			return
		}
		decode := func(raw json.RawMessage) string { var s string; _ = json.Unmarshal(raw, &s); return s }
		last := request.Messages[len(request.Messages)-1]
		modelCalls.Add(1)
		text := decode(last.Content)
		message := map[string]any{"role": "assistant", "content": text}
		finish := "stop"
		if last.Role == "user" {
			switch text {
			case "recall":
				answer := "no session fact"
				for _, m := range request.Messages {
					if s := decode(m.Content); m.Role == "user" && strings.HasPrefix(s, "seed:") {
						answer = s
					}
				}
				message["content"] = answer
			case "memory", "knowledge", "forbidden":
				name, args := "memory_load", `{}`
				if text == "knowledge" {
					name, args = "knowledge_search", `{"query":"shared query"}`
				}
				if text == "forbidden" {
					name, args = "echo", `{"message":"must-not-execute"}`
				}
				message = map[string]any{"role": "assistant", "tool_calls": []any{map[string]any{"id": "call-" + text, "type": "function", "function": map[string]any{"name": name, "arguments": args}}}}
				finish = "tool_calls"
			case "hold":
				if held.Add(1) == 1 {
					blocked <- struct{}{}
					select {
					case <-r.Context().Done():
					case <-stopModel:
					}
					return
				}
				message["content"] = "recovered by surviving worker"
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "joint-response", "object": "chat.completion", "model": request.Model, "choices": []any{map[string]any{"index": 0, "message": message, "finish_reason": finish}}, "usage": map[string]any{"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}})
	}))
	t.Cleanup(modelServer.Close)
	t.Cleanup(func() { close(stopModel) })
	qHost, qPortText, _ := net.SplitHostPort(qAddr)
	qPort, _ := strconv.Atoi(qPortText)
	var all controlplane.BootstrapData
	var grants []secret.Grant
	store := secret.StaticStore{"env://JOINT_MEMORY_DSN": dsn}
	for _, id := range []string{"a", "b"} {
		data := controlplane.DefaultBootstrapData()
		tenantID, appID, revID := "joint-"+id, "joint-app-"+id, "joint-rev-"+id
		tenant := data.Tenants[0]
		tenant.ID = tenantID
		tenant.DisplayName = tenantID
		app := data.Apps[0]
		app.ID = appID
		app.TenantID = tenantID
		app.StableRevisionID = revID
		rev := data.Revisions[0]
		rev.ID = revID
		rev.TenantID = tenantID
		rev.AppID = appID
		modelKey := "JOINT_MODEL_" + strings.ToUpper(id) + "_KEY"
		embeddingKey := "JOINT_EMBED_" + strings.ToUpper(id) + "_KEY"
		rev.ModelConfig = jointJSON(map[string]any{"source": "revision", "provider": "openai", "name": "chat-" + id, "base_url": modelServer.URL + "/v1", "api_key_ref": "env://" + modelKey, "timeout_seconds": 45})
		rev.KnowledgeConfig = jointJSON(map[string]any{"enabled": true, "embedding": map[string]any{"provider": "openai", "model": "embed-" + id, "base_url": modelServer.URL + "/v1", "dimensions": 3, "secret_ref": "env://" + embeddingKey}})
		rev.MemoryConfig = json.RawMessage(`{"direct_only":true,"auto_extract":false}`)
		allowed := []string{"memory_load"}
		if id == "a" {
			allowed = append(allowed, "echo")
		}
		rev.ToolPolicy = jointJSON(map[string]any{"allowed_tools": allowed, "max_tool_calls": 3})
		rev.Checksum = controlplane.RevisionChecksum(rev)
		binding := data.ChannelBindings[0]
		binding.ID = "joint-binding-" + id
		binding.TenantID = tenantID
		binding.AppID = appID
		binding.CallbackKey = "joint-http-" + id
		all.Tenants = append(all.Tenants, tenant)
		all.Apps = append(all.Apps, app)
		all.Revisions = append(all.Revisions, rev)
		all.ChannelBindings = append(all.ChannelBindings, binding)
		for _, resource := range []string{"session", "memory", "knowledge"} {
			b := data.BackendBindings[0]
			b.ID = resource + "-" + id
			b.TenantID = tenantID
			b.AppID = appID
			b.ResourceType = resource
			if resource == "memory" {
				b.BackendType = "postgres"
				b.SecretRef = "env://JOINT_MEMORY_DSN"
				b.Config = json.RawMessage(`{"table_name":"joint_memories"}`)
			}
			if resource == "knowledge" {
				b.BackendType = "qdrant"
				b.Config = jointJSON(map[string]any{"host": qHost, "port": qPort, "collection_name": "joint_shared_vectors", "dimensions": 3})
			}
			all.BackendBindings = append(all.BackendBindings, b)
		}
		grants = append(grants, secret.Grant{TenantID: tenantID, Purpose: secret.Memory, Reference: "env://JOINT_MEMORY_DSN"}, secret.Grant{TenantID: tenantID, Purpose: secret.Model, Reference: "env://" + modelKey}, secret.Grant{TenantID: tenantID, Purpose: secret.Embedding, Reference: "env://" + embeddingKey})
		store["env://"+modelKey] = "key-chat-" + id
		store["env://"+embeddingKey] = "key-embed-" + id
	}
	if err := controlplane.SeedBootstrap(ctx, db, all); err != nil {
		t.Fatal(err)
	}
	repo, _ := controlplane.NewPostgresRepository(db)
	mem, _ := storage.NewMemoryRouter(repo, store)
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(mem)
	kb, _ := storage.NewKnowledgeRouter(repo, store)
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(kb)
	for i, id := range []string{"a", "b"} {
		scope, _ := runtimecontext.NewScope("joint-"+id, "joint-app-"+id, "joint-rev-"+id, "http", "joint-binding-"+id)
		if err := mem.AddMemory(ctx, memory.UserKey{AppName: scope.StorageScope, UserID: "alice"}, "ONLY_"+strings.ToUpper(id)+"_MEMORY", nil); err != nil {
			t.Fatal(err)
		}
		if _, err := kb.UpsertDocument(ctx, scope, all.Revisions[i], storage.KnowledgeDocument{ID: "same-doc", Content: "ONLY_" + strings.ToUpper(id) + "_KNOWLEDGE"}); err != nil {
			t.Fatal(err)
		}
	}
	root, _ := filepath.Abs("../..")
	scratch := t.TempDir()
	binary := filepath.Join(scratch, "trpc-service")
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/trpc-service")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build isolated binary: %v %s", err, out)
	}
	baseEnv := []string{"PATH=" + os.Getenv("PATH"), "TRPC_AGENT_MODEL_PROVIDER=mock", "TRPC_AGENT_CONTROL_PLANE_BACKEND=postgres", "TRPC_AGENT_POSTGRES_URL=" + dsn, "TRPC_AGENT_POSTGRES_AUTO_MIGRATE=false", "TRPC_AGENT_POSTGRES_BOOTSTRAP_TUTORIAL=false", "TRPC_AGENT_SESSION_BACKEND=redis", "TRPC_AGENT_COORDINATOR_BACKEND=redis", "TRPC_AGENT_IDEMPOTENCY_BACKEND=redis", "TRPC_AGENT_QUEUE_BACKEND=redis", "TRPC_AGENT_QUOTA_BACKEND=redis", "REDIS_URL=redis://" + redisAddr + "/0", "REDIS_KEY_PREFIX=joint", "TRPC_AGENT_COORDINATOR_LEASE_TTL=2s", "TRPC_AGENT_COORDINATOR_RENEW_INTERVAL=200ms", "TRPC_AGENT_QUEUE_CLAIM_MIN_IDLE=1s", "TRPC_AGENT_IDEMPOTENCY_PROCESSING_TTL=2s", "TRPC_AGENT_IDEMPOTENCY_RENEW_INTERVAL=200ms", "TRPC_AGENT_OTEL_ENABLED=false", "TRPC_AGENT_ADMIN_ENABLED=false", "TRPC_AGENT_HTTP_API_ENABLED=true", "TRPC_AGENT_SECRET_GRANTS_JSON=" + string(jointJSON(grants))}
	for key, value := range store {
		baseEnv = append(baseEnv, strings.TrimPrefix(key, "env://")+"="+value)
	}
	var principals []any
	// Keep the pool layout explicit; exercise expiry of the result cache too.
	baseEnv = append(baseEnv, "TRPC_AGENT_WORKER_CONCURRENCY=4", "TRPC_AGENT_IDEMPOTENCY_COMPLETED_TTL=1s")
	tokens := map[string]string{}
	for _, id := range []string{"a", "b"} {
		tokens[id] = "joint-token-" + strings.Repeat(id, 32)
		principals = append(principals, map[string]any{"name": id, "token": tokens[id], "tenant_id": "joint-" + id, "binding_keys": []string{"joint-http-" + id}, "user_ids": []string{"alice"}})
	}
	baseEnv = append(baseEnv, "TRPC_AGENT_HTTP_API_PRINCIPALS_JSON="+string(jointJSON(principals)))
	start := func(role, node string, extra ...string) *jointProcess {
		return jointStart(t, ctx, binary, scratch, role, node, append(append([]string(nil), baseEnv...), extra...))
	}
	address := jointFreeAddress(t)
	start("gateway", "gateway", "TRPC_AGENT_ADDR="+address)
	start("relay", "relay")
	start("sender", "sender")
	workers := map[string]*jointProcess{"worker-one": start("worker", "one")}
	client := &http.Client{Timeout: 3 * time.Second}
	jointEventually(t, ctx, func() bool {
		resp, err := client.Get("http://" + address + "/readyz")
		if err != nil {
			return false
		}
		defer func(closer interface{ Close() error }) { _ = closer.Close() }(resp.Body)
		return resp.StatusCode == 200
	})
	post := func(actor, binding, msg, text string) (string, bool, int) {
		body := jointJSON(map[string]any{"binding_key": "joint-http-" + binding, "message_id": msg, "user_id": "alice", "session_id": "same-session", "chat_type": "direct", "message": text})
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+address+"/inbound", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+tokens[actor])
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func(closer interface{ Close() error }) { _ = closer.Close() }(resp.Body)
		var result struct {
			RequestID string `json:"request_id"`
			Duplicate bool   `json:"duplicate"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&result)
		return result.RequestID, result.Duplicate, resp.StatusCode
	}
	waitReply := func(requestID string) string {
		var reply, status string
		jointEventually(t, ctx, func() bool {
			err := db.QueryRowContext(ctx, `SELECT status,payload->>'text' FROM outbound_message WHERE request_id=$1`, requestID).Scan(&status, &reply)
			return err == nil && (status == "sent" || status == "dead")
		})
		if status != "sent" {
			t.Fatal("isolated reply failed")
		}
		return reply
	}
	call := func(id, msg, text string) string {
		rid, _, status := post(id, id, msg, text)
		if status != 202 || rid == "" {
			t.Fatalf("intake status=%d request=%q", status, rid)
		}
		return waitReply(rid)
	}
	for _, id := range []string{"a", "b"} {
		call(id, "seed-"+id, "seed:ONLY_"+strings.ToUpper(id)+"_SESSION")
	}
	workers["worker-two"] = start("worker", "two")
	for _, id := range []string{"a", "b"} {
		for _, kind := range []string{"recall", "memory", "knowledge"} {
			reply := call(id, kind+"-"+id, kind)
			suffix := strings.ToUpper(kind)
			if kind == "recall" {
				suffix = "SESSION"
			}
			if !strings.Contains(reply, "ONLY_"+strings.ToUpper(id)+"_"+suffix) {
				t.Fatalf("tenant %s missing %s evidence: %s", id, kind, reply)
			}
			other := "A"
			if id == "a" {
				other = "B"
			}
			if strings.Contains(reply, "ONLY_"+other+"_") {
				t.Fatal("tenant context leaked")
			}
		}
	}
	if _, _, status := post("a", "b", "cross-binding", "recall"); status != 403 {
		t.Fatalf("cross-tenant HTTP caller status=%d", status)
	}
	forbidden, _, status := post("b", "b", "forbidden-b", "forbidden")
	if status != 202 {
		t.Fatal("forbidden fixture not accepted")
	}
	_ = waitReply(forbidden)
	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM tool_execution WHERE request_id=$1 AND tool_name='echo'`, forbidden).Scan(&count); err != nil || count != 0 {
		t.Fatal("unlisted tenant tool executed")
	}
	heldRequest, _, status := post("a", "a", "hold-a", "hold")
	if status != 202 {
		t.Fatal("hold request not accepted")
	}
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal("model did not enter hold")
	}
	duplicatePending, pendingDuplicate, pendingStatus := post("a", "a", "hold-a", "hold")
	if pendingStatus != 202 || !pendingDuplicate || duplicatePending != heldRequest {
		t.Fatal("in-flight callback was not deduplicated")
	}
	var owner string
	if err := db.QueryRowContext(ctx, `SELECT worker_id FROM agent_run WHERE request_id=$1`, heldRequest).Scan(&owner); err != nil {
		t.Fatal(err)
	}
	dead := jointOwner(workers, owner)
	if dead == nil {
		t.Fatal("claim owner does not belong to test")
	}
	dead.stop(t, true)
	if reply := waitReply(heldRequest); reply != "recovered by surviving worker" {
		t.Fatal("surviving worker did not finish pending request")
	}
	var finalOwner string
	if err := db.QueryRowContext(ctx, `SELECT worker_id FROM agent_run WHERE request_id=$1`, heldRequest).Scan(&finalOwner); err != nil || jointOwner(workers, finalOwner) == nil || jointOwner(workers, finalOwner) == dead {
		t.Fatal("request not taken over by another Worker")
	}
	duplicate, wasDuplicate, status := post("a", "a", "hold-a", "hold")
	if status != 202 || !wasDuplicate || duplicate != heldRequest {
		t.Fatal("completed callback was not deduplicated")
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM outbound_message WHERE request_id=$1`, heldRequest).Scan(&count); err != nil || count != 1 {
		t.Fatal("takeover created multiple replies")
	}
	if !strings.Contains(call("b", "after-crash-b", "recall"), "ONLY_B_SESSION") {
		t.Fatal("other tenant session lost after crash")
	}
	// Redelivery after the cache TTL must restore PostgreSQL facts, not call
	// the model again. The synthetic queue is entirely owned by this fixture.
	time.Sleep(1100 * time.Millisecond)
	before := modelCalls.Load()
	var payload []byte
	if err := db.QueryRowContext(ctx, `SELECT payload FROM queue_outbox WHERE payload->>'request_id'=$1 ORDER BY created_at DESC LIMIT 1`, heldRequest).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	var task workqueue.AgentTask
	if err := json.Unmarshal(payload, &task); err != nil {
		t.Fatal(err)
	}
	queue, err := workqueue.NewRedisQueue(ctx, workqueue.RedisOptions{URL: "redis://" + redisAddr, KeyPrefix: "joint", Stream: "agent-runs", Group: "agent-workers", Consumer: "probe", BlockTimeout: time.Second, ClaimMinIdle: time.Second, MaxLen: 10000})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = queue.Close() }()
	if err = queue.Publish(ctx, task); err != nil {
		t.Fatal(err)
	}
	rc := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer func() { _ = rc.Close() }()
	jointEventually(t, ctx, func() bool {
		groups, e := rc.XInfoGroups(ctx, "joint:stream:agent-runs").Result()
		return e == nil && len(groups) == 1 && groups[0].Lag == 0 && groups[0].Pending == 0
	})
	if modelCalls.Load() != before {
		t.Fatal("completed redelivery executed model after cache expiry")
	}
	t.Log("two tenants, two pooled Workers, SIGKILL takeover, callback deduplication and completed redelivery after cache expiry passed; no live env/model/IM used")
}

func jointOwner(workers map[string]*jointProcess, owner string) *jointProcess {
	for id, process := range workers {
		if owner == id {
			return process
		}
		if lane, ok := strings.CutPrefix(owner, id+"-"); ok {
			n, err := strconv.Atoi(lane)
			if err == nil && n >= 0 && n < 4 {
				return process
			}
		}
	}
	return nil
}

func jointJSON(value any) json.RawMessage {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return raw
}
func jointEventually(t *testing.T, ctx context.Context, predicate func() bool) {
	t.Helper()
	for !predicate() {
		select {
		case <-ctx.Done():
			t.Fatal("isolated condition timed out")
		case <-time.After(100 * time.Millisecond):
		}
	}
}
func jointFreeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}
func jointContainer(t *testing.T, ctx context.Context, image, port, mount string) string {
	t.Helper()
	tag := fmt.Sprintf("joint-%x", time.Now().UnixNano())
	cid := docker(t, ctx, "run", "-d", "--pull=never", "--label", "trpc-agent.joint-test="+tag, "--tmpfs", mount+":rw", "-p", "127.0.0.1::"+port, image)
	if !regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(cid) {
		t.Fatal("invalid isolated container ID")
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		label, err := exec.CommandContext(cleanup, "docker", "inspect", "--format", `{{index .Config.Labels "trpc-agent.joint-test"}}`, cid).Output()
		if err != nil || strings.TrimSpace(string(label)) != tag {
			t.Error("test container ownership unknown; retained")
			return
		}
		if err := exec.CommandContext(cleanup, "docker", "rm", "-f", cid).Run(); err != nil {
			t.Error("owned container cleanup failed")
		}
	})
	address := docker(t, ctx, "port", cid, port+"/tcp")
	host, _, err := net.SplitHostPort(address)
	if err != nil || host != "127.0.0.1" {
		t.Fatal("isolated service not loopback")
	}
	jointEventually(t, ctx, func() bool {
		c, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err != nil {
			return false
		}
		_ = c.Close()
		return true
	})
	return address
}

type jointProcess struct {
	cmd     *exec.Cmd
	done    chan error
	stopped bool
}

func jointStart(t *testing.T, ctx context.Context, binary, scratch, role, node string, env []string) *jointProcess {
	t.Helper()
	logFile, err := os.OpenFile(filepath.Join(scratch, node+".log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, binary, "-env-file=", "-role", role)
	cmd.Dir = scratch
	cmd.Env = append(env, "TRPC_AGENT_QUEUE_CONSUMER="+node)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatal(err)
	}
	p := &jointProcess{cmd: cmd, done: make(chan error, 1)}
	go func() { p.done <- cmd.Wait(); _ = logFile.Close() }()
	t.Cleanup(func() {
		p.stop(t, false)
		if t.Failed() {
			raw, _ := os.ReadFile(filepath.Join(scratch, node+".log"))
			if len(raw) > 6000 {
				raw = raw[len(raw)-6000:]
			}
			t.Logf("isolated %s log: %s", node, raw)
		}
	})
	return p
}
func (p *jointProcess) stop(t *testing.T, kill bool) {
	t.Helper()
	if p.stopped {
		return
	}
	p.stopped = true
	if kill {
		_ = p.cmd.Process.Kill()
	} else {
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
	}
	select {
	case <-p.done:
	case <-time.After(8 * time.Second):
		_ = p.cmd.Process.Kill()
		select {
		case <-p.done:
		case <-time.After(2 * time.Second):
			t.Error("owned process failed to exit")
		}
	}
}
