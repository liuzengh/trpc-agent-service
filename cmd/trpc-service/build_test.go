package main

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Local compose services (PG, Redis, MinIO) back the success paths of the
// build*/start* helpers; unreachable endpoints exercise their degrade paths.
const (
	testPGDSN     = "postgres://trpc:trpc-dev-only@localhost:5432/trpc?sslmode=disable"
	testRedisAddr = "localhost:6379"
	testMinIOAddr = "localhost:9000"
)

// writeSecret drops one secret file into a temp secrets dir.
func writeSecret(t *testing.T, dir, name, value string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(value+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

// testSecretsDir creates a secrets dir holding every ref the build*/start*
// helpers resolve, so their success paths run without network dependencies.
func testSecretsDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for name, value := range map[string]string{
		"model-key":    "sk-model-test",
		"embedder-key": "sk-embed-test",
		"kms-token":    "kms-bearer-token",
		"wecom-token":  "wecom-token-value",
		"wecom-aeskey": "wecom-aeskey-value",
		"wecom-secret": "wecom-secret-value",
		"wxkf-token":   "wxkf-token-value",
		"wxkf-aeskey":  "wxkf-aeskey-value",
		"wxkf-secret":  "wxkf-secret-value",
		"s3-accesskey": "trpc",
		"s3-secretkey": "trpc-dev-only",
	} {
		writeSecret(t, dir, name, value)
	}
	return dir
}

func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := storage.NewPG(context.Background(), testPGDSN)
	if err != nil {
		t.Skipf("postgres unavailable (%v)", err)
	}
	t.Cleanup(func() { pool.Close() })
	return pool
}

func newTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: testRedisAddr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis unavailable (%v)", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func TestBuildSecretResolverFileBackend(t *testing.T) {
	dir := testSecretsDir(t)
	r, err := buildSecretResolver(context.Background(),
		config.Config{SecretsDir: dir, SecretCacheTTL: "30s"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.Resolve(context.Background(), "model-key")
	if err != nil || got != "sk-model-test" {
		t.Fatalf("file backend resolve: got %q, %v", got, err)
	}
}

// TestBuildSecretResolverKMSBackend verifies the KMS sidecar wiring: the
// bootstrap token comes from the file resolver, secrets from the endpoint.
func TestBuildSecretResolverKMSBackend(t *testing.T) {
	dir := testSecretsDir(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "Bearer kms-bearer-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if req.URL.Path == "/v1/secrets/db-password" {
			_, _ = io.WriteString(w, "pg-pa55\n")
			return
		}
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	r, err := buildSecretResolver(context.Background(), config.Config{
		SecretsDir:         dir,
		SecretResolverType: "kms",
		KMSEndpoint:        srv.URL,
		KMSTokenRef:        "kms-token",
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.Resolve(context.Background(), "db-password")
	if err != nil || got != "pg-pa55" {
		t.Fatalf("kms backend resolve: got %q, %v", got, err)
	}
}

// TestBuildSecretResolverKMSFailsClosed pins the refusal: a KMS backend that
// cannot be built stops startup instead of degrading to the file resolver.
// Both cases are configuration errors that retrying cannot fix.
func TestBuildSecretResolverKMSFailsClosed(t *testing.T) {
	dir := testSecretsDir(t)
	cases := []struct {
		name string
		cfg  config.Config
		want string
	}{
		{"missing bootstrap token", config.Config{SecretsDir: dir, SecretResolverType: "kms",
			KMSTokenRef: "no-such-token", KMSEndpoint: "http://127.0.0.1:1"}, "kms bootstrap token"},
		{"empty endpoint", config.Config{SecretsDir: dir, SecretResolverType: "kms",
			KMSTokenRef: "kms-token", KMSEndpoint: ""}, "kms resolver"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := buildSecretResolver(context.Background(), tc.cfg)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want an error naming %q, got %v", tc.want, err)
			}
			if r != nil {
				// The point of the refusal: nothing may serve the plaintext
				// files after kms was asked for.
				t.Fatal("a refused KMS backend must not hand back a file-backed resolver")
			}
		})
	}
}

func TestBuildSummarizer(t *testing.T) {
	ctx := context.Background()
	if sm := buildSummarizer(ctx, config.Config{ModelAPIKeyRef: "no-such-ref"},
		config.NewFileResolver(t.TempDir())); sm != nil {
		t.Fatal("unresolvable model key must disable session summaries")
	}

	cfg := config.Config{
		SecretsDir:            testSecretsDir(t),
		ModelAPIKeyRef:        "model-key",
		ModelName:             "test-model",
		ModelBaseURL:          "http://127.0.0.1:1",
		SummaryEventThreshold: "35",
	}
	if sm := buildSummarizer(ctx, cfg, config.NewFileResolver(cfg.SecretsDir)); sm == nil {
		t.Fatal("resolvable model key must build the summarizer")
	}
}

func TestBuildEmbedder(t *testing.T) {
	ctx := context.Background()
	if e, dim, err := buildEmbedder(ctx, config.Config{}, config.NewFileResolver(t.TempDir())); e != nil || dim != 0 || err != nil {
		t.Fatalf("unset TRPC_EMBEDDER_MODEL must yield (nil, 0, nil), got (%v, %d, %v)", e, dim, err)
	}

	dir := testSecretsDir(t)
	secrets := config.NewFileResolver(dir)
	if _, _, err := buildEmbedder(ctx, config.Config{
		EmbedderModel: "text-embed", EmbedderKeyRef: "no-such-ref",
	}, secrets); err == nil {
		t.Fatal("unresolvable embedder key must error")
	}
	if _, _, err := buildEmbedder(ctx, config.Config{
		EmbedderModel: "text-embed", EmbedderKeyRef: "embedder-key", EmbedderDim: "abc",
	}, secrets); err == nil {
		t.Fatal("invalid TRPC_EMBEDDER_DIMENSION must error")
	}
	e, dim, err := buildEmbedder(ctx, config.Config{
		EmbedderModel:   "text-embed",
		EmbedderKeyRef:  "embedder-key",
		EmbedderDim:     "768",
		EmbedderBaseURL: "http://127.0.0.1:1",
	}, secrets)
	if err != nil || e == nil || dim != 768 {
		t.Fatalf("valid embedder config must build, got (%v, %d, %v)", e, dim, err)
	}
}

func TestBuildKnowledge(t *testing.T) {
	ctx := context.Background()
	dir := testSecretsDir(t)
	secrets := config.NewFileResolver(dir)

	if kb := buildKnowledge(ctx, config.Config{}, secrets); kb != nil {
		t.Fatal("no embedder config must leave knowledge disabled")
	}
	if kb := buildKnowledge(ctx, config.Config{
		EmbedderModel: "text-embed", EmbedderKeyRef: "no-such-ref",
	}, secrets); kb != nil {
		t.Fatal("embedder key error must leave knowledge disabled")
	}
	if kb := buildKnowledge(ctx, config.Config{
		EmbedderModel:  "text-embed",
		EmbedderKeyRef: "embedder-key",
		EmbedderDim:    "768",
		PGDSN:          "postgres://trpc:trpc-dev-only@localhost:1/trpc?sslmode=disable",
		KnowledgeTable: "knowledge_embeddings",
	}, secrets); kb != nil {
		t.Fatal("unreachable pgvector must leave knowledge disabled")
	}

	// The pgvector success path needs a reachable PG (compose runs the
	// pgvector image); the guard doubles as the skip probe.
	_ = newTestPool(t)
	kb := buildKnowledge(ctx, config.Config{
		EmbedderModel:  "text-embed",
		EmbedderKeyRef: "embedder-key",
		EmbedderDim:    "768",
		PGDSN:          testPGDSN,
		KnowledgeTable: "knowledge_embeddings",
	}, secrets)
	if kb == nil {
		t.Fatal("valid embedder + reachable PG must build the knowledge base")
	}
}

func TestBuildArtifact(t *testing.T) {
	if svc := buildArtifact(config.Config{}, nil); svc != nil {
		t.Fatal("no S3 endpoint must disable artifacts")
	}

	dir := testSecretsDir(t)
	secrets := config.NewFileResolver(dir)
	if svc := buildArtifact(config.Config{
		S3Endpoint: "127.0.0.1:1", S3Bucket: "bucket",
		S3AccessKeyRef: "s3-accesskey", S3SecretKeyRef: "s3-secretkey",
	}, secrets); svc != nil {
		t.Fatal("unreachable endpoint must degrade to nil")
	}
	if svc := buildArtifact(config.Config{
		S3Endpoint: testMinIOAddr, S3Bucket: "bucket",
		S3AccessKeyRef: "no-such-ref", S3SecretKeyRef: "s3-secretkey",
	}, secrets); svc != nil {
		t.Fatal("unresolvable credentials must degrade to nil")
	}

	conn, err := net.DialTimeout("tcp", testMinIOAddr, 2*time.Second)
	if err != nil {
		t.Skipf("minio unavailable (%v)", err)
	}
	_ = conn.Close()
	svc := buildArtifact(config.Config{
		S3Endpoint: testMinIOAddr, S3Bucket: "trpc-cmd-test-artifacts",
		S3AccessKeyRef: "s3-accesskey", S3SecretKeyRef: "s3-secretkey",
	}, secrets)
	if _, ok := svc.(*storage.S3ArtifactService); !ok {
		t.Fatalf("reachable MinIO must build the artifact store, got %T", svc)
	}
}

func TestBuildSessionServicesWithoutPG(t *testing.T) {
	ctx := context.Background()
	// TRPC_SESSION_BACKEND=postgres but PG unreachable: degrade to redis.
	byType, def, err := buildSessionServices(ctx, config.Config{
		RedisAddr: testRedisAddr, SessionBackend: "postgres",
	}, nil, nil)
	if err != nil {
		t.Fatalf("redis-only session services must build without PG: %v", err)
	}
	if len(byType) != 1 {
		t.Fatalf("expected only the redis backend, got %v", byType)
	}
	if _, ok := byType["redis"].(*storage.MetricsSessionService); !ok {
		t.Fatalf("session services must be latency-metered, got %T", byType["redis"])
	}
	if def != "redis" {
		t.Fatalf("unavailable postgres backend must default to redis, got %q", def)
	}
	for _, s := range byType {
		_ = s.Close()
	}

	// An unparsable redis address fails the redis session service outright.
	_, _, err = buildSessionServices(ctx, config.Config{
		RedisAddr: "bad redis addr",
	}, nil, nil)
	if err == nil {
		t.Fatal("an unparsable redis address must fail the redis session service")
	}
}

func TestBuildSessionServicesWithPG(t *testing.T) {
	pool := newTestPool(t)
	dir := testSecretsDir(t)
	cfg := config.Config{
		RedisAddr:             testRedisAddr,
		SessionBackend:        "postgres",
		ModelAPIKeyRef:        "model-key",
		ModelName:             "test-model",
		ModelBaseURL:          "http://127.0.0.1:1",
		SummaryEventThreshold: "20",
	}
	byType, def, err := buildSessionServices(context.Background(), cfg, pool, config.NewFileResolver(dir))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := byType["redis"]; !ok {
		t.Fatal("redis session service must always be built")
	}
	if _, ok := byType["postgres"]; !ok {
		t.Fatal("postgres session service must be built with a live pool")
	}
	// Both backends are metered, and the decorator unwraps back to the
	// concrete service the migrator type-asserts.
	pgWrapped, ok := byType["postgres"].(*storage.MetricsSessionService)
	if !ok {
		t.Fatalf("session services must be latency-metered, got %T", byType["postgres"])
	}
	if pgWrapped.Backend != "postgres" {
		t.Fatalf("backend label = %q, want postgres", pgWrapped.Backend)
	}
	if _, ok := storage.UnwrapSessionService(pgWrapped).(*storage.PGSessionService); !ok {
		t.Fatalf("unwrap must restore *PGSessionService, got %T", storage.UnwrapSessionService(pgWrapped))
	}
	if def != "postgres" {
		t.Fatalf("default must follow TRPC_SESSION_BACKEND, got %q", def)
	}
	for _, s := range byType {
		_ = s.Close()
	}

	// An unknown backend name falls back to redis with a warning.
	byType, def, err = buildSessionServices(context.Background(), config.Config{
		RedisAddr: testRedisAddr, SessionBackend: "bogus",
	}, pool, config.NewFileResolver(dir))
	if err != nil {
		t.Fatal(err)
	}
	if def != "redis" {
		t.Fatalf("unknown backend must default to redis, got %q", def)
	}
	if _, ok := byType["bogus"]; ok {
		t.Fatal("an unknown backend must not be registered")
	}
	for _, s := range byType {
		_ = s.Close()
	}
}

func TestBuildProcessorEchoFallback(t *testing.T) {
	ctx := context.Background()
	// Guarded.Process audits through the async lane, so the test supplies a
	// live auditor.
	pool := newTestPool(t)
	auditor := storage.NewAuditor(pool)
	auditor.Start()
	defer auditor.Close()

	// No session backends: the echo fallback wrapped in the guardrail, with
	// an invalid TRPC_MODEL_PRICES payload (warn path) on top.
	proc, cleanup := buildProcessor(ctx, config.Config{ModelPrices: "{not json"},
		nil, auditor, nil, nil, nil, nil, "", nil, nil)
	guarded, ok := proc.(*agent.Guarded)
	if !ok {
		t.Fatalf("processor must be a Guarded wrapper, got %T", proc)
	}
	if _, ok := guarded.Inner.(agent.EchoProcessor); !ok {
		t.Fatalf("echo fallback expected, got %T", guarded.Inner)
	}
	cleanup()

	// End to end: a plain message echoes back through the guardrail.
	out, err := guarded.Process(ctx, channels.InboundMessage{
		Channel: "mock", MsgID: "m1", SessionKey: "dm:mock:s1", Text: "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Text != "echo: hello" || out.MsgID != "m1" {
		t.Fatalf("echo broken: %+v", out)
	}

	// Valid prices install the cost table.
	_, cleanup2 := buildProcessor(ctx, config.Config{ModelPrices: `{"cmd-test-model":[0.5,1.5]}`},
		nil, auditor, nil, nil, nil, nil, "", nil, nil)
	cleanup2()
	if got := agent.CostUSD("cmd-test-model", 1_000_000, 0); got != 0.5 {
		t.Fatalf("TRPC_MODEL_PRICES must feed cost accounting, got %v", got)
	}
}

func TestBuildProcessorFull(t *testing.T) {
	pool := newTestPool(t)
	rdb := newTestRedis(t)
	ctx := context.Background()
	dir := testSecretsDir(t)
	secrets := config.NewFileResolver(dir)
	resolver := tenant.NewResolver(tenant.NewPGStore(pool))

	cfg := config.Config{
		RedisAddr:      testRedisAddr,
		ModelName:      "test-model",
		ModelBaseURL:   "http://127.0.0.1:1",
		ModelAPIKeyRef: "model-key",
		ModelTimeout:   "bogus", // exercises the default fallback
		ModelPrices:    `{"cmd-test-model":[0.5,1.5]}`,
		// Embeddings endpoint: wires the semantic-recall embedder onto the
		// memory service and the pgvector knowledge base.
		EmbedderModel:   "text-embed",
		EmbedderKeyRef:  "embedder-key",
		EmbedderDim:     "768",
		EmbedderBaseURL: "http://127.0.0.1:1",
		PGDSN:           testPGDSN,
		KnowledgeTable:  "knowledge_embeddings",
	}
	kb := buildKnowledge(ctx, cfg, secrets)
	if kb == nil {
		t.Fatal("valid embedder + reachable PG must build the knowledge base")
	}
	sessByType, def, err := buildSessionServices(ctx, cfg, pool, secrets)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, s := range sessByType {
			_ = s.Close()
		}
	}()

	proc, cleanup := buildProcessor(ctx, cfg, rdb, nil, pool, kb, resolver,
		sessByType, def, nil, secrets)
	guarded, ok := proc.(*agent.Guarded)
	if !ok {
		t.Fatalf("processor must be a Guarded wrapper, got %T", proc)
	}
	if guarded.PolicyFor == nil {
		t.Fatal("a tenant resolver must install PolicyFor")
	}
	if guarded.StateMark == nil {
		t.Fatal("session backends must install StateMark")
	}

	// Unknown tenant: the policy lookup must surface the routing error.
	if _, err := guarded.PolicyFor(ctx, "no-such-tenant-cmd-test"); err == nil {
		t.Fatal("PolicyFor for an unknown tenant must fail")
	}
	// Seeded tenant: the platform baseline policy resolves.
	const seedTenant = "00000000-0000-0000-0000-000000000001"
	if _, err := guarded.PolicyFor(ctx, seedTenant); err != nil {
		t.Fatalf("PolicyFor for a known tenant must resolve: %v", err)
	}

	// Unknown app: the state mark must surface the routing error, not swallow it.
	if err := guarded.StateMark(ctx, channels.InboundMessage{
		AppID: "no-such-app-cmd-test", UserID: "u1", SessionKey: "dm:mock:s1",
	}, "recall_event", []byte("x")); err == nil {
		t.Fatal("StateMark for an unknown app must fail")
	}
	cleanup()

	// Without a valid embedder the memory service runs without semantic recall.
	cfgNoEmb := cfg
	cfgNoEmb.EmbedderModel = ""
	proc2, cleanup3 := buildProcessor(ctx, cfgNoEmb, rdb, nil, pool, nil, resolver,
		sessByType, def, nil, secrets)
	if _, ok := proc2.(*agent.Guarded); !ok {
		t.Fatalf("processor must be a Guarded wrapper, got %T", proc2)
	}
	cleanup3()
}

func TestStartWecom(t *testing.T) {
	secrets := config.NewFileResolver(testSecretsDir(t))
	if wc := startWecom(config.Config{}, nil, secrets, nil); wc != nil {
		t.Fatal("unset TRPC_WECOM_CORP_ID must disable the channel")
	}
	base := config.Config{
		WecomCorpID: "corp", WecomTokenRef: "wecom-token", WecomAESKeyRef: "wecom-aeskey",
		WecomAPIBase: "https://127.0.0.1:1",
	}
	if wc := startWecom(config.Config{
		WecomCorpID: "corp", WecomAgentID: "1000002", WecomAPIBase: "http://127.0.0.1:1",
	}, nil, secrets, nil); wc != nil {
		t.Fatal("an http API base must disable the channel (token rides the query)")
	}
	for _, agentID := range []string{"not-a-number", "0", ""} {
		cfg := base
		cfg.WecomAgentID = agentID
		if wc := startWecom(cfg, nil, secrets, nil); wc != nil {
			t.Fatalf("invalid TRPC_WECOM_AGENT_ID %q must disable the channel", agentID)
		}
	}
	if wc := startWecom(config.Config{
		WecomCorpID: "corp", WecomAgentID: "1000002",
		WecomTokenRef: "missing", WecomAESKeyRef: "missing",
	}, nil, config.NewFileResolver(t.TempDir()), nil); wc != nil {
		t.Fatal("unresolvable secrets must disable the channel")
	}

	wc := startWecom(config.Config{
		WecomCorpID: "corp", WecomAgentID: "1000002",
		WecomTokenRef: "wecom-token", WecomAESKeyRef: "wecom-aeskey",
		WecomAPIBase: "https://127.0.0.1:1",
	}, nil, secrets, nil)
	if wc == nil {
		t.Fatal("valid config must enable the wecom channel")
	}
	if wc.Name() != "wecom" {
		t.Fatalf("channel name: got %q", wc.Name())
	}
}

// stubCursors is a no-op wxkf.CursorStore for configs that never pull.
type stubCursors struct{}

func (stubCursors) Get(context.Context, string) (string, error) { return "", nil }
func (stubCursors) Set(context.Context, string, string) error   { return nil }

func TestStartWxkf(t *testing.T) {
	secrets := config.NewFileResolver(testSecretsDir(t))
	if ch := startWxkf(config.Config{}, secrets, nil, nil); ch != nil {
		t.Fatal("unset wxkf env must disable the channel")
	}
	if ch := startWxkf(config.Config{WxkfCorpID: "corp"}, secrets, nil, nil); ch != nil {
		t.Fatal("missing KF account must disable the channel")
	}
	if ch := startWxkf(config.Config{
		WxkfCorpID: "corp", WxkfKfAccount: "wk0001",
		WxkfTokenRef: "missing", WxkfAESKeyRef: "missing",
	}, config.NewFileResolver(t.TempDir()), nil, stubCursors{}); ch != nil {
		t.Fatal("unresolvable secrets must disable the channel")
	}
	if ch := startWxkf(config.Config{
		WxkfCorpID: "corp", WxkfKfAccount: "wk0001", WxkfAPIBase: "http://127.0.0.1:1",
	}, secrets, nil, stubCursors{}); ch != nil {
		t.Fatal("an http API base must disable the channel (secret rides the query)")
	}

	cfg := config.Config{
		WxkfCorpID: "corp", WxkfKfAccount: "wk0001",
		WxkfTokenRef: "wxkf-token", WxkfAESKeyRef: "wxkf-aeskey",
		WxkfAPIBase: "https://127.0.0.1:1",
	}
	ch := startWxkf(cfg, secrets, nil, stubCursors{})
	if ch == nil {
		t.Fatal("valid config must enable the wxkf channel")
	}
	if ch.Name() != "wxkf" {
		t.Fatalf("channel name: got %q", ch.Name())
	}
}

func TestStartWecomws(t *testing.T) {
	if ws := startWecomws(config.Config{}, nil, nil); ws != nil {
		t.Fatal("unset TRPC_WECOMWS_ADDR must disable the channel")
	}
	cfg := config.Config{
		WecomwsAddr:           "wss://openws.example.invalid",
		WecomwsPingInterval:   "5s",
		WecomwsSegmentBytes:   "256",
		WecomwsResyncInterval: "3s",
		WecomwsLeaderTTL:      "7s",
	}
	if ws := startWecomws(cfg, nil, nil); ws != nil {
		t.Fatal("a nil secret resolver must disable the channel")
	}
	ws := startWecomws(cfg, config.NewFileResolver(testSecretsDir(t)), nil)
	if ws == nil {
		t.Fatal("a configured addr must enable the wecomws channel")
	}
	if ws.Name() != "wecomws" {
		t.Fatalf("channel name: got %q", ws.Name())
	}
}

// TestWSRoutesByChannel verifies the resolver→wecomws projection against the
// real tenant tables, plus the failed-snapshot error path.
func TestWSRoutesByChannel(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()

	const (
		tnID   = "00000000-0000-0000-0000-0000000000c1"
		appID  = "00000000-0000-0000-0000-0000000001c1"
		bindID = "00000000-0000-0000-0000-0000000002c1"
	)
	if _, err := pool.Exec(ctx,
		`INSERT INTO tenant (id, name, status) VALUES ($1, 'cmd-ws-route-test', 'active')
		 ON CONFLICT (id) DO NOTHING`, tnID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO agent_app (id, tenant_id, name, agent_type, config, version, status)
		 VALUES ($1, $2, 'cmd-ws-route-test', 'llm', '{}', 1, 'published')
		 ON CONFLICT DO NOTHING`, appID, tnID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO channel_binding (id, tenant_id, channel, app_id, webhook_path, config, status)
		 VALUES ($1, $2, 'wecomws', $3, '/callback/wecomws/cmd-test', $4::jsonb, 'active')
		 ON CONFLICT (id) DO NOTHING`, bindID, tnID, appID, `{"bot_id":"bot-1","secret_ref":"wecom-secret"}`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM channel_binding WHERE id = $1`, bindID)
		_, _ = pool.Exec(ctx, `DELETE FROM agent_app WHERE id = $1`, appID)
		_, _ = pool.Exec(ctx, `DELETE FROM tenant WHERE id = $1`, tnID)
	})

	resolver := tenant.NewResolver(tenant.NewPGStore(pool))
	bindings, err := wsRoutes{resolver}.RoutesByChannel(ctx, "wecomws")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, b := range bindings {
		if b.ID != bindID {
			continue
		}
		found = true
		if b.WebhookPath != "/callback/wecomws/cmd-test" {
			t.Errorf("webhook path not projected: %q", b.WebhookPath)
		}
		if len(b.Config) == 0 {
			t.Error("binding config jsonb must be projected")
		}
	}
	if !found {
		t.Fatalf("wecomws binding missing from routes: %+v", bindings)
	}

	// A store that cannot load (closed pool) surfaces as an error.
	p2, err := storage.NewPG(ctx, testPGDSN)
	if err != nil {
		t.Fatal(err)
	}
	p2.Close()
	broken := wsRoutes{tenant.NewResolver(tenant.NewPGStore(p2))}
	if _, err := broken.RoutesByChannel(ctx, "wecomws"); err == nil {
		t.Fatal("a failed snapshot load must surface as an error")
	}
}

func TestStartPGConsumers(t *testing.T) {
	ctx := context.Background()

	// PG unreachable: the lazy pool keeps audit/routing alive, failing closed.
	a, res, pool, cleanup := startPGConsumers(ctx, config.Config{
		PGDSN: "postgres://trpc:trpc-dev-only@localhost:1/trpc?sslmode=disable",
	})
	if a == nil || res == nil || pool == nil {
		t.Fatalf("lazy pool must keep consumers alive, got (%v, %v, %v)", a, res, pool)
	}
	cleanup()

	// Unparsable DSN: everything degrades to nil, cleanup is a noop.
	a2, res2, pool2, cleanup2 := startPGConsumers(ctx, config.Config{PGDSN: "definitely not a dsn"})
	if a2 != nil || res2 != nil || pool2 != nil {
		t.Fatalf("invalid DSN must degrade to nil, got (%v, %v, %v)", a2, res2, pool2)
	}
	cleanup2()

	// Real PG: full consumers plus a working cleanup.
	a3, res3, pool3, cleanup3 := startPGConsumers(ctx, config.Config{PGDSN: testPGDSN})
	if a3 == nil || res3 == nil || pool3 == nil {
		t.Fatalf("reachable PG must start consumers, got (%v, %v, %v)", a3, res3, pool3)
	}
	cleanup3()
}

// fakeStarter implements channels.Starter for the leader-loop tests: Start
// blocks until its context is canceled, like the wecomws connection loop.
type fakeStarter struct{ starts atomic.Int64 }

func (f *fakeStarter) Name() string                                        { return "fake-ws" }
func (f *fakeStarter) RegisterRoutes(_ *http.ServeMux, _ channels.Handler) {}
func (f *fakeStarter) Send(_ context.Context, _ channels.OutboundMessage) error {
	return nil
}
func (f *fakeStarter) Start(ctx context.Context, _ channels.Handler) error {
	f.starts.Add(1)
	<-ctx.Done()
	return nil
}

func noopHandler(_ context.Context, _ channels.InboundMessage) (channels.OutboundMessage, error) {
	return channels.OutboundMessage{}, nil
}

func newTestSender(rdb *redis.Client, starter channels.Channel) *channels.Sender {
	return &channels.Sender{
		Stream:   storage.NewStream(rdb),
		Channels: map[string]channels.Channel{starter.Name(): starter},
		Name:     "test-sws", Group: "senders-ws",
		Limiter: storage.NewLimiter(rdb),
	}
}

func clearLeaderKey(t *testing.T, rdb *redis.Client) {
	t.Helper()
	if err := rdb.Del(context.Background(), "lock:leader:wecomws").Err(); err != nil {
		t.Fatal(err)
	}
}

func TestRunWecomwsLeaderRunsUntilCancel(t *testing.T) {
	rdb := newTestRedis(t)
	clearLeaderKey(t, rdb)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	starter := &fakeStarter{}
	err := runWecomwsLeader(ctx, storage.NewLeaderLock(rdb), starter,
		newTestSender(rdb, starter), channels.HandlerFunc(noopHandler),
		"test-leader", 300*time.Millisecond)
	if err != nil {
		t.Fatalf("leader loop must exit cleanly on ctx cancel: %v", err)
	}
}

func TestRunWecomwsLeaderAcquireFailure(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}) // unreachable
	defer func() { _ = rdb.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	started := time.Now()
	err := runWecomwsLeader(ctx, storage.NewLeaderLock(rdb), &fakeStarter{},
		&channels.Sender{}, channels.HandlerFunc(noopHandler),
		"test-acquire-fail", time.Second)
	if err != nil {
		t.Fatalf("acquire failure must degrade to a retry loop, not an error: %v", err)
	}
	if elapsed := time.Since(started); elapsed >= 1900*time.Millisecond {
		t.Errorf("cancellation during the retry backoff must return early, took %v", elapsed)
	}
}

// TestRunWecomwsLeaderLosesLease steals the lease mid-flight by deleting the
// redis key: the watchdog must notice, tear the children down and re-campaign.
func TestRunWecomwsLeaderLosesLease(t *testing.T) {
	rdb := newTestRedis(t)
	clearLeaderKey(t, rdb)
	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()

	starter := &fakeStarter{}
	go func() {
		time.Sleep(120 * time.Millisecond)
		_ = rdb.Del(context.Background(), "lock:leader:wecomws").Err()
	}()
	err := runWecomwsLeader(ctx, storage.NewLeaderLock(rdb), starter,
		newTestSender(rdb, starter), channels.HandlerFunc(noopHandler),
		"test-loser", 300*time.Millisecond)
	if err != nil {
		t.Fatalf("leadership loss must recover by re-campaigning: %v", err)
	}
	if n := starter.starts.Load(); n < 1 {
		t.Errorf("the leader must start its channel at least once, started %d times", n)
	}
}

// failStarter fails immediately, like a websocket endpoint that refuses the
// connection: the leader must tear down, log and re-campaign.
type failStarter struct{}

func (f *failStarter) Name() string                                        { return "fake-ws" }
func (f *failStarter) RegisterRoutes(_ *http.ServeMux, _ channels.Handler) {}
func (f *failStarter) Send(_ context.Context, _ channels.OutboundMessage) error {
	return nil
}
func (f *failStarter) Start(_ context.Context, _ channels.Handler) error {
	return errors.New("websocket dial refused (test)")
}

func TestRunWecomwsLeaderChildFailureRecampaigns(t *testing.T) {
	rdb := newTestRedis(t)
	clearLeaderKey(t, rdb)
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()

	err := runWecomwsLeader(ctx, storage.NewLeaderLock(rdb), &failStarter{},
		newTestSender(rdb, &fakeStarter{}), channels.HandlerFunc(noopHandler),
		"test-child-failure", time.Second)
	if err != nil {
		t.Fatalf("a child failure must degrade to re-campaigning, not an error: %v", err)
	}
}

// TestAdminTLSConfigBadCA covers the remaining mTLS failure modes: an
// unreadable CA file and a CA file holding no PEM certificates.
func TestAdminTLSConfigBadCA(t *testing.T) {
	dir := t.TempDir()
	ca := makeCA(t)
	serverCert, serverKey := signCert(t, ca, "server-badca",
		[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})

	if _, err := adminTLSConfig(config.Config{
		AdminTLSCert: serverCert, AdminTLSKey: serverKey,
		AdminTLSClientCA: filepath.Join(dir, "nope.crt"),
	}); err == nil {
		t.Fatal("an unreadable CA file must fail the admin TLS config")
	}

	garbage := filepath.Join(dir, "garbage.crt")
	if err := os.WriteFile(garbage, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := adminTLSConfig(config.Config{
		AdminTLSCert: serverCert, AdminTLSKey: serverKey,
		AdminTLSClientCA: garbage,
	}); err == nil {
		t.Fatal("a CA file without PEM certificates must fail the admin TLS config")
	}
}
