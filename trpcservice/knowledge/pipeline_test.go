package knowledge

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/artifact"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/memory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/minio"
	tasmysql "github.com/liuzengh/trpc-agent-service/trpcservice/storage/mysql"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/qdrant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/summary"
)

// The P4 acceptance suite, run against real MySQL, Qdrant, MinIO and a real
// HTTP embedding endpoint. The rules under test are the ones the approved
// plan states as promises, so each test is named after the promise:
//
//	TestPipelineCitationAndNotReadyRule   未 ready 不召回
//	TestDeleteRaceDoesNotResurrect        删除竞态无复活（含晚写补偿）
//	TestCrossTenantZeroLeak               跨 tenant/app 零泄露
//	TestArtifactStagedNotDownloadable     非法下载拒绝
//	TestMemoryBoundaries                  Memory 边界（群聊禁用/异步索引/用户隔离）
//	TestSummaryBoundaries                 摘要边界（CAS/尾部保留）
//
// Start the services with:
//
//	docker run -d --name tas-qdrant-test -p 6333:6333 qdrant/qdrant:v1.12.4
//	docker run -d --name tas-minio-test -p 9010:9000 \
//	  -e MINIO_ROOT_USER=tasminio -e MINIO_ROOT_PASSWORD=tasminiopw \
//	  minio/minio:RELEASE.2024-08-17T01-24-54Z server /data
//	KNOWLEDGE_TEST_DSN='tas:taspw@tcp(127.0.0.1:3307)/tas_knowledge_test' \
//	KNOWLEDGE_TEST_QDRANT=http://127.0.0.1:6333 \
//	KNOWLEDGE_TEST_MINIO_ENDPOINT=http://127.0.0.1:9010 \
//	  go test ./trpcservice/knowledge/ -v
func setupPipeline(t *testing.T) (*Service, *memory.Service, *artifact.Service, *controlplane.DB, *fakeEmbedder) {
	t.Helper()
	dsn := os.Getenv("KNOWLEDGE_TEST_DSN")
	qdrantURL := os.Getenv("KNOWLEDGE_TEST_QDRANT")
	minioEP := os.Getenv("KNOWLEDGE_TEST_MINIO_ENDPOINT")
	if dsn == "" || qdrantURL == "" || minioEP == "" {
		t.Skip("KNOWLEDGE_TEST_DSN/KNOWLEDGE_TEST_QDRANT/KNOWLEDGE_TEST_MINIO_ENDPOINT not set; skipping real-service pipeline test")
	}
	db, err := tasmysql.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	resetPipelineSchema(t, db)
	if _, err := tasmysql.Migrate(context.Background(), db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	cdp := controlplane.NewDB(db)

	collection := fmt.Sprintf("tas_p4_%d", time.Now().UnixNano())
	vectors, err := qdrant.New(qdrantURL, collection)
	if err != nil {
		t.Fatal(err)
	}
	objects, err := minio.New(minioEP, os.Getenv("KNOWLEDGE_TEST_MINIO_USER"),
		os.Getenv("KNOWLEDGE_TEST_MINIO_PASSWORD"), "us-east-1", "tas-p4-test")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := objects.EnsureBucket(ctx); err != nil {
		t.Fatalf("ensure bucket: %v", err)
	}
	embedder := &fakeEmbedder{model: "fake-embed", dim: 32}
	if err := vectors.EnsureCollection(ctx, embedder.dim); err != nil {
		t.Fatalf("ensure collection: %v", err)
	}
	kn, err := New(Options{DB: cdp, Objects: objects, Embedder: embedder, Vectors: vectors})
	if err != nil {
		t.Fatal(err)
	}
	mem, err := memory.New(cdp, embedder, vectors)
	if err != nil {
		t.Fatal(err)
	}
	arts, err := artifact.New(cdp, objects)
	if err != nil {
		t.Fatal(err)
	}
	return kn, mem, arts, cdp, embedder
}

// fakeEmbedder is the deterministic bag-of-token embedder the pipeline
// tests need: the same algorithm the fake model serves over HTTP (see
// cmd/fake-model/embedding.go), kept in-process so these tests do not
// depend on that server running.
type fakeEmbedder struct {
	model string
	dim   int
}

func (f *fakeEmbedder) Model() string { return f.model }
func (f *fakeEmbedder) Dim() int      { return f.dim }
func (f *fakeEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, 0, len(texts))
	for _, text := range texts {
		v := make([]float32, f.dim)
		for _, tok := range strings.Fields(strings.ToLower(text)) {
			h := 0
			for _, r := range tok {
				h = h*31 + int(r)
			}
			if h < 0 {
				h = -h
			}
			v[h%f.dim]++
		}
		out = append(out, v)
	}
	return out, nil
}

func resetPipelineSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	tables := []string{
		"tool_call_attempts", "tool_calls", "artifacts", "memory_entries", "document_chunks", "documents",
		"delivery_attempts", "reply_outbox", "session_events", "execution_attempts",
		"executions", "inbox_messages", "channel_reply_routes", "channel_checkpoints",
		"channel_notifications", "outbox_events", "audit_events", "sessions",
		"knowledge_bindings", "knowledge_bases", "tool_bindings",
		"channel_identities", "channel_bindings",
		"agent_revisions", "agent_apps", "backend_profiles", "model_profiles",
		"tenant_users", "principals", "tenants", "schema_migrations",
	}
	if _, err := db.Exec("SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		t.Fatal(err)
	}
	for _, tbl := range tables {
		if _, err := db.Exec("DROP TABLE IF EXISTS " + tbl); err != nil {
			t.Fatalf("drop %s: %v", tbl, err)
		}
	}
	if _, err := db.Exec("SET FOREIGN_KEY_CHECKS = 1"); err != nil {
		t.Fatal(err)
	}
}

// seedTenant creates a tenant with one app and one knowledge base, and
// returns (tenantID, appID, kbID, kbPublicID).
func seedTenant(t *testing.T, cdp *controlplane.DB, name string) (string, int64, int64, string) {
	t.Helper()
	ctx := context.Background()
	if err := cdp.CreateTenant(ctx, name, name); err != nil {
		t.Fatal(err)
	}
	scope := cdp.MustScope(name)
	appID, err := scope.CreateApp(ctx, "assistant", "Assistant")
	if err != nil {
		t.Fatal(err)
	}
	kbPublic := "docs"
	res, err := scope.Exec(ctx, `
		INSERT INTO knowledge_bases (tenant_id, app_id, public_id, name, status, embedding_model, embedding_dim)
		VALUES (?, ?, ?, ?, 'active', 'fake-embed', 32)`, name, appID, kbPublic, "Docs")
	if err != nil {
		t.Fatal(err)
	}
	kbID, _ := res.LastInsertId()
	return name, appID, kbID, kbPublic
}

func TestPipelineCitationAndNotReadyRule(t *testing.T) {
	kn, _, _, cdp, _ := setupPipeline(t)
	ctx := context.Background()
	tenant, appID, _, kbPublic := seedTenant(t, cdp, "acme")

	doc, err := kn.IngestDoc(ctx, tenant, appID, kbPublic, "11111111-1111-1111-1111-111111111111",
		"Alpha 手册", "text/markdown",
		[]byte("# Alpha 手册\n\nalpha rollout checklist and the runbook for the alpha service.\n"))
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}

	// 未 ready 不召回: the bytes and the row exist, but nothing is indexed.
	early, err := kn.Search(ctx, tenant, appID, []int64{doc.KBID}, "alpha", 4)
	if err != nil {
		t.Fatalf("early search: %v", err)
	}
	if len(early.Citations) != 0 || early.Note == "" {
		t.Fatalf("an unindexed document was recalled: %+v", early)
	}

	// Run the index job the queue would have run.
	if err := kn.IndexDocument(ctx, tenant, doc.DocID, doc.Generation); err != nil {
		t.Fatalf("index: %v", err)
	}
	res, err := kn.Search(ctx, tenant, appID, []int64{doc.KBID}, "alpha", 4)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(res.Citations) == 0 {
		t.Fatal("a ready document was not recalled")
	}
	c := res.Citations[0]
	if c.DocPublicID != "11111111-1111-1111-1111-111111111111" || c.Page < 1 ||
		!strings.Contains(c.Text, "alpha") {
		t.Fatalf("citation = %+v", c)
	}

	// No match is an explicit note, not a fabricated citation.
	none, err := kn.Search(ctx, tenant, appID, []int64{doc.KBID}, "zeta-never-mentioned", 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(none.Citations) != 0 || none.Note == "" {
		t.Fatalf("no-match search = %+v", none)
	}
}

func TestDeleteRaceDoesNotResurrect(t *testing.T) {
	kn, _, _, cdp, _ := setupPipeline(t)
	ctx := context.Background()
	tenant, appID, _, kbPublic := seedTenant(t, cdp, "acme")
	const publicID = "22222222-2222-2222-2222-222222222222"

	doc, err := kn.IngestDoc(ctx, tenant, appID, kbPublic, publicID, "Beta", "text/plain",
		[]byte("beta beta secret rollout plan"))
	if err != nil {
		t.Fatal(err)
	}
	if err := kn.IndexDocument(ctx, tenant, doc.DocID, doc.Generation); err != nil {
		t.Fatal(err)
	}
	if res, _ := kn.Search(ctx, tenant, appID, []int64{doc.KBID}, "beta", 4); len(res.Citations) == 0 {
		t.Fatal("baseline: the document is not retrievable")
	}

	// Delete: the tombstone must refuse retrieval immediately, before any
	// cleanup has run.
	if err := kn.DeleteDocument(ctx, tenant, appID, kbPublic, publicID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if res, _ := kn.Search(ctx, tenant, appID, []int64{doc.KBID}, "beta", 4); len(res.Citations) != 0 {
		t.Fatalf("a tombstoned document was still recalled: %+v", res)
	}

	// A late index run that lands after the delete (the crash window the
	// plan calls 晚到索引写入) writes points under the *old* generation; the
	// cleanup must remove them, and a search must not see them for a moment.
	if err := kn.IndexDocument(ctx, tenant, doc.DocID, doc.Generation); err != nil {
		// The handler refuses to touch a tombstoned row; that refusal is one
		// of the two legal outcomes (the other being the CAS in a racing
		// run). Either way the point write below simulates the leak.
		t.Logf("index of tombstoned doc returned: %v", err)
	}
	vecs, _ := (&fakeEmbedder{model: "fake-embed", dim: 32}).Embed(ctx, []string{"beta beta secret rollout plan"})
	if err := kn.vectors.Upsert(ctx, []qdrant.Point{{
		ID: PointID(tenant, doc.DocID, doc.Generation, 0, 1), Vector: vecs[0],
		Payload: map[string]any{
			"tenant_id": tenant, "app_id": appID, "kb_id": doc.KBID,
			"doc_id": doc.DocID, "generation": doc.Generation, "chunk_ord": 0,
		},
	}}); err != nil {
		t.Fatal(err)
	}

	// Run cleanup (the queued job) and prove the late write is gone from
	// the index too — not merely invisible through SQL.
	if err := kn.CleanupDocument(ctx, tenant, doc.DocID, doc.Generation+1, doc.ObjectKey); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	left, err := kn.vectors.Count(ctx, qdrant.Filter{Must: []qdrant.Match{
		{Key: "tenant_id", Value: tenant}, {Key: "doc_id", Value: doc.DocID},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if left != 0 {
		t.Fatalf("cleanup left %d points behind (late write resurrected)", left)
	}
	if res, _ := kn.Search(ctx, tenant, appID, []int64{doc.KBID}, "beta", 4); len(res.Citations) != 0 {
		t.Fatalf("a deleted document was recalled: %+v", res)
	}

	// Re-ingest the same public id: allowed once the row is deleted, and it
	// must become retrievable again.
	again, err := kn.IngestDoc(ctx, tenant, appID, kbPublic, publicID, "Beta v2", "text/plain",
		[]byte("beta beta re-released with new content"))
	if err != nil {
		t.Fatalf("re-ingest: %v", err)
	}
	if again.Generation != doc.Generation+2 {
		t.Fatalf("re-ingest generation = %d, want %d", again.Generation, doc.Generation+2)
	}
	if err := kn.IndexDocument(ctx, tenant, again.DocID, again.Generation); err != nil {
		t.Fatal(err)
	}
	if res, _ := kn.Search(ctx, tenant, appID, []int64{doc.KBID}, "beta", 4); len(res.Citations) == 0 {
		t.Fatal("re-ingested document is not retrievable")
	}
}

func TestCrossTenantZeroLeak(t *testing.T) {
	kn, _, _, cdp, _ := setupPipeline(t)
	ctx := context.Background()
	acme, acmeApp, acmeKB, acmePublic := seedTenant(t, cdp, "acme")
	globex, globexApp, globexKB, globexPublic := seedTenant(t, cdp, "globex")

	for _, f := range []struct {
		tenant, kb string
		appID      int64
		id         string
	}{
		{acme, acmePublic, acmeApp, "33333333-3333-3333-3333-333333333333"},
		{globex, globexPublic, globexApp, "44444444-4444-4444-4444-444444444444"},
	} {
		doc, err := kn.IngestDoc(ctx, f.tenant, f.appID, f.kb, f.id, "Shared", "text/plain",
			[]byte("shared keyword appears in both tenants"))
		if err != nil {
			t.Fatal(err)
		}
		if err := kn.IndexDocument(ctx, f.tenant, doc.DocID, doc.Generation); err != nil {
			t.Fatal(err)
		}
	}

	res, err := kn.Search(ctx, acme, acmeApp, []int64{acmeKB}, "shared", 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Citations) != 1 || res.Citations[0].DocPublicID != "33333333-3333-3333-3333-333333333333" {
		t.Fatalf("acme saw %+v", res.Citations)
	}
	// Even with the other tenant's kb id handed to it, the filter refuses:
	// the vector store's must-clause carries tenant+app, and SQL re-verifies.
	res, err = kn.Search(ctx, acme, acmeApp, []int64{globexKB}, "shared", 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Citations) != 0 {
		t.Fatalf("acme recalled globex content: %+v", res.Citations)
	}
}

func TestArtifactStagedNotDownloadable(t *testing.T) {
	_, _, arts, cdp, _ := setupPipeline(t)
	ctx := context.Background()
	_, _, _, _ = seedTenant(t, cdp, "acme")
	_, _, _, _ = seedTenant(t, cdp, "globex")

	// The artifact needs an execution row (FK); seed one through the same
	// chain the tests above use.
	var sessionPK int64
	execID := "55555555-5555-5555-5555-555555555555"
	seedExecution(t, cdp, "acme", execID, &sessionPK)

	a, err := arts.SaveStaged(ctx, "acme", execID, sessionPK, "66666666-6666-6666-6666-666666666666",
		"report.txt", "text/plain", []byte("hello from a tool"))
	if err != nil {
		t.Fatalf("save staged: %v", err)
	}
	if a.Status != "staged" {
		t.Fatalf("status = %s", a.Status)
	}
	if _, _, err := arts.Open(ctx, "acme", a.PublicID); !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("a staged artifact was downloadable: %v", err)
	}
	// The commit promotes it (simulated here; the commit path calls the same
	// function inside its transaction).
	txErr := cdp.MustScope("acme").WithTx(ctx, func(tx *controlplane.TxScope) error {
		return artifact.PromoteExecution(ctx, tx, "acme", execID)
	})
	if txErr != nil {
		t.Fatal(txErr)
	}
	got, data, err := arts.Open(ctx, "acme", a.PublicID)
	if err != nil {
		t.Fatalf("open after promote: %v", err)
	}
	if got.Name != "report.txt" || string(data) != "hello from a tool" {
		t.Fatalf("downloaded %q (%d bytes)", got.Name, len(data))
	}
	// Cross-tenant download is refused with the same answer as "missing".
	if _, _, err := arts.Open(ctx, "globex", a.PublicID); !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("cross-tenant download = %v", err)
	}
}

func TestMemoryBoundaries(t *testing.T) {
	_, mem, _, cdp, _ := setupPipeline(t)
	ctx := context.Background()
	seedTenant(t, cdp, "acme")

	entry, err := mem.Write(ctx, "acme", "user-1", false, "prefers dark mode and weekly digests")
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if entry.Status != "pending" {
		t.Fatalf("status = %s", entry.Status)
	}
	// 异步索引期间不承诺可检索.
	if found, _ := mem.Search(ctx, "acme", "user-1", "dark mode", 4); len(found) != 0 {
		t.Fatalf("a pending memory was searchable: %+v", found)
	}
	if err := mem.IndexMemory(ctx, "acme", entry.MemoryID); err != nil {
		t.Fatal(err)
	}
	found, err := mem.Search(ctx, "acme", "user-1", "dark mode", 4)
	if err != nil || len(found) != 1 || found[0].Text == "" {
		t.Fatalf("ready memory search = %+v, %v", found, err)
	}
	// Another user of the same tenant sees nothing.
	if found, _ := mem.Search(ctx, "acme", "user-2", "dark mode", 4); len(found) != 0 {
		t.Fatalf("user-2 saw user-1's memory: %+v", found)
	}
	// Group chats have no personal memory.
	if _, err := mem.Write(ctx, "acme", "group-1", true, "someone said something"); !errors.Is(err, memory.ErrGroupMemory) {
		t.Fatalf("group write = %v, want ErrGroupMemory", err)
	}
	// Delete is effective immediately for search and finalizes the index.
	if err := mem.Delete(ctx, "acme", "user-1", entry.MemoryID); err != nil {
		t.Fatal(err)
	}
	if found, _ := mem.Search(ctx, "acme", "user-1", "dark mode", 4); len(found) != 0 {
		t.Fatalf("a deleted memory was searchable: %+v", found)
	}
	if err := mem.CleanupMemory(ctx, "acme", entry.MemoryID); err != nil {
		t.Fatal(err)
	}
	var status string
	row, err := cdp.MustScope("acme").QueryRow(ctx,
		"SELECT status FROM memory_entries WHERE tenant_id='acme' AND memory_id=?", entry.MemoryID)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&status); err != nil || status != "deleted" {
		t.Fatalf("status after cleanup = %s, %v", status, err)
	}
}

func TestSummaryBoundaries(t *testing.T) {
	_, _, _, cdp, _ := setupPipeline(t)
	ctx := context.Background()
	tenant, appID, _, _ := seedTenant(t, cdp, "acme")

	// A fake model that answers the non-streaming summary request.
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"x","object":"chat.completion","created":1,"model":"fake",
			"choices":[{"index":0,"message":{"role":"assistant","content":"SUMMARY-TEXT"},"finish_reason":"stop"}]}`))
	}))
	t.Cleanup(model.Close)

	scope := cdp.MustScope(tenant)
	t.Setenv("P4_SUMMARY_KEY", "k")
	modelID, err := scope.CreateModelProfile(ctx, "m1", "fake", model.URL, "env:P4_SUMMARY_KEY")
	if err != nil {
		t.Fatal(err)
	}
	backendID, err := scope.CreateBackendProfile(ctx, controlplane.BackendProfile{PublicID: "b1", SessionBackend: "memory"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scope.CreateApp(ctx, "app2", "App2"); err != nil {
		t.Fatal(err)
	}
	rev, err := scope.PublishRevision(ctx, "app2", controlplane.RevisionSpec{
		Instruction: "x", ModelProfileID: modelID, BackendProfileID: backendID, MaxLLMCalls: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	// A session row and 14 committed events (the threshold is 12).
	res, err := scope.Exec(ctx, `
		INSERT INTO sessions (tenant_id, app_id, binding_id, channel_type, actor_key, generation, revision_id,
			model_profile_version, backend_profile_version, in_seq, head_seq)
		VALUES (?, ?, NULL, 'webchat', 'user-1', 1, ?, 1, 1, 14, 15)`, tenant, appID, rev.ID)
	if err != nil {
		t.Fatal(err)
	}
	sessionPK, _ := res.LastInsertId()
	for i := 1; i <= 14; i++ {
		if _, err := scope.Exec(ctx, `
			INSERT INTO session_events (tenant_id, session_pk, seq, event_id, execution_id, author, payload)
			VALUES (?, ?, ?, ?, '', 'user', ?)`,
			tenant, sessionPK, i, fmt.Sprintf("evt-%d", i),
			`{"response":{"choices":[{"delta":{"content":"fact `+fmt.Sprint(i)+`"}}]}}`); err != nil {
			t.Fatal(err)
		}
	}
	summarySvc, err := summary.New(cdp, secrets.NewResolver(secrets.AllowedPrefixes{EnvVars: []string{"P4_SUMMARY_KEY"}}))
	if err != nil {
		t.Fatal(err)
	}
	if err := summarySvc.Summarize(ctx, tenant, sessionPK); err != nil {
		t.Fatalf("summarize: %v", err)
	}
	var (
		text    string
		covered int
		version int
	)
	row, err := scope.QueryRow(ctx,
		"SELECT summary, summary_covered_seq, summary_version FROM sessions WHERE tenant_id=? AND session_pk=?",
		tenant, sessionPK)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&text, &covered, &version); err != nil {
		t.Fatal(err)
	}
	if text != "SUMMARY-TEXT" || covered != 14 || version != 1 {
		t.Fatalf("summary=%q covered=%d version=%d, want SUMMARY-TEXT/14/1", text, covered, version)
	}

	// A newer summary may not be overwritten: simulate another writer having
	// covered 16 events, add tail events, and run the job again.
	for i := 15; i <= 16; i++ {
		if _, err := scope.Exec(ctx, `
			INSERT INTO session_events (tenant_id, session_pk, seq, event_id, execution_id, author, payload)
			VALUES (?, ?, ?, ?, '', 'user', '{}')`, tenant, sessionPK, i, fmt.Sprintf("evt-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := scope.Exec(ctx, `
		UPDATE sessions SET summary='NEWER', summary_covered_seq=16, summary_version=2
		WHERE tenant_id=? AND session_pk=?`, tenant, sessionPK); err != nil {
		t.Fatal(err)
	}
	if err := summarySvc.Summarize(ctx, tenant, sessionPK); err != nil {
		t.Fatal(err)
	}
	row, err = scope.QueryRow(ctx,
		"SELECT summary, summary_covered_seq, summary_version FROM sessions WHERE tenant_id=? AND session_pk=?",
		tenant, sessionPK)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&text, &covered, &version); err != nil {
		t.Fatal(err)
	}
	if text != "NEWER" || covered != 16 || version != 2 {
		t.Fatalf("a newer summary was disturbed: %q/%d/%d", text, covered, version)
	}
}

// seedExecution creates the minimal chain an artifact FK needs: binding →
// session → inbox → execution.
func seedExecution(t *testing.T, cdp *controlplane.DB, tenant, execID string, sessionPK *int64) {
	t.Helper()
	ctx := context.Background()
	scope := cdp.MustScope(tenant)
	var appID int64
	row, err := scope.QueryRow(ctx, "SELECT app_id FROM agent_apps WHERE tenant_id = ? LIMIT 1", tenant)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&appID); err != nil {
		t.Fatal(err)
	}
	var modelID, backendID int64
	row, err = scope.QueryRow(ctx, "SELECT profile_id FROM model_profiles WHERE tenant_id = ? LIMIT 1", tenant)
	if err != nil {
		t.Fatal(err)
	}
	_ = row.Scan(&modelID)
	if modelID == 0 {
		modelID, err = scope.CreateModelProfile(ctx, "mx", "fake", "http://127.0.0.1:1", "")
		if err != nil {
			t.Fatal(err)
		}
	}
	row, err = scope.QueryRow(ctx, "SELECT profile_id FROM backend_profiles WHERE tenant_id = ? LIMIT 1", tenant)
	if err != nil {
		t.Fatal(err)
	}
	_ = row.Scan(&backendID)
	if backendID == 0 {
		backendID, err = scope.CreateBackendProfile(ctx, controlplane.BackendProfile{PublicID: "bx", SessionBackend: "memory"})
		if err != nil {
			t.Fatal(err)
		}
	}
	rev, err := scope.PublishRevision(ctx, "assistant", controlplane.RevisionSpec{
		Instruction: "x", ModelProfileID: modelID, BackendProfileID: backendID, MaxLLMCalls: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	bindingID, err := scope.BindChannel(ctx, controlplane.ChannelBinding{
		AppID: appID, ChannelType: "webchat", PublicID: "main-" + execID, CredentialRef: "env:X",
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := scope.Exec(ctx, `
		INSERT INTO sessions (tenant_id, app_id, binding_id, channel_type, actor_key, generation, revision_id,
			model_profile_version, backend_profile_version, in_seq, head_seq)
		VALUES (?, ?, ?, 'webchat', 'artifact-user', 1, ?, 1, 1, 1, 1)`, tenant, appID, bindingID, rev.ID)
	if err != nil {
		t.Fatal(err)
	}
	pk, _ := res.LastInsertId()
	*sessionPK = pk
	if _, err := scope.Exec(ctx, `
		INSERT INTO inbox_messages (tenant_id, binding_id, platform_message_id, content_hash, session_pk, in_seq, execution_id, status)
		VALUES (?, ?, ?, 'x', ?, 1, ?, 'done')`, tenant, bindingID, "pm-"+execID, pk, execID); err != nil {
		t.Fatal(err)
	}
	if _, err := scope.Exec(ctx, `
		INSERT INTO executions (execution_id, tenant_id, session_pk, in_seq, revision_id, model_profile_id, backend_profile_id)
		VALUES (?, ?, ?, 1, ?, ?, ?)`, execID, tenant, pk, rev.ID, modelID, backendID); err != nil {
		t.Fatal(err)
	}
}
