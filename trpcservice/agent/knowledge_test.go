package agent_test

import (
	"context"
	"hash/fnv"
	"slices"
	"strconv"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/testenv"
)

// fakeEmbedder produces deterministic bag-of-runes vectors: texts sharing
// runes are close, so retrieval is testable without an embeddings API.
type fakeEmbedder struct{ dim int }

func (f fakeEmbedder) GetEmbedding(_ context.Context, text string) ([]float64, error) {
	v := make([]float64, f.dim)
	for _, r := range text {
		h := fnv.New32a()
		_, _ = h.Write([]byte(string(r)))
		v[int(h.Sum32())%f.dim]++
	}
	return v, nil
}

func (f fakeEmbedder) GetEmbeddingWithUsage(ctx context.Context, text string) ([]float64, map[string]any, error) {
	v, err := f.GetEmbedding(ctx, text)
	return v, nil, err
}

func (f fakeEmbedder) GetDimensions() int { return f.dim }

// docID reads the ID a DocSource assigns its single document.
func docID(t *testing.T, s *agent.DocSource) string {
	t.Helper()
	docs, err := s.ReadDocuments(context.Background())
	if err != nil {
		t.Fatalf("ReadDocuments: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("want 1 document, got %d", len(docs))
	}
	return docs[0].ID
}

// The document ID is the pgvector upsert key, so it has to carry the tenant
// scope: an ID derived from name+content alone would land two tenants
// ingesting the same document on one row, the second ingest rewriting the
// first's content, embedding and tenant_id.
func TestDocSourceIDScopesByTenantAndApp(t *testing.T) {
	doc := func(tenantID, appID, name, content string) *agent.DocSource {
		md := map[string]any{}
		if tenantID != "" {
			md[agent.MetadataTenantID] = tenantID
		}
		if appID != "" {
			md[agent.MetadataAppID] = appID
		}
		return &agent.DocSource{DocName: name, Content: content, Metadata: md}
	}

	t1 := docID(t, doc("t1", "a1", "退款政策", "七天内无理由退款"))
	t2 := docID(t, doc("t2", "a1", "退款政策", "七天内无理由退款"))
	if t1 == t2 {
		t.Fatalf("the same document under two tenants must not share an ID: %s", t1)
	}

	// Re-ingesting the same document is an upsert, not a duplicate.
	if again := docID(t, doc("t1", "a1", "退款政策", "七天内无理由退款")); again != t1 {
		t.Fatalf("re-ingest must be idempotent: %s != %s", again, t1)
	}

	// A second app of the same tenant is a separate document, and so is
	// different content under the same name.
	if other := docID(t, doc("t1", "a2", "退款政策", "七天内无理由退款")); other == t1 {
		t.Fatal("a different app must not share the document ID")
	}
	if edited := docID(t, doc("t1", "a1", "退款政策", "三十天内无理由退款")); edited == t1 {
		t.Fatal("different content must not share the document ID")
	}

	// Field boundaries are length-prefixed: a shift that concatenates to the
	// same bytes must still hash differently.
	if a, b := docID(t, doc("t1", "a1x", "n", "c")), docID(t, doc("t1a", "1x", "n", "c")); a == b {
		t.Fatalf("shifted scope boundaries must not collide: %s", a)
	}

	// An unscoped document (the env-only dev fallback) keeps a stable ID
	// instead of failing, and stays distinct from a scoped one.
	unscoped := docID(t, doc("", "", "退款政策", "七天内无理由退款"))
	if unscoped == "" || unscoped == t1 {
		t.Fatalf("unscoped ID must be stable and distinct from the scoped one: %q", unscoped)
	}
	if unscoped != docID(t, &agent.DocSource{DocName: "退款政策", Content: "七天内无理由退款"}) {
		t.Fatal("a nil metadata map must hash like an empty one")
	}
}

// TestKnowledgeBaseRoundTrip ingests a document through the platform source
// and retrieves it via vector search. Needs the compose PG (pgvector image);
// skips when unreachable.
func TestKnowledgeBaseRoundTrip(t *testing.T) {
	ctx := context.Background()
	pool := testenv.PG(t)

	const dim = 64
	table := "knowledge_test_" + "roundtrip"
	// testenv closes the pool in a t.Cleanup that runs after this test's; a
	// deferred pool.Close() here would close it first and silently void the
	// DROP, leaking the table into the next run. Drop leftovers up front for
	// the same reason: a crashed run leaves its rows behind.
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS `+table); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS `+table)
	})

	kb, _, err := agent.NewKnowledgeBase(
		"postgres://trpc:trpc-dev-only@localhost:5432/trpc?sslmode=disable",
		table, dim, fakeEmbedder{dim: dim})
	if err != nil {
		t.Fatal(err)
	}

	src := &agent.DocSource{
		DocName:  "退款政策",
		Content:  "我们的退款政策：签收后七天内支持无理由退款，运费由平台承担。",
		Metadata: map[string]any{agent.MetadataTenantID: "t1", agent.MetadataAppID: "a1"},
	}
	if err := kb.AddSource(ctx, src); err != nil {
		t.Fatal(err)
	}

	result, err := kb.Search(ctx, &knowledge.SearchRequest{Query: "怎么退款"})
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || result.Text == "" {
		t.Fatal("search returned no document")
	}
	t.Logf("search hit: score=%.3f text=%q", result.Score, result.Text)
}

// A knowledge base on an unreachable or malformed DSN fails at construction
// instead of producing a half-initialized agent.
func TestKnowledgeBaseRejectsBadDSN(t *testing.T) {
	if _, _, err := agent.NewKnowledgeBase("not a valid dsn", "knowledge_test_bad", 64, fakeEmbedder{dim: 64}); err == nil {
		t.Fatal("malformed DSN must fail NewKnowledgeBase")
	}
}

// legacyDocID is the unscoped scheme: fnv64a of name+content alone.
func legacyDocID(name, content string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(name))
	_, _ = h.Write([]byte(content))
	return strconv.FormatUint(h.Sum64(), 16)
}

// vectorAt builds a dim-length embedding with one distinguishing slot, so a
// re-keyed row's carried embedding is exact-comparable after the float32
// round trip.
func vectorAt(dim, slot int, val float64) []float64 {
	v := make([]float64, dim)
	v[slot%dim] = val
	return v
}

// A document keyed by the unscoped ID cannot be removed any other way (there
// is no delete endpoint) and is duplicated by a re-ingest, which hashes to the
// scoped ID and inserts a second row. RekeyLegacyDocuments must move every
// unscoped row to its scoped ID — embedding carried verbatim, no embedder
// involved — and collapse the duplicate back to one row, after which
// re-ingesting is an upsert again.
func TestRekeyLegacyDocuments(t *testing.T) {
	ctx := context.Background()
	pool := testenv.PG(t)

	const dim = 64
	table := "knowledge_test_rekey"
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS `+table); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS `+table)
	})

	kb, vs, err := agent.NewKnowledgeBase(
		"postgres://trpc:trpc-dev-only@localhost:5432/trpc?sslmode=disable",
		table, dim, fakeEmbedder{dim: dim})
	if err != nil {
		t.Fatal(err)
	}

	md := map[string]any{agent.MetadataTenantID: "t-rekey", agent.MetadataAppID: "a-rekey"}

	// An unscoped-ID row with no scoped twin. Its embedding (slot 3) differs
	// from anything the fake embedder would make, so the re-key carrying it
	// verbatim is observable.
	stranded := &agent.DocSource{DocName: "配送范围", Content: "仅限同城配送", Metadata: md}
	strandedID := legacyDocID(stranded.DocName, stranded.Content)
	if err := vs.Add(ctx, &document.Document{
		ID: strandedID, Name: stranded.DocName, Content: stranded.Content, Metadata: md,
	}, vectorAt(dim, 3, 7)); err != nil {
		t.Fatal(err)
	}

	// duplicated: an unscoped-ID row plus its scoped twin — the same document
	// one tenant retrieves twice.
	duplicated := &agent.DocSource{DocName: "发票说明", Content: "支持电子发票", Metadata: md}
	dupID := legacyDocID(duplicated.DocName, duplicated.Content)
	if err := vs.Add(ctx, &document.Document{
		ID: dupID, Name: duplicated.DocName, Content: duplicated.Content, Metadata: md,
	}, vectorAt(dim, 5, 9)); err != nil {
		t.Fatal(err)
	}
	if err := kb.AddSource(ctx, duplicated); err != nil {
		t.Fatal(err)
	}

	meta, err := vs.GetMetadata(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(meta) != 3 {
		t.Fatalf("premise: 2 legacy rows + 1 scoped twin must be present, got %d: %v", len(meta), keysOf(meta))
	}

	n, err := agent.RekeyLegacyDocuments(ctx, vs)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("both legacy rows must be re-keyed, got %d", n)
	}

	// Exactly two rows remain: both scoped IDs, both legacy IDs gone.
	meta, err = vs.GetMetadata(ctx)
	if err != nil {
		t.Fatal(err)
	}
	strandedScoped := docID(t, stranded)
	dupScoped := docID(t, duplicated)
	if len(meta) != 2 || !keyIn(meta, strandedScoped) || !keyIn(meta, dupScoped) {
		t.Fatalf("want exactly the two scoped IDs %s + %s, got %v", strandedScoped, dupScoped, keysOf(meta))
	}

	// The stranded document's embedding crossed over verbatim — the pass
	// never calls the embedder.
	doc, emb, err := vs.Get(ctx, strandedScoped)
	if err != nil {
		t.Fatal(err)
	}
	if doc.Content != stranded.Content {
		t.Fatalf("re-keyed content changed: %q", doc.Content)
	}
	if !slices.Equal(emb, vectorAt(dim, 3, 7)) {
		t.Fatalf("re-keyed embedding must be carried verbatim, got %v", emb)
	}

	// The duplicate collapsed onto one row whose content survived.
	if doc, _, err = vs.Get(ctx, dupScoped); err != nil {
		t.Fatal(err)
	}
	if doc.Content != duplicated.Content {
		t.Fatalf("duplicate collapse changed content: %q", doc.Content)
	}

	// A second pass is a no-op, and re-ingesting the migrated document is an
	// upsert again instead of a third row.
	if n, err = agent.RekeyLegacyDocuments(ctx, vs); err != nil || n != 0 {
		t.Fatalf("second pass must re-key nothing, got %d (%v)", n, err)
	}
	if err := kb.AddSource(ctx, stranded); err != nil {
		t.Fatal(err)
	}
	if meta, err = vs.GetMetadata(ctx); err != nil {
		t.Fatal(err)
	}
	if len(meta) != 2 {
		t.Fatalf("re-ingest after the re-key must upsert, still 2 rows, got %v", keysOf(meta))
	}
}

func keyIn(m map[string]vectorstore.DocumentMetadata, id string) bool {
	_, ok := m[id]
	return ok
}

func keysOf(m map[string]vectorstore.DocumentMetadata) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}
