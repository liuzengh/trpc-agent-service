package vector

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/memory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var (
	_ EmbeddingProvider = (*DeterministicEmbedder)(nil)
	_ VectorStore       = DisabledStore{}
)

func vectorTestContext(tenantID string) context.Context {
	return tenant.WithContext(context.Background(), tenant.TenantContext{
		TenantID: tenantID, AgentAppID: "agent-a", BindingID: "binding-a", Channel: "web",
		RequestID: "request-a", MessageID: "message-a", TraceID: "trace-a", ConfigVersion: 1,
		BackendPolicy: tenant.BackendPolicy{Session: "memory", Memory: "memory", Vector: BackendNone, Object: "memory"},
	})
}

func vectorTestSource() SourceDocument {
	return SourceDocument{
		SourceType: "memory", SourceID: "memory-a", ProjectionScope: "default",
		SourceVersion: 1, SourceSequence: 9, Content: "remember this",
		Model: "fake-model", ModelVersion: "v1", Dimension: 16, SchemaVersion: "schema-1",
	}
}

func vectorTestEmbedding(ref VectorDocumentRef) Embedding {
	return Embedding{Values: make([]float64, ref.Dimension), Model: ref.Model, ModelVersion: ref.ModelVersion, Dimension: ref.Dimension}
}

func TestDocumentIdentityStableAcrossRetryAndSourceChange(t *testing.T) {
	ctxA := vectorTestContext("tenant-a")
	first := vectorTestSource()
	firstRef, err := BuildDocumentRef(ctxA, first)
	if err != nil {
		t.Fatal(err)
	}
	retryRef, err := BuildDocumentRef(ctxA, first)
	if err != nil {
		t.Fatal(err)
	}
	if firstRef.DocumentID != retryRef.DocumentID {
		t.Fatalf("retry changed document identity")
	}
	changed := first
	changed.SourceVersion = 2
	changed.SourceSequence = 10
	changed.Content = "changed content"
	changedRef, err := BuildDocumentRef(ctxA, changed)
	if err != nil {
		t.Fatal(err)
	}
	if firstRef.DocumentID != changedRef.DocumentID {
		t.Fatalf("source update changed stable document identity")
	}
	if firstRef.ContentHash == changedRef.ContentHash || firstRef.SourceVersion == changedRef.SourceVersion {
		t.Fatalf("source update did not change projection markers")
	}
	otherTenantRef, err := BuildDocumentRef(vectorTestContext("tenant-b"), first)
	if err != nil {
		t.Fatal(err)
	}
	if firstRef.DocumentID == otherTenantRef.DocumentID {
		t.Fatalf("different tenants shared document identity")
	}
	modelChange := first
	modelChange.ModelVersion = "v2"
	modelRef, err := BuildDocumentRef(ctxA, modelChange)
	if err != nil {
		t.Fatal(err)
	}
	if firstRef.DocumentID == modelRef.DocumentID {
		t.Fatalf("incompatible model projection reused document identity")
	}
}

func TestTombstoneBuildsDeleteRequestWithoutContent(t *testing.T) {
	source := vectorTestSource()
	source.Deleted = true
	source.Content = ""
	source.SourceVersion = 3
	request, err := BuildDeleteRequest(vectorTestContext("tenant-a"), source)
	if err != nil {
		t.Fatal(err)
	}
	if !request.Tombstone || request.Ref.Operation != OperationDelete || !request.Ref.Deleted {
		t.Fatalf("delete request is not a tombstone: %#v", request)
	}
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := BuildUpsertRequest(vectorTestContext("tenant-a"), source, vectorTestEmbedding(request.Ref)); !errors.Is(err, ErrInvalidDocument) {
		t.Fatalf("deleted source was accepted for upsert: %v", err)
	}
}

func TestMetadataIsBoundedAndOmitsSensitiveValues(t *testing.T) {
	source := vectorTestSource()
	source.SourceID = "external-user-123"
	source.Content = "prompt=private history=secret Authorization Bearer topsecret"
	ref, err := BuildDocumentRef(vectorTestContext("tenant-a"), source)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := ref.SafeMetadata()
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range metadata {
		if strings.Contains(value, source.Content) || strings.Contains(value, source.SourceID) || strings.Contains(value, "topsecret") {
			t.Fatalf("metadata %q contains sensitive source material", key)
		}
	}
	if len(metadata) == 0 || len(metadata) > 20 {
		t.Fatalf("unexpected metadata size: %d", len(metadata))
	}
}

func TestContextAndCallerOverridesFailClosed(t *testing.T) {
	if _, err := BuildDocumentRef(context.Background(), vectorTestSource()); !errors.Is(err, ErrInvalidContext) {
		t.Fatalf("missing tenant context error = %v", err)
	}
	badSource := vectorTestSource()
	badSource.SourceVersion = 0
	if _, err := BuildDocumentRef(vectorTestContext("tenant-a"), badSource); !errors.Is(err, ErrInvalidDocument) {
		t.Fatalf("invalid source error = %v", err)
	}
	ref, err := BuildDocumentRef(vectorTestContext("tenant-a"), vectorTestSource())
	if err != nil {
		t.Fatal(err)
	}
	request, err := BuildUpsertRequest(vectorTestContext("tenant-a"), vectorTestSource(), vectorTestEmbedding(ref))
	if err != nil {
		t.Fatal(err)
	}
	request.Ref.DocumentID = "caller-selected-id"
	if !errors.Is(request.Validate(), ErrInvalidDocument) {
		t.Fatalf("caller document ID was accepted")
	}
	otherRef, err := BuildDocumentRef(vectorTestContext("tenant-b"), vectorTestSource())
	if err != nil {
		t.Fatal(err)
	}
	request.Ref = otherRef
	request.Metadata, err = otherRef.SafeMetadata()
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(request.ValidateForContext(vectorTestContext("tenant-a")), ErrInvalidTenant) {
		t.Fatalf("caller tenant override was accepted")
	}
}

func TestRequestModelDimensionAndQueryBounds(t *testing.T) {
	source := vectorTestSource()
	ref, err := BuildDocumentRef(vectorTestContext("tenant-a"), source)
	if err != nil {
		t.Fatal(err)
	}
	request, err := BuildUpsertRequest(vectorTestContext("tenant-a"), source, vectorTestEmbedding(ref))
	if err != nil {
		t.Fatal(err)
	}
	request.Embedding.Model = "other-model"
	if !errors.Is(request.Validate(), ErrInvalidModel) {
		t.Fatalf("model mismatch was accepted")
	}
	request, err = BuildUpsertRequest(vectorTestContext("tenant-a"), source, vectorTestEmbedding(ref))
	if err != nil {
		t.Fatal(err)
	}
	request.Embedding.Dimension++
	if !errors.Is(request.Validate(), ErrInvalidDimension) {
		t.Fatalf("dimension mismatch was accepted")
	}
	if err := (SearchRequest{Query: []float64{math.NaN()}, TopK: 1}).Validate(); !errors.Is(err, ErrInvalidDimension) {
		t.Fatalf("non-finite query was accepted: %v", err)
	}
	if err := (SearchRequest{Query: []float64{1}, TopK: MaxTopK + 1}).Validate(); !errors.Is(err, ErrInvalidFilter) {
		t.Fatalf("unbounded topK was accepted: %v", err)
	}
}

func TestDeterministicEmbeddingIsStableAndBounded(t *testing.T) {
	embedder, err := NewDeterministicEmbedder("fake-model", "v1", 32, 128)
	if err != nil {
		t.Fatal(err)
	}
	first, err := embedder.Embed(context.Background(), "same input")
	if err != nil {
		t.Fatal(err)
	}
	second, err := embedder.Embed(context.Background(), "same input")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) || len(first.Values) != 32 || first.Dimension != 32 || first.Model != "fake-model" || first.ModelVersion != "v1" {
		t.Fatalf("embedding is not deterministic or correctly bounded")
	}
	third, err := embedder.Embed(context.Background(), "different input")
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(first.Values, third.Values) {
		t.Fatalf("content change did not change deterministic vector")
	}
	if _, err := embedder.Embed(context.Background(), strings.Repeat("x", 129)); !errors.Is(err, ErrInvalidDocument) {
		t.Fatalf("oversized embedding input error = %v", err)
	}
}

func TestDeterministicEmbeddingHonorsCancellationAndDeadline(t *testing.T) {
	embedder, err := NewDeterministicEmbedder("fake-model", "v1", 32, 128)
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := embedder.Embed(cancelled, "input"); !errors.Is(err, ErrCancelled) {
		t.Fatalf("cancelled embedding error = %v", err)
	}
	deadline, cancelDeadline := context.WithTimeout(context.Background(), 0)
	defer cancelDeadline()
	if _, err := embedder.Embed(deadline, "input"); !errors.Is(err, ErrTimeout) {
		t.Fatalf("deadline embedding error = %v", err)
	}
}

func TestDisabledBackendDoesNotConstructFactory(t *testing.T) {
	called := false
	store, err := ResolveBackend(context.Background(), BackendConfig{Mode: BackendNone}, func(context.Context, BackendConfig) (VectorStore, error) {
		called = true
		return DisabledStore{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatalf("disabled backend invoked client factory")
	}
	if _, ok := store.(DisabledStore); !ok {
		t.Fatalf("disabled backend returned %T", store)
	}
	ctx := vectorTestContext("tenant-a")
	if _, err := store.Search(ctx, SearchRequest{Query: []float64{1}, TopK: 1}); !errors.Is(err, ErrDisabled) {
		t.Fatalf("disabled search error = %v", err)
	}
	if err := store.Ready(context.Background()); !errors.Is(err, ErrDisabled) {
		t.Fatalf("disabled readiness error = %v", err)
	}
	incomplete := BackendConfig{Mode: BackendMilvus}
	if _, err := ResolveBackend(context.Background(), incomplete, func(context.Context, BackendConfig) (VectorStore, error) {
		t.Fatalf("incomplete config reached factory")
		return nil, nil
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("incomplete enabled config error = %v", err)
	}
}

func TestVectorErrorsDoNotContainInput(t *testing.T) {
	secret := "Authorization Bearer synthetic-secret"
	cfg := BackendConfig{Mode: BackendMilvus, EndpointRef: secret}
	_, err := ResolveBackend(context.Background(), cfg, nil)
	if err == nil || !errors.Is(err, ErrInvalidConfig) || strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "synthetic-secret") {
		t.Fatalf("unsafe config error: %v", err)
	}
}

func TestContextDeadlineClassificationIsBounded(t *testing.T) {
	ctx, cancel := context.WithTimeout(vectorTestContext("tenant-a"), time.Nanosecond)
	defer cancel()
	<-ctx.Done()
	if _, err := TrustedTenantContext(ctx); !errors.Is(err, ErrTimeout) {
		t.Fatalf("context timeout error = %v", err)
	}
}

func TestMemorySourceAndTenantFilterAreServerOwned(t *testing.T) {
	ctx := vectorTestContext("tenant-a")
	value := memory.Memory{
		TenantID: "tenant-a", ID: "memory-a", Scope: memory.ScopeSession,
		ScopeID: "session-a", SessionID: "session-a", Kind: "fact",
		Content: "durable memory", Version: 4, SourceSeq: 12,
	}
	config := ProjectionConfig{Model: "fake-model", ModelVersion: "v1", Dimension: 16, SchemaVersion: "schema-1"}
	source, err := MemorySource(ctx, value, config)
	if err != nil {
		t.Fatal(err)
	}
	if source.SourceType != SourceTypeMemory || source.SourceID != value.ID || source.SourceVersion != value.Version || source.SourceSequence != value.SourceSeq || source.ProjectionScope != "memory:session" {
		t.Fatalf("memory source mapping lost durable identity: %#v", source)
	}
	if value.VectorRef != "" {
		t.Fatalf("test fixture unexpectedly has vector_ref")
	}
	if _, err := MemorySource(ctx, value, ProjectionConfig{}); !errors.Is(err, ErrInvalidModel) {
		t.Fatalf("incomplete projection config error = %v", err)
	}
	value.TenantID = "tenant-b"
	if _, err := MemorySource(ctx, value, config); !errors.Is(err, ErrInvalidTenant) {
		t.Fatalf("cross-tenant memory mapping error = %v", err)
	}
	filter, err := BuildTenantFilter(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if filter.TenantID() != "tenant-a" {
		t.Fatalf("tenant filter was not server-owned")
	}
	if _, err := BuildTenantFilter(context.Background()); !errors.Is(err, ErrInvalidContext) {
		t.Fatalf("missing context filter error = %v", err)
	}
}

func TestProjectionVersionAndSchemaChangesAreNotSilentReuse(t *testing.T) {
	ctx := vectorTestContext("tenant-a")
	source := vectorTestSource()
	base, err := BuildDocumentRef(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	versionChange := source
	versionChange.SourceVersion++
	versionRef, err := BuildDocumentRef(ctx, versionChange)
	if err != nil {
		t.Fatal(err)
	}
	if base.DocumentID != versionRef.DocumentID || base.SourceVersion == versionRef.SourceVersion || base.ContentHash != versionRef.ContentHash {
		t.Fatalf("source version changed stable identity or hash unexpectedly")
	}
	schemaChange := source
	schemaChange.SchemaVersion = "schema-2"
	schemaRef, err := BuildDocumentRef(ctx, schemaChange)
	if err != nil {
		t.Fatal(err)
	}
	if base.DocumentID == schemaRef.DocumentID || base.SchemaVersion == schemaRef.SchemaVersion {
		t.Fatalf("schema change reused incompatible projection identity")
	}
	invalidSchema := base
	invalidSchema.SchemaVersion = ""
	if !errors.Is(invalidSchema.Validate(), ErrInvalidSchema) {
		t.Fatalf("invalid schema was accepted")
	}
}

func TestSearchSurfaceHasNoCallerBackendControlsOrVectorLeak(t *testing.T) {
	searchType := reflect.TypeOf(SearchRequest{})
	for _, name := range []string{"TenantID", "Collection", "Namespace", "Partition", "Filter", "Expression"} {
		if _, ok := searchType.FieldByName(name); ok {
			t.Fatalf("SearchRequest exposes caller backend control %q", name)
		}
	}
	secretVector := "987654321.125"
	err := (SearchRequest{Query: []float64{math.NaN(), 987654321.125}, TopK: 1}).ValidateForContext(vectorTestContext("tenant-a"))
	if !errors.Is(err, ErrInvalidDimension) || strings.Contains(err.Error(), secretVector) {
		t.Fatalf("query validation leaked vector value: %v", err)
	}
}
