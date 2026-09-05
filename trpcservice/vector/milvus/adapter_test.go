package milvus

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	sdk "github.com/milvus-io/milvus/client/v2/milvusclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
)

type fakeClient struct {
	collection   *entity.Collection
	index        sdk.IndexDescription
	load         entity.LoadState
	upsertResult sdk.UpsertResult
	deleteResult sdk.DeleteResult
	searchResult []sdk.ResultSet
	queryResult  sdk.ResultSet
	upsertOption sdk.UpsertOption
	deleteOption sdk.DeleteOption
	searchOption sdk.SearchOption
	closeErr     error
	mu           sync.Mutex
	calls        int
	closeCalls   int
}

func (f *fakeClient) DescribeCollection(context.Context, sdk.DescribeCollectionOption, ...grpc.CallOption) (*entity.Collection, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.collection, nil
}

func (f *fakeClient) DescribeIndex(context.Context, sdk.DescribeIndexOption, ...grpc.CallOption) (sdk.IndexDescription, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.index, nil
}

func (f *fakeClient) GetLoadState(context.Context, sdk.GetLoadStateOption, ...grpc.CallOption) (entity.LoadState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.load, nil
}

func (f *fakeClient) Upsert(_ context.Context, option sdk.UpsertOption, _ ...grpc.CallOption) (sdk.UpsertResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.upsertOption = option
	return f.upsertResult, nil
}

func (f *fakeClient) Delete(_ context.Context, option sdk.DeleteOption, _ ...grpc.CallOption) (sdk.DeleteResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.deleteOption = option
	return f.deleteResult, nil
}

func (f *fakeClient) Search(_ context.Context, option sdk.SearchOption, _ ...grpc.CallOption) ([]sdk.ResultSet, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.searchOption = option
	return f.searchResult, nil
}

func (f *fakeClient) Query(_ context.Context, option sdk.QueryOption, _ ...grpc.CallOption) (sdk.ResultSet, error) {
	return f.queryResult, nil
}

func (f *fakeClient) Close(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCalls++
	return f.closeErr
}

func testConfig() vector.BackendConfig {
	return vector.BackendConfig{Mode: vector.BackendMilvus, EndpointRef: "local-endpoint", Collection: "p106b_collection", CredentialRef: "local-credential", Model: "fake-model", ModelVersion: "v1", Dimension: 4, SchemaVersion: "schema-1", Metric: "cosine", MaxInputBytes: 128, MaxMetadataBytes: vector.MaxMetadataBytes, MaxTopK: 5, OperationTimeout: time.Second, ReadinessTimeout: time.Second}
}

func testContext(id string) context.Context {
	return tenant.WithContext(context.Background(), tenant.TenantContext{TenantID: id, AgentAppID: "agent-a", BindingID: "binding-a", Channel: "web", RequestID: "request-a", MessageID: "message-a", TraceID: "trace-a", ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "memory", Memory: "memory", Vector: vector.BackendMilvus, Object: "memory"}})
}

func testSource() vector.SourceDocument {
	return vector.SourceDocument{SourceType: vector.SourceTypeMemory, SourceID: "memory-a", ProjectionScope: "default", SourceVersion: 1, SourceSequence: 1, Content: "content", Model: "fake-model", ModelVersion: "v1", Dimension: 4, SchemaVersion: "schema-1"}
}

func testUpsert(t *testing.T, ctx context.Context) vector.UpsertRequest {
	t.Helper()
	source := testSource()
	ref, err := vector.BuildDocumentRef(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	request, err := vector.BuildUpsertRequest(ctx, source, vector.Embedding{Values: []float64{1, 0, 0, 0}, Model: ref.Model, ModelVersion: ref.ModelVersion, Dimension: ref.Dimension})
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func readyFake(cfg vector.BackendConfig) *fakeClient {
	return &fakeClient{collection: &entity.Collection{Schema: expectedSchema(cfg), Properties: expectedProperties(cfg)}, index: sdk.IndexDescription{Index: index.NewGenericIndex(indexName, map[string]string{index.IndexTypeKey: string(index.Flat), index.MetricTypeKey: strings.ToUpper(cfg.Metric)}), State: index.IndexState(commonpb.IndexState_Finished)}, load: entity.LoadState{State: entity.LoadStateLoaded}}
}

func TestDisabledBackendDoesNotResolveOrConstruct(t *testing.T) {
	store, err := New(context.Background(), vector.BackendConfig{Mode: vector.BackendNone}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := store.(vector.DisabledStore); !ok {
		t.Fatalf("disabled backend returned %T", store)
	}
}

func TestEnabledBackendRequiresCompleteServerOwnedConfig(t *testing.T) {
	cfg := testConfig()
	cfg.Collection = ""
	if _, err := newStore(cfg, &fakeClient{}); !errors.Is(err, vector.ErrInvalidConfig) {
		t.Fatalf("invalid collection error = %v", err)
	}
	if _, err := New(context.Background(), testConfig(), nil, nil); !errors.Is(err, vector.ErrInvalidConfig) {
		t.Fatalf("missing resolver error = %v", err)
	}
	if _, err := New(context.Background(), testConfig(), endpointResolverFunc(func(context.Context, string) (string, error) { return "127.0.0.1:1", nil }), credentialResolverFunc(func(context.Context, string) (Credentials, error) { return Credentials{}, nil })); !errors.Is(err, vector.ErrInvalidConfig) {
		t.Fatalf("empty credential error = %v", err)
	}
}

type endpointResolverFunc func(context.Context, string) (string, error)

func (f endpointResolverFunc) Resolve(ctx context.Context, ref string) (string, error) {
	return f(ctx, ref)
}

type credentialResolverFunc func(context.Context, string) (Credentials, error)

func (f credentialResolverFunc) Resolve(ctx context.Context, ref string) (Credentials, error) {
	return f(ctx, ref)
}

func TestReadyValidatesOwnedSchemaIndexAndLoad(t *testing.T) {
	cfg := testConfig()
	fake := readyFake(cfg)
	store, err := newStore(cfg, fake)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func resultSet(ref vector.VectorDocumentRef, score float32) sdk.ResultSet {
	metadata, _ := ref.SafeMetadata()
	return sdk.ResultSet{ResultCount: 1, IDs: column.NewColumnVarChar(fieldID, []string{ref.DocumentID}), Scores: []float32{score}, Fields: sdk.DataSet{
		column.NewColumnVarChar(fieldTenant, []string{ref.TenantID}),
		column.NewColumnVarChar(fieldSourceType, []string{ref.SourceType}),
		column.NewColumnVarChar(fieldSourceID, []string{ref.SourceID}),
		column.NewColumnVarChar(fieldSourceRef, []string{metadata["source_ref"]}),
		column.NewColumnVarChar(fieldProjection, []string{ref.ProjectionScope}),
		column.NewColumnInt64(fieldSourceVersion, []int64{ref.SourceVersion}),
		column.NewColumnInt64(fieldSourceSequence, []int64{ref.SourceSequence}),
		column.NewColumnVarChar(fieldContentHash, []string{ref.ContentHash}),
		column.NewColumnVarChar(fieldOperation, []string{string(ref.Operation)}),
		column.NewColumnVarChar(fieldModel, []string{ref.Model}),
		column.NewColumnVarChar(fieldModelVersion, []string{ref.ModelVersion}),
		column.NewColumnInt64(fieldDimension, []int64{int64(ref.Dimension)}),
		column.NewColumnVarChar(fieldSchemaVersion, []string{ref.SchemaVersion}),
	}}
}

func fieldPayload(request *milvuspb.UpsertRequest, name string) *schemapb.FieldData {
	for _, field := range request.FieldsData {
		if field.GetFieldName() == name {
			return field
		}
	}
	return nil
}

func TestUpsertMapsOnlyBoundedProjectionFields(t *testing.T) {
	cfg := testConfig()
	ctx := testContext("tenant-a")
	request := testUpsert(t, ctx)
	fake := readyFake(cfg)
	fake.upsertResult = sdk.UpsertResult{UpsertCount: 1, IDs: column.NewColumnVarChar(fieldID, []string{request.Ref.DocumentID})}
	store, err := newStore(cfg, fake)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Upsert(ctx, request); err != nil {
		t.Fatal(err)
	}
	payload, err := fake.upsertOption.UpsertRequest(fake.collection)
	if err != nil {
		t.Fatal(err)
	}
	if payload.CollectionName != cfg.Collection || payload.NumRows != 1 {
		t.Fatalf("unexpected upsert payload: collection=%q rows=%d", payload.CollectionName, payload.NumRows)
	}
	names := make([]string, 0, len(payload.FieldsData))
	for _, field := range payload.FieldsData {
		names = append(names, field.GetFieldName())
	}
	if len(names) != len(expectedSchema(cfg).Fields) || fieldPayload(payload, "content") != nil {
		t.Fatalf("upsert payload contains unexpected fields: %v", names)
	}
	if reflect.DeepEqual(request.Ref.DocumentID, request.Ref.SourceID) {
		t.Fatalf("document id unexpectedly came from source id")
	}
}

func TestTenantAndDeleteExpressionAreServerOwned(t *testing.T) {
	cfg := testConfig()
	ctxA := testContext("tenant-a")
	request := testUpsert(t, ctxA)
	fake := readyFake(cfg)
	fake.deleteResult = sdk.DeleteResult{DeleteCount: 1}
	store, err := newStore(cfg, fake)
	if err != nil {
		t.Fatal(err)
	}
	deleted := request.Ref
	deleted.Operation = vector.OperationDelete
	deleted.Deleted = true
	deleteMetadata, err := deleted.SafeMetadata()
	if err != nil {
		t.Fatal(err)
	}
	deleteRequest := vector.DeleteRequest{Ref: deleted, Tombstone: true, Metadata: deleteMetadata}
	if err := store.Delete(ctxA, deleteRequest); err != nil {
		t.Fatal(err)
	}
	expression := fake.deleteOption.Request().Expr
	if !strings.Contains(expression, `tenant_id == "tenant-a"`) || !strings.Contains(expression, `id == "`+request.Ref.DocumentID+`"`) || strings.Contains(expression, "content") {
		t.Fatalf("unsafe delete expression: %q", expression)
	}
	other := testUpsert(t, testContext("tenant-b"))
	if err := store.Upsert(ctxA, other); !errors.Is(err, vector.ErrInvalidTenant) {
		t.Fatalf("cross-tenant upsert error = %v", err)
	}
	otherDeleted := other.Ref
	otherDeleted.Operation = vector.OperationDelete
	otherDeleted.Deleted = true
	otherMetadata, err := otherDeleted.SafeMetadata()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctxA, vector.DeleteRequest{Ref: otherDeleted, Tombstone: true, Metadata: otherMetadata}); !errors.Is(err, vector.ErrInvalidTenant) {
		t.Fatalf("cross-tenant delete error = %v", err)
	}
}
func TestSearchMapsResultAndBindsTenant(t *testing.T) {
	cfg := testConfig()
	ctx := testContext("tenant-a")
	request := testUpsert(t, ctx)
	fake := readyFake(cfg)
	fake.searchResult = []sdk.ResultSet{resultSet(request.Ref, 0.91)}
	store, err := newStore(cfg, fake)
	if err != nil {
		t.Fatal(err)
	}
	results, err := store.Search(ctx, vector.SearchRequest{Query: []float64{1, 0, 0, 0}, TopK: 2, MinScore: 0.9})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Ref.DocumentID != request.Ref.DocumentID || results[0].Score < 0.90 || results[0].Score > 0.92 {
		t.Fatalf("unexpected search result: %#v", results)
	}
	payload, err := fake.searchOption.Request()
	if err != nil {
		t.Fatal(err)
	}
	if payload.CollectionName != cfg.Collection || payload.GetDsl() != tenantFilterExpression() || payload.ExprTemplateValues[tenantTemplateName] == nil {
		t.Fatalf("tenant filter was not server-owned: expr=%q template=%v", payload.GetDsl(), payload.ExprTemplateValues)
	}
	if strings.Contains(strings.Join(payload.OutputFields, ","), fieldVector) {
		t.Fatalf("search requested vector output")
	}
	fake.searchResult = []sdk.ResultSet{resultSet(request.Ref, 0.1)}
	results, err = store.Search(ctx, vector.SearchRequest{Query: []float64{1, 0, 0, 0}, TopK: 2, MinScore: 0.9})
	if err != nil || len(results) != 0 {
		t.Fatalf("min score filtering failed: results=%#v err=%v", results, err)
	}
}

func TestBoundsCancellationClassificationAndRedaction(t *testing.T) {
	cfg := testConfig()
	ctx := testContext("tenant-a")
	store, err := newStore(cfg, readyFake(cfg))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Search(ctx, vector.SearchRequest{Query: []float64{1}, TopK: cfg.MaxTopK + 1}); !errors.Is(err, vector.ErrInvalidFilter) {
		t.Fatalf("topK error = %v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.Search(cancelled, vector.SearchRequest{Query: []float64{1, 0, 0, 0}, TopK: 1}); !errors.Is(err, vector.ErrCancelled) {
		t.Fatalf("cancelled error = %v", err)
	}
	if !errors.Is(classify(status.Error(codes.Unavailable, "secret endpoint")), vector.ErrUnavailable) {
		t.Fatal("unavailable status was not classified")
	}
	if !errors.Is(classify(status.Error(codes.ResourceExhausted, "secret token")), vector.ErrRetryable) {
		t.Fatal("resource exhaustion was not classified")
	}
	redacted := classify(errors.New("raw dsn postgres://user:password@host/db"))
	if strings.Contains(redacted.Error(), "postgres://") || strings.Contains(redacted.Error(), "password") {
		t.Fatalf("raw provider error leaked: %v", redacted)
	}
}

func TestCloseIsIdempotentConcurrentAndPostCloseFails(t *testing.T) {
	cfg := testConfig()
	fake := readyFake(cfg)
	store, err := newStore(cfg, fake)
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			if err := store.Close(context.Background()); err != nil {
				t.Errorf("close error = %v", err)
			}
		}()
	}
	group.Wait()
	fake.mu.Lock()
	closeCalls := fake.closeCalls
	fake.mu.Unlock()
	if closeCalls != 1 {
		t.Fatalf("close called %d times", closeCalls)
	}
	if _, err := store.Search(testContext("tenant-a"), vector.SearchRequest{Query: []float64{1, 0, 0, 0}, TopK: 1}); !errors.Is(err, vector.ErrUnavailable) {
		t.Fatalf("post-close search error = %v", err)
	}
}

func TestRequestMustMatchServerOwnedBackendConfig(t *testing.T) {
	cfg := testConfig()
	ctx := testContext("tenant-a")
	store, err := newStore(cfg, readyFake(cfg))
	if err != nil {
		t.Fatal(err)
	}
	build := func(dimension int, model, schema string) vector.UpsertRequest {
		t.Helper()
		source := testSource()
		source.Dimension = dimension
		source.Model = model
		source.SchemaVersion = schema
		request, err := vector.BuildUpsertRequest(ctx, source, vector.Embedding{Values: make([]float64, dimension), Model: model, ModelVersion: "v1", Dimension: dimension})
		if err != nil {
			t.Fatal(err)
		}
		return request
	}
	if err := store.Upsert(ctx, build(cfg.Dimension+1, cfg.Model, cfg.SchemaVersion)); !errors.Is(err, vector.ErrInvalidDimension) {
		t.Fatalf("dimension mismatch error = %v", err)
	}
	if err := store.Upsert(ctx, build(cfg.Dimension, "other-model", cfg.SchemaVersion)); !errors.Is(err, vector.ErrInvalidModel) {
		t.Fatalf("model mismatch error = %v", err)
	}
	if err := store.Upsert(ctx, build(cfg.Dimension, cfg.Model, "schema-other")); !errors.Is(err, vector.ErrInvalidSchema) {
		t.Fatalf("schema mismatch error = %v", err)
	}
	if _, err := store.Search(ctx, vector.SearchRequest{Query: make([]float64, cfg.Dimension+1), TopK: 1}); !errors.Is(err, vector.ErrInvalidDimension) {
		t.Fatalf("search dimension mismatch error = %v", err)
	}
	mismatched := build(cfg.Dimension+1, cfg.Model, cfg.SchemaVersion)
	deleted := mismatched.Ref
	deleted.Operation = vector.OperationDelete
	deleted.Deleted = true
	metadata, err := deleted.SafeMetadata()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, vector.DeleteRequest{Ref: deleted, Tombstone: true, Metadata: metadata}); !errors.Is(err, vector.ErrInvalidSchema) {
		t.Fatalf("delete projection mismatch error = %v", err)
	}
}
