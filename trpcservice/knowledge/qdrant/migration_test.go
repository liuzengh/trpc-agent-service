package qdrant

import (
	"context"
	"reflect"
	"testing"

	platformknowledge "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	qdrantpb "github.com/qdrant/go-client/qdrant"
	"google.golang.org/protobuf/proto"
)

type fakeMigrationClient struct {
	points         []*qdrantpb.RetrievedPoint
	scrollRequests []*qdrantpb.ScrollPoints
	upsertRequests []*qdrantpb.UpsertPoints
	closeCalls     int
	scrollErr      error
	upsertErr      error
}

func (f *fakeMigrationClient) CollectionExists(context.Context, string) (bool, error) {
	return true, nil
}

func (f *fakeMigrationClient) GetCollectionInfo(context.Context, string) (*qdrantpb.CollectionInfo, error) {
	return &qdrantpb.CollectionInfo{}, nil
}

func (f *fakeMigrationClient) CreateCollection(context.Context, *qdrantpb.CreateCollection) error {
	return nil
}

func (f *fakeMigrationClient) DeleteCollection(context.Context, string) error {
	return nil
}

func (f *fakeMigrationClient) CreateFieldIndex(context.Context, *qdrantpb.CreateFieldIndexCollection) (*qdrantpb.UpdateResult, error) {
	return nil, nil
}

func (f *fakeMigrationClient) Upsert(_ context.Context, request *qdrantpb.UpsertPoints) (*qdrantpb.UpdateResult, error) {
	f.upsertRequests = append(f.upsertRequests, request)
	if f.upsertErr != nil {
		return nil, f.upsertErr
	}
	return nil, nil
}

func (f *fakeMigrationClient) Get(context.Context, *qdrantpb.GetPoints) ([]*qdrantpb.RetrievedPoint, error) {
	return nil, nil
}

func (f *fakeMigrationClient) Delete(context.Context, *qdrantpb.DeletePoints) (*qdrantpb.UpdateResult, error) {
	return nil, nil
}

func (f *fakeMigrationClient) SetPayload(context.Context, *qdrantpb.SetPayloadPoints) (*qdrantpb.UpdateResult, error) {
	return nil, nil
}

func (f *fakeMigrationClient) Query(context.Context, *qdrantpb.QueryPoints) ([]*qdrantpb.ScoredPoint, error) {
	return nil, nil
}

func (f *fakeMigrationClient) Count(context.Context, *qdrantpb.CountPoints) (uint64, error) {
	return 0, nil
}

func (f *fakeMigrationClient) Scroll(_ context.Context, request *qdrantpb.ScrollPoints) ([]*qdrantpb.RetrievedPoint, error) {
	f.scrollRequests = append(f.scrollRequests, request)
	if f.scrollErr != nil {
		return nil, f.scrollErr
	}
	return f.points, nil
}

func (f *fakeMigrationClient) Close() error {
	f.closeCalls++
	return nil
}

func testChunkRef() platformknowledge.ChunkRef {
	return platformknowledge.ChunkRef{
		Scope:           tenant.Scope{TenantID: "tenant-a", AppID: "app-a"},
		ConfigVersion:   "v1",
		KnowledgeBaseID: "kb-a",
		DocumentID:      "doc-a",
		DocumentVersion: "7",
		ChunkID:         "chunk-a",
		IndexGeneration: "generation-a",
	}
}

func testRetrievedPoint() *qdrantpb.RetrievedPoint {
	return &qdrantpb.RetrievedPoint{
		Id: &qdrantpb.PointId{PointIdOptions: &qdrantpb.PointId_Uuid{Uuid: "point-a"}},
		Payload: qdrantpb.NewValueMap(map[string]any{
			"metadata": map[string]any{
				platformknowledge.MetadataTenantID:        "tenant-a",
				platformknowledge.MetadataAppID:           "app-a",
				platformknowledge.MetadataKnowledgeBaseID: "kb-a",
				platformknowledge.MetadataDocumentID:      "doc-a",
				platformknowledge.MetadataDocumentVersion: "7",
				platformknowledge.MetadataChunkID:         "chunk-a",
				platformknowledge.MetadataIndexGeneration: "generation-a",
			},
		}),
		Vectors: &qdrantpb.VectorsOutput{
			VectorsOptions: &qdrantpb.VectorsOutput_Vector{
				Vector: &qdrantpb.VectorOutput{Vector: &qdrantpb.VectorOutput_Dense{Dense: &qdrantpb.DenseVector{Data: []float32{0.1, 0.2, 0.3}}}},
			},
		},
	}
}

func TestMigrationCopierCopiesAndVerifiesScopedPoint(t *testing.T) {
	ref := testChunkRef()
	point := testRetrievedPoint()
	source := &fakeMigrationClient{points: []*qdrantpb.RetrievedPoint{point}}
	target := &fakeMigrationClient{points: []*qdrantpb.RetrievedPoint{proto.Clone(point).(*qdrantpb.RetrievedPoint)}}
	copier := &MigrationCopier{
		source:           source,
		target:           target,
		sourceCollection: "source-generation-a",
		targetCollection: "target-generation-a",
	}

	if err := copier.CopyKnowledgeChunk(context.Background(), ref); err != nil {
		t.Fatalf("copy knowledge chunk: %v", err)
	}
	if len(source.scrollRequests) != 1 || len(target.upsertRequests) != 1 {
		t.Fatalf("source scrolls = %d, target upserts = %d, want one each", len(source.scrollRequests), len(target.upsertRequests))
	}
	request := source.scrollRequests[0]
	if request.CollectionName != "source-generation-a" || request.GetLimit() != 2 || request.GetWithPayload().GetEnable() != true || request.GetWithVectors().GetEnable() != true {
		t.Fatalf("unexpected source read request: %v", request)
	}
	if got := target.upsertRequests[0].GetCollectionName(); got != "target-generation-a" {
		t.Fatalf("target collection = %q, want target-generation-a", got)
	}
	if !target.upsertRequests[0].GetWait() {
		t.Fatal("target upsert must wait for durable acknowledgement")
	}
	if got := target.upsertRequests[0].GetPoints()[0].GetVectors().GetVector().GetDense().GetData(); !reflect.DeepEqual(got, []float32{0.1, 0.2, 0.3}) {
		t.Fatalf("copied vector = %v", got)
	}

	gotConditions := make(map[string]string)
	for _, condition := range request.GetFilter().GetMust() {
		field := condition.GetField()
		if field == nil || field.GetMatch() == nil {
			t.Fatalf("unexpected non-match condition: %v", condition)
		}
		gotConditions[field.GetKey()] = field.GetMatch().GetKeyword()
	}
	wantConditions := map[string]string{
		"metadata.tenant_id":         "tenant-a",
		"metadata.app_id":            "app-a",
		"metadata.knowledge_base_id": "kb-a",
		"metadata.document_id":       "doc-a",
		"metadata.document_version":  "7",
		"metadata.chunk_id":          "chunk-a",
		"metadata.index_generation":  "generation-a",
	}
	if !reflect.DeepEqual(gotConditions, wantConditions) {
		t.Fatalf("scope filter = %#v, want %#v", gotConditions, wantConditions)
	}

	if err := copier.VerifyKnowledgeChunk(context.Background(), ref); err != nil {
		t.Fatalf("verify copied knowledge chunk: %v", err)
	}
	target.points[0].Payload["metadata"] = qdrantpb.NewValueMap(map[string]any{"changed": "yes"})["metadata"]
	if err := copier.VerifyKnowledgeChunk(context.Background(), ref); err == nil {
		t.Fatal("verification must reject a changed target point")
	}
}

func TestMigrationCopierCloseIsIdempotent(t *testing.T) {
	source := &fakeMigrationClient{}
	target := &fakeMigrationClient{}
	copier := &MigrationCopier{source: source, target: target}
	if err := copier.Close(); err != nil {
		t.Fatalf("close copier: %v", err)
	}
	if err := copier.Close(); err != nil {
		t.Fatalf("close copier twice: %v", err)
	}
	if source.closeCalls != 1 || target.closeCalls != 1 {
		t.Fatalf("close calls = source %d target %d, want one each", source.closeCalls, target.closeCalls)
	}
}

func TestInputVectorsCopiesSupportedRepresentations(t *testing.T) {
	denseOutput := func(data []float32) *qdrantpb.VectorOutput {
		return &qdrantpb.VectorOutput{Vector: &qdrantpb.VectorOutput_Dense{Dense: &qdrantpb.DenseVector{Data: data}}}
	}
	cases := []struct {
		name   string
		output *qdrantpb.VectorsOutput
		check  func(*qdrantpb.Vectors) bool
	}{
		{
			name: "dense",
			output: &qdrantpb.VectorsOutput{VectorsOptions: &qdrantpb.VectorsOutput_Vector{
				Vector: denseOutput([]float32{1, 2}),
			}},
			check: func(vectors *qdrantpb.Vectors) bool {
				return reflect.DeepEqual(vectors.GetVector().GetDense().GetData(), []float32{1, 2})
			},
		},
		{
			name: "legacy-dense",
			output: &qdrantpb.VectorsOutput{VectorsOptions: &qdrantpb.VectorsOutput_Vector{
				Vector: &qdrantpb.VectorOutput{Data: []float32{6, 7, 8}},
			}},
			check: func(vectors *qdrantpb.Vectors) bool {
				return reflect.DeepEqual(vectors.GetVector().GetDense().GetData(), []float32{6, 7, 8})
			},
		},
		{
			name: "sparse",
			output: &qdrantpb.VectorsOutput{VectorsOptions: &qdrantpb.VectorsOutput_Vector{
				Vector: &qdrantpb.VectorOutput{Vector: &qdrantpb.VectorOutput_Sparse{Sparse: &qdrantpb.SparseVector{Indices: []uint32{2, 9}, Values: []float32{0.5, 0.8}}}},
			}},
			check: func(vectors *qdrantpb.Vectors) bool {
				return reflect.DeepEqual(vectors.GetVector().GetSparse().GetIndices(), []uint32{2, 9}) && reflect.DeepEqual(vectors.GetVector().GetSparse().GetValues(), []float32{0.5, 0.8})
			},
		},
		{
			name: "multi-dense",
			output: &qdrantpb.VectorsOutput{VectorsOptions: &qdrantpb.VectorsOutput_Vector{
				Vector: &qdrantpb.VectorOutput{Vector: &qdrantpb.VectorOutput_MultiDense{MultiDense: &qdrantpb.MultiDenseVector{Vectors: []*qdrantpb.DenseVector{{Data: []float32{1}}, {Data: []float32{2, 3}}}}}},
			}},
			check: func(vectors *qdrantpb.Vectors) bool {
				got := vectors.GetVector().GetMultiDense().GetVectors()
				return len(got) == 2 && reflect.DeepEqual(got[0].GetData(), []float32{1}) && reflect.DeepEqual(got[1].GetData(), []float32{2, 3})
			},
		},
		{
			name: "named",
			output: &qdrantpb.VectorsOutput{VectorsOptions: &qdrantpb.VectorsOutput_Vectors{
				Vectors: &qdrantpb.NamedVectorsOutput{Vectors: map[string]*qdrantpb.VectorOutput{"title": denseOutput([]float32{4, 5})}},
			}},
			check: func(vectors *qdrantpb.Vectors) bool {
				return reflect.DeepEqual(vectors.GetVectors().GetVectors()["title"].GetDense().GetData(), []float32{4, 5})
			},
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			got, err := inputVectors(test.output)
			if err != nil {
				t.Fatalf("input vectors: %v", err)
			}
			if !test.check(got) {
				t.Fatalf("unexpected converted vectors: %v", got)
			}
		})
	}
}
