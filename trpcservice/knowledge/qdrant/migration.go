package qdrant

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	platformconfig "github.com/liuzengh/trpc-agent-service/trpcservice/config"
	platformknowledge "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	qdrantpb "github.com/qdrant/go-client/qdrant"
	"google.golang.org/protobuf/proto"
	frameworkqdrant "trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/qdrant"
	qdrantstorage "trpc.group/trpc-go/trpc-agent-go/storage/qdrant"
)

var _ migration.KnowledgeCopier = (*MigrationCopier)(nil)

// MigrationCopier copies authorized vector points between the immutable source
// and target Knowledge collections selected by a migration's config versions.
// It never enumerates a collection: every point is addressed by a SQL-owned
// ChunkRef and an exact Qdrant payload filter.
type MigrationCopier struct {
	source           qdrantstorage.Client
	target           qdrantstorage.Client
	sourceCollection string
	targetCollection string

	closeOnce sync.Once
	closeErr  error
}

// NewMigrationCopier resolves both Qdrant backends through the same trusted
// config, secret, and endpoint boundaries used by the worker runtime.
func NewMigrationCopier(
	ctx context.Context,
	configs platformconfig.Resolver,
	secrets platformsecret.SecretProvider,
	endpoints EndpointResolver,
	record migration.Record,
) (*MigrationCopier, error) {
	if configs == nil || secrets == nil || endpoints == nil {
		return nil, migration.NewPermanentError(errors.New("knowledge migration dependencies are required"))
	}
	if err := record.Validate(); err != nil {
		return nil, migration.NewPermanentError(err)
	}
	if record.EffectiveDomain() != migration.DomainKnowledge {
		return nil, migration.NewPermanentError(errors.New("knowledge migration domain is required"))
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	sourceConfig, err := configs.ResolveAppConfig(ctx, record.TenantID, record.AppID, record.SourceConfigVersion)
	if err != nil {
		return nil, fmt.Errorf("resolve source knowledge config: %w", err)
	}
	targetConfig, err := configs.ResolveAppConfig(ctx, record.TenantID, record.AppID, record.TargetConfigVersion)
	if err != nil {
		return nil, fmt.Errorf("resolve target knowledge config: %w", err)
	}
	sourceSettings, err := validateBackend(sourceConfig.BackendConfig.Knowledge)
	if err != nil {
		return nil, migration.NewPermanentError(fmt.Errorf("validate source knowledge backend: %w", err))
	}
	targetSettings, err := validateBackend(targetConfig.BackendConfig.Knowledge)
	if err != nil {
		return nil, migration.NewPermanentError(fmt.Errorf("validate target knowledge backend: %w", err))
	}
	if sourceSettings.embeddingDimensions != targetSettings.embeddingDimensions {
		return nil, migration.NewPermanentError(errors.New("knowledge migration source and target dimensions must match"))
	}
	if sourceSettings.indexGeneration != targetSettings.indexGeneration {
		return nil, migration.NewPermanentError(errors.New("knowledge migration source and target index generations must match"))
	}

	scope := tenant.Scope{TenantID: record.TenantID, AppID: record.AppID}
	sourceEndpoint, err := endpoints.ResolveQdrantEndpoint(ctx, sourceConfig.BackendConfig.Knowledge.Name)
	if err != nil {
		return nil, fmt.Errorf("resolve source qdrant endpoint: %w", err)
	}
	if err := sourceEndpoint.Validate(); err != nil {
		return nil, migration.NewPermanentError(fmt.Errorf("validate source qdrant endpoint: %w", err))
	}
	targetEndpoint, err := endpoints.ResolveQdrantEndpoint(ctx, targetConfig.BackendConfig.Knowledge.Name)
	if err != nil {
		return nil, fmt.Errorf("resolve target qdrant endpoint: %w", err)
	}
	if err := targetEndpoint.Validate(); err != nil {
		return nil, migration.NewPermanentError(fmt.Errorf("validate target qdrant endpoint: %w", err))
	}

	sourceClient, err := newMigrationClient(ctx, secrets, scope, sourceConfig.BackendConfig.Knowledge, sourceEndpoint)
	if err != nil {
		return nil, fmt.Errorf("create source qdrant client: %w", err)
	}
	closeSourceOnError := true
	defer func() {
		if closeSourceOnError {
			_ = sourceClient.Close()
		}
	}()

	sourceCollection := sourceSettings.collectionName()
	exists, err := sourceClient.CollectionExists(ctx, sourceCollection)
	if err != nil {
		return nil, fmt.Errorf("check source qdrant collection: %w", err)
	}
	if !exists {
		return nil, migration.NewPermanentError(errors.New("source qdrant collection does not exist"))
	}
	if err := validateMigrationCollection(ctx, sourceClient, sourceCollection, sourceSettings.embeddingDimensions); err != nil {
		if errors.Is(err, frameworkqdrant.ErrCollectionMismatch) {
			return nil, migration.NewPermanentError(fmt.Errorf("validate source qdrant collection: %w", err))
		}
		return nil, fmt.Errorf("validate source qdrant collection: %w", err)
	}

	targetClient, err := newMigrationClient(ctx, secrets, scope, targetConfig.BackendConfig.Knowledge, targetEndpoint)
	if err != nil {
		return nil, fmt.Errorf("create target qdrant client: %w", err)
	}
	closeTargetOnError := true
	defer func() {
		if closeTargetOnError {
			_ = targetClient.Close()
		}
	}()
	targetCollection := targetSettings.collectionName()
	if err := validateMigrationCollection(ctx, targetClient, targetCollection, targetSettings.embeddingDimensions); err != nil {
		if errors.Is(err, frameworkqdrant.ErrCollectionMismatch) {
			return nil, migration.NewPermanentError(fmt.Errorf("validate target qdrant collection: %w", err))
		}
		return nil, fmt.Errorf("validate target qdrant collection: %w", err)
	}

	closeSourceOnError = false
	closeTargetOnError = false
	return &MigrationCopier{
		source:           sourceClient,
		target:           targetClient,
		sourceCollection: sourceCollection,
		targetCollection: targetCollection,
	}, nil
}

func newMigrationClient(
	ctx context.Context,
	secrets platformsecret.SecretProvider,
	scope tenant.Scope,
	ref tenant.BackendRef,
	endpoint Endpoint,
) (qdrantstorage.Client, error) {
	options := []qdrantstorage.ClientBuilderOpt{
		qdrantstorage.WithHost(endpoint.Host),
		qdrantstorage.WithPort(endpoint.Port),
		qdrantstorage.WithTLS(endpoint.TLS),
	}
	if ref.SecretRef != (tenant.SecretRef{}) {
		apiKey, err := secrets.ResolveSecret(ctx, scope, ref.SecretRef)
		if err != nil {
			return nil, fmt.Errorf("resolve qdrant api key: %w", err)
		}
		if apiKey == "" {
			return nil, migration.NewPermanentError(errors.New("qdrant api key is required"))
		}
		options = append(options, qdrantstorage.WithAPIKey(apiKey))
	}
	return qdrantstorage.NewClient(ctx, options...)
}

func validateMigrationCollection(
	ctx context.Context,
	client qdrantstorage.Client,
	collection string,
	dimension int,
) error {
	store, err := frameworkqdrant.New(ctx,
		frameworkqdrant.WithClient(client),
		frameworkqdrant.WithCollectionName(collection),
		frameworkqdrant.WithDimension(dimension),
	)
	if err != nil {
		return err
	}
	return store.Close()
}

// Close releases the two clients exactly once.
func (c *MigrationCopier) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		if c.source != nil {
			c.closeErr = errors.Join(c.closeErr, c.source.Close())
		}
		if c.target != nil {
			c.closeErr = errors.Join(c.closeErr, c.target.Close())
		}
	})
	return c.closeErr
}

// CopyKnowledgeChunk copies one SQL-authorized point idempotently. The exact
// metadata filter prevents a point from another tenant, app, document, or
// index generation being selected even when Qdrant collections are shared.
func (c *MigrationCopier) CopyKnowledgeChunk(ctx context.Context, ref platformknowledge.ChunkRef) error {
	if c == nil || c.source == nil || c.target == nil {
		return errors.New("knowledge migration copier is not initialized")
	}
	if err := ref.Validate(); err != nil {
		return err
	}
	point, err := c.findPoint(ctx, c.source, c.sourceCollection, ref)
	if err != nil {
		return fmt.Errorf("read source knowledge point: %w", err)
	}
	inputVectors, err := inputVectors(point.Vectors)
	if err != nil {
		return fmt.Errorf("convert source knowledge vectors: %w", err)
	}
	clonePayload, err := clonePointPayload(point.Payload)
	if err != nil {
		return fmt.Errorf("clone source knowledge payload: %w", err)
	}
	_, err = c.target.Upsert(ctx, &qdrantpb.UpsertPoints{
		CollectionName: c.targetCollection,
		Wait:           boolPointer(true),
		Points: []*qdrantpb.PointStruct{{
			Id:      clonePointID(point.Id),
			Payload: clonePayload,
			Vectors: inputVectors,
		}},
	})
	if err != nil {
		return fmt.Errorf("write target knowledge point: %w", err)
	}
	return nil
}

// VerifyKnowledgeChunk reads both points using the same exact scope filter
// and compares their deterministic protobuf representation.
func (c *MigrationCopier) VerifyKnowledgeChunk(ctx context.Context, ref platformknowledge.ChunkRef) error {
	if c == nil || c.source == nil || c.target == nil {
		return errors.New("knowledge migration copier is not initialized")
	}
	if err := ref.Validate(); err != nil {
		return err
	}
	source, err := c.findPoint(ctx, c.source, c.sourceCollection, ref)
	if err != nil {
		return fmt.Errorf("read source knowledge point for verification: %w", err)
	}
	target, err := c.findPoint(ctx, c.target, c.targetCollection, ref)
	if err != nil {
		return fmt.Errorf("read target knowledge point for verification: %w", err)
	}
	sourceBytes, err := deterministicPointBytes(source)
	if err != nil {
		return fmt.Errorf("marshal source knowledge point: %w", err)
	}
	targetBytes, err := deterministicPointBytes(target)
	if err != nil {
		return fmt.Errorf("marshal target knowledge point: %w", err)
	}
	if !bytes.Equal(sourceBytes, targetBytes) {
		return errors.New("knowledge point verification mismatch")
	}
	return nil
}

func (c *MigrationCopier) findPoint(
	ctx context.Context,
	client qdrantstorage.Client,
	collection string,
	ref platformknowledge.ChunkRef,
) (*qdrantpb.RetrievedPoint, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	limit := uint32(2)
	points, err := client.Scroll(ctx, &qdrantpb.ScrollPoints{
		CollectionName: collection,
		Filter:         chunkFilter(ref),
		Limit:          &limit,
		WithPayload:    qdrantpb.NewWithPayload(true),
		WithVectors:    qdrantpb.NewWithVectors(true),
	})
	if err != nil {
		return nil, err
	}
	if len(points) == 0 {
		return nil, errors.New("knowledge point was not found")
	}
	if len(points) != 1 {
		return nil, errors.New("knowledge point identity is not unique")
	}
	if points[0] == nil || points[0].Id == nil || points[0].Vectors == nil {
		return nil, errors.New("knowledge point is incomplete")
	}
	return points[0], nil
}

func chunkFilter(ref platformknowledge.ChunkRef) *qdrantpb.Filter {
	metadataPrefix := "metadata."
	return &qdrantpb.Filter{Must: []*qdrantpb.Condition{
		qdrantpb.NewMatch(metadataPrefix+platformknowledge.MetadataTenantID, ref.Scope.TenantID),
		qdrantpb.NewMatch(metadataPrefix+platformknowledge.MetadataAppID, ref.Scope.AppID),
		qdrantpb.NewMatch(metadataPrefix+platformknowledge.MetadataKnowledgeBaseID, ref.KnowledgeBaseID),
		qdrantpb.NewMatch(metadataPrefix+platformknowledge.MetadataDocumentID, ref.DocumentID),
		qdrantpb.NewMatch(metadataPrefix+platformknowledge.MetadataDocumentVersion, ref.DocumentVersion),
		qdrantpb.NewMatch(metadataPrefix+platformknowledge.MetadataChunkID, ref.ChunkID),
		qdrantpb.NewMatch(metadataPrefix+platformknowledge.MetadataIndexGeneration, ref.IndexGeneration),
	}}
}

func inputVectors(output *qdrantpb.VectorsOutput) (*qdrantpb.Vectors, error) {
	if output == nil {
		return nil, errors.New("knowledge point vectors are required")
	}
	if output.VectorsOptions == nil {
		return nil, fmt.Errorf("knowledge point vector representation is unsupported: %T", output.VectorsOptions)
	}
	switch value := output.VectorsOptions.(type) {
	case *qdrantpb.VectorsOutput_Vector:
		vector, err := inputVector(value.Vector)
		if err != nil {
			return nil, err
		}
		return &qdrantpb.Vectors{VectorsOptions: &qdrantpb.Vectors_Vector{Vector: vector}}, nil
	case *qdrantpb.VectorsOutput_Vectors:
		if value.Vectors == nil {
			return nil, errors.New("named knowledge vectors are empty")
		}
		vectors := make(map[string]*qdrantpb.Vector, len(value.Vectors.Vectors))
		for name, outputVector := range value.Vectors.Vectors {
			vector, err := inputVector(outputVector)
			if err != nil {
				return nil, fmt.Errorf("named vector %q: %w", name, err)
			}
			vectors[name] = vector
		}
		if len(vectors) == 0 {
			return nil, errors.New("named knowledge vectors are empty")
		}
		return qdrantpb.NewVectorsMap(vectors), nil
	default:
		return nil, fmt.Errorf("knowledge point vector representation is unsupported: %T", output.VectorsOptions)
	}
}

func legacyInputVector(data []float32, indices *qdrantpb.SparseIndices, vectorsCount *uint32) (*qdrantpb.Vector, error) {
	if indices != nil {
		if len(data) == 0 || len(indices.Data) == 0 || len(indices.Data) != len(data) {
			return nil, errors.New("legacy sparse knowledge vector is invalid")
		}
		return qdrantpb.NewVectorSparse(append([]uint32(nil), indices.Data...), append([]float32(nil), data...)), nil
	}
	if len(data) == 0 {
		return nil, errors.New("legacy dense knowledge vector is empty")
	}
	if vectorsCount == nil || *vectorsCount == 0 {
		return qdrantpb.NewVectorDense(append([]float32(nil), data...)), nil
	}
	if uint64(*vectorsCount) > uint64(len(data)) || len(data)%int(*vectorsCount) != 0 {
		return nil, errors.New("legacy multi-dense knowledge vector is invalid")
	}
	width := len(data) / int(*vectorsCount)
	vectors := make([][]float32, *vectorsCount)
	for index := range vectors {
		start := index * width
		vectors[index] = append([]float32(nil), data[start:start+width]...)
	}
	return qdrantpb.NewVectorMulti(vectors), nil
}

func inputVector(output *qdrantpb.VectorOutput) (*qdrantpb.Vector, error) {
	if output == nil {
		return nil, errors.New("knowledge vector is empty")
	}
	if output.Vector == nil {
		return legacyInputVector(output.Data, output.Indices, output.VectorsCount)
	}
	switch value := output.Vector.(type) {
	case *qdrantpb.VectorOutput_Dense:
		if value.Dense == nil || len(value.Dense.Data) == 0 {
			return nil, errors.New("dense knowledge vector is empty")
		}
		return qdrantpb.NewVectorDense(append([]float32(nil), value.Dense.Data...)), nil
	case *qdrantpb.VectorOutput_Sparse:
		if value.Sparse == nil || len(value.Sparse.Values) == 0 || len(value.Sparse.Indices) != len(value.Sparse.Values) {
			return nil, errors.New("sparse knowledge vector is invalid")
		}
		return qdrantpb.NewVectorSparse(
			append([]uint32(nil), value.Sparse.Indices...),
			append([]float32(nil), value.Sparse.Values...),
		), nil
	case *qdrantpb.VectorOutput_MultiDense:
		if value.MultiDense == nil || len(value.MultiDense.Vectors) == 0 {
			return nil, errors.New("multi-dense knowledge vector is empty")
		}
		vectors := make([][]float32, len(value.MultiDense.Vectors))
		for index, dense := range value.MultiDense.Vectors {
			if dense == nil || len(dense.Data) == 0 {
				return nil, fmt.Errorf("multi-dense vector %d is empty", index)
			}
			vectors[index] = append([]float32(nil), dense.Data...)
		}
		return qdrantpb.NewVectorMulti(vectors), nil
	default:
		return nil, fmt.Errorf("knowledge vector representation is unsupported: %T", output.Vector)
	}
}

func clonePointPayload(payload map[string]*qdrantpb.Value) (map[string]*qdrantpb.Value, error) {
	if len(payload) == 0 {
		return nil, errors.New("knowledge point payload is empty")
	}
	cloned := make(map[string]*qdrantpb.Value, len(payload))
	for key, value := range payload {
		if value == nil {
			return nil, fmt.Errorf("knowledge point payload %q is empty", key)
		}
		clonedValue, ok := proto.Clone(value).(*qdrantpb.Value)
		if !ok {
			return nil, fmt.Errorf("knowledge point payload %q cannot be cloned", key)
		}
		cloned[key] = clonedValue
	}
	return cloned, nil
}

func clonePointID(id *qdrantpb.PointId) *qdrantpb.PointId {
	if id == nil {
		return nil
	}
	cloned, ok := proto.Clone(id).(*qdrantpb.PointId)
	if !ok {
		return id
	}
	return cloned
}

func deterministicPointBytes(point *qdrantpb.RetrievedPoint) ([]byte, error) {
	if point == nil {
		return nil, errors.New("knowledge point is empty")
	}
	return (proto.MarshalOptions{Deterministic: true}).Marshal(point)
}

func boolPointer(value bool) *bool {
	return &value
}
