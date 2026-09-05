// Package milvus contains the provider-specific implementation of the
// provider-neutral vector store. Milvus types intentionally stop here.
package milvus

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	sdk "github.com/milvus-io/milvus/client/v2/milvusclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
)

const (
	fieldID             = "id"
	fieldTenant         = "tenant_id"
	fieldSourceType     = "source_type"
	fieldSourceID       = "source_id"
	fieldSourceRef      = "source_ref"
	fieldProjection     = "projection_scope"
	fieldSourceVersion  = "source_version"
	fieldSourceSequence = "source_sequence"
	fieldContentHash    = "content_hash"
	fieldOperation      = "operation"
	fieldModel          = "model"
	fieldModelVersion   = "model_version"
	fieldDimension      = "dimension"
	fieldSchemaVersion  = "schema_version"
	fieldVector         = "vector"

	indexName          = "vector_index_v1"
	tenantTemplateName = "p106b_tenant_id"
	maxQueryBytes      = 32 * 1024

	maxDocumentIDLength = 128
	maxSourceRefLength  = 64

	propertySchemaVersion = "p106b_schema_version"
	propertyModel         = "p106b_model"
	propertyModelVersion  = "p106b_model_version"
)

// Credentials is resolved from a server-owned CredentialRef. It is not part
// of the vector contract and must never be logged or returned in an error.
type Credentials struct {
	Username string
	Password string
	APIKey   string
}

type EndpointResolver interface {
	Resolve(context.Context, string) (string, error)
}

type CredentialResolver interface {
	Resolve(context.Context, string) (Credentials, error)
}

// sdkClient is deliberately private. It gives unit tests an injected seam
// without exposing Milvus request or result types through VectorStore.
type sdkClient interface {
	DescribeCollection(context.Context, sdk.DescribeCollectionOption, ...grpc.CallOption) (*entity.Collection, error)
	DescribeIndex(context.Context, sdk.DescribeIndexOption, ...grpc.CallOption) (sdk.IndexDescription, error)
	GetLoadState(context.Context, sdk.GetLoadStateOption, ...grpc.CallOption) (entity.LoadState, error)
	Upsert(context.Context, sdk.UpsertOption, ...grpc.CallOption) (sdk.UpsertResult, error)
	Delete(context.Context, sdk.DeleteOption, ...grpc.CallOption) (sdk.DeleteResult, error)
	Search(context.Context, sdk.SearchOption, ...grpc.CallOption) ([]sdk.ResultSet, error)
	Query(context.Context, sdk.QueryOption, ...grpc.CallOption) (sdk.ResultSet, error)
	Close(context.Context) error
}

// New resolves server-owned endpoint and credentials, constructs the SDK
// client with a bounded context, and returns only the project contract.
func New(ctx context.Context, cfg vector.BackendConfig, endpoints EndpointResolver, credentials CredentialResolver) (vector.VectorStore, error) {
	if ctx == nil {
		return nil, vector.ErrInvalidContext
	}
	if strings.EqualFold(strings.TrimSpace(cfg.Mode), vector.BackendNone) {
		return vector.DisabledStore{}, nil
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if endpoints == nil || credentials == nil {
		return nil, vector.ErrInvalidConfig
	}
	if err := contextGate(ctx); err != nil {
		return nil, err
	}
	resolveCtx, cancel := context.WithTimeout(ctx, cfg.OperationTimeout)
	defer cancel()
	endpoint, err := endpoints.Resolve(resolveCtx, cfg.EndpointRef)
	if err != nil || !validResolvedEndpoint(endpoint) {
		return nil, vector.ErrInvalidConfig
	}
	credential, err := credentials.Resolve(resolveCtx, cfg.CredentialRef)
	if err != nil || !validCredentials(credential) {
		return nil, vector.ErrInvalidConfig
	}
	client, err := newSDKClient(resolveCtx, endpoint, credential)
	if err != nil {
		return nil, classify(err)
	}
	return newStore(cfg, client)
}

func newSDKClient(ctx context.Context, endpoint string, credentials Credentials) (sdkClient, error) {
	return sdk.New(ctx, &sdk.ClientConfig{Address: endpoint, Username: credentials.Username, Password: credentials.Password, APIKey: credentials.APIKey})
}

func newStore(cfg vector.BackendConfig, client sdkClient) (vector.VectorStore, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if client == nil {
		return nil, vector.ErrInvalidConfig
	}
	return &Store{cfg: cfg, client: client}, nil
}

// Store implements vector.VectorStore while keeping all Milvus details local
// to this provider package.
type Store struct {
	mu     sync.Mutex
	cfg    vector.BackendConfig
	client sdkClient
	closed bool
}

var _ vector.VectorStore = (*Store)(nil)

func (s *Store) Ready(ctx context.Context) error {
	return s.withCall(ctx, s.cfg.ReadinessTimeout, false, func(callCtx context.Context, client sdkClient) error {
		collection, err := client.DescribeCollection(callCtx, sdk.NewDescribeCollectionOption(s.cfg.Collection))
		if err != nil {
			return classify(err)
		}
		if !schemaMatches(collection, s.cfg) {
			return vector.ErrInvalidSchema
		}
		description, err := client.DescribeIndex(callCtx, sdk.NewDescribeIndexOption(s.cfg.Collection, indexName))
		if err != nil {
			return classify(err)
		}
		if !indexMatches(description, s.cfg) {
			return vector.ErrInvalidSchema
		}
		loadState, err := client.GetLoadState(callCtx, sdk.NewGetLoadStateOption(s.cfg.Collection))
		if err != nil {
			return classify(err)
		}
		if loadState.State != entity.LoadStateLoaded {
			return vector.ErrUnavailable
		}
		return nil
	})
}

func (s *Store) Upsert(ctx context.Context, request vector.UpsertRequest) error {
	return s.withCall(ctx, s.cfg.OperationTimeout, true, func(callCtx context.Context, client sdkClient) error {
		if err := request.ValidateForContext(ctx); err != nil {
			return err
		}
		option, err := upsertOption(s.cfg, request)
		if err != nil {
			return err
		}
		result, err := client.Upsert(callCtx, option)
		if err != nil {
			return classify(err)
		}
		if result.UpsertCount != 1 || result.IDs == nil || result.IDs.Len() != 1 {
			return vector.ErrUnknown
		}
		id, err := result.IDs.Get(0)
		if err != nil {
			return vector.ErrUnknown
		}
		if value, ok := id.(string); !ok || value != request.Ref.DocumentID {
			return vector.ErrUnknown
		}
		return nil
	})
}

func (s *Store) Delete(ctx context.Context, request vector.DeleteRequest) error {
	return s.withCall(ctx, s.cfg.OperationTimeout, true, func(callCtx context.Context, client sdkClient) error {
		if err := request.ValidateForContext(ctx); err != nil {
			return err
		}
		if request.Ref.Dimension != s.cfg.Dimension || request.Ref.Model != s.cfg.Model || request.Ref.ModelVersion != s.cfg.ModelVersion || request.Ref.SchemaVersion != s.cfg.SchemaVersion {
			return vector.ErrInvalidSchema
		}
		expression, err := exactDeleteExpression(request.Ref.TenantID, request.Ref.DocumentID)
		if err != nil {
			return err
		}
		result, err := client.Delete(callCtx, sdk.NewDeleteOption(s.cfg.Collection).WithExpr(expression))
		if err != nil {
			return classify(err)
		}
		if result.DeleteCount != 1 {
			return vector.ErrUnknown
		}
		return nil
	})
}

func (s *Store) Search(ctx context.Context, request vector.SearchRequest) ([]vector.SearchResult, error) {
	var results []vector.SearchResult
	err := s.withCall(ctx, s.cfg.OperationTimeout, false, func(callCtx context.Context, client sdkClient) error {
		tenantContext, err := vector.TrustedTenantContext(ctx)
		if err != nil {
			return err
		}
		if err := request.ValidateForContext(ctx); err != nil {
			return err
		}
		if request.TopK > s.cfg.MaxTopK {
			return vector.ErrInvalidFilter
		}
		if len(request.Query) != s.cfg.Dimension {
			return vector.ErrInvalidDimension
		}
		if len(request.Query) != s.cfg.Dimension || len(request.Query)*8 > maxQueryBytes {
			return vector.ErrInvalidDimension
		}
		query := make([]float32, len(request.Query))
		for i, value := range request.Query {
			query[i] = float32(value)
			if math.IsInf(float64(query[i]), 0) || math.IsNaN(float64(query[i])) {
				return vector.ErrInvalidDimension
			}
		}
		option := sdk.NewSearchOption(s.cfg.Collection, request.TopK, []entity.Vector{entity.FloatVector(query)}).
			WithANNSField(fieldVector).
			WithFilter(tenantFilterExpression()).
			WithTemplateParam(tenantTemplateName, tenantContext.TenantID).
			WithOutputFields(searchOutputFields()...)
		sets, err := client.Search(callCtx, option)
		if err != nil {
			return classify(err)
		}
		results, err = mapSearchResults(sets, request, tenantContext.TenantID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

func (s *Store) Close(ctx context.Context) error {
	if err := contextGate(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	callCtx, cancel := context.WithTimeout(ctx, s.cfg.OperationTimeout)
	defer cancel()
	if err := s.client.Close(callCtx); err != nil {
		return classify(err)
	}
	return nil
}

func (s *Store) withCall(ctx context.Context, timeout time.Duration, write bool, fn func(context.Context, sdkClient) error) error {
	if err := contextGate(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.client == nil {
		return vector.ErrUnavailable
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := fn(callCtx, s.client)
	if err == nil {
		return nil
	}
	if errors.Is(err, vector.ErrUnknown) || errors.Is(err, vector.ErrInvalidDocument) || errors.Is(err, vector.ErrInvalidTenant) || errors.Is(err, vector.ErrInvalidDimension) || errors.Is(err, vector.ErrInvalidFilter) || errors.Is(err, vector.ErrInvalidModel) || errors.Is(err, vector.ErrInvalidSchema) {
		return err
	}
	if write && errors.Is(err, vector.ErrTimeout) {
		return vector.ErrUnknown
	}
	return err
}

func contextGate(ctx context.Context) error {
	if ctx == nil {
		return vector.ErrInvalidContext
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return vector.ErrCancelled
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return vector.ErrTimeout
	}
	return nil
}

func classify(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return vector.ErrCancelled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return vector.ErrTimeout
	}
	switch status.Code(err) {
	case codes.Canceled:
		return vector.ErrCancelled
	case codes.DeadlineExceeded:
		return vector.ErrTimeout
	case codes.Unavailable:
		return vector.ErrUnavailable
	case codes.ResourceExhausted, codes.Aborted:
		return vector.ErrRetryable
	case codes.NotFound:
		return vector.ErrNotFound
	case codes.InvalidArgument, codes.FailedPrecondition:
		return vector.ErrPermanent
	case codes.Unauthenticated, codes.PermissionDenied, codes.Unimplemented:
		return vector.ErrPermanent
	default:
		return vector.ErrUnknown
	}
}

func expectedSchema(cfg vector.BackendConfig) *entity.Schema {
	return entity.NewSchema().WithName(cfg.Collection).WithAutoID(false).WithDynamicFieldEnabled(false).
		WithField(entity.NewField().WithName(fieldID).WithDataType(entity.FieldTypeVarChar).WithIsPrimaryKey(true).WithMaxLength(maxDocumentIDLength)).
		WithField(entity.NewField().WithName(fieldTenant).WithDataType(entity.FieldTypeVarChar).WithMaxLength(128)).
		WithField(entity.NewField().WithName(fieldSourceType).WithDataType(entity.FieldTypeVarChar).WithMaxLength(vector.MaxSourceTypeBytes)).
		WithField(entity.NewField().WithName(fieldSourceID).WithDataType(entity.FieldTypeVarChar).WithMaxLength(vector.MaxSourceIDBytes)).
		WithField(entity.NewField().WithName(fieldSourceRef).WithDataType(entity.FieldTypeVarChar).WithMaxLength(maxSourceRefLength)).
		WithField(entity.NewField().WithName(fieldProjection).WithDataType(entity.FieldTypeVarChar).WithMaxLength(vector.MaxProjectionScopeBytes)).
		WithField(entity.NewField().WithName(fieldSourceVersion).WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName(fieldSourceSequence).WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName(fieldContentHash).WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)).
		WithField(entity.NewField().WithName(fieldOperation).WithDataType(entity.FieldTypeVarChar).WithMaxLength(16)).
		WithField(entity.NewField().WithName(fieldModel).WithDataType(entity.FieldTypeVarChar).WithMaxLength(256)).
		WithField(entity.NewField().WithName(fieldModelVersion).WithDataType(entity.FieldTypeVarChar).WithMaxLength(128)).
		WithField(entity.NewField().WithName(fieldDimension).WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName(fieldSchemaVersion).WithDataType(entity.FieldTypeVarChar).WithMaxLength(128)).
		WithField(entity.NewField().WithName(fieldVector).WithDataType(entity.FieldTypeFloatVector).WithDim(int64(cfg.Dimension)))
}

func schemaMatches(collection *entity.Collection, cfg vector.BackendConfig) bool {
	if collection == nil || collection.Schema == nil || collection.Schema.CollectionName != cfg.Collection || collection.Schema.AutoID || collection.Schema.EnableDynamicField {
		return false
	}
	want := expectedSchema(cfg)
	if collection.Properties[propertySchemaVersion] != cfg.SchemaVersion || collection.Properties[propertyModel] != cfg.Model || collection.Properties[propertyModelVersion] != cfg.ModelVersion {
		return false
	}

	if len(collection.Schema.Fields) != len(want.Fields) || collection.Schema.PKFieldName() != fieldID {
		return false
	}
	for i, got := range collection.Schema.Fields {
		expected := want.Fields[i]
		if got == nil || got.Name != expected.Name || got.DataType != expected.DataType || got.PrimaryKey != expected.PrimaryKey || got.AutoID != expected.AutoID {
			return false
		}
		for key, value := range expected.TypeParams {
			if got.TypeParams[key] != value {
				return false
			}
		}
		if len(got.TypeParams) != len(expected.TypeParams) {
			return false
		}
	}
	return true
}

func indexMatches(description sdk.IndexDescription, cfg vector.BackendConfig) bool {
	if description.Index == nil || description.Name() != indexName || description.State != index.IndexState(commonpb.IndexState_Finished) {
		return false
	}
	params := description.Params()
	return params[index.IndexTypeKey] == string(index.Flat) && params[index.MetricTypeKey] == strings.ToUpper(cfg.Metric)
}
func expectedProperties(cfg vector.BackendConfig) map[string]string {
	return map[string]string{
		propertySchemaVersion: cfg.SchemaVersion,
		propertyModel:         cfg.Model,
		propertyModelVersion:  cfg.ModelVersion,
	}
}

func expectedIndex(cfg vector.BackendConfig) index.Index {
	return index.NewFlatIndex(entity.MetricType(strings.ToUpper(cfg.Metric)))
}

func upsertOption(cfg vector.BackendConfig, request vector.UpsertRequest) (sdk.UpsertOption, error) {
	// The collection is provisioned for exactly one server-owned projection;
	// anything else must fail closed before the SDK call instead of relying
	// on backend rejection (which can be silent for metadata fields).
	if request.Ref.Dimension != cfg.Dimension {
		return nil, vector.ErrInvalidDimension
	}
	if request.Ref.Model != cfg.Model || request.Ref.ModelVersion != cfg.ModelVersion {
		return nil, vector.ErrInvalidModel
	}
	if request.Ref.SchemaVersion != cfg.SchemaVersion {
		return nil, vector.ErrInvalidSchema
	}
	if len(request.Content) > cfg.MaxInputBytes {
		return nil, vector.ErrInvalidDocument
	}
	values := make([]float32, len(request.Embedding.Values))
	for i, value := range request.Embedding.Values {
		values[i] = float32(value)
		if math.IsInf(float64(values[i]), 0) || math.IsNaN(float64(values[i])) {
			return nil, vector.ErrInvalidDimension
		}
	}
	metadata, err := request.Ref.SafeMetadata()
	if err != nil || len(metadata) > cfg.MaxMetadataBytes {
		return nil, vector.ErrInvalidDocument
	}
	columns := []column.Column{
		column.NewColumnVarChar(fieldID, []string{request.Ref.DocumentID}),
		column.NewColumnVarChar(fieldTenant, []string{request.Ref.TenantID}),
		column.NewColumnVarChar(fieldSourceType, []string{request.Ref.SourceType}),
		column.NewColumnVarChar(fieldSourceID, []string{request.Ref.SourceID}),
		column.NewColumnVarChar(fieldSourceRef, []string{metadata["source_ref"]}),
		column.NewColumnVarChar(fieldProjection, []string{request.Ref.ProjectionScope}),
		column.NewColumnInt64(fieldSourceVersion, []int64{request.Ref.SourceVersion}),
		column.NewColumnInt64(fieldSourceSequence, []int64{request.Ref.SourceSequence}),
		column.NewColumnVarChar(fieldContentHash, []string{request.Ref.ContentHash}),
		column.NewColumnVarChar(fieldOperation, []string{string(request.Ref.Operation)}),
		column.NewColumnVarChar(fieldModel, []string{request.Ref.Model}),
		column.NewColumnVarChar(fieldModelVersion, []string{request.Ref.ModelVersion}),
		column.NewColumnInt64(fieldDimension, []int64{int64(request.Ref.Dimension)}),
		column.NewColumnVarChar(fieldSchemaVersion, []string{request.Ref.SchemaVersion}),
		column.NewColumnFloatVector(fieldVector, request.Ref.Dimension, [][]float32{values}),
	}
	return sdk.NewColumnBasedInsertOption(cfg.Collection, columns...), nil
}

func tenantFilterExpression() string { return fieldTenant + " == {" + tenantTemplateName + "}" }

func exactDeleteExpression(tenantID, documentID string) (string, error) {
	tenantLiteral, err := strictStringLiteral(tenantID)
	if err != nil {
		return "", vector.ErrInvalidFilter
	}
	idLiteral, err := strictStringLiteral(documentID)
	if err != nil {
		return "", vector.ErrInvalidFilter
	}
	return fieldTenant + " == " + tenantLiteral + " && " + fieldID + " == " + idLiteral, nil
}

func strictStringLiteral(value string) (string, error) {
	if value == "" || !utf8.ValidString(value) || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\\\"'") {
		return "", vector.ErrInvalidFilter
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return "", vector.ErrInvalidFilter
		}
	}
	return strconv.Quote(value), nil
}

func searchOutputFields() []string {
	return []string{fieldTenant, fieldSourceType, fieldSourceID, fieldSourceRef, fieldProjection, fieldSourceVersion, fieldSourceSequence, fieldContentHash, fieldOperation, fieldModel, fieldModelVersion, fieldDimension, fieldSchemaVersion}
}

func mapSearchResults(sets []sdk.ResultSet, request vector.SearchRequest, tenantID string) ([]vector.SearchResult, error) {
	if len(sets) != 1 {
		return nil, vector.ErrUnknown
	}
	set := sets[0]
	if set.Err != nil || set.IDs == nil || set.ResultCount < 0 || set.ResultCount > request.TopK || set.ResultCount != len(set.Scores) || set.ResultCount != set.IDs.Len() {
		if set.Err != nil {
			return nil, classify(set.Err)
		}
		return nil, vector.ErrUnknown
	}
	results := make([]vector.SearchResult, 0, set.ResultCount)
	for i := 0; i < set.ResultCount; i++ {
		id, err := set.IDs.Get(i)
		if err != nil {
			return nil, vector.ErrUnknown
		}
		documentID, ok := id.(string)
		if !ok {
			return nil, vector.ErrInvalidSchema
		}
		values, err := readSearchRow(set.Fields, i)
		if err != nil {
			return nil, err
		}
		if values.tenantID != tenantID {
			return nil, vector.ErrInvalidTenant
		}
		ref := vector.VectorDocumentRef{TenantID: tenantID, SourceType: values.sourceType, SourceID: values.sourceID, ProjectionScope: values.projection, SourceVersion: values.sourceVersion, SourceSequence: values.sourceSequence, ContentHash: values.contentHash, Operation: vector.OperationUpsert, Deleted: false, DocumentID: documentID, Model: values.model, ModelVersion: values.modelVersion, Dimension: values.dimension, SchemaVersion: values.schemaVersion}
		if values.operation != string(vector.OperationUpsert) || ref.Validate() != nil {
			return nil, vector.ErrInvalidSchema
		}
		metadata, err := ref.SafeMetadata()
		if err != nil || metadata["source_ref"] != values.sourceRef {
			return nil, vector.ErrInvalidSchema
		}
		score := float64(set.Scores[i])
		if math.IsNaN(score) || math.IsInf(score, 0) || score < request.MinScore {
			continue
		}
		results = append(results, vector.SearchResult{Ref: ref, Score: score, Metadata: metadata})
	}
	return results, nil
}

type searchRow struct {
	tenantID, sourceType, sourceID, sourceRef, projection string
	sourceVersion, sourceSequence                         int64
	contentHash, operation, model, modelVersion           string
	dimension                                             int
	schemaVersion                                         string
}

func readSearchRow(fields sdk.DataSet, row int) (searchRow, error) {
	getString := func(name string) (string, error) {
		value := fieldsColumn(fields, name)
		if value == nil || value.Type() != entity.FieldTypeVarChar {
			return "", vector.ErrInvalidSchema
		}
		result, err := value.Get(row)
		if err != nil {
			return "", vector.ErrUnknown
		}
		text, ok := result.(string)
		if !ok {
			return "", vector.ErrInvalidSchema
		}
		return text, nil
	}
	getInt64 := func(name string) (int64, error) {
		value := fieldsColumn(fields, name)
		if value == nil || value.Type() != entity.FieldTypeInt64 {
			return 0, vector.ErrInvalidSchema
		}
		result, err := value.Get(row)
		if err != nil {
			return 0, vector.ErrUnknown
		}
		integer, ok := result.(int64)
		if !ok {
			return 0, vector.ErrInvalidSchema
		}
		return integer, nil
	}
	tenantID, err := getString(fieldTenant)
	if err != nil {
		return searchRow{}, err
	}
	sourceType, err := getString(fieldSourceType)
	if err != nil {
		return searchRow{}, err
	}
	sourceID, err := getString(fieldSourceID)
	if err != nil {
		return searchRow{}, err
	}
	sourceRef, err := getString(fieldSourceRef)
	if err != nil {
		return searchRow{}, err
	}
	projection, err := getString(fieldProjection)
	if err != nil {
		return searchRow{}, err
	}
	sourceVersion, err := getInt64(fieldSourceVersion)
	if err != nil {
		return searchRow{}, err
	}
	sourceSequence, err := getInt64(fieldSourceSequence)
	if err != nil {
		return searchRow{}, err
	}
	contentHash, err := getString(fieldContentHash)
	if err != nil {
		return searchRow{}, err
	}
	operation, err := getString(fieldOperation)
	if err != nil {
		return searchRow{}, err
	}
	model, err := getString(fieldModel)
	if err != nil {
		return searchRow{}, err
	}
	modelVersion, err := getString(fieldModelVersion)
	if err != nil {
		return searchRow{}, err
	}
	dimension, err := getInt64(fieldDimension)
	if err != nil || dimension < 1 || dimension > vector.MaxVectorDimension {
		return searchRow{}, vector.ErrInvalidDimension
	}
	schemaVersion, err := getString(fieldSchemaVersion)
	if err != nil {
		return searchRow{}, err
	}
	return searchRow{tenantID: tenantID, sourceType: sourceType, sourceID: sourceID, sourceRef: sourceRef, projection: projection, sourceVersion: sourceVersion, sourceSequence: sourceSequence, contentHash: contentHash, operation: operation, model: model, modelVersion: modelVersion, dimension: int(dimension), schemaVersion: schemaVersion}, nil
}

func fieldsColumn(fields sdk.DataSet, name string) column.Column {
	for _, field := range fields {
		if field != nil && field.Name() == name {
			return field
		}
	}
	return nil
}

func validResolvedEndpoint(endpoint string) bool {
	return endpoint != "" && len(endpoint) <= 256 && strings.TrimSpace(endpoint) == endpoint && utf8.ValidString(endpoint) && !strings.ContainsAny(endpoint, "\\\r\n")
}

func validCredentials(credentials Credentials) bool {
	if credentials.APIKey != "" {
		return len(credentials.APIKey) <= 4096 && utf8.ValidString(credentials.APIKey)
	}
	return credentials.Username != "" && credentials.Password != "" && len(credentials.Username) <= 256 && len(credentials.Password) <= 4096 && utf8.ValidString(credentials.Username) && utf8.ValidString(credentials.Password)
}
