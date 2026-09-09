// Package vector contains the server-owned boundary for derived vector
// projections. It deliberately does not expose a provider SDK.
package vector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/memory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type VectorOperation string

const (
	BackendNone   = "none"
	BackendMilvus = "milvus"

	OperationUpsert VectorOperation = "upsert"
	OperationDelete VectorOperation = "delete"

	MaxSourceTypeBytes      = 64
	MaxSourceIDBytes        = 256
	MaxProjectionScopeBytes = 128
	MaxContentBytes         = 1 << 20
	MaxMetadataBytes        = 8 << 10
	MaxVectorDimension      = 4096
	MaxTopK                 = 100
)

type ErrorCategory string

const (
	CategoryInvalidContext   ErrorCategory = "invalid_context"
	CategoryInvalidTenant    ErrorCategory = "invalid_tenant"
	CategoryInvalidDocument  ErrorCategory = "invalid_document"
	CategoryInvalidDimension ErrorCategory = "invalid_dimension"
	CategoryInvalidModel     ErrorCategory = "invalid_model"
	CategoryInvalidSchema    ErrorCategory = "invalid_schema"
	CategoryInvalidFilter    ErrorCategory = "invalid_filter"
	CategoryInvalidConfig    ErrorCategory = "invalid_config"
	CategoryUnsupported      ErrorCategory = "unsupported_backend"
	CategoryNotFound         ErrorCategory = "not_found"
	CategoryDisabled         ErrorCategory = "disabled"
	CategoryUnavailable      ErrorCategory = "unavailable"
	CategoryTimeout          ErrorCategory = "timeout"
	CategoryCancelled        ErrorCategory = "cancelled"
	CategoryRetryable        ErrorCategory = "retryable"
	CategoryPermanent        ErrorCategory = "permanent"
	CategoryUnknown          ErrorCategory = "unknown"
	CategoryConflict         ErrorCategory = "conflict"
	CategoryStale            ErrorCategory = "stale"
)

// Error is intentionally category-only. Provider, database, request and
// source values must never be included in the error text.
type Error struct{ Category ErrorCategory }

func (e Error) Error() string { return "vector: " + string(e.Category) }

func (e Error) Is(target error) bool {
	switch value := target.(type) {
	case Error:
		return e.Category == value.Category
	case *Error:
		return value != nil && e.Category == value.Category
	default:
		return false
	}
}

var (
	ErrInvalidContext   = Error{Category: CategoryInvalidContext}
	ErrInvalidTenant    = Error{Category: CategoryInvalidTenant}
	ErrInvalidDocument  = Error{Category: CategoryInvalidDocument}
	ErrInvalidDimension = Error{Category: CategoryInvalidDimension}
	ErrInvalidModel     = Error{Category: CategoryInvalidModel}
	ErrInvalidSchema    = Error{Category: CategoryInvalidSchema}
	ErrInvalidFilter    = Error{Category: CategoryInvalidFilter}
	ErrInvalidConfig    = Error{Category: CategoryInvalidConfig}
	ErrUnsupported      = Error{Category: CategoryUnsupported}
	ErrNotFound         = Error{Category: CategoryNotFound}
	ErrDisabled         = Error{Category: CategoryDisabled}
	ErrUnavailable      = Error{Category: CategoryUnavailable}
	ErrTimeout          = Error{Category: CategoryTimeout}
	ErrCancelled        = Error{Category: CategoryCancelled}
	ErrRetryable        = Error{Category: CategoryRetryable}
	ErrPermanent        = Error{Category: CategoryPermanent}
	ErrUnknown          = Error{Category: CategoryUnknown}
	ErrConflict         = Error{Category: CategoryConflict}
	ErrStale            = Error{Category: CategoryStale}
)

func contextError(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidContext
	}
	switch {
	case errors.Is(ctx.Err(), context.Canceled):
		return ErrCancelled
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return ErrTimeout
	default:
		return nil
	}
}

// TrustedTenantContext reads the server-owned context installed by the
// ingress/resolver boundary. A missing or invalid context fails closed.
func TrustedTenantContext(ctx context.Context) (tenant.TenantContext, error) {
	if ctx == nil {
		return tenant.TenantContext{}, ErrInvalidContext
	}
	if err := contextError(ctx); err != nil {
		return tenant.TenantContext{}, err
	}
	tc, ok := tenant.FromContext(ctx)
	if !ok {
		return tenant.TenantContext{}, ErrInvalidContext
	}
	if err := tc.Validate(); err != nil {
		return tenant.TenantContext{}, ErrInvalidTenant
	}
	return tc, nil
}

type SourceDocument struct {
	SourceType      string
	SourceID        string
	ProjectionScope string
	SourceVersion   int64
	SourceSequence  int64
	Content         string
	Deleted         bool
	Model           string
	ModelVersion    string
	Dimension       int
	SchemaVersion   string
}

const SourceTypeMemory = "memory"

// ProjectionConfig is server-owned model and schema configuration.
type ProjectionConfig struct {
	Model         string
	ModelVersion  string
	Dimension     int
	SchemaVersion string
}

func (c ProjectionConfig) validate() error {
	if !validComponent(c.Model, 256) || !validComponent(c.ModelVersion, 128) {
		return ErrInvalidModel
	}
	if c.Dimension < 1 || c.Dimension > MaxVectorDimension {
		return ErrInvalidDimension
	}
	if !validComponent(c.SchemaVersion, 128) {
		return ErrInvalidSchema
	}
	return nil
}

// MemorySource maps the existing durable Memory fact to a vector projection.
// It does not read or mutate vector_ref, which remains an opaque compatibility field.
func MemorySource(ctx context.Context, value memory.Memory, config ProjectionConfig) (SourceDocument, error) {
	tc, err := TrustedTenantContext(ctx)
	if err != nil {
		return SourceDocument{}, err
	}
	if err := value.Validate(); err != nil {
		return SourceDocument{}, ErrInvalidDocument
	}
	if value.TenantID != tc.TenantID {
		return SourceDocument{}, ErrInvalidTenant
	}
	if err := config.validate(); err != nil {
		return SourceDocument{}, err
	}
	return SourceDocument{SourceType: SourceTypeMemory, SourceID: value.ID, ProjectionScope: "memory:" + string(value.Scope), SourceVersion: value.Version, SourceSequence: value.SourceSeq, Content: value.Content, Deleted: value.Deleted, Model: config.Model, ModelVersion: config.ModelVersion, Dimension: config.Dimension, SchemaVersion: config.SchemaVersion}, nil
}

// TenantFilter can only be created from a validated server-owned context.
// It has no raw expression and cannot be supplied by a caller.
type TenantFilter struct{ tenantID string }

func BuildTenantFilter(ctx context.Context) (TenantFilter, error) {
	tc, err := TrustedTenantContext(ctx)
	if err != nil {
		return TenantFilter{}, err
	}
	return TenantFilter{tenantID: tc.TenantID}, nil
}

func (f TenantFilter) TenantID() string { return f.tenantID }

type VectorDocumentRef struct {
	TenantID        string          `json:"tenant_id"`
	SourceType      string          `json:"source_type"`
	SourceID        string          `json:"-"`
	ProjectionScope string          `json:"projection_scope"`
	SourceVersion   int64           `json:"source_version"`
	SourceSequence  int64           `json:"source_sequence"`
	ContentHash     string          `json:"content_hash"`
	Operation       VectorOperation `json:"operation"`
	Deleted         bool            `json:"deleted"`
	DocumentID      string          `json:"document_id"`
	Model           string          `json:"model"`
	ModelVersion    string          `json:"model_version"`
	Dimension       int             `json:"dimension"`
	SchemaVersion   string          `json:"schema_version"`
}

// ContentHash is versioned and independent from document identity. It is
// safe to persist as a change marker, but it is not a document primary key.
func ContentHash(content string) string {
	digest := sha256.Sum256([]byte("trpc-agent/vector-content/v1\x00" + content))
	return hex.EncodeToString(digest[:])
}

// BuildDocumentRef is the only constructor for a document identity. Source
// content, version and sequence are projection state; they are intentionally
// excluded from the identity digest so retries and source updates reuse the
// same server-owned document ID.
func BuildDocumentRef(ctx context.Context, source SourceDocument) (VectorDocumentRef, error) {
	tc, err := TrustedTenantContext(ctx)
	if err != nil {
		return VectorDocumentRef{}, err
	}
	if err := validateSource(source); err != nil {
		return VectorDocumentRef{}, err
	}
	ref := VectorDocumentRef{
		TenantID:        tc.TenantID,
		SourceType:      source.SourceType,
		SourceID:        source.SourceID,
		ProjectionScope: source.ProjectionScope,
		SourceVersion:   source.SourceVersion,
		SourceSequence:  source.SourceSequence,
		ContentHash:     ContentHash(source.Content),
		Operation:       OperationUpsert,
		Deleted:         source.Deleted,
		Model:           source.Model,
		ModelVersion:    source.ModelVersion,
		Dimension:       source.Dimension,
		SchemaVersion:   source.SchemaVersion,
	}
	if source.Deleted {
		ref.Operation = OperationDelete
	}
	ref.DocumentID = serverDocumentID(ref)
	return ref, nil
}

func (r VectorDocumentRef) Validate() error {
	if !validComponent(r.TenantID, 128) || !validComponent(r.SourceType, MaxSourceTypeBytes) || !validOpaque(r.SourceID, MaxSourceIDBytes) || !validComponent(r.ProjectionScope, MaxProjectionScopeBytes) {
		return ErrInvalidDocument
	}
	if r.SourceVersion < 1 || r.SourceSequence < 0 || !validHex(r.ContentHash, 64) {
		return ErrInvalidDocument
	}
	if r.Operation != OperationUpsert && r.Operation != OperationDelete {
		return ErrInvalidDocument
	}
	if r.Deleted != (r.Operation == OperationDelete) {
		return ErrInvalidDocument
	}
	if !validComponent(r.Model, 256) || !validComponent(r.ModelVersion, 128) {
		return ErrInvalidModel
	}
	if r.Dimension < 1 || r.Dimension > MaxVectorDimension {
		return ErrInvalidDimension
	}
	if !validComponent(r.SchemaVersion, 128) {
		return ErrInvalidSchema
	}
	if !validDocumentID(r.DocumentID) || serverDocumentID(r) != r.DocumentID {
		return ErrInvalidDocument
	}
	return nil
}

func (r VectorDocumentRef) SafeSourceRef() string {
	return audit.Fingerprint(r.SourceType + "\x00" + r.SourceID)
}

// SafeMetadata is the complete metadata allowlist for a derived index. It
// deliberately omits source content, source ID, prompts, credentials and
// embedding values.
func (r VectorDocumentRef) SafeMetadata() (map[string]string, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	metadata := map[string]string{
		"tenant_id":       r.TenantID,
		"source_type":     r.SourceType,
		"source_ref":      r.SafeSourceRef(),
		"source_version":  strconv.FormatInt(r.SourceVersion, 10),
		"source_sequence": strconv.FormatInt(r.SourceSequence, 10),
		"content_hash":    r.ContentHash,
		"operation":       string(r.Operation),
		"model":           r.Model,
		"model_version":   r.ModelVersion,
		"dimension":       strconv.Itoa(r.Dimension),
		"schema_version":  r.SchemaVersion,
	}
	encoded, err := json.Marshal(metadata)
	if err != nil || len(encoded) > MaxMetadataBytes {
		return nil, ErrInvalidDocument
	}
	return metadata, nil
}

type Embedding struct {
	Values       []float64
	Model        string
	ModelVersion string
	Dimension    int
}

func (e Embedding) ValidateFor(ref VectorDocumentRef) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	if e.Model != ref.Model || e.ModelVersion != ref.ModelVersion {
		return ErrInvalidModel
	}
	if e.Dimension != ref.Dimension || len(e.Values) != ref.Dimension {
		return ErrInvalidDimension
	}
	for _, value := range e.Values {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return ErrInvalidDimension
		}
	}
	return nil
}

type UpsertRequest struct {
	Ref       VectorDocumentRef
	Content   string
	Embedding Embedding
	Metadata  map[string]string
}

func BuildUpsertRequest(ctx context.Context, source SourceDocument, embedding Embedding) (UpsertRequest, error) {
	if source.Deleted {
		return UpsertRequest{}, ErrInvalidDocument
	}
	ref, err := BuildDocumentRef(ctx, source)
	if err != nil {
		return UpsertRequest{}, err
	}
	request := UpsertRequest{Ref: ref, Content: source.Content, Embedding: embedding}
	request.Metadata, err = ref.SafeMetadata()
	if err != nil {
		return UpsertRequest{}, err
	}
	if err := request.Validate(); err != nil {
		return UpsertRequest{}, err
	}
	return request, nil
}

func (r UpsertRequest) Validate() error {
	if err := r.Ref.Validate(); err != nil {
		return err
	}
	if r.Ref.Operation != OperationUpsert || r.Ref.Deleted || strings.TrimSpace(r.Content) == "" || len(r.Content) > MaxContentBytes || !utf8.ValidString(r.Content) {
		return ErrInvalidDocument
	}
	if ContentHash(r.Content) != r.Ref.ContentHash {
		return ErrInvalidDocument
	}
	if err := r.Embedding.ValidateFor(r.Ref); err != nil {
		return err
	}
	return validateMetadata(r.Metadata, r.Ref)
}

type DeleteRequest struct {
	Ref       VectorDocumentRef
	Tombstone bool
	Metadata  map[string]string
}

func BuildDeleteRequest(ctx context.Context, source SourceDocument) (DeleteRequest, error) {
	if !source.Deleted {
		return DeleteRequest{}, ErrInvalidDocument
	}
	ref, err := BuildDocumentRef(ctx, source)
	if err != nil {
		return DeleteRequest{}, err
	}
	metadata, err := ref.SafeMetadata()
	if err != nil {
		return DeleteRequest{}, err
	}
	request := DeleteRequest{Ref: ref, Tombstone: true, Metadata: metadata}
	if err := request.Validate(); err != nil {
		return DeleteRequest{}, err
	}
	return request, nil
}

func (r DeleteRequest) Validate() error {
	if err := r.Ref.Validate(); err != nil {
		return err
	}
	if r.Ref.Operation != OperationDelete || !r.Ref.Deleted || !r.Tombstone {
		return ErrInvalidDocument
	}
	return validateMetadata(r.Metadata, r.Ref)
}

type SearchRequest struct {
	Query    []float64
	TopK     int
	MinScore float64
}

func (r SearchRequest) Validate() error {
	if r.TopK < 1 || r.TopK > MaxTopK || math.IsNaN(r.MinScore) || math.IsInf(r.MinScore, 0) || r.MinScore < 0 || r.MinScore > 1 {
		return ErrInvalidFilter
	}
	if len(r.Query) < 1 || len(r.Query) > MaxVectorDimension {
		return ErrInvalidDimension
	}
	for _, value := range r.Query {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return ErrInvalidDimension
		}
	}
	return nil
}

type SearchResult struct {
	Ref      VectorDocumentRef
	Score    float64
	Metadata map[string]string
}

func (r SearchResult) Validate() error {
	if err := r.Ref.Validate(); err != nil {
		return err
	}
	if r.Ref.Deleted || r.Ref.Operation != OperationUpsert || math.IsNaN(r.Score) || math.IsInf(r.Score, 0) {
		return ErrInvalidDocument
	}
	return validateMetadata(r.Metadata, r.Ref)
}

// VectorStore is provider-neutral. Tenant, collection and filter are not
// request fields; implementations derive the tenant predicate from context
// and keep backend details inside the adapter.
type VectorStore interface {
	Ready(context.Context) error
	Upsert(context.Context, UpsertRequest) error
	Delete(context.Context, DeleteRequest) error
	Search(context.Context, SearchRequest) ([]SearchResult, error)
	Close(context.Context) error
}

type BackendConfig struct {
	Mode             string
	EndpointRef      string
	Collection       string
	CredentialRef    string
	Model            string
	ModelVersion     string
	Dimension        int
	SchemaVersion    string
	Metric           string
	MaxInputBytes    int
	MaxMetadataBytes int
	MaxTopK          int
	OperationTimeout time.Duration
	ReadinessTimeout time.Duration
}

func (c BackendConfig) Validate() error {
	mode := strings.ToLower(strings.TrimSpace(c.Mode))
	if mode == BackendNone {
		return nil
	}
	if mode != BackendMilvus {
		return ErrUnsupported
	}
	for _, value := range []string{c.EndpointRef, c.Collection, c.CredentialRef} {
		if !validOpaque(value, 256) {
			return ErrInvalidConfig
		}
	}
	if !validComponent(c.Model, 256) || !validComponent(c.ModelVersion, 128) {
		return ErrInvalidModel
	}
	if c.Dimension < 1 || c.Dimension > MaxVectorDimension {
		return ErrInvalidDimension
	}
	if !validComponent(c.SchemaVersion, 128) {
		return ErrInvalidSchema
	}
	if c.Metric != "cosine" && c.Metric != "ip" && c.Metric != "l2" {
		return ErrInvalidConfig
	}
	if c.MaxInputBytes < 1 || c.MaxInputBytes > MaxContentBytes || c.MaxMetadataBytes < 1 || c.MaxMetadataBytes > MaxMetadataBytes || c.MaxTopK < 1 || c.MaxTopK > MaxTopK || c.OperationTimeout <= 0 || c.ReadinessTimeout <= 0 {
		return ErrInvalidConfig
	}
	return nil
}

type BackendFactory func(context.Context, BackendConfig) (VectorStore, error)

// ResolveBackend never constructs a fake production backend. The disabled
// path returns a no-client store; enabled paths require complete server-owned
// configuration and an explicit factory.
func ResolveBackend(ctx context.Context, cfg BackendConfig, factory BackendFactory) (VectorStore, error) {
	if ctx == nil {
		return nil, ErrInvalidContext
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	mode := strings.ToLower(strings.TrimSpace(cfg.Mode))
	if mode == BackendNone {
		return DisabledStore{}, nil
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if factory == nil {
		return nil, ErrInvalidConfig
	}
	store, err := factory(ctx, cfg)
	if err != nil || store == nil {
		return nil, ErrUnavailable
	}
	return store, nil
}

type DisabledStore struct{}

func (DisabledStore) Ready(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	return ErrDisabled
}

func (DisabledStore) Upsert(ctx context.Context, request UpsertRequest) error {
	if _, err := TrustedTenantContext(ctx); err != nil {
		return err
	}
	return ErrDisabled
}

func (DisabledStore) Delete(ctx context.Context, request DeleteRequest) error {
	if _, err := TrustedTenantContext(ctx); err != nil {
		return err
	}
	return ErrDisabled
}

func (DisabledStore) Search(ctx context.Context, request SearchRequest) ([]SearchResult, error) {
	if _, err := TrustedTenantContext(ctx); err != nil {
		return nil, err
	}
	return nil, ErrDisabled
}

func (DisabledStore) Close(ctx context.Context) error { return contextError(ctx) }

func validateSource(source SourceDocument) error {
	if !validComponent(source.SourceType, MaxSourceTypeBytes) || !validOpaque(source.SourceID, MaxSourceIDBytes) || !validComponent(source.ProjectionScope, MaxProjectionScopeBytes) {
		return ErrInvalidDocument
	}
	if source.SourceVersion < 1 || source.SourceSequence < 0 || len(source.Content) > MaxContentBytes || !utf8.ValidString(source.Content) || (!source.Deleted && strings.TrimSpace(source.Content) == "") {
		return ErrInvalidDocument
	}
	if !validComponent(source.Model, 256) || !validComponent(source.ModelVersion, 128) {
		return ErrInvalidModel
	}
	if source.Dimension < 1 || source.Dimension > MaxVectorDimension {
		return ErrInvalidDimension
	}
	if !validComponent(source.SchemaVersion, 128) {
		return ErrInvalidSchema
	}
	return nil
}

func validateMetadata(metadata map[string]string, ref VectorDocumentRef) error {
	expected, err := ref.SafeMetadata()
	if err != nil || len(metadata) != len(expected) {
		return ErrInvalidDocument
	}
	for key, value := range expected {
		if metadata[key] != value {
			return ErrInvalidDocument
		}
	}
	encoded, err := json.Marshal(metadata)
	if err != nil || len(encoded) > MaxMetadataBytes {
		return ErrInvalidDocument
	}
	return nil
}

func serverDocumentID(ref VectorDocumentRef) string {
	material := strings.Join([]string{
		"trpc-agent/vector-document/v1", ref.TenantID, ref.SourceType, ref.SourceID,
		ref.ProjectionScope, ref.Model, ref.ModelVersion, strconv.Itoa(ref.Dimension), ref.SchemaVersion,
	}, "\x00")
	digest := sha256.Sum256([]byte(material))
	return "vd-v1-" + hex.EncodeToString(digest[:])
}

func validDocumentID(value string) bool {
	prefix := "vd-v1-"
	return len(value) == len(prefix)+64 && strings.HasPrefix(value, prefix) && validHex(value[len(prefix):], 64)
}

func validHex(value string, length int) bool {
	if len(value) != length {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validComponent(value string, max int) bool {
	if value == "" || len(value) > max || strings.TrimSpace(value) != value || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func validOpaque(value string, max int) bool { return validComponent(value, max) }
func (r UpsertRequest) ValidateForContext(ctx context.Context) error {
	tc, err := TrustedTenantContext(ctx)
	if err != nil {
		return err
	}
	if err := r.Validate(); err != nil {
		return err
	}
	if r.Ref.TenantID != tc.TenantID {
		return ErrInvalidTenant
	}
	return nil
}
func (r DeleteRequest) ValidateForContext(ctx context.Context) error {
	tc, err := TrustedTenantContext(ctx)
	if err != nil {
		return err
	}
	if err := r.Validate(); err != nil {
		return err
	}
	if r.Ref.TenantID != tc.TenantID {
		return ErrInvalidTenant
	}
	return nil
}
func (r SearchRequest) ValidateForContext(ctx context.Context) error {
	if _, err := TrustedTenantContext(ctx); err != nil {
		return err
	}
	return r.Validate()
}
