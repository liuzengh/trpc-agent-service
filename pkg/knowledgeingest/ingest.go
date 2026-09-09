// Package knowledgeingest provides the bounded, tenant-scoped knowledge import
// primitive used by an Admin API or an operator-controlled import worker.
//
// The package intentionally does not open a database, vector service, or
// credential. Callers must provide a store that has already been resolved for
// the tenant and wrapped by the data-plane migration coordinator.
package knowledgeingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
)

const (
	// MaxContentBytes bounds a single import before embedding work starts.
	MaxContentBytes = 1 << 20
	// MaxChunks prevents a caller from turning one request into an unbounded
	// number of vector writes. The current chunk size keeps normal imports below
	// this limit while retaining room for future chunking strategies.
	MaxChunks        = 256
	maxChunkBytes    = 16 << 10
	maxSourceIDLen   = 128
	maxNameLen       = 256
	maxMetadataBytes = 64 << 10
)

var (
	ErrInvalidRequest     = errors.New("knowledge import request is invalid")
	ErrContentTooLarge    = errors.New("knowledge import content exceeds limit")
	ErrTooManyChunks      = errors.New("knowledge import produces too many chunks")
	ErrEmptyEmbedding     = errors.New("knowledge embedder returned an empty vector")
	ErrInvalidMetadata    = errors.New("knowledge import metadata is invalid")
	ErrStoreUnavailable   = errors.New("knowledge import store is unavailable")
	ErrEmbeddingDimension = errors.New("knowledge embedding dimension does not match configured embedder")
)

// Request is the operator-approved source payload. SourceID is stable for the
// source record (for example a CMS article ID), rather than a random request
// ID; that is what makes retries and changed-content replacement idempotent.
type Request struct {
	TenantID   string
	AgentAppID string
	SourceID   string
	Name       string
	Content    string
	Metadata   map[string]any
}

// Result describes the durable vector operations performed by Import.
type Result struct {
	ImportID   string
	ChunkCount int
	Added      int
	Updated    int
	Skipped    int
	Deleted    int
}

// StoreProvider must return a tenant/app-scoped VectorStore. The provider is
// also responsible for applying migrationruntime's read/write decorator. The
// returned close function is always called before Import returns.
type StoreProvider func(context.Context, string, string) (vectorstore.VectorStore, func() error, error)

// Services contains dependencies resolved by the platform runtime. It keeps
// credentials and backend profiles outside the import request surface.
type Services struct {
	OpenStore StoreProvider
	Embedder  embedder.Embedder
}

// Import creates or replaces all chunks for one source. It first reads the
// source's existing metadata, then writes deterministic IDs, and finally
// removes stale chunks. A failed operation may leave a partial import, but a
// retry converges to the same source state without duplicating vectors.
func Import(ctx context.Context, request Request, services Services) (result Result, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateRequest(request); err != nil {
		return Result{}, err
	}
	if services.OpenStore == nil || services.Embedder == nil {
		return Result{}, ErrStoreUnavailable
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}

	metadata := cloneMetadata(request.Metadata)
	fingerprint, err := documentFingerprint(request.Name, request.Content, metadata)
	if err != nil {
		return Result{}, err
	}
	contentHash := sha256Hex([]byte(request.Content))
	result.ImportID = "imp_" + sha256Hex([]byte(request.TenantID + "\x00" + request.AgentAppID + "\x00" + request.SourceID + "\x00" + fingerprint))[:32]

	chunks, err := splitContent(request.Content)
	if err != nil {
		return Result{}, err
	}
	result.ChunkCount = len(chunks)
	store, closeStore, err := services.OpenStore(ctx, request.TenantID, request.AgentAppID)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %w", ErrStoreUnavailable, err)
	}
	if store == nil {
		if closeStore != nil {
			_ = closeStore()
		}
		return Result{}, ErrStoreUnavailable
	}
	if closeStore == nil {
		closeStore = func() error { return nil }
	}
	defer func() { err = errors.Join(err, closeStore()) }()

	// The metadata filter is tenant-scoped by ScopedStore and therefore cannot
	// return another tenant's source even when a physical collection is shared.
	// Bound the read as well as the write. A corrupt or legacy source with an
	// unbounded number of chunks must be repaired explicitly instead of making
	// one request scan and delete an unbounded vector set.
	existing, err := store.GetMetadata(ctx,
		vectorstore.WithGetMetadataFilter(map[string]any{"source_id": request.SourceID}),
		vectorstore.WithGetMetadataLimit(MaxChunks+1),
	)
	if err != nil {
		return result, fmt.Errorf("read existing knowledge import: %w", err)
	}
	if len(existing) > MaxChunks {
		return result, ErrTooManyChunks
	}

	desired := make(map[string]struct{}, len(chunks))
	for index, chunk := range chunks {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		id := chunkID(request.TenantID, request.AgentAppID, request.SourceID, index)
		desired[id] = struct{}{}
		current, found := existing[id]
		if found && current.Metadata["document_sha256"] == fingerprint {
			result.Skipped++
			continue
		}
		embedding, embedErr := services.Embedder.GetEmbedding(ctx, chunk)
		if embedErr != nil {
			return result, fmt.Errorf("embed knowledge chunk %d: %w", index, embedErr)
		}
		if len(embedding) == 0 {
			return result, ErrEmptyEmbedding
		}
		if dimensions := services.Embedder.GetDimensions(); dimensions > 0 && len(embedding) != dimensions {
			return result, fmt.Errorf("%w: got %d, want %d", ErrEmbeddingDimension, len(embedding), dimensions)
		}
		docMetadata := cloneMetadata(metadata)
		docMetadata["source_id"] = request.SourceID
		docMetadata["import_id"] = result.ImportID
		docMetadata["document_sha256"] = fingerprint
		docMetadata["content_sha256"] = contentHash
		docMetadata["chunk_index"] = index
		docMetadata["chunk_count"] = len(chunks)
		doc := &document.Document{ID: id, Name: chunkName(request.Name, index, len(chunks)), Content: chunk, Metadata: docMetadata, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
		if found {
			if err := store.Update(ctx, doc, embedding); err != nil {
				return result, fmt.Errorf("update knowledge chunk %d: %w", index, err)
			}
			result.Updated++
		} else {
			if err := store.Add(ctx, doc, embedding); err != nil {
				return result, fmt.Errorf("add knowledge chunk %d: %w", index, err)
			}
			result.Added++
		}
	}

	for id := range existing {
		if _, keep := desired[id]; keep {
			continue
		}
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if err := store.Delete(ctx, id); err != nil {
			return result, fmt.Errorf("delete stale knowledge chunk: %w", err)
		}
		result.Deleted++
	}
	return result, nil
}

func validateRequest(request Request) error {
	if err := tenant.ValidateTenantID(request.TenantID); err != nil {
		return fmt.Errorf("%w: tenant ID", ErrInvalidRequest)
	}
	if err := tenant.ValidateAgentAppName(request.AgentAppID); err != nil {
		return fmt.Errorf("%w: agent app ID", ErrInvalidRequest)
	}
	if request.SourceID == "" || len(request.SourceID) > maxSourceIDLen || strings.TrimSpace(request.SourceID) != request.SourceID || strings.ContainsAny(request.SourceID, "\x00\r\n") {
		return fmt.Errorf("%w: source ID", ErrInvalidRequest)
	}
	if request.Name != "" && (len(request.Name) > maxNameLen || strings.TrimSpace(request.Name) != request.Name || strings.ContainsAny(request.Name, "\x00\r\n")) {
		return fmt.Errorf("%w: name", ErrInvalidRequest)
	}
	if request.Content == "" || !utf8.ValidString(request.Content) || strings.TrimSpace(request.Content) == "" || strings.ContainsRune(request.Content, '\x00') {
		return fmt.Errorf("%w: content", ErrInvalidRequest)
	}
	if len([]byte(request.Content)) > MaxContentBytes {
		return ErrContentTooLarge
	}
	for key := range request.Metadata {
		if key == "" || strings.HasPrefix(key, "_tsa_") || managedMetadataKey(key) || strings.ContainsAny(key, "\x00\r\n") {
			return ErrInvalidMetadata
		}
	}
	if _, err := json.Marshal(request.Metadata); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidMetadata, err)
	}
	return nil
}

func splitContent(content string) ([]string, error) {
	runes := []rune(content)
	chunks := make([]string, 0, (len(content)+maxChunkBytes-1)/maxChunkBytes)
	start := 0
	bytes := 0
	for index, value := range runes {
		width := len(string(value))
		if bytes > 0 && bytes+width > maxChunkBytes {
			chunks = append(chunks, string(runes[start:index]))
			start, bytes = index, 0
			if len(chunks) >= MaxChunks {
				return nil, ErrTooManyChunks
			}
		}
		bytes += width
	}
	if start < len(runes) {
		chunks = append(chunks, string(runes[start:]))
	}
	if len(chunks) == 0 || len(chunks) > MaxChunks {
		return nil, ErrTooManyChunks
	}
	return chunks, nil
}

func documentFingerprint(name, content string, metadata map[string]any) (string, error) {
	body, err := json.Marshal(struct {
		Name     string         `json:"name"`
		Content  string         `json:"content"`
		Metadata map[string]any `json:"metadata,omitempty"`
	}{name, content, metadata})
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidMetadata, err)
	}
	if len(body) > maxMetadataBytes+MaxContentBytes {
		return "", ErrInvalidMetadata
	}
	return sha256Hex(body), nil
}

func managedMetadataKey(key string) bool {
	switch key {
	case "source_id", "import_id", "document_sha256", "content_sha256", "chunk_index", "chunk_count":
		return true
	default:
		return false
	}
}

func chunkID(tenantID, appID, sourceID string, index int) string {
	return "chunk_" + sha256Hex([]byte(fmt.Sprintf("%s\x00%s\x00%s\x00%d", tenantID, appID, sourceID, index)))[:48]
}

func chunkName(name string, index, total int) string {
	if name == "" {
		name = "knowledge-import"
	}
	if total == 1 {
		return name
	}
	return fmt.Sprintf("%s [%d/%d]", name, index+1, total)
}

func sha256Hex(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func cloneMetadata(input map[string]any) map[string]any {
	if len(input) == 0 {
		return make(map[string]any)
	}
	output := make(map[string]any, len(input)+6)
	for key, value := range input {
		output[key] = value
	}
	return output
}
