package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"sort"
	"strings"
	"time"

	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	pgstorage "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

func checkCapability(ctx context.Context, store *Store) error {
	if err := check(ctx); err != nil {
		return err
	}
	if store == nil || store.db == nil {
		return runtimestorage.ErrStorage
	}
	return nil
}

// PutMemory implements the tenant-scoped runtime storage contract.
func (s *Store) PutMemory(ctx context.Context, input runtimestorage.MemoryInput) (runtimestorage.MemoryRecord, error) {
	if err := checkCapability(ctx, s); err != nil {
		return runtimestorage.MemoryRecord{}, err
	}
	if runtimestorage.ValidateTenant(input.TenantID) != nil || !runtimestorage.ValidateText(input.UserID, 256, true) || !runtimestorage.ValidateText(input.Content, 0, true) || !runtimestorage.ValidateText(input.MemoryID, 256, false) || !runtimestorage.ValidateText(input.SessionID, 256, false) || !runtimestorage.ValidateEmbedding(input.Embedding) {
		return runtimestorage.MemoryRecord{}, runtimestorage.ErrInvalid
	}
	if input.MemoryID == "" {
		input.MemoryID = "mem_" + uuid.NewString()
	}
	topicsValue := input.Topics
	if topicsValue == nil {
		topicsValue = []string{}
	}
	metadataValue := input.Metadata
	if metadataValue == nil {
		metadataValue = map[string]any{}
	}
	embeddingValue := input.Embedding
	if embeddingValue == nil {
		embeddingValue = []float64{}
	}
	topics, err := pgstorage.EncodeJSON(topicsValue)
	if err != nil {
		return runtimestorage.MemoryRecord{}, runtimestorage.ErrInvalid
	}
	metadata, err := pgstorage.EncodeJSON(metadataValue)
	if err != nil {
		return runtimestorage.MemoryRecord{}, runtimestorage.ErrInvalid
	}
	embedding, err := pgstorage.EncodeJSON(embeddingValue)
	if err != nil {
		return runtimestorage.MemoryRecord{}, runtimestorage.ErrInvalid
	}
	var value runtimestorage.MemoryRecord
	var topicsRaw, metadataRaw []byte
	var embeddingRaw []byte
	err = s.db.QueryRowContext(ctx, "INSERT INTO public.runtime_memory (tenant_id,memory_id,user_id,session_id,content,topics,metadata,embedding,version,deleted_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,1,NULL) ON CONFLICT (tenant_id,memory_id) DO UPDATE SET user_id=EXCLUDED.user_id,session_id=EXCLUDED.session_id,content=EXCLUDED.content,topics=EXCLUDED.topics,metadata=EXCLUDED.metadata,embedding=EXCLUDED.embedding,version=public.runtime_memory.version+1,deleted_at=NULL,updated_at=now() RETURNING tenant_id,memory_id,user_id,session_id,content,topics,metadata,embedding,version,deleted_at,created_at,updated_at", input.TenantID, input.MemoryID, input.UserID, input.SessionID, input.Content, topics, metadata, embedding).Scan(&value.TenantID, &value.MemoryID, &value.UserID, &value.SessionID, &value.Content, &topicsRaw, &metadataRaw, &embeddingRaw, &value.Version, &value.DeletedAt, &value.CreatedAt, &value.UpdatedAt)
	if err != nil {
		return runtimestorage.MemoryRecord{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if json.Unmarshal(topicsRaw, &value.Topics) != nil || pgstorage.DecodeJSON(metadataRaw, &value.Metadata) != nil || pgstorage.DecodeJSON(embeddingRaw, &value.Embedding) != nil {
		return runtimestorage.MemoryRecord{}, runtimestorage.ErrStorage
	}
	result := cloneMemory(value)
	if err := s.EnqueueMemoryIndex(ctx, result); err != nil {
		return result, err
	}
	return result, nil
}

// GetMemory implements the tenant-scoped runtime storage contract.
func (s *Store) GetMemory(ctx context.Context, tenantID, memoryID string) (runtimestorage.MemoryRecord, error) {
	if err := checkCapability(ctx, s); err != nil {
		return runtimestorage.MemoryRecord{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || memoryID == "" {
		return runtimestorage.MemoryRecord{}, runtimestorage.ErrInvalid
	}
	value, err := scanMemory(s.db.QueryRowContext(ctx, "SELECT tenant_id,memory_id,user_id,session_id,content,topics,metadata,embedding,version,deleted_at,created_at,updated_at FROM public.runtime_memory WHERE tenant_id=$1 AND memory_id=$2 AND deleted_at IS NULL", tenantID, memoryID))
	if err != nil {
		return runtimestorage.MemoryRecord{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	return value, nil
}

// ListMemories implements the tenant-scoped runtime storage contract.
func (s *Store) ListMemories(ctx context.Context, tenantID, userID string, limit int) ([]runtimestorage.MemoryRecord, error) {
	if err := checkCapability(ctx, s); err != nil {
		return nil, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || strings.TrimSpace(userID) == "" || limit < 0 {
		return nil, runtimestorage.ErrInvalid
	}
	rows, err := s.db.QueryContext(ctx, "SELECT tenant_id,memory_id,user_id,session_id,content,topics,metadata,embedding,version,deleted_at,created_at,updated_at FROM public.runtime_memory WHERE tenant_id=$1 AND user_id=$2 AND deleted_at IS NULL ORDER BY updated_at DESC,memory_id LIMIT NULLIF($3,0)", tenantID, userID, limit)
	if err != nil {
		return nil, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	defer rows.Close()
	values := make([]runtimestorage.MemoryRecord, 0)
	for rows.Next() {
		value, scanErr := scanMemory(rows)
		if scanErr != nil {
			return nil, runtimestorage.ErrStorage
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, runtimestorage.ErrStorage
	}
	return values, nil
}

// SearchMemories implements the tenant-scoped runtime storage contract.
func (s *Store) SearchMemories(ctx context.Context, tenantID, userID, query string, limit int) ([]runtimestorage.MemorySearchResult, error) {
	if err := checkCapability(ctx, s); err != nil {
		return nil, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || strings.TrimSpace(userID) == "" || strings.TrimSpace(query) == "" || limit < 0 {
		return nil, runtimestorage.ErrInvalid
	}
	rows, err := s.db.QueryContext(ctx, "SELECT tenant_id,memory_id,user_id,session_id,content,topics,metadata,embedding,version,deleted_at,created_at,updated_at FROM public.runtime_memory WHERE tenant_id=$1 AND user_id=$2 AND deleted_at IS NULL", tenantID, userID)
	if err != nil {
		return nil, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	defer rows.Close()
	terms := strings.Fields(strings.ToLower(query))
	values := make([]runtimestorage.MemorySearchResult, 0)
	for rows.Next() {
		value, scanErr := scanMemory(rows)
		if scanErr != nil {
			return nil, runtimestorage.ErrStorage
		}
		hits := 0
		text := strings.ToLower(value.Content)
		for _, term := range terms {
			if strings.Contains(text, term) {
				hits++
			}
		}
		if hits > 0 {
			values = append(values, runtimestorage.MemorySearchResult{Memory: value, Score: float64(hits) / float64(len(terms))})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, runtimestorage.ErrStorage
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i].Score == values[j].Score {
			return values[i].Memory.MemoryID < values[j].Memory.MemoryID
		}
		return values[i].Score > values[j].Score
	})
	if limit > 0 && len(values) > limit {
		values = values[:limit]
	}
	return values, nil
}

// DeleteMemory implements the tenant-scoped runtime storage contract.
func (s *Store) DeleteMemory(ctx context.Context, tenantID, memoryID string) error {
	if err := checkCapability(ctx, s); err != nil {
		return err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || memoryID == "" {
		return runtimestorage.ErrInvalid
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return runtimestorage.ErrStorage
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, "UPDATE public.runtime_memory SET deleted_at=now(),version=version+1,updated_at=now() WHERE tenant_id=$1 AND memory_id=$2 AND deleted_at IS NULL", tenantID, memoryID)
	if err != nil {
		return mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	count, _ := result.RowsAffected()
	if count == 0 {
		return runtimestorage.ErrNotFound
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM public.runtime_vector_index WHERE tenant_id=$1 AND source=$2 AND document_id=$3", tenantID, runtimestorage.VectorSourceMemory, memoryID); err != nil {
		return mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if err := tx.Commit(); err != nil {
		return runtimestorage.ErrStorage
	}
	return nil
}

// EnqueueMemoryIndex implements the tenant-scoped runtime storage contract.
func (s *Store) EnqueueMemoryIndex(ctx context.Context, value runtimestorage.MemoryRecord) error {
	if err := checkCapability(ctx, s); err != nil {
		return err
	}
	if runtimestorage.ValidateTenant(value.TenantID) != nil || value.MemoryID == "" || value.Version < 1 {
		return runtimestorage.ErrInvalid
	}
	result, err := s.db.ExecContext(ctx, "INSERT INTO public.runtime_vector_index (tenant_id,source,document_id,content,metadata,embedding,version) SELECT tenant_id,'memory',memory_id,content,metadata,embedding,version FROM public.runtime_memory WHERE tenant_id=$1 AND memory_id=$2 AND version=$3 AND deleted_at IS NULL AND embedding <> '[]'::jsonb ON CONFLICT (tenant_id,source,document_id) DO UPDATE SET content=EXCLUDED.content,metadata=EXCLUDED.metadata,embedding=EXCLUDED.embedding,version=EXCLUDED.version,updated_at=now() WHERE EXCLUDED.version >= public.runtime_vector_index.version", value.TenantID, value.MemoryID, value.Version)
	if err != nil {
		return mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if count, _ := result.RowsAffected(); count > 0 {
		return nil
	}
	current, getErr := s.GetMemory(ctx, value.TenantID, value.MemoryID)
	if getErr != nil {
		return getErr
	}
	if current.Version != value.Version {
		return runtimestorage.ErrConflict
	}
	if len(current.Embedding) > 0 {
		// A newer vector projection may already represent this durable version.
		// Do not remove it merely because this idempotent enqueue was ignored.
		return nil
	}
	// A current memory without an embedding has no vector projection. Remove
	// any projection left by an earlier version so reads remain monotonic.
	_, err = s.db.ExecContext(ctx, "DELETE FROM public.runtime_vector_index WHERE tenant_id=$1 AND source=$2 AND document_id=$3", value.TenantID, runtimestorage.VectorSourceMemory, value.MemoryID)
	if err != nil {
		return mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	return nil
}

// PutSummary implements the tenant-scoped runtime storage contract.
func (s *Store) PutSummary(ctx context.Context, value runtimestorage.SummaryRecord) (runtimestorage.SummaryRecord, error) {
	if err := checkCapability(ctx, s); err != nil {
		return runtimestorage.SummaryRecord{}, err
	}
	if runtimestorage.ValidateSession(value.TenantID, value.SessionID) != nil || strings.TrimSpace(value.Text) == "" || value.EventSeq < 0 {
		return runtimestorage.SummaryRecord{}, runtimestorage.ErrInvalid
	}
	var out runtimestorage.SummaryRecord
	err := s.db.QueryRowContext(ctx, "INSERT INTO public.runtime_summary (tenant_id,session_id,filter_key,text,event_seq,version) VALUES ($1,$2,$3,$4,$5,1) ON CONFLICT (tenant_id,session_id,filter_key) DO UPDATE SET text=EXCLUDED.text,event_seq=EXCLUDED.event_seq,version=public.runtime_summary.version+1,updated_at=now() WHERE EXCLUDED.event_seq >= public.runtime_summary.event_seq RETURNING tenant_id,session_id,filter_key,text,event_seq,version,created_at,updated_at", value.TenantID, value.SessionID, value.FilterKey, value.Text, value.EventSeq).Scan(&out.TenantID, &out.SessionID, &out.FilterKey, &out.Text, &out.EventSeq, &out.Version, &out.CreatedAt, &out.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtimestorage.SummaryRecord{}, runtimestorage.ErrConflict
		}
		return runtimestorage.SummaryRecord{}, mapSummaryError(ctx, err)
	}
	return out, nil
}

// GetSummary implements the tenant-scoped runtime storage contract.
func (s *Store) GetSummary(ctx context.Context, tenantID, sessionID, filterKey string) (runtimestorage.SummaryRecord, error) {
	if err := checkCapability(ctx, s); err != nil {
		return runtimestorage.SummaryRecord{}, err
	}
	if runtimestorage.ValidateSession(tenantID, sessionID) != nil {
		return runtimestorage.SummaryRecord{}, runtimestorage.ErrInvalid
	}
	var value runtimestorage.SummaryRecord
	err := s.db.QueryRowContext(ctx, "SELECT tenant_id,session_id,filter_key,text,event_seq,version,created_at,updated_at FROM public.runtime_summary WHERE tenant_id=$1 AND session_id=$2 AND filter_key=$3", tenantID, sessionID, filterKey).Scan(&value.TenantID, &value.SessionID, &value.FilterKey, &value.Text, &value.EventSeq, &value.Version, &value.CreatedAt, &value.UpdatedAt)
	if err != nil {
		return runtimestorage.SummaryRecord{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	return value, nil
}

// EnqueueSummary implements the tenant-scoped runtime storage contract.
func (s *Store) EnqueueSummary(ctx context.Context, value runtimestorage.SummaryRecord) error {
	_, err := s.PutSummary(ctx, value)
	return err
}

type scanner interface{ Scan(...any) error }

func scanMemory(row scanner) (runtimestorage.MemoryRecord, error) {
	var value runtimestorage.MemoryRecord
	var topics, metadata, embedding []byte
	err := row.Scan(&value.TenantID, &value.MemoryID, &value.UserID, &value.SessionID, &value.Content, &topics, &metadata, &embedding, &value.Version, &value.DeletedAt, &value.CreatedAt, &value.UpdatedAt)
	if err != nil {
		return runtimestorage.MemoryRecord{}, err
	}
	if json.Unmarshal(topics, &value.Topics) != nil || pgstorage.DecodeJSON(metadata, &value.Metadata) != nil || pgstorage.DecodeJSON(embedding, &value.Embedding) != nil {
		return runtimestorage.MemoryRecord{}, runtimestorage.ErrStorage
	}
	return cloneMemory(value), nil
}
func cloneMemory(value runtimestorage.MemoryRecord) runtimestorage.MemoryRecord {
	value.Topics = append([]string(nil), value.Topics...)
	if value.Metadata != nil {
		value.Metadata = cloneMap(value.Metadata)
	}
	if value.DeletedAt != nil {
		copy := *value.DeletedAt
		value.DeletedAt = &copy
	}
	return value
}
func cloneMap(value map[string]any) map[string]any {
	if value == nil {
		return nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var output map[string]any
	if json.Unmarshal(data, &output) != nil {
		return nil
	}
	return output
}

func cloneKnowledge(value runtimestorage.KnowledgeDocument) runtimestorage.KnowledgeDocument {
	value.Metadata = cloneMap(value.Metadata)
	value.Embedding = append([]float64(nil), value.Embedding...)
	return value
}

func cosine(left, right []float64) (float64, bool) {
	if len(left) == 0 || len(left) != len(right) {
		return 0, false
	}
	var dot, leftNorm, rightNorm float64
	for i := range left {
		dot += left[i] * right[i]
		leftNorm += left[i] * left[i]
		rightNorm += right[i] * right[i]
	}
	if leftNorm == 0 || rightNorm == 0 {
		return 0, false
	}
	return dot / (math.Sqrt(leftNorm) * math.Sqrt(rightNorm)), true
}

// PutKnowledge implements the tenant-scoped runtime storage contract.
func (s *Store) PutKnowledge(ctx context.Context, value runtimestorage.KnowledgeDocument) (runtimestorage.KnowledgeDocument, error) {
	if err := checkCapability(ctx, s); err != nil {
		return runtimestorage.KnowledgeDocument{}, err
	}
	if runtimestorage.ValidateTenant(value.TenantID) != nil || !runtimestorage.ValidateText(value.DocumentID, 256, true) || !runtimestorage.ValidateText(value.Content, 0, true) || !runtimestorage.ValidateText(value.Digest, 128, false) || !runtimestorage.ValidateEmbedding(value.Embedding) {
		return runtimestorage.KnowledgeDocument{}, runtimestorage.ErrInvalid
	}
	metadataValue := value.Metadata
	if metadataValue == nil {
		metadataValue = map[string]any{}
	}
	embeddingValue := value.Embedding
	if embeddingValue == nil {
		embeddingValue = []float64{}
	}
	metadata, err := pgstorage.EncodeJSON(metadataValue)
	if err != nil {
		return runtimestorage.KnowledgeDocument{}, runtimestorage.ErrInvalid
	}
	embedding, err := pgstorage.EncodeJSON(embeddingValue)
	if err != nil {
		return runtimestorage.KnowledgeDocument{}, runtimestorage.ErrInvalid
	}
	if value.Digest == "" {
		sum := sha256.Sum256([]byte(value.Content))
		value.Digest = hex.EncodeToString(sum[:])
	}
	var out runtimestorage.KnowledgeDocument
	var metaRaw, embRaw []byte
	err = s.db.QueryRowContext(ctx, "INSERT INTO public.runtime_knowledge (tenant_id,document_id,content,metadata,embedding,digest,version) VALUES ($1,$2,$3,$4,$5,$6,1) ON CONFLICT (tenant_id,document_id) DO UPDATE SET content=EXCLUDED.content,metadata=EXCLUDED.metadata,embedding=EXCLUDED.embedding,digest=EXCLUDED.digest,version=public.runtime_knowledge.version+1,updated_at=now() RETURNING tenant_id,document_id,content,metadata,embedding,digest,version,created_at,updated_at", value.TenantID, value.DocumentID, value.Content, metadata, embedding, value.Digest).Scan(&out.TenantID, &out.DocumentID, &out.Content, &metaRaw, &embRaw, &out.Digest, &out.Version, &out.CreatedAt, &out.UpdatedAt)
	if err != nil {
		return runtimestorage.KnowledgeDocument{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if pgstorage.DecodeJSON(metaRaw, &out.Metadata) != nil || pgstorage.DecodeJSON(embRaw, &out.Embedding) != nil {
		return runtimestorage.KnowledgeDocument{}, runtimestorage.ErrStorage
	}
	var projectionErr error
	if len(out.Embedding) > 0 {
		_, projectionErr = s.db.ExecContext(ctx, "INSERT INTO public.runtime_vector_index (tenant_id,source,document_id,content,metadata,embedding,version) SELECT tenant_id,'knowledge',document_id,content,metadata,embedding,version FROM public.runtime_knowledge WHERE tenant_id=$1 AND document_id=$2 AND version=$3 AND embedding <> '[]'::jsonb ON CONFLICT (tenant_id,source,document_id) DO UPDATE SET content=EXCLUDED.content,metadata=EXCLUDED.metadata,embedding=EXCLUDED.embedding,version=EXCLUDED.version,updated_at=now() WHERE EXCLUDED.version >= public.runtime_vector_index.version", out.TenantID, out.DocumentID, out.Version)
	} else {
		_, projectionErr = s.db.ExecContext(ctx, "DELETE FROM public.runtime_vector_index WHERE tenant_id=$1 AND source=$2 AND document_id=$3", out.TenantID, runtimestorage.VectorSourceKnowledge, out.DocumentID)
	}
	if projectionErr != nil {
		return runtimestorage.KnowledgeDocument{}, mapError(ctx, projectionErr, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	return out, nil
}

// GetKnowledge implements the tenant-scoped runtime storage contract.
func (s *Store) GetKnowledge(ctx context.Context, tenantID, documentID string) (runtimestorage.KnowledgeDocument, error) {
	if err := checkCapability(ctx, s); err != nil {
		return runtimestorage.KnowledgeDocument{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || documentID == "" {
		return runtimestorage.KnowledgeDocument{}, runtimestorage.ErrInvalid
	}
	var out runtimestorage.KnowledgeDocument
	var metaRaw, embRaw []byte
	err := s.db.QueryRowContext(ctx, "SELECT tenant_id,document_id,content,metadata,embedding,digest,version,created_at,updated_at FROM public.runtime_knowledge WHERE tenant_id=$1 AND document_id=$2", tenantID, documentID).Scan(&out.TenantID, &out.DocumentID, &out.Content, &metaRaw, &embRaw, &out.Digest, &out.Version, &out.CreatedAt, &out.UpdatedAt)
	if err != nil {
		return runtimestorage.KnowledgeDocument{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if pgstorage.DecodeJSON(metaRaw, &out.Metadata) != nil || pgstorage.DecodeJSON(embRaw, &out.Embedding) != nil {
		return runtimestorage.KnowledgeDocument{}, runtimestorage.ErrStorage
	}
	return out, nil
}

// SearchKnowledge implements the tenant-scoped runtime storage contract.
func (s *Store) SearchKnowledge(ctx context.Context, tenantID string, embedding []float64, limit int) ([]runtimestorage.KnowledgeSearchResult, error) {
	if err := checkCapability(ctx, s); err != nil {
		return nil, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || len(embedding) == 0 || limit < 0 {
		return nil, runtimestorage.ErrInvalid
	}
	rows, err := s.db.QueryContext(ctx, "SELECT tenant_id,document_id,content,metadata,embedding,digest,version,created_at,updated_at FROM public.runtime_knowledge WHERE tenant_id=$1 AND embedding <> '[]'::jsonb", tenantID)
	if err != nil {
		return nil, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	defer rows.Close()
	values := make([]runtimestorage.KnowledgeSearchResult, 0)
	for rows.Next() {
		var value runtimestorage.KnowledgeDocument
		var metadata, emb []byte
		if err := rows.Scan(&value.TenantID, &value.DocumentID, &value.Content, &metadata, &emb, &value.Digest, &value.Version, &value.CreatedAt, &value.UpdatedAt); err != nil || pgstorage.DecodeJSON(metadata, &value.Metadata) != nil || pgstorage.DecodeJSON(emb, &value.Embedding) != nil {
			return nil, runtimestorage.ErrStorage
		}
		score, ok := cosine(embedding, value.Embedding)
		if ok {
			values = append(values, runtimestorage.KnowledgeSearchResult{Document: cloneKnowledge(value), Score: score})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, runtimestorage.ErrStorage
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i].Score == values[j].Score {
			return values[i].Document.DocumentID < values[j].Document.DocumentID
		}
		return values[i].Score > values[j].Score
	})
	if limit > 0 && len(values) > limit {
		values = values[:limit]
	}
	return values, nil
}

// DeleteKnowledge implements the tenant-scoped runtime storage contract.
func (s *Store) DeleteKnowledge(ctx context.Context, tenantID, documentID string) error {
	if err := checkCapability(ctx, s); err != nil {
		return err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || documentID == "" {
		return runtimestorage.ErrInvalid
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return runtimestorage.ErrStorage
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, "DELETE FROM public.runtime_knowledge WHERE tenant_id=$1 AND document_id=$2", tenantID, documentID)
	if err != nil {
		return mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	count, _ := result.RowsAffected()
	if count == 0 {
		return runtimestorage.ErrNotFound
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM public.runtime_vector_index WHERE tenant_id=$1 AND source=$2 AND document_id=$3", tenantID, runtimestorage.VectorSourceKnowledge, documentID); err != nil {
		return mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if err := tx.Commit(); err != nil {
		return runtimestorage.ErrStorage
	}
	return nil
}

// PutArtifact implements the tenant-scoped runtime storage contract.
func (s *Store) PutArtifact(ctx context.Context, value runtimestorage.ArtifactRecord) (runtimestorage.ArtifactRecord, error) {
	if err := checkCapability(ctx, s); err != nil {
		return runtimestorage.ArtifactRecord{}, err
	}
	if runtimestorage.ValidateTenant(value.TenantID) != nil || !runtimestorage.ValidateText(value.ArtifactID, 256, true) || !runtimestorage.ValidateText(value.SessionID, 256, false) || !runtimestorage.ValidateText(value.Name, 512, false) || !runtimestorage.ValidateText(value.MimeType, 256, false) || len(value.Content) == 0 {
		return runtimestorage.ArtifactRecord{}, runtimestorage.ErrInvalid
	}
	var out runtimestorage.ArtifactRecord
	err := s.db.QueryRowContext(ctx, "INSERT INTO public.runtime_artifact (tenant_id,artifact_id,session_id,name,mime_type,content,version) VALUES ($1,$2,$3,$4,$5,$6,1) ON CONFLICT (tenant_id,artifact_id) DO UPDATE SET session_id=EXCLUDED.session_id,name=EXCLUDED.name,mime_type=EXCLUDED.mime_type,content=EXCLUDED.content,version=public.runtime_artifact.version+1,updated_at=now() RETURNING tenant_id,artifact_id,session_id,name,mime_type,content,version,created_at,updated_at", value.TenantID, value.ArtifactID, value.SessionID, value.Name, value.MimeType, value.Content).Scan(&out.TenantID, &out.ArtifactID, &out.SessionID, &out.Name, &out.MimeType, &out.Content, &out.Version, &out.CreatedAt, &out.UpdatedAt)
	if err != nil {
		return runtimestorage.ArtifactRecord{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	return out, nil
}

// GetArtifact implements the tenant-scoped runtime storage contract.
func (s *Store) GetArtifact(ctx context.Context, tenantID, artifactID string) (runtimestorage.ArtifactRecord, error) {
	if err := checkCapability(ctx, s); err != nil {
		return runtimestorage.ArtifactRecord{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || artifactID == "" {
		return runtimestorage.ArtifactRecord{}, runtimestorage.ErrInvalid
	}
	var value runtimestorage.ArtifactRecord
	err := s.db.QueryRowContext(ctx, "SELECT tenant_id,artifact_id,session_id,name,mime_type,content,version,created_at,updated_at FROM public.runtime_artifact WHERE tenant_id=$1 AND artifact_id=$2", tenantID, artifactID).Scan(&value.TenantID, &value.ArtifactID, &value.SessionID, &value.Name, &value.MimeType, &value.Content, &value.Version, &value.CreatedAt, &value.UpdatedAt)
	if err != nil {
		return runtimestorage.ArtifactRecord{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	return value, nil
}

// ListArtifacts implements the tenant-scoped runtime storage contract.
func (s *Store) ListArtifacts(ctx context.Context, tenantID, sessionID string) ([]runtimestorage.ArtifactRecord, error) {
	if err := checkCapability(ctx, s); err != nil {
		return nil, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil {
		return nil, runtimestorage.ErrInvalid
	}
	rows, err := s.db.QueryContext(ctx, "SELECT tenant_id,artifact_id,session_id,name,mime_type,content,version,created_at,updated_at FROM public.runtime_artifact WHERE tenant_id=$1 AND ($2='' OR session_id=$2) ORDER BY artifact_id", tenantID, sessionID)
	if err != nil {
		return nil, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	defer rows.Close()
	values := make([]runtimestorage.ArtifactRecord, 0)
	for rows.Next() {
		var value runtimestorage.ArtifactRecord
		if err := rows.Scan(&value.TenantID, &value.ArtifactID, &value.SessionID, &value.Name, &value.MimeType, &value.Content, &value.Version, &value.CreatedAt, &value.UpdatedAt); err != nil {
			return nil, runtimestorage.ErrStorage
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, runtimestorage.ErrStorage
	}
	return values, nil
}

func mapSummaryError(ctx context.Context, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23503" {
		return runtimestorage.ErrNotFound
	}
	return mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
}

// DeleteArtifact implements the tenant-scoped runtime storage contract.
func (s *Store) DeleteArtifact(ctx context.Context, tenantID, artifactID string) error {
	if err := checkCapability(ctx, s); err != nil {
		return err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || artifactID == "" {
		return runtimestorage.ErrInvalid
	}
	result, err := s.db.ExecContext(ctx, "DELETE FROM public.runtime_artifact WHERE tenant_id=$1 AND artifact_id=$2", tenantID, artifactID)
	if err != nil {
		return mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	count, _ := result.RowsAffected()
	if count == 0 {
		return runtimestorage.ErrNotFound
	}
	return nil
}

var _ runtimestorage.MemoryStore = (*Store)(nil)
var _ runtimestorage.SummaryStore = (*Store)(nil)
var _ runtimestorage.KnowledgeStore = (*Store)(nil)
var _ runtimestorage.ArtifactStore = (*Store)(nil)

// AppendAudit implements the tenant-scoped runtime storage contract.
func (s *Store) AppendAudit(ctx context.Context, value runtimestorage.AuditRecord) (runtimestorage.AuditRecord, error) {
	if err := checkCapability(ctx, s); err != nil {
		return runtimestorage.AuditRecord{}, err
	}
	if runtimestorage.ValidateTenant(value.TenantID) != nil || !runtimestorage.ValidateText(value.EventType, 128, true) || !runtimestorage.ValidateText(value.AuditID, 256, false) {
		return runtimestorage.AuditRecord{}, runtimestorage.ErrInvalid
	}
	if value.AuditID == "" {
		value.AuditID = uuid.NewString()
	}
	payloadValue := value.Payload
	if payloadValue == nil {
		payloadValue = map[string]any{}
	}
	payload, err := pgstorage.EncodeJSON(payloadValue)
	if err != nil {
		return runtimestorage.AuditRecord{}, runtimestorage.ErrInvalid
	}
	if value.OccurredAt.IsZero() {
		value.OccurredAt = time.Now().UTC()
	}
	var out runtimestorage.AuditRecord
	var raw []byte
	err = s.db.QueryRowContext(ctx, "INSERT INTO public.runtime_audit_log (tenant_id,audit_id,event_type,payload,occurred_at) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (tenant_id,audit_id) DO UPDATE SET audit_id=EXCLUDED.audit_id WHERE public.runtime_audit_log.event_type=EXCLUDED.event_type AND public.runtime_audit_log.payload=EXCLUDED.payload RETURNING tenant_id,audit_id,event_type,payload,occurred_at", value.TenantID, value.AuditID, value.EventType, payload, value.OccurredAt).Scan(&out.TenantID, &out.AuditID, &out.EventType, &raw, &out.OccurredAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtimestorage.AuditRecord{}, runtimestorage.ErrConflict
		}
		return runtimestorage.AuditRecord{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if pgstorage.DecodeJSON(raw, &out.Payload) != nil {
		return runtimestorage.AuditRecord{}, runtimestorage.ErrStorage
	}
	return out, nil
}

// ListAudit implements the tenant-scoped runtime storage contract.
func (s *Store) ListAudit(ctx context.Context, tenantID string, since time.Time, limit int) ([]runtimestorage.AuditRecord, error) {
	if err := checkCapability(ctx, s); err != nil {
		return nil, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || limit < 0 {
		return nil, runtimestorage.ErrInvalid
	}
	rows, err := s.db.QueryContext(ctx, "SELECT tenant_id,audit_id,event_type,payload,occurred_at FROM public.runtime_audit_log WHERE tenant_id=$1 AND ($2::timestamptz IS NULL OR occurred_at >= $2) ORDER BY occurred_at,audit_id LIMIT NULLIF($3,0)", tenantID, nullTime(since), limit)
	if err != nil {
		return nil, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	defer rows.Close()
	values := make([]runtimestorage.AuditRecord, 0)
	for rows.Next() {
		var value runtimestorage.AuditRecord
		var raw []byte
		if err := rows.Scan(&value.TenantID, &value.AuditID, &value.EventType, &raw, &value.OccurredAt); err != nil || pgstorage.DecodeJSON(raw, &value.Payload) != nil {
			return nil, runtimestorage.ErrStorage
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, runtimestorage.ErrStorage
	}
	return values, nil
}

// UpsertVector implements the tenant-scoped runtime storage contract.
func (s *Store) UpsertVector(ctx context.Context, value runtimestorage.VectorRecord) error {
	if err := checkCapability(ctx, s); err != nil {
		return err
	}
	if runtimestorage.ValidateTenant(value.TenantID) != nil || !runtimestorage.ValidateText(value.DocumentID, 256, true) || len(value.Embedding) == 0 || !runtimestorage.ValidateEmbedding(value.Embedding) || !runtimestorage.ValidateText(string(value.Source), 128, false) {
		return runtimestorage.ErrInvalid
	}
	if value.Source == "" {
		value.Source = runtimestorage.VectorSourceGeneric
	}
	if strings.TrimSpace(string(value.Source)) == "" {
		return runtimestorage.ErrInvalid
	}
	embedding, err := pgstorage.EncodeJSON(value.Embedding)
	if err != nil {
		return runtimestorage.ErrInvalid
	}
	metadataValue := value.Metadata
	if metadataValue == nil {
		metadataValue = map[string]any{}
	}
	metadata, err := pgstorage.EncodeJSON(metadataValue)
	if err != nil {
		return runtimestorage.ErrInvalid
	}
	if value.Version < 1 {
		value.Version = 1
	}
	result, err := s.db.ExecContext(ctx, "INSERT INTO public.runtime_vector_index (tenant_id,source,document_id,content,metadata,embedding,version) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (tenant_id,source,document_id) DO UPDATE SET content=EXCLUDED.content,metadata=EXCLUDED.metadata,embedding=EXCLUDED.embedding,version=EXCLUDED.version,updated_at=now() WHERE EXCLUDED.version >= public.runtime_vector_index.version", value.TenantID, value.Source, value.DocumentID, value.Content, metadata, embedding, value.Version)
	if err != nil {
		return mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if count, err := result.RowsAffected(); err == nil && count == 0 {
		return runtimestorage.ErrConflict
	}
	return nil
}

// SearchVectors implements the tenant-scoped runtime storage contract.
func (s *Store) SearchVectors(ctx context.Context, tenantID string, embedding []float64, limit int) ([]runtimestorage.VectorSearchResult, error) {
	if err := checkCapability(ctx, s); err != nil {
		return nil, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || len(embedding) == 0 || limit < 0 {
		return nil, runtimestorage.ErrInvalid
	}
	rows, err := s.db.QueryContext(ctx, "SELECT tenant_id,source,document_id,content,metadata,embedding,version,updated_at FROM public.runtime_vector_index WHERE tenant_id=$1", tenantID)
	if err != nil {
		return nil, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	defer rows.Close()
	values := make([]runtimestorage.VectorSearchResult, 0)
	for rows.Next() {
		var value runtimestorage.VectorRecord
		var metadata, raw []byte
		if err := rows.Scan(&value.TenantID, &value.Source, &value.DocumentID, &value.Content, &metadata, &raw, &value.Version, &value.UpdatedAt); err != nil || pgstorage.DecodeJSON(metadata, &value.Metadata) != nil || pgstorage.DecodeJSON(raw, &value.Embedding) != nil {
			return nil, runtimestorage.ErrStorage
		}
		score, ok := cosine(embedding, value.Embedding)
		if ok {
			value.Metadata = cloneMap(value.Metadata)
			value.Embedding = append([]float64(nil), value.Embedding...)
			values = append(values, runtimestorage.VectorSearchResult{Record: value, Score: score})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, runtimestorage.ErrStorage
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i].Score == values[j].Score {
			if values[i].Record.DocumentID == values[j].Record.DocumentID {
				return values[i].Record.Source < values[j].Record.Source
			}
			return values[i].Record.DocumentID < values[j].Record.DocumentID
		}
		return values[i].Score > values[j].Score
	})
	if limit > 0 && len(values) > limit {
		values = values[:limit]
	}
	return values, nil
}

// DeleteVector implements the tenant-scoped runtime storage contract.
func (s *Store) DeleteVector(ctx context.Context, tenantID, documentID string) error {
	if err := checkCapability(ctx, s); err != nil {
		return err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || documentID == "" {
		return runtimestorage.ErrInvalid
	}
	result, err := s.db.ExecContext(ctx, "DELETE FROM public.runtime_vector_index WHERE tenant_id=$1 AND document_id=$2", tenantID, documentID)
	if err != nil {
		return mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	count, _ := result.RowsAffected()
	if count == 0 {
		return runtimestorage.ErrNotFound
	}
	return nil
}

// PutObject implements the tenant-scoped runtime storage contract.
func (s *Store) PutObject(ctx context.Context, tenantID, objectKey string, content io.Reader, contentType string) (runtimestorage.ObjectInfo, error) {
	if err := checkCapability(ctx, s); err != nil {
		return runtimestorage.ObjectInfo{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || !runtimestorage.ValidateText(objectKey, 1024, true) || !runtimestorage.ValidateText(contentType, 256, false) || content == nil {
		return runtimestorage.ObjectInfo{}, runtimestorage.ErrInvalid
	}
	data, err := io.ReadAll(content)
	if err != nil {
		return runtimestorage.ObjectInfo{}, runtimestorage.ErrStorage
	}
	sum := sha256.Sum256(data)
	var value runtimestorage.ObjectInfo
	err = s.db.QueryRowContext(ctx, "INSERT INTO public.runtime_object (tenant_id,object_key,content_type,content,size,etag) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (tenant_id,object_key) DO UPDATE SET content_type=EXCLUDED.content_type,content=EXCLUDED.content,size=EXCLUDED.size,etag=EXCLUDED.etag,updated_at=now() RETURNING tenant_id,object_key,content_type,size,etag,created_at", tenantID, objectKey, contentType, data, len(data), hex.EncodeToString(sum[:])).Scan(&value.TenantID, &value.ObjectKey, &value.ContentType, &value.Size, &value.ETag, &value.CreatedAt)
	if err != nil {
		return runtimestorage.ObjectInfo{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	return value, nil
}

// GetObject implements the tenant-scoped runtime storage contract.
func (s *Store) GetObject(ctx context.Context, tenantID, objectKey string) (io.ReadCloser, runtimestorage.ObjectInfo, error) {
	if err := checkCapability(ctx, s); err != nil {
		return nil, runtimestorage.ObjectInfo{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || objectKey == "" {
		return nil, runtimestorage.ObjectInfo{}, runtimestorage.ErrInvalid
	}
	var value runtimestorage.ObjectInfo
	var data []byte
	err := s.db.QueryRowContext(ctx, "SELECT tenant_id,object_key,content_type,content,size,etag,created_at FROM public.runtime_object WHERE tenant_id=$1 AND object_key=$2", tenantID, objectKey).Scan(&value.TenantID, &value.ObjectKey, &value.ContentType, &data, &value.Size, &value.ETag, &value.CreatedAt)
	if err != nil {
		return nil, runtimestorage.ObjectInfo{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	return io.NopCloser(bytes.NewReader(data)), value, nil
}

// DeleteObject implements the tenant-scoped runtime storage contract.
func (s *Store) DeleteObject(ctx context.Context, tenantID, objectKey string) error {
	if err := checkCapability(ctx, s); err != nil {
		return err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || objectKey == "" {
		return runtimestorage.ErrInvalid
	}
	result, err := s.db.ExecContext(ctx, "DELETE FROM public.runtime_object WHERE tenant_id=$1 AND object_key=$2", tenantID, objectKey)
	if err != nil {
		return mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	count, _ := result.RowsAffected()
	if count == 0 {
		return runtimestorage.ErrNotFound
	}
	return nil
}

func nullTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

var _ runtimestorage.AuditStore = (*Store)(nil)
var _ runtimestorage.VectorStore = (*Store)(nil)
var _ runtimestorage.ObjectStore = (*Store)(nil)
