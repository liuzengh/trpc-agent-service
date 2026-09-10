package inmemory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"

	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/google/uuid"
)

// PutMemory implements the tenant-scoped runtime storage contract.
func (s *Store) PutMemory(ctx context.Context, input runtimestorage.MemoryInput) (runtimestorage.MemoryRecord, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.MemoryRecord{}, err
	}
	if runtimestorage.ValidateTenant(input.TenantID) != nil || !runtimestorage.ValidateText(input.UserID, 256, true) || !runtimestorage.ValidateText(input.Content, 0, true) || !runtimestorage.ValidateText(input.MemoryID, 256, false) || !runtimestorage.ValidateText(input.SessionID, 256, false) || !runtimestorage.ValidateEmbedding(input.Embedding) {
		return runtimestorage.MemoryRecord{}, runtimestorage.ErrInvalid
	}
	if input.Metadata != nil && cloneMap(input.Metadata) == nil {
		return runtimestorage.MemoryRecord{}, runtimestorage.ErrInvalid
	}
	if input.MemoryID == "" {
		input.MemoryID = "mem_" + uuid.NewString()
	}
	if input.Topics == nil {
		input.Topics = []string{}
	}
	if input.Metadata == nil {
		input.Metadata = map[string]any{}
	}
	if input.Embedding == nil {
		input.Embedding = []float64{}
	}
	now := time.Now().UTC()
	s.mu.Lock()
	k := key(input.TenantID, input.MemoryID)
	if existing, ok := s.memories[k]; ok {
		existing.Content, existing.Topics, existing.Metadata = input.Content, append([]string(nil), input.Topics...), cloneMap(input.Metadata)
		existing.Embedding = append([]float64(nil), input.Embedding...)
		existing.UserID, existing.SessionID, existing.Version, existing.UpdatedAt, existing.DeletedAt = input.UserID, input.SessionID, existing.Version+1, now, nil
		s.memories[k] = existing
		value := cloneMemory(existing)
		s.mu.Unlock()
		return value, s.enqueueIndex(ctx, existing)
	}
	value := runtimestorage.MemoryRecord{TenantID: input.TenantID, MemoryID: input.MemoryID, UserID: input.UserID, SessionID: input.SessionID, Content: input.Content, Topics: append([]string(nil), input.Topics...), Metadata: cloneMap(input.Metadata), Embedding: append([]float64(nil), input.Embedding...), Version: 1, CreatedAt: now, UpdatedAt: now}
	s.memories[k] = value
	result := cloneMemory(value)
	s.mu.Unlock()
	return result, s.enqueueIndex(ctx, value)
}

// GetMemory implements the tenant-scoped runtime storage contract.
func (s *Store) GetMemory(ctx context.Context, tenantID, memoryID string) (runtimestorage.MemoryRecord, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.MemoryRecord{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || memoryID == "" {
		return runtimestorage.MemoryRecord{}, runtimestorage.ErrInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.memories[key(tenantID, memoryID)]
	if !ok || value.DeletedAt != nil {
		return runtimestorage.MemoryRecord{}, runtimestorage.ErrNotFound
	}
	return cloneMemory(value), nil
}

// ListMemories implements the tenant-scoped runtime storage contract.
func (s *Store) ListMemories(ctx context.Context, tenantID, userID string, limit int) ([]runtimestorage.MemoryRecord, error) {
	if err := check(ctx); err != nil {
		return nil, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || strings.TrimSpace(userID) == "" || limit < 0 {
		return nil, runtimestorage.ErrInvalid
	}
	s.mu.RLock()
	values := make([]runtimestorage.MemoryRecord, 0)
	for _, value := range s.memories {
		if value.TenantID == tenantID && value.UserID == userID && value.DeletedAt == nil {
			values = append(values, cloneMemory(value))
		}
	}
	s.mu.RUnlock()
	sort.Slice(values, func(i, j int) bool {
		if values[i].UpdatedAt.Equal(values[j].UpdatedAt) {
			return values[i].MemoryID < values[j].MemoryID
		}
		return values[i].UpdatedAt.After(values[j].UpdatedAt)
	})
	if limit > 0 && len(values) > limit {
		values = values[:limit]
	}
	return values, nil
}

// SearchMemories implements the tenant-scoped runtime storage contract.
func (s *Store) SearchMemories(ctx context.Context, tenantID, userID, query string, limit int) ([]runtimestorage.MemorySearchResult, error) {
	if err := check(ctx); err != nil {
		return nil, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || strings.TrimSpace(userID) == "" || strings.TrimSpace(query) == "" || limit < 0 {
		return nil, runtimestorage.ErrInvalid
	}
	terms := strings.Fields(strings.ToLower(query))
	s.mu.RLock()
	values := make([]runtimestorage.MemorySearchResult, 0)
	for _, value := range s.memories {
		if value.TenantID != tenantID || value.UserID != userID || value.DeletedAt != nil {
			continue
		}
		hits := 0
		text := strings.ToLower(value.Content)
		for _, term := range terms {
			if strings.Contains(text, term) {
				hits++
			}
		}
		if hits > 0 {
			values = append(values, runtimestorage.MemorySearchResult{Memory: cloneMemory(value), Score: float64(hits) / float64(len(terms))})
		}
	}
	s.mu.RUnlock()
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
	if err := check(ctx); err != nil {
		return err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || memoryID == "" {
		return runtimestorage.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(tenantID, memoryID)
	value, ok := s.memories[k]
	if !ok || value.DeletedAt != nil {
		return runtimestorage.ErrNotFound
	}
	now := time.Now().UTC()
	value.DeletedAt, value.UpdatedAt, value.Version = &now, now, value.Version+1
	s.memories[k] = value
	delete(s.vectors, key(tenantID, string(runtimestorage.VectorSourceMemory), memoryID))
	return nil
}

// EnqueueMemoryIndex implements the tenant-scoped runtime storage contract.
func (s *Store) EnqueueMemoryIndex(ctx context.Context, value runtimestorage.MemoryRecord) error {
	if err := check(ctx); err != nil {
		return err
	}
	if runtimestorage.ValidateTenant(value.TenantID) != nil || value.MemoryID == "" || value.Version < 1 {
		return runtimestorage.ErrInvalid
	}
	s.mu.RLock()
	current, exists := s.memories[key(value.TenantID, value.MemoryID)]
	s.mu.RUnlock()
	if !exists || current.DeletedAt != nil {
		return runtimestorage.ErrNotFound
	}
	if current.Version != value.Version {
		return runtimestorage.ErrConflict
	}
	return s.enqueueIndex(ctx, current)
}

// WaitForMemoryIndex waits until the specified memory version is visible in
// the local vector index or the context is cancelled.
func (s *Store) WaitForMemoryIndex(ctx context.Context, tenantID, memoryID string, version int64) error {
	if err := check(ctx); err != nil {
		return err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || memoryID == "" || version < 1 {
		return runtimestorage.ErrInvalid
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		s.mu.RLock()
		indexed, ok := s.vectors[key(tenantID, string(runtimestorage.VectorSourceMemory), memoryID)]
		ready := ok && indexed.Version >= version
		s.mu.RUnlock()
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.indexDone:
			return runtimestorage.ErrStorage
		case <-ticker.C:
		}
	}
}

func (s *Store) enqueueIndex(ctx context.Context, value runtimestorage.MemoryRecord) error {
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()
	select {
	case <-s.indexDone:
		return runtimestorage.ErrStorage
	default:
	}
	select {
	case <-s.indexDone:
		return runtimestorage.ErrStorage
	case <-ctx.Done():
		return ctx.Err()
	case s.indexQueue <- cloneMemory(value):
		return nil
	}
}

func (s *Store) indexWorker() {
	for {
		select {
		case <-s.indexDone:
			return
		case value := <-s.indexQueue:
			if value.DeletedAt != nil {
				continue
			}
			s.mu.Lock()
			current, exists := s.memories[key(value.TenantID, value.MemoryID)]
			if exists && current.Version == value.Version && current.DeletedAt == nil {
				vectorKey := key(value.TenantID, string(runtimestorage.VectorSourceMemory), value.MemoryID)
				if len(current.Embedding) == 0 {
					delete(s.vectors, vectorKey)
				} else {
					s.vectors[vectorKey] = runtimestorage.VectorRecord{TenantID: current.TenantID, Source: runtimestorage.VectorSourceMemory, DocumentID: current.MemoryID, Content: current.Content, Metadata: cloneMap(current.Metadata), Embedding: append([]float64(nil), current.Embedding...), Version: current.Version, UpdatedAt: current.UpdatedAt}
				}
			}
			s.mu.Unlock()
		}
	}
}

// PutSummary implements the tenant-scoped runtime storage contract.
func (s *Store) PutSummary(ctx context.Context, value runtimestorage.SummaryRecord) (runtimestorage.SummaryRecord, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.SummaryRecord{}, err
	}
	if runtimestorage.ValidateSession(value.TenantID, value.SessionID) != nil || strings.TrimSpace(value.Text) == "" || value.EventSeq < 0 {
		return runtimestorage.SummaryRecord{}, runtimestorage.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.sessions[key(value.TenantID, value.SessionID)]; !ok {
		return runtimestorage.SummaryRecord{}, runtimestorage.ErrNotFound
	}
	k := key(value.TenantID, value.SessionID, value.FilterKey)
	if existing, ok := s.summaries[k]; ok {
		if value.EventSeq < existing.EventSeq {
			return runtimestorage.SummaryRecord{}, runtimestorage.ErrConflict
		}
		value.Version, value.CreatedAt = existing.Version+1, existing.CreatedAt
	} else {
		value.Version, value.CreatedAt = 1, time.Now().UTC()
	}
	value.UpdatedAt = time.Now().UTC()
	s.summaries[k] = value
	return value, nil
}

// GetSummary implements the tenant-scoped runtime storage contract.
func (s *Store) GetSummary(ctx context.Context, tenantID, sessionID, filterKey string) (runtimestorage.SummaryRecord, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.SummaryRecord{}, err
	}
	if runtimestorage.ValidateSession(tenantID, sessionID) != nil {
		return runtimestorage.SummaryRecord{}, runtimestorage.ErrInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.summaries[key(tenantID, sessionID, filterKey)]
	if !ok {
		return runtimestorage.SummaryRecord{}, runtimestorage.ErrNotFound
	}
	return value, nil
}

// EnqueueSummary implements the tenant-scoped runtime storage contract.
func (s *Store) EnqueueSummary(ctx context.Context, value runtimestorage.SummaryRecord) error {
	_, err := s.PutSummary(ctx, value)
	return err
}

// PutKnowledge implements the tenant-scoped runtime storage contract.
func (s *Store) PutKnowledge(ctx context.Context, value runtimestorage.KnowledgeDocument) (runtimestorage.KnowledgeDocument, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.KnowledgeDocument{}, err
	}
	if runtimestorage.ValidateTenant(value.TenantID) != nil || !runtimestorage.ValidateText(value.DocumentID, 256, true) || !runtimestorage.ValidateText(value.Content, 0, true) || !runtimestorage.ValidateText(value.Digest, 128, false) || !runtimestorage.ValidateEmbedding(value.Embedding) {
		return runtimestorage.KnowledgeDocument{}, runtimestorage.ErrInvalid
	}
	if value.Metadata != nil && cloneMap(value.Metadata) == nil {
		return runtimestorage.KnowledgeDocument{}, runtimestorage.ErrInvalid
	}
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(value.TenantID, value.DocumentID)
	if existing, ok := s.knowledge[k]; ok {
		value.Version, value.CreatedAt = existing.Version+1, existing.CreatedAt
	} else {
		value.Version, value.CreatedAt = 1, now
	}
	value.UpdatedAt = now
	if value.Digest == "" {
		sum := sha256.Sum256([]byte(value.Content))
		value.Digest = hex.EncodeToString(sum[:])
	}
	value.Metadata, value.Embedding = cloneMap(value.Metadata), append([]float64(nil), value.Embedding...)
	if value.Metadata == nil {
		value.Metadata = map[string]any{}
	}
	if value.Embedding == nil {
		value.Embedding = []float64{}
	}
	s.knowledge[k] = value
	if len(value.Embedding) > 0 {
		s.vectors[key(value.TenantID, string(runtimestorage.VectorSourceKnowledge), value.DocumentID)] = runtimestorage.VectorRecord{TenantID: value.TenantID, Source: runtimestorage.VectorSourceKnowledge, DocumentID: value.DocumentID, Content: value.Content, Metadata: cloneMap(value.Metadata), Embedding: append([]float64(nil), value.Embedding...), Version: value.Version, UpdatedAt: value.UpdatedAt}
	} else {
		delete(s.vectors, key(value.TenantID, string(runtimestorage.VectorSourceKnowledge), value.DocumentID))
	}
	return cloneKnowledge(value), nil
}

// GetKnowledge implements the tenant-scoped runtime storage contract.
func (s *Store) GetKnowledge(ctx context.Context, tenantID, documentID string) (runtimestorage.KnowledgeDocument, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.KnowledgeDocument{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || documentID == "" {
		return runtimestorage.KnowledgeDocument{}, runtimestorage.ErrInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.knowledge[key(tenantID, documentID)]
	if !ok {
		return runtimestorage.KnowledgeDocument{}, runtimestorage.ErrNotFound
	}
	return cloneKnowledge(value), nil
}

// SearchKnowledge implements the tenant-scoped runtime storage contract.
func (s *Store) SearchKnowledge(ctx context.Context, tenantID string, embedding []float64, limit int) ([]runtimestorage.KnowledgeSearchResult, error) {
	if err := check(ctx); err != nil {
		return nil, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || len(embedding) == 0 || limit < 0 {
		return nil, runtimestorage.ErrInvalid
	}
	s.mu.RLock()
	values := make([]runtimestorage.KnowledgeSearchResult, 0)
	for _, value := range s.knowledge {
		if value.TenantID != tenantID || len(value.Embedding) == 0 {
			continue
		}
		score, ok := cosine(embedding, value.Embedding)
		if ok {
			values = append(values, runtimestorage.KnowledgeSearchResult{Document: cloneKnowledge(value), Score: score})
		}
	}
	s.mu.RUnlock()
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
	if err := check(ctx); err != nil {
		return err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || documentID == "" {
		return runtimestorage.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(tenantID, documentID)
	if _, ok := s.knowledge[k]; !ok {
		return runtimestorage.ErrNotFound
	}
	delete(s.knowledge, k)
	delete(s.vectors, key(tenantID, string(runtimestorage.VectorSourceKnowledge), documentID))
	return nil
}

// PutArtifact implements the tenant-scoped runtime storage contract.
func (s *Store) PutArtifact(ctx context.Context, value runtimestorage.ArtifactRecord) (runtimestorage.ArtifactRecord, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.ArtifactRecord{}, err
	}
	if runtimestorage.ValidateTenant(value.TenantID) != nil || !runtimestorage.ValidateText(value.ArtifactID, 256, true) || !runtimestorage.ValidateText(value.SessionID, 256, false) || !runtimestorage.ValidateText(value.Name, 512, false) || !runtimestorage.ValidateText(value.MimeType, 256, false) || len(value.Content) == 0 {
		return runtimestorage.ArtifactRecord{}, runtimestorage.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(value.TenantID, value.ArtifactID)
	if existing, ok := s.artifacts[k]; ok {
		value.Version, value.CreatedAt = existing.Version+1, existing.CreatedAt
	} else {
		value.Version, value.CreatedAt = 1, time.Now().UTC()
	}
	value.UpdatedAt, value.Content = time.Now().UTC(), append([]byte(nil), value.Content...)
	s.artifacts[k] = value
	return cloneArtifact(value), nil
}

// GetArtifact implements the tenant-scoped runtime storage contract.
func (s *Store) GetArtifact(ctx context.Context, tenantID, artifactID string) (runtimestorage.ArtifactRecord, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.ArtifactRecord{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || artifactID == "" {
		return runtimestorage.ArtifactRecord{}, runtimestorage.ErrInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.artifacts[key(tenantID, artifactID)]
	if !ok {
		return runtimestorage.ArtifactRecord{}, runtimestorage.ErrNotFound
	}
	return cloneArtifact(value), nil
}

// ListArtifacts implements the tenant-scoped runtime storage contract.
func (s *Store) ListArtifacts(ctx context.Context, tenantID, sessionID string) ([]runtimestorage.ArtifactRecord, error) {
	if err := check(ctx); err != nil {
		return nil, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil {
		return nil, runtimestorage.ErrInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	values := make([]runtimestorage.ArtifactRecord, 0)
	for _, value := range s.artifacts {
		if value.TenantID == tenantID && (sessionID == "" || value.SessionID == sessionID) {
			values = append(values, cloneArtifact(value))
		}
	}
	sort.Slice(values, func(i, j int) bool { return values[i].ArtifactID < values[j].ArtifactID })
	return values, nil
}

// DeleteArtifact implements the tenant-scoped runtime storage contract.
func (s *Store) DeleteArtifact(ctx context.Context, tenantID, artifactID string) error {
	if err := check(ctx); err != nil {
		return err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || artifactID == "" {
		return runtimestorage.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(tenantID, artifactID)
	if _, ok := s.artifacts[k]; !ok {
		return runtimestorage.ErrNotFound
	}
	delete(s.artifacts, k)
	return nil
}

// AppendAudit implements the tenant-scoped runtime storage contract.
func (s *Store) AppendAudit(ctx context.Context, value runtimestorage.AuditRecord) (runtimestorage.AuditRecord, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.AuditRecord{}, err
	}
	if runtimestorage.ValidateTenant(value.TenantID) != nil || !runtimestorage.ValidateText(value.EventType, 128, true) || !runtimestorage.ValidateText(value.AuditID, 256, false) {
		return runtimestorage.AuditRecord{}, runtimestorage.ErrInvalid
	}
	if value.Payload != nil && cloneMap(value.Payload) == nil {
		return runtimestorage.AuditRecord{}, runtimestorage.ErrInvalid
	}
	if value.AuditID == "" {
		value.AuditID = uuid.NewString()
	}
	if value.OccurredAt.IsZero() {
		value.OccurredAt = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := s.audits[value.TenantID]
	for _, row := range rows {
		if row.AuditID == value.AuditID {
			if !reflect.DeepEqual(row.Payload, value.Payload) || row.EventType != value.EventType {
				return runtimestorage.AuditRecord{}, runtimestorage.ErrConflict
			}
			return cloneAudit(row), nil
		}
	}
	value.Payload = cloneMap(value.Payload)
	if value.Payload == nil {
		value.Payload = map[string]any{}
	}
	s.audits[value.TenantID] = append(rows, value)
	return cloneAudit(value), nil
}

// ListAudit implements the tenant-scoped runtime storage contract.
func (s *Store) ListAudit(ctx context.Context, tenantID string, since time.Time, limit int) ([]runtimestorage.AuditRecord, error) {
	if err := check(ctx); err != nil {
		return nil, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || limit < 0 {
		return nil, runtimestorage.ErrInvalid
	}
	s.mu.RLock()
	values := make([]runtimestorage.AuditRecord, 0)
	for _, row := range s.audits[tenantID] {
		if since.IsZero() || !row.OccurredAt.Before(since) {
			values = append(values, cloneAudit(row))
		}
	}
	s.mu.RUnlock()
	sort.Slice(values, func(i, j int) bool {
		if values[i].OccurredAt.Equal(values[j].OccurredAt) {
			return values[i].AuditID < values[j].AuditID
		}
		return values[i].OccurredAt.Before(values[j].OccurredAt)
	})
	if limit > 0 && len(values) > limit {
		values = values[:limit]
	}
	return values, nil
}

// UpsertVector implements the tenant-scoped runtime storage contract.
func (s *Store) UpsertVector(ctx context.Context, value runtimestorage.VectorRecord) error {
	if err := check(ctx); err != nil {
		return err
	}
	if runtimestorage.ValidateTenant(value.TenantID) != nil || !runtimestorage.ValidateText(value.DocumentID, 256, true) || len(value.Embedding) == 0 || !runtimestorage.ValidateEmbedding(value.Embedding) || !runtimestorage.ValidateText(string(value.Source), 128, false) {
		return runtimestorage.ErrInvalid
	}
	if value.Metadata != nil && cloneMap(value.Metadata) == nil {
		return runtimestorage.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	value.Embedding, value.Metadata = append([]float64(nil), value.Embedding...), cloneMap(value.Metadata)
	if value.Metadata == nil {
		value.Metadata = map[string]any{}
	}
	if value.Version < 1 {
		value.Version = 1
	}
	if value.UpdatedAt.IsZero() {
		value.UpdatedAt = time.Now().UTC()
	}
	if value.Source == "" {
		value.Source = runtimestorage.VectorSourceGeneric
	}
	if strings.TrimSpace(string(value.Source)) == "" {
		return runtimestorage.ErrInvalid
	}
	k := key(value.TenantID, string(value.Source), value.DocumentID)
	if existing, ok := s.vectors[k]; ok && existing.Version > value.Version {
		return runtimestorage.ErrConflict
	}
	s.vectors[k] = value
	return nil
}

// SearchVectors implements the tenant-scoped runtime storage contract.
func (s *Store) SearchVectors(ctx context.Context, tenantID string, embedding []float64, limit int) ([]runtimestorage.VectorSearchResult, error) {
	if err := check(ctx); err != nil {
		return nil, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || len(embedding) == 0 || limit < 0 {
		return nil, runtimestorage.ErrInvalid
	}
	s.mu.RLock()
	values := make([]runtimestorage.VectorSearchResult, 0)
	for _, value := range s.vectors {
		if value.TenantID != tenantID {
			continue
		}
		score, ok := cosine(embedding, value.Embedding)
		if ok {
			values = append(values, runtimestorage.VectorSearchResult{Record: cloneVector(value), Score: score})
		}
	}
	s.mu.RUnlock()
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
	if err := check(ctx); err != nil {
		return err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || documentID == "" {
		return runtimestorage.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	found := false
	for k, value := range s.vectors {
		if value.TenantID == tenantID && value.DocumentID == documentID {
			delete(s.vectors, k)
			found = true
		}
	}
	if !found {
		return runtimestorage.ErrNotFound
	}
	return nil
}

// PutObject implements the tenant-scoped runtime storage contract.
func (s *Store) PutObject(ctx context.Context, tenantID, objectKey string, content io.Reader, contentType string) (runtimestorage.ObjectInfo, error) {
	if err := check(ctx); err != nil {
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
	value := runtimestorage.ObjectInfo{TenantID: tenantID, ObjectKey: objectKey, ContentType: contentType, Size: int64(len(data)), ETag: hex.EncodeToString(sum[:]), CreatedAt: time.Now().UTC()}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(tenantID, objectKey)
	if existing, ok := s.objects[k]; ok {
		value.CreatedAt = existing.CreatedAt
	}
	s.objectData[k], s.objects[k] = append([]byte(nil), data...), value
	return value, nil
}

// GetObject implements the tenant-scoped runtime storage contract.
func (s *Store) GetObject(ctx context.Context, tenantID, objectKey string) (io.ReadCloser, runtimestorage.ObjectInfo, error) {
	if err := check(ctx); err != nil {
		return nil, runtimestorage.ObjectInfo{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || objectKey == "" {
		return nil, runtimestorage.ObjectInfo{}, runtimestorage.ErrInvalid
	}
	s.mu.RLock()
	value, ok := s.objects[key(tenantID, objectKey)]
	data := append([]byte(nil), s.objectData[key(tenantID, objectKey)]...)
	s.mu.RUnlock()
	if !ok {
		return nil, runtimestorage.ObjectInfo{}, runtimestorage.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), value, nil
}

// DeleteObject implements the tenant-scoped runtime storage contract.
func (s *Store) DeleteObject(ctx context.Context, tenantID, objectKey string) error {
	if err := check(ctx); err != nil {
		return err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || objectKey == "" {
		return runtimestorage.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(tenantID, objectKey)
	if _, ok := s.objects[k]; !ok {
		return runtimestorage.ErrNotFound
	}
	delete(s.objects, k)
	delete(s.objectData, k)
	return nil
}

func cosine(left, right []float64) (float64, bool) {
	if len(left) == 0 || len(left) != len(right) {
		return 0, false
	}
	var dot, nl, nr float64
	for i := range left {
		dot, nl, nr = dot+left[i]*right[i], nl+left[i]*left[i], nr+right[i]*right[i]
	}
	if nl == 0 || nr == 0 {
		return 0, false
	}
	return dot / (math.Sqrt(nl) * math.Sqrt(nr)), true
}
func cloneMemory(value runtimestorage.MemoryRecord) runtimestorage.MemoryRecord {
	value.Topics, value.Metadata, value.Embedding = append([]string(nil), value.Topics...), cloneMap(value.Metadata), append([]float64(nil), value.Embedding...)
	if value.DeletedAt != nil {
		copy := *value.DeletedAt
		value.DeletedAt = &copy
	}
	return value
}
func cloneKnowledge(value runtimestorage.KnowledgeDocument) runtimestorage.KnowledgeDocument {
	value.Metadata, value.Embedding = cloneMap(value.Metadata), append([]float64(nil), value.Embedding...)
	return value
}
func cloneArtifact(value runtimestorage.ArtifactRecord) runtimestorage.ArtifactRecord {
	value.Content = append([]byte(nil), value.Content...)
	return value
}
func cloneAudit(value runtimestorage.AuditRecord) runtimestorage.AuditRecord {
	value.Payload = cloneMap(value.Payload)
	return value
}
func cloneVector(value runtimestorage.VectorRecord) runtimestorage.VectorRecord {
	value.Metadata, value.Embedding = cloneMap(value.Metadata), append([]float64(nil), value.Embedding...)
	return value
}

var _ runtimestorage.MemoryStore = (*Store)(nil)
var _ runtimestorage.SummaryStore = (*Store)(nil)
var _ runtimestorage.KnowledgeStore = (*Store)(nil)
var _ runtimestorage.ArtifactStore = (*Store)(nil)
var _ runtimestorage.AuditStore = (*Store)(nil)
var _ runtimestorage.VectorStore = (*Store)(nil)
var _ runtimestorage.ObjectStore = (*Store)(nil)
