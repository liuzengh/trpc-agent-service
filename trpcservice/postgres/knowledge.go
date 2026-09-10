package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	platformknowledge "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge"
)

var _ platformknowledge.Catalog = (*Store)(nil)

// AvailableKnowledgeChunk reports whether a Qdrant result still has an
// available document, chunk, active base, and immutable config binding.
func (s *Store) AvailableKnowledgeChunk(ctx context.Context, ref platformknowledge.ChunkRef) (bool, error) {
	if err := s.validate(); err != nil {
		return false, err
	}
	if err := ref.Validate(); err != nil {
		return false, err
	}
	version, err := strconv.Atoi(ref.DocumentVersion)
	if err != nil || version < 0 {
		return false, errors.New("document version is invalid")
	}
	var available bool
	err = s.pool.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1
    FROM platform.knowledge_chunk AS chunk
    JOIN platform.knowledge_document AS document
      ON document.tenant_id = chunk.tenant_id
     AND document.app_id = chunk.app_id
     AND document.knowledge_base_id = chunk.knowledge_base_id
     AND document.document_id = chunk.document_id
     AND document.version = chunk.document_version
    JOIN platform.knowledge_base AS base
      ON base.tenant_id = chunk.tenant_id
     AND base.app_id = chunk.app_id
     AND base.knowledge_base_id = chunk.knowledge_base_id
    JOIN platform.app_config_version AS config
      ON config.tenant_id = chunk.tenant_id
     AND config.app_id = chunk.app_id
     AND config.version = $3
     AND config.status = 'PUBLISHED'
     AND config.knowledge_base_ids @> jsonb_build_array(chunk.knowledge_base_id)
    WHERE chunk.tenant_id = $1
      AND chunk.app_id = $2
      AND chunk.knowledge_base_id = $4
      AND chunk.document_id = $5
      AND chunk.document_version = $6
      AND chunk.index_generation = $7
      AND chunk.chunk_id = $8
      AND chunk.status = 'AVAILABLE'
      AND document.status = 'AVAILABLE'
      AND base.status = 'ACTIVE'
)`,
		ref.Scope.TenantID,
		ref.Scope.AppID,
		ref.ConfigVersion,
		ref.KnowledgeBaseID,
		ref.DocumentID,
		version,
		ref.IndexGeneration,
		ref.ChunkID,
	).Scan(&available)
	if err != nil {
		return false, fmt.Errorf("check available knowledge chunk: %w", err)
	}
	return available, nil
}
