// Package postgres implements the Knowledge ingestion/version-publish
// authority over the durable schema.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/knowledge"
)

type Store struct{ db *sql.DB }

func New(db *sql.DB) *Store { return &Store{db: db} }

func (s *Store) BeginManifest(ctx context.Context, in knowledge.BeginManifestInput) (knowledge.Manifest, error) {
	if s == nil || s.db == nil {
		return knowledge.Manifest{}, runtime.ErrBackendUnavailable
	}
	if in.TenantID == "" || in.KnowledgeID == "" || in.Version < 1 || in.SourceURI == "" ||
		!validDigest(in.SourceDigest) || in.ChunkingPipelineVersion == "" || in.EmbedderProfileID == "" ||
		in.EmbedderVersion < 1 || in.VectorCollectionGeneration == "" || !validSchema(in.MetadataSchema) || in.ContentWatermark == "" || in.CreatedAt.IsZero() {
		return knowledge.Manifest{}, runtime.ErrInvariantViolation
	}
	schema, err := json.Marshal(in.MetadataSchema)
	if err != nil {
		return knowledge.Manifest{}, runtime.ErrInvariantViolation
	}
	if in.MetadataSchema == nil {
		schema = []byte("[]")
	}
	_, err = s.db.ExecContext(ctx, `SELECT public.begin_knowledge_manifest($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12)`,
		in.TenantID, in.KnowledgeID, in.Version, in.SourceURI, in.SourceDigest, in.ChunkingPipelineVersion,
		in.EmbedderProfileID, in.EmbedderVersion, in.VectorCollectionGeneration, string(schema), in.ContentWatermark,
		in.CreatedAt.UTC().Truncate(time.Microsecond))
	if err != nil {
		return knowledge.Manifest{}, mapError(err)
	}
	return s.GetManifest(ctx, in.TenantID, in.KnowledgeID, in.Version)
}

func (s *Store) StageChunk(ctx context.Context, in knowledge.ChunkRecord) (knowledge.ChunkRecord, error) {
	if s == nil || s.db == nil {
		return knowledge.ChunkRecord{}, runtime.ErrBackendUnavailable
	}
	if err := validateChunk(in); err != nil {
		return knowledge.ChunkRecord{}, err
	}
	mutationDigest, err := in.MutationDigest()
	if err != nil {
		return knowledge.ChunkRecord{}, err
	}
	metadata, err := json.Marshal(in.Metadata)
	if err != nil {
		return knowledge.ChunkRecord{}, runtime.ErrInvariantViolation
	}
	vector, err := json.Marshal(in.Vector)
	if err != nil {
		return knowledge.ChunkRecord{}, runtime.ErrInvariantViolation
	}
	_, err = s.db.ExecContext(ctx, `SELECT public.stage_knowledge_chunk($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13::jsonb,$14,$15)`,
		in.TenantID, in.KnowledgeID, in.KnowledgeVersion, in.ChunkID, in.SourceDigest, in.ContentDigest,
		in.MetadataDigest, in.EmbeddingProfileID, in.EmbeddingVersion, in.VectorGeneration, in.Content,
		string(metadata), string(vector), mutationDigest, in.CreatedAt.UTC().Truncate(time.Microsecond))
	if err != nil {
		return knowledge.ChunkRecord{}, mapError(err)
	}
	in.IndexedAt = time.Time{}
	return in, nil
}

func (s *Store) BeginIndexing(ctx context.Context, tenantID, knowledgeID string, version, chunkTotal int64, at time.Time) (knowledge.Manifest, error) {
	if s == nil || s.db == nil {
		return knowledge.Manifest{}, runtime.ErrBackendUnavailable
	}
	if tenantID == "" || knowledgeID == "" || version < 1 || chunkTotal < 1 || at.IsZero() {
		return knowledge.Manifest{}, runtime.ErrInvariantViolation
	}
	if _, err := s.db.ExecContext(ctx, `SELECT public.begin_knowledge_indexing($1,$2,$3,$4,$5)`,
		tenantID, knowledgeID, version, chunkTotal, at.UTC().Truncate(time.Microsecond)); err != nil {
		return knowledge.Manifest{}, mapError(err)
	}
	return s.GetManifest(ctx, tenantID, knowledgeID, version)
}

func (s *Store) MarkChunkIndexed(ctx context.Context, tenantID, knowledgeID string, version int64, chunkID string, at time.Time) error {
	if s == nil || s.db == nil {
		return runtime.ErrBackendUnavailable
	}
	if tenantID == "" || knowledgeID == "" || version < 1 || chunkID == "" || at.IsZero() {
		return runtime.ErrInvariantViolation
	}
	_, err := s.db.ExecContext(ctx, `SELECT public.mark_knowledge_chunk_indexed($1,$2,$3,$4,$5)`,
		tenantID, knowledgeID, version, chunkID, at.UTC().Truncate(time.Microsecond))
	return mapError(err)
}

func (s *Store) BeginVerifying(ctx context.Context, tenantID, knowledgeID string, version int64, verificationDigest string, at time.Time) (knowledge.Manifest, error) {
	if s == nil || s.db == nil {
		return knowledge.Manifest{}, runtime.ErrBackendUnavailable
	}
	if tenantID == "" || knowledgeID == "" || version < 1 || !validDigest(verificationDigest) || at.IsZero() {
		return knowledge.Manifest{}, runtime.ErrInvariantViolation
	}
	if _, err := s.db.ExecContext(ctx, `SELECT public.begin_knowledge_verifying($1,$2,$3,$4,$5)`,
		tenantID, knowledgeID, version, verificationDigest, at.UTC().Truncate(time.Microsecond)); err != nil {
		return knowledge.Manifest{}, mapError(err)
	}
	return s.GetManifest(ctx, tenantID, knowledgeID, version)
}

func (s *Store) RecordProbe(ctx context.Context, in knowledge.ProbeRecord) (knowledge.ProbeRecord, error) {
	if s == nil || s.db == nil {
		return knowledge.ProbeRecord{}, runtime.ErrBackendUnavailable
	}
	if in.TenantID == "" || in.KnowledgeID == "" || in.KnowledgeVersion < 1 || in.ProbeID == "" || in.Query == "" ||
		len(in.ExpectedChunks) == 0 || in.MinRecallPPM < 1 || in.MinRecallPPM > 1_000_000 || in.CreatedAt.IsZero() {
		return knowledge.ProbeRecord{}, runtime.ErrInvariantViolation
	}
	expected, err := json.Marshal(in.ExpectedChunks)
	if err != nil {
		return knowledge.ProbeRecord{}, runtime.ErrInvariantViolation
	}
	_, err = s.db.ExecContext(ctx, `SELECT public.record_knowledge_probe($1,$2,$3,$4,$5,$6::jsonb,$7,$8)`,
		in.TenantID, in.KnowledgeID, in.KnowledgeVersion, in.ProbeID, in.Query, string(expected), in.MinRecallPPM,
		in.CreatedAt.UTC().Truncate(time.Microsecond))
	if err != nil {
		return knowledge.ProbeRecord{}, mapError(err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT verified FROM public.knowledge_probe
WHERE tenant_id=$1 AND knowledge_id=$2 AND knowledge_version=$3 AND probe_id=$4`,
		in.TenantID, in.KnowledgeID, in.KnowledgeVersion, in.ProbeID).Scan(&in.Verified); err != nil {
		return knowledge.ProbeRecord{}, mapError(err)
	}
	return in, nil
}

func (s *Store) MarkProbeVerified(ctx context.Context, tenantID, knowledgeID string, version int64, probeID string) error {
	if s == nil || s.db == nil {
		return runtime.ErrBackendUnavailable
	}
	if tenantID == "" || knowledgeID == "" || version < 1 || probeID == "" {
		return runtime.ErrInvariantViolation
	}
	_, err := s.db.ExecContext(ctx, `SELECT public.mark_knowledge_probe_verified($1,$2,$3,$4)`,
		tenantID, knowledgeID, version, probeID)
	return mapError(err)
}

func (s *Store) PublishVersion(ctx context.Context, tenantID, knowledgeID string, version int64, at time.Time) (knowledge.Manifest, error) {
	if s == nil || s.db == nil {
		return knowledge.Manifest{}, runtime.ErrBackendUnavailable
	}
	if tenantID == "" || knowledgeID == "" || version < 1 || at.IsZero() {
		return knowledge.Manifest{}, runtime.ErrInvariantViolation
	}
	if _, err := s.db.ExecContext(ctx, `SELECT public.publish_knowledge_version($1,$2,$3,$4)`,
		tenantID, knowledgeID, version, at.UTC().Truncate(time.Microsecond)); err != nil {
		return knowledge.Manifest{}, mapError(err)
	}
	return s.GetManifest(ctx, tenantID, knowledgeID, version)
}

func (s *Store) FailVersion(ctx context.Context, tenantID, knowledgeID string, version int64, at time.Time) (knowledge.Manifest, error) {
	if s == nil || s.db == nil {
		return knowledge.Manifest{}, runtime.ErrBackendUnavailable
	}
	if tenantID == "" || knowledgeID == "" || version < 1 || at.IsZero() {
		return knowledge.Manifest{}, runtime.ErrInvariantViolation
	}
	if _, err := s.db.ExecContext(ctx, `SELECT public.fail_knowledge_version($1,$2,$3,$4)`,
		tenantID, knowledgeID, version, at.UTC().Truncate(time.Microsecond)); err != nil {
		return knowledge.Manifest{}, mapError(err)
	}
	return s.GetManifest(ctx, tenantID, knowledgeID, version)
}

func (s *Store) GetManifest(ctx context.Context, tenantID, knowledgeID string, version int64) (knowledge.Manifest, error) {
	if s == nil || s.db == nil {
		return knowledge.Manifest{}, runtime.ErrBackendUnavailable
	}
	if tenantID == "" || knowledgeID == "" || version < 1 {
		return knowledge.Manifest{}, runtime.ErrInvariantViolation
	}
	var value knowledge.Manifest
	var metadataSchema []byte
	var chunkTotal sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT tenant_id,knowledge_id,version,source_uri,source_digest,
chunking_pipeline_version,embedder_profile_id,embedder_version,vector_collection_generation,metadata_schema,
content_watermark,state,chunk_total,verification_digest,created_at,updated_at,record_version
FROM public.knowledge_manifest WHERE tenant_id=$1 AND knowledge_id=$2 AND version=$3`, tenantID, knowledgeID, version).
		Scan(&value.TenantID, &value.KnowledgeID, &value.Version, &value.SourceURI, &value.SourceDigest,
			&value.ChunkingPipelineVersion, &value.EmbedderProfileID, &value.EmbedderVersion,
			&value.VectorCollectionGeneration, &metadataSchema, &value.ContentWatermark, &value.State,
			&chunkTotal, &value.VerificationDigest, &value.CreatedAt, &value.UpdatedAt, &value.RecordVersion); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return knowledge.Manifest{}, runtime.ErrNotFound
		}
		return knowledge.Manifest{}, mapError(err)
	}
	if chunkTotal.Valid {
		value.ChunkTotal = chunkTotal.Int64
	}
	if err := json.Unmarshal(metadataSchema, &value.MetadataSchema); err != nil {
		return knowledge.Manifest{}, runtime.ErrInvariantViolation
	}
	value.CreatedAt = value.CreatedAt.UTC()
	value.UpdatedAt = value.UpdatedAt.UTC()
	return value, nil
}

func validateChunk(in knowledge.ChunkRecord) error {
	if in.TenantID == "" || in.KnowledgeID == "" || in.KnowledgeVersion < 1 || in.ChunkID == "" ||
		!validDigest(in.SourceDigest) || !validDigest(in.ContentDigest) || !validDigest(in.MetadataDigest) ||
		in.EmbeddingProfileID == "" || in.EmbeddingVersion < 1 || in.VectorGeneration == "" ||
		in.Content == "" || len(in.Vector) == 0 || in.CreatedAt.IsZero() {
		return runtime.ErrInvariantViolation
	}
	return nil
}

func validDigest(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func validSchema(values []string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return false
		}
		if _, ok := seen[value]; ok {
			return false
		}
		seen[value] = struct{}{}
	}
	return true
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			return runtime.ErrIdempotencyCollision
		case "40001", "23514":
			return runtime.ErrVersionConflict
		case "22023", "23503", "55000", "23000":
			return runtime.ErrInvariantViolation
		case "P0002":
			return runtime.ErrNotFound
		}
	}
	return err
}

var _ knowledge.IngestionStore = (*Store)(nil)
