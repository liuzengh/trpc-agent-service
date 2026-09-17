// Package inmemory implements the Knowledge ingestion authority for contracts.
package inmemory

import (
	"context"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/knowledge"
)

type Store struct {
	mu        sync.Mutex
	manifests map[string]knowledge.Manifest
	chunks    map[string]knowledge.ChunkRecord
	probes    map[string]knowledge.ProbeRecord
}

func New() *Store {
	return &Store{
		manifests: make(map[string]knowledge.Manifest),
		chunks:    make(map[string]knowledge.ChunkRecord),
		probes:    make(map[string]knowledge.ProbeRecord),
	}
}

func manifestKey(tenantID, knowledgeID string, version int64) string {
	return tenantID + "\x00" + knowledgeID + "\x00" + strconv.FormatInt(version, 10)
}

func chunkKey(tenantID, knowledgeID string, version int64, chunkID string) string {
	return tenantID + "\x00" + knowledgeID + "\x00" + strconv.FormatInt(version, 10) + "\x00" + chunkID
}

func probeKey(tenantID, knowledgeID string, version int64, probeID string) string {
	return tenantID + "\x00" + knowledgeID + "\x00" + strconv.FormatInt(version, 10) + "\x00" + probeID
}

func (s *Store) BeginManifest(_ context.Context, in knowledge.BeginManifestInput) (knowledge.Manifest, error) {
	if in.TenantID == "" || in.KnowledgeID == "" || in.Version < 1 || in.SourceURI == "" ||
		!validDigest(in.SourceDigest) || in.ChunkingPipelineVersion == "" || in.EmbedderProfileID == "" ||
		in.EmbedderVersion < 1 || in.VectorCollectionGeneration == "" || !validSchema(in.MetadataSchema) || in.ContentWatermark == "" || in.CreatedAt.IsZero() {
		return knowledge.Manifest{}, runtime.ErrInvariantViolation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := manifestKey(in.TenantID, in.KnowledgeID, in.Version)
	if existing, ok := s.manifests[key]; ok {
		if existing.SourceURI != in.SourceURI || existing.SourceDigest != in.SourceDigest ||
			existing.ChunkingPipelineVersion != in.ChunkingPipelineVersion ||
			existing.EmbedderProfileID != in.EmbedderProfileID || existing.EmbedderVersion != in.EmbedderVersion ||
			existing.VectorCollectionGeneration != in.VectorCollectionGeneration ||
			!reflect.DeepEqual(existing.MetadataSchema, in.MetadataSchema) || existing.ContentWatermark != in.ContentWatermark || !existing.CreatedAt.Equal(in.CreatedAt.UTC()) {
			return knowledge.Manifest{}, runtime.ErrIdempotencyCollision
		}
		return cloneManifest(existing), nil
	}
	value := knowledge.Manifest{
		TenantID: in.TenantID, KnowledgeID: in.KnowledgeID, Version: in.Version,
		SourceURI: in.SourceURI, SourceDigest: in.SourceDigest,
		ChunkingPipelineVersion: in.ChunkingPipelineVersion, EmbedderProfileID: in.EmbedderProfileID,
		EmbedderVersion: in.EmbedderVersion, VectorCollectionGeneration: in.VectorCollectionGeneration,
		MetadataSchema: cloneStrings(in.MetadataSchema), ContentWatermark: in.ContentWatermark,
		State: knowledge.ManifestStaging, CreatedAt: in.CreatedAt.UTC().Truncate(time.Microsecond),
		UpdatedAt: in.CreatedAt.UTC().Truncate(time.Microsecond), RecordVersion: 1,
	}
	s.manifests[key] = value
	return cloneManifest(value), nil
}

func (s *Store) StageChunk(_ context.Context, in knowledge.ChunkRecord) (knowledge.ChunkRecord, error) {
	if err := validateChunk(in); err != nil {
		return knowledge.ChunkRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	mkey := manifestKey(in.TenantID, in.KnowledgeID, in.KnowledgeVersion)
	manifest, ok := s.manifests[mkey]
	if !ok {
		return knowledge.ChunkRecord{}, runtime.ErrNotFound
	}
	if manifest.State != knowledge.ManifestStaging {
		return knowledge.ChunkRecord{}, runtime.ErrVersionConflict
	}
	if in.SourceDigest != manifest.SourceDigest || in.EmbeddingProfileID != manifest.EmbedderProfileID ||
		in.EmbeddingVersion != manifest.EmbedderVersion || in.VectorGeneration != manifest.VectorCollectionGeneration ||
		!metadataAllowed(in.Metadata, manifest.MetadataSchema) {
		return knowledge.ChunkRecord{}, runtime.ErrInvariantViolation
	}
	key := chunkKey(in.TenantID, in.KnowledgeID, in.KnowledgeVersion, in.ChunkID)
	if existing, ok := s.chunks[key]; ok {
		if !sameChunk(existing, in) {
			return knowledge.ChunkRecord{}, runtime.ErrIdempotencyCollision
		}
		return cloneChunk(existing), nil
	}
	value := cloneChunk(in)
	value.CreatedAt = value.CreatedAt.UTC().Truncate(time.Microsecond)
	value.IndexedAt = time.Time{}
	s.chunks[key] = value
	return cloneChunk(value), nil
}

func (s *Store) BeginIndexing(_ context.Context, tenantID, knowledgeID string, version, chunkTotal int64, at time.Time) (knowledge.Manifest, error) {
	if tenantID == "" || knowledgeID == "" || version < 1 || chunkTotal < 1 || at.IsZero() {
		return knowledge.Manifest{}, runtime.ErrInvariantViolation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := manifestKey(tenantID, knowledgeID, version)
	value, ok := s.manifests[key]
	if !ok {
		return knowledge.Manifest{}, runtime.ErrNotFound
	}
	if value.State != knowledge.ManifestStaging {
		return knowledge.Manifest{}, runtime.ErrVersionConflict
	}
	if countChunks(s.chunks, tenantID, knowledgeID, version) != chunkTotal {
		return knowledge.Manifest{}, runtime.ErrVersionConflict
	}
	value.State = knowledge.ManifestIndexing
	value.ChunkTotal = chunkTotal
	value.UpdatedAt = at.UTC()
	value.RecordVersion++
	s.manifests[key] = value
	return cloneManifest(value), nil
}

func (s *Store) MarkChunkIndexed(_ context.Context, tenantID, knowledgeID string, version int64, chunkID string, at time.Time) error {
	if tenantID == "" || knowledgeID == "" || version < 1 || chunkID == "" || at.IsZero() {
		return runtime.ErrInvariantViolation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	manifest, ok := s.manifests[manifestKey(tenantID, knowledgeID, version)]
	if !ok {
		return runtime.ErrNotFound
	}
	if manifest.State != knowledge.ManifestIndexing {
		return runtime.ErrVersionConflict
	}
	key := chunkKey(tenantID, knowledgeID, version, chunkID)
	value, ok := s.chunks[key]
	if !ok {
		return runtime.ErrNotFound
	}
	if value.IndexedAt.IsZero() {
		value.IndexedAt = at.UTC()
	}
	s.chunks[key] = value
	return nil
}

func (s *Store) BeginVerifying(_ context.Context, tenantID, knowledgeID string, version int64, verificationDigest string, at time.Time) (knowledge.Manifest, error) {
	if tenantID == "" || knowledgeID == "" || version < 1 || !validDigest(verificationDigest) || at.IsZero() {
		return knowledge.Manifest{}, runtime.ErrInvariantViolation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := manifestKey(tenantID, knowledgeID, version)
	value, ok := s.manifests[key]
	if !ok {
		return knowledge.Manifest{}, runtime.ErrNotFound
	}
	if value.State != knowledge.ManifestIndexing {
		return knowledge.Manifest{}, runtime.ErrVersionConflict
	}
	if indexedChunks(s.chunks, tenantID, knowledgeID, version) != value.ChunkTotal {
		return knowledge.Manifest{}, runtime.ErrVersionConflict
	}
	computed, err := knowledge.VerificationDigest(chunksForScope(s.chunks, tenantID, knowledgeID, version))
	if err != nil || computed != verificationDigest {
		return knowledge.Manifest{}, runtime.ErrVersionConflict
	}
	value.State = knowledge.ManifestVerifying
	value.VerificationDigest = verificationDigest
	value.UpdatedAt = at.UTC()
	value.RecordVersion++
	s.manifests[key] = value
	return cloneManifest(value), nil
}

func (s *Store) RecordProbe(_ context.Context, in knowledge.ProbeRecord) (knowledge.ProbeRecord, error) {
	if in.TenantID == "" || in.KnowledgeID == "" || in.KnowledgeVersion < 1 || in.ProbeID == "" || in.Query == "" ||
		len(in.ExpectedChunks) == 0 || in.MinRecallPPM < 1 || in.MinRecallPPM > 1_000_000 || in.CreatedAt.IsZero() {
		return knowledge.ProbeRecord{}, runtime.ErrInvariantViolation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	manifest, ok := s.manifests[manifestKey(in.TenantID, in.KnowledgeID, in.KnowledgeVersion)]
	if !ok {
		return knowledge.ProbeRecord{}, runtime.ErrNotFound
	}
	if manifest.State != knowledge.ManifestVerifying || !validExpectedChunks(s.chunks, in) {
		return knowledge.ProbeRecord{}, runtime.ErrVersionConflict
	}
	key := probeKey(in.TenantID, in.KnowledgeID, in.KnowledgeVersion, in.ProbeID)
	if existing, ok := s.probes[key]; ok {
		if existing.Query != in.Query || existing.MinRecallPPM != in.MinRecallPPM || !existing.CreatedAt.Equal(in.CreatedAt.UTC().Truncate(time.Microsecond)) ||
			!equalStrings(existing.ExpectedChunks, in.ExpectedChunks) {
			return knowledge.ProbeRecord{}, runtime.ErrIdempotencyCollision
		}
		return cloneProbe(existing), nil
	}
	value := cloneProbe(in)
	value.CreatedAt = value.CreatedAt.UTC().Truncate(time.Microsecond)
	value.Verified = false
	s.probes[key] = value
	return cloneProbe(value), nil
}

func (s *Store) MarkProbeVerified(_ context.Context, tenantID, knowledgeID string, version int64, probeID string) error {
	if tenantID == "" || knowledgeID == "" || version < 1 || probeID == "" {
		return runtime.ErrInvariantViolation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	manifest, ok := s.manifests[manifestKey(tenantID, knowledgeID, version)]
	if !ok {
		return runtime.ErrNotFound
	}
	if manifest.State != knowledge.ManifestVerifying {
		return runtime.ErrVersionConflict
	}
	key := probeKey(tenantID, knowledgeID, version, probeID)
	value, ok := s.probes[key]
	if !ok {
		return runtime.ErrNotFound
	}
	value.Verified = true
	s.probes[key] = value
	return nil
}

func (s *Store) PublishVersion(_ context.Context, tenantID, knowledgeID string, version int64, at time.Time) (knowledge.Manifest, error) {
	if tenantID == "" || knowledgeID == "" || version < 1 || at.IsZero() {
		return knowledge.Manifest{}, runtime.ErrInvariantViolation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := manifestKey(tenantID, knowledgeID, version)
	value, ok := s.manifests[key]
	if !ok {
		return knowledge.Manifest{}, runtime.ErrNotFound
	}
	if value.State != knowledge.ManifestVerifying {
		return knowledge.Manifest{}, runtime.ErrVersionConflict
	}
	if countProbes(s.probes, tenantID, knowledgeID, version) == 0 || unverifiedProbes(s.probes, tenantID, knowledgeID, version) > 0 {
		return knowledge.Manifest{}, runtime.ErrVersionConflict
	}
	if indexedChunks(s.chunks, tenantID, knowledgeID, version) != value.ChunkTotal {
		return knowledge.Manifest{}, runtime.ErrVersionConflict
	}
	computed, err := knowledge.VerificationDigest(chunksForScope(s.chunks, tenantID, knowledgeID, version))
	if err != nil || computed != value.VerificationDigest {
		return knowledge.Manifest{}, runtime.ErrVersionConflict
	}
	value.State = knowledge.ManifestPublished
	value.UpdatedAt = at.UTC()
	value.RecordVersion++
	s.manifests[key] = value
	return cloneManifest(value), nil
}

func (s *Store) FailVersion(_ context.Context, tenantID, knowledgeID string, version int64, at time.Time) (knowledge.Manifest, error) {
	if tenantID == "" || knowledgeID == "" || version < 1 || at.IsZero() {
		return knowledge.Manifest{}, runtime.ErrInvariantViolation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := manifestKey(tenantID, knowledgeID, version)
	value, ok := s.manifests[key]
	if !ok {
		return knowledge.Manifest{}, runtime.ErrNotFound
	}
	if value.State != knowledge.ManifestStaging && value.State != knowledge.ManifestIndexing && value.State != knowledge.ManifestVerifying {
		return knowledge.Manifest{}, runtime.ErrVersionConflict
	}
	value.State = knowledge.ManifestFailed
	value.UpdatedAt = at.UTC()
	value.RecordVersion++
	s.manifests[key] = value
	return cloneManifest(value), nil
}

func (s *Store) GetManifest(_ context.Context, tenantID, knowledgeID string, version int64) (knowledge.Manifest, error) {
	if tenantID == "" || knowledgeID == "" || version < 1 {
		return knowledge.Manifest{}, runtime.ErrInvariantViolation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := manifestKey(tenantID, knowledgeID, version)
	value, ok := s.manifests[key]
	if !ok {
		return knowledge.Manifest{}, runtime.ErrNotFound
	}
	return cloneManifest(value), nil
}

func countChunks(chunks map[string]knowledge.ChunkRecord, tenantID, knowledgeID string, version int64) int64 {
	var count int64
	prefix := tenantID + "\x00" + knowledgeID + "\x00" + strconv.FormatInt(version, 10) + "\x00"
	for key := range chunks {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			count++
		}
	}
	return count
}

func indexedChunks(chunks map[string]knowledge.ChunkRecord, tenantID, knowledgeID string, version int64) int64 {
	var count int64
	prefix := tenantID + "\x00" + knowledgeID + "\x00" + strconv.FormatInt(version, 10) + "\x00"
	for key, value := range chunks {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix && !value.IndexedAt.IsZero() {
			count++
		}
	}
	return count
}

func unverifiedProbes(probes map[string]knowledge.ProbeRecord, tenantID, knowledgeID string, version int64) int64 {
	var count int64
	prefix := tenantID + "\x00" + knowledgeID + "\x00" + strconv.FormatInt(version, 10) + "\x00"
	for key, value := range probes {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix && !value.Verified {
			count++
		}
	}
	return count
}

func countProbes(probes map[string]knowledge.ProbeRecord, tenantID, knowledgeID string, version int64) int64 {
	var count int64
	prefix := tenantID + "\x00" + knowledgeID + "\x00" + strconv.FormatInt(version, 10) + "\x00"
	for key := range probes {
		if strings.HasPrefix(key, prefix) {
			count++
		}
	}
	return count
}

func chunksForScope(chunks map[string]knowledge.ChunkRecord, tenantID, knowledgeID string, version int64) []knowledge.ChunkRecord {
	result := make([]knowledge.ChunkRecord, 0)
	prefix := tenantID + "\x00" + knowledgeID + "\x00" + strconv.FormatInt(version, 10) + "\x00"
	for key, value := range chunks {
		if strings.HasPrefix(key, prefix) {
			result = append(result, cloneChunk(value))
		}
	}
	return result
}

func validExpectedChunks(chunks map[string]knowledge.ChunkRecord, in knowledge.ProbeRecord) bool {
	seen := make(map[string]struct{}, len(in.ExpectedChunks))
	for _, chunkID := range in.ExpectedChunks {
		if strings.TrimSpace(chunkID) == "" {
			return false
		}
		if _, ok := seen[chunkID]; ok {
			return false
		}
		seen[chunkID] = struct{}{}
		if _, ok := chunks[chunkKey(in.TenantID, in.KnowledgeID, in.KnowledgeVersion, chunkID)]; !ok {
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

func metadataAllowed(metadata map[string]string, schema []string) bool {
	allowed := make(map[string]struct{}, len(schema))
	for _, key := range schema {
		allowed[key] = struct{}{}
	}
	for key := range metadata {
		if _, ok := allowed[key]; !ok {
			return false
		}
	}
	return true
}

func cloneStrings(values []string) []string { return append([]string(nil), values...) }
func cloneChunk(value knowledge.ChunkRecord) knowledge.ChunkRecord {
	value.Metadata = mapsClone(value.Metadata)
	value.Vector = append([]float32(nil), value.Vector...)
	return value
}
func cloneProbe(value knowledge.ProbeRecord) knowledge.ProbeRecord {
	value.ExpectedChunks = cloneStrings(value.ExpectedChunks)
	return value
}
func cloneManifest(value knowledge.Manifest) knowledge.Manifest {
	value.MetadataSchema = cloneStrings(value.MetadataSchema)
	return value
}
func mapsClone(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func validateChunk(in knowledge.ChunkRecord) error {
	if in.TenantID == "" || in.KnowledgeID == "" || in.KnowledgeVersion < 1 || in.ChunkID == "" ||
		!validDigest(in.SourceDigest) || !validDigest(in.ContentDigest) || !validDigest(in.MetadataDigest) ||
		in.EmbeddingProfileID == "" || in.EmbeddingVersion < 1 || in.VectorGeneration == "" ||
		in.Content == "" || len(in.Vector) == 0 || in.CreatedAt.IsZero() {
		return runtime.ErrInvariantViolation
	}
	if _, err := in.MutationDigest(); err != nil {
		return runtime.ErrInvariantViolation
	}
	return nil
}

func sameChunk(left, right knowledge.ChunkRecord) bool {
	return left.SourceDigest == right.SourceDigest && left.ContentDigest == right.ContentDigest &&
		left.MetadataDigest == right.MetadataDigest && left.EmbeddingProfileID == right.EmbeddingProfileID &&
		left.EmbeddingVersion == right.EmbeddingVersion && left.VectorGeneration == right.VectorGeneration &&
		left.Content == right.Content && reflect.DeepEqual(left.Metadata, right.Metadata) && reflect.DeepEqual(left.Vector, right.Vector) &&
		left.CreatedAt.Equal(right.CreatedAt.UTC().Truncate(time.Microsecond))
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
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

var _ knowledge.IngestionStore = (*Store)(nil)
