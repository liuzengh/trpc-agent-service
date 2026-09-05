// Package retrieval implements the P1-06D server-owned retrieval boundary:
// bounded query embedding, tenant-safe VectorStore search outside PostgreSQL
// transactions, candidate validation with stale/tombstone filtering, and
// authoritative PostgreSQL hydration. Milvus hits are derived candidates only;
// the memory table remains the single source of truth.
package retrieval

import (
	"strings"
	"time"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
)

// Config is the server-owned retrieval configuration. Every bound is
// server-owned; callers can only narrow inside it. Zero values fail closed.
type Config struct {
	Model         string
	ModelVersion  string
	SchemaVersion string
	Dimension     int
	// SourceTypes is the server-owned allowlist of hydatable source types.
	// This phase only proves the memory source.
	SourceTypes []string
	// MaxTopK is the caller-facing result ceiling; it never exceeds the
	// provider-neutral vector.MaxTopK contract.
	MaxTopK int
	// CandidateMultiplier widens the first search round.
	CandidateMultiplier int
	// MaxTotalCandidates bounds all rounds together and every hydration batch.
	MaxTotalCandidates int
	// MaxRounds bounds candidate expansion; results may stay below TopK.
	MaxRounds int
	// MaxQueryBytes bounds the query text before embedding.
	MaxQueryBytes int
	// HydrationBatchLimit bounds one PostgreSQL hydration statement.
	HydrationBatchLimit int
	// MaxHydrationChunks bounds batch chunking; total hydrated candidates are
	// already capped by MaxTotalCandidates.
	MaxHydrationChunks int
	OperationTimeout   time.Duration
}

const (
	maxCandidateMultiplier = 8
	maxTotalCandidatesCap  = 64
	maxRoundsCap           = 3
	maxHydrationChunksCap  = 8
	maxDimensionCap        = vector.MaxVectorDimension
)

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

// WithDefaults validates the configuration and fills bounded defaults. Missing
// model/schema/dimension/allowlist or out-of-bounds values fail closed; there
// is no fallback to fake or memory backends.
func (c Config) WithDefaults() (Config, error) {
	if !validComponent(c.Model, 256) || !validComponent(c.ModelVersion, 128) {
		return Config{}, ErrInvalidConfig
	}
	if !validComponent(c.SchemaVersion, 128) {
		return Config{}, ErrInvalidConfig
	}
	if c.Dimension < 1 || c.Dimension > maxDimensionCap {
		return Config{}, ErrInvalidConfig
	}
	if len(c.SourceTypes) == 0 {
		c.SourceTypes = []string{vector.SourceTypeMemory}
	}
	if len(c.SourceTypes) > 8 {
		return Config{}, ErrInvalidConfig
	}
	seen := make(map[string]struct{}, len(c.SourceTypes))
	for _, sourceType := range c.SourceTypes {
		if !validComponent(sourceType, 64) {
			return Config{}, ErrInvalidConfig
		}
		if _, duplicate := seen[sourceType]; duplicate {
			return Config{}, ErrInvalidConfig
		}
		seen[sourceType] = struct{}{}
	}
	if c.MaxTopK == 0 {
		c.MaxTopK = 10
	}
	if c.MaxTopK < 1 || c.MaxTopK > vector.MaxTopK {
		return Config{}, ErrInvalidConfig
	}
	if c.CandidateMultiplier == 0 {
		c.CandidateMultiplier = 4
	}
	if c.CandidateMultiplier < 1 || c.CandidateMultiplier > maxCandidateMultiplier {
		return Config{}, ErrInvalidConfig
	}
	if c.MaxTotalCandidates == 0 {
		c.MaxTotalCandidates = 32
	}
	if c.MaxTotalCandidates < 1 || c.MaxTotalCandidates > maxTotalCandidatesCap {
		return Config{}, ErrInvalidConfig
	}
	if c.MaxTotalCandidates < c.MaxTopK {
		return Config{}, ErrInvalidConfig
	}
	if c.MaxRounds == 0 {
		c.MaxRounds = 2
	}
	if c.MaxRounds < 1 || c.MaxRounds > maxRoundsCap {
		return Config{}, ErrInvalidConfig
	}
	if c.MaxQueryBytes == 0 {
		c.MaxQueryBytes = 8192
	}
	if c.MaxQueryBytes < 1 || c.MaxQueryBytes > vector.MaxContentBytes {
		return Config{}, ErrInvalidConfig
	}
	if c.HydrationBatchLimit == 0 {
		c.HydrationBatchLimit = c.MaxTotalCandidates
	}
	if c.HydrationBatchLimit < 1 || c.HydrationBatchLimit > 128 {
		return Config{}, ErrInvalidConfig
	}
	if c.MaxHydrationChunks == 0 {
		c.MaxHydrationChunks = maxHydrationChunksCap
	}
	if c.MaxHydrationChunks < 1 || c.MaxHydrationChunks > maxHydrationChunksCap {
		return Config{}, ErrInvalidConfig
	}
	if c.HydrationBatchLimit*c.MaxHydrationChunks < c.MaxTotalCandidates {
		return Config{}, ErrInvalidConfig
	}
	if c.OperationTimeout == 0 {
		c.OperationTimeout = 5 * time.Second
	}
	if c.OperationTimeout < time.Second || c.OperationTimeout > time.Minute {
		return Config{}, ErrInvalidConfig
	}
	return c, nil
}

func (c Config) allowsSourceType(sourceType string) bool {
	for _, allowed := range c.SourceTypes {
		if allowed == sourceType {
			return true
		}
	}
	return false
}
