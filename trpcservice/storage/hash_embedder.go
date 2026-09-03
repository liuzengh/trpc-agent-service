package storage

import (
	"context"
	"hash/fnv"
	"math"
	"strings"
	"unicode"

	"trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
)

// HashEmbedder is deterministic and dependency-free. It is intended for
// tutorials/tests; production revisions should select a real embedding model.
type HashEmbedder struct{ dimensions int }

func NewHashEmbedder(dimensions int) *HashEmbedder {
	if dimensions <= 0 {
		dimensions = 128
	}
	return &HashEmbedder{dimensions: dimensions}
}

func (e *HashEmbedder) GetEmbedding(ctx context.Context, text string) ([]float64, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	result := make([]float64, e.dimensions)
	tokens := strings.FieldsFunc(strings.ToLower(text), func(value rune) bool {
		return unicode.IsSpace(value) || unicode.IsPunct(value)
	})
	if len(tokens) == 0 && text != "" {
		tokens = []string{text}
	}
	for _, token := range tokens {
		hasher := fnv.New64a()
		_, _ = hasher.Write([]byte(token))
		value := hasher.Sum64()
		index := int(value % uint64(e.dimensions))
		sign := 1.0
		if value&(1<<63) != 0 {
			sign = -1
		}
		result[index] += sign
	}
	var norm float64
	for _, value := range result {
		norm += value * value
	}
	if norm > 0 {
		norm = math.Sqrt(norm)
		for index := range result {
			result[index] /= norm
		}
	}
	return result, nil
}

func (e *HashEmbedder) GetEmbeddingWithUsage(
	ctx context.Context,
	text string,
) ([]float64, map[string]any, error) {
	value, err := e.GetEmbedding(ctx, text)
	return value, map[string]any{"provider": "hash", "tokens": len(strings.Fields(text))}, err
}

func (e *HashEmbedder) GetDimensions() int { return e.dimensions }

var _ embedder.Embedder = (*HashEmbedder)(nil)
