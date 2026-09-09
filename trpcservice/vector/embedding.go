package vector

import (
	"context"
	"crypto/sha256"
	"strconv"
)

// EmbeddingProvider is the project boundary for embedding generation. A real
// provider must be server-owned; this package only supplies a deterministic
// local implementation for contract tests.
type EmbeddingProvider interface {
	Embed(context.Context, string) (Embedding, error)
}

type DeterministicEmbedder struct {
	model        string
	modelVersion string
	dimension    int
	maxInputByte int
}

func NewDeterministicEmbedder(model, modelVersion string, dimension, maxInputBytes int) (*DeterministicEmbedder, error) {
	if !validComponent(model, 256) || !validComponent(modelVersion, 128) {
		return nil, ErrInvalidModel
	}
	if dimension < 1 || dimension > MaxVectorDimension {
		return nil, ErrInvalidDimension
	}
	if maxInputBytes < 1 || maxInputBytes > MaxContentBytes {
		return nil, ErrInvalidConfig
	}
	return &DeterministicEmbedder{model: model, modelVersion: modelVersion, dimension: dimension, maxInputByte: maxInputBytes}, nil
}

func (e *DeterministicEmbedder) Embed(ctx context.Context, text string) (Embedding, error) {
	if e == nil {
		return Embedding{}, ErrInvalidConfig
	}
	if err := contextError(ctx); err != nil {
		return Embedding{}, err
	}
	if len(text) > e.maxInputByte {
		return Embedding{}, ErrInvalidDocument
	}
	values := make([]float64, e.dimension)
	for block := 0; block*32 < e.dimension; block++ {
		if err := contextError(ctx); err != nil {
			return Embedding{}, err
		}
		digest := sha256.Sum256([]byte("trpc-agent/vector/fake-embedding/v1\x00" + e.model + "\x00" + e.modelVersion + "\x00" + strconv.Itoa(block) + "\x00" + text))
		for offset := 0; offset < 32 && block*32+offset < e.dimension; offset++ {
			// The byte-to-float mapping is deterministic, not a semantic quality claim.
			values[block*32+offset] = float64(int8(digest[offset])) / 128
		}
	}
	return Embedding{Values: values, Model: e.model, ModelVersion: e.modelVersion, Dimension: e.dimension}, nil
}
