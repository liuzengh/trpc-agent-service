package knowledge

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"strconv"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/provider"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	upstreamembedder "trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
	openaiembedder "trpc.group/trpc-go/trpc-agent-go/knowledge/embedder/openai"
)

// ModelProfileReader is intentionally the small public subset required for a
// version-pinned embedding profile. Runtime Knowledge never asks for a
// "current" profile, so a published manifest remains reproducible.
type ModelProfileReader interface {
	GetModel(context.Context, string, string, int64) (provider.ModelProfileSnapshot, error)
}

// EmbedderResolver materializes the public trpc-agent-go Embedder from an
// immutable provider profile and a least-privilege SecretRef.
type EmbedderResolver struct {
	Profiles ModelProfileReader
	Secrets  secrets.Provider
	Subject  string
}

func (r EmbedderResolver) Resolve(ctx context.Context, tenantID, profileID string, version int64) (upstreamembedder.Embedder, error) {
	if r.Profiles == nil || tenantID == "" || profileID == "" || version < 1 ||
		strings.TrimSpace(r.Subject) != r.Subject || r.Subject == "" {
		return nil, runtime.ErrCapabilityUnsupported
	}
	profile, err := r.Profiles.GetModel(ctx, tenantID, profileID, version)
	if err != nil {
		return nil, err
	}
	if profile.TenantID != tenantID || profile.ProfileID != profileID {
		return nil, runtime.ErrTenantScope
	}
	if profile.Version == version && profile.Status == "active" && profile.Provider == "fake-embedding" && profile.SchemaVersion == 1 &&
		profile.Model == "fake-embedding-v1" && profile.Endpoint == "" && profile.SecretRef == (secrets.SecretRef{}) {
		dimensions, err := embeddingDimensions(profile.Options)
		if err != nil {
			return nil, err
		}
		return DeterministicEmbedder{Dimensions: dimensions}, nil
	}
	if profile.Version != version || profile.Status != "active" || profile.Provider != "openai-embedding" || profile.SchemaVersion != 1 ||
		profile.Model == "" || profile.Endpoint == "" || profile.SecretRef.Ref == "" || profile.SecretRef.Version < 1 || r.Secrets == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	dimensions, err := embeddingDimensions(profile.Options)
	if err != nil {
		return nil, err
	}
	scope := secrets.Scope{TenantID: tenantID, Subject: r.Subject, Purpose: secrets.PurposeModelCall,
		ResourceID: profileID, ResourceVersion: version}
	secret, err := r.Secrets.Resolve(ctx, scope, profile.SecretRef)
	if err != nil {
		return nil, err
	}
	defer clearSecret(secret.Bytes)
	apiKey := strings.TrimSpace(string(secret.Bytes))
	if secret.Version != profile.SecretRef.Version || apiKey == "" || strings.ContainsAny(apiKey, "\r\n\x00") {
		return nil, runtime.ErrVersionMismatch
	}
	return openaiembedder.New(openaiembedder.WithAPIKey(apiKey), openaiembedder.WithBaseURL(profile.Endpoint),
		openaiembedder.WithModel(profile.Model), openaiembedder.WithDimensions(dimensions)), nil
}

func embeddingDimensions(options map[string]string) (int, error) {
	dimensions, err := strconv.Atoi(options["dimensions"])
	if err != nil || dimensions < 1 || dimensions > 65536 {
		return 0, runtime.ErrInvariantViolation
	}
	return dimensions, nil
}

// DeterministicEmbedder implements the public tRPC-Agent-Go Embedder contract
// without credentials or a network dependency. It is intentionally useful
// only for demos and tests: production profiles remain OpenAI-backed.
type DeterministicEmbedder struct{ Dimensions int }

func (e DeterministicEmbedder) GetEmbedding(ctx context.Context, text string) ([]float64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if e.Dimensions < 1 || text == "" {
		return nil, runtime.ErrInvariantViolation
	}
	result := make([]float64, e.Dimensions)
	for index := range result {
		sum := sha256.Sum256([]byte("trpc-agent-service/fake-embedding/v1\x00" + text + "\x00" + strconv.Itoa(index)))
		result[index] = float64(binary.BigEndian.Uint64(sum[:8]))/float64(math.MaxUint64)*2 - 1
	}
	return result, nil
}

func (e DeterministicEmbedder) GetEmbeddingWithUsage(ctx context.Context, text string) ([]float64, map[string]any, error) {
	value, err := e.GetEmbedding(ctx, text)
	if err != nil {
		return nil, nil, err
	}
	return value, map[string]any{"provider": "fake-embedding", "input_bytes": len(text)}, nil
}

func (e DeterministicEmbedder) GetDimensions() int { return e.Dimensions }

func clearSecret(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
