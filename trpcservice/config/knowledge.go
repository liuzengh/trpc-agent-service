package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// The knowledge section is deployment plumbing, not tenant configuration:
// like control_plane, it names the services every role connects to, and the
// KNOWLEDGE_*/EMBEDDING_* variables override the file so a Compose stack can
// point the same image at different endpoints. An absent section means the
// knowledge features are off — a deployment that never sets these four
// endpoints simply cannot pin knowledge_search, which is a configuration an
// operator can see rather than a runtime error they cannot.
const (
	envKnowledgeQdrantURL = "KNOWLEDGE_QDRANT_URL"
	envKnowledgeCollect   = "KNOWLEDGE_COLLECTION"
	envKnowledgeMinIOEP   = "KNOWLEDGE_MINIO_ENDPOINT"
	envKnowledgeMinIOUser = "KNOWLEDGE_MINIO_USER"
	envKnowledgeMinIOPass = "KNOWLEDGE_MINIO_PASSWORD"
	envKnowledgeMinIOBuck = "KNOWLEDGE_MINIO_BUCKET"
	envKnowledgeMinIOReg  = "KNOWLEDGE_MINIO_REGION"
	envEmbeddingBaseURL   = "EMBEDDING_BASE_URL"
	envEmbeddingModel     = "EMBEDDING_MODEL"
	envEmbeddingDim       = "EMBEDDING_DIM"
	envEmbeddingAPIKeyRef = "EMBEDDING_API_KEY_REF"

	// DefaultCollection is the single collection this phase writes; the
	// tenant/app/kb filters live in the payload, not in collection names.
	DefaultCollection = "tas_knowledge_v1"
)

type knowledgeYAML struct {
	QdrantURL     string        `yaml:"qdrant_url,omitempty"`
	Collection    string        `yaml:"collection,omitempty"`
	MinIOEndpoint string        `yaml:"minio_endpoint,omitempty"`
	MinIOUser     string        `yaml:"minio_user,omitempty"`
	MinIOPassword string        `yaml:"minio_password,omitempty"`
	MinIOBucket   string        `yaml:"minio_bucket,omitempty"`
	MinIORegion   string        `yaml:"minio_region,omitempty"`
	Embedding     embeddingYAML `yaml:"embedding,omitempty"`
}

type embeddingYAML struct {
	BaseURL   string `yaml:"base_url,omitempty"`
	Model     string `yaml:"model,omitempty"`
	Dim       int    `yaml:"dim,omitempty"`
	APIKeyRef string `yaml:"api_key_ref,omitempty"`
}

// Knowledge is the validated, env-overridden knowledge configuration.
type Knowledge struct {
	QdrantURL     string
	Collection    string
	MinIOEndpoint string
	MinIOUser     string
	MinIOPassword string
	MinIOBucket   string
	MinIORegion   string

	EmbeddingBaseURL   string
	EmbeddingModel     string
	EmbeddingDim       int
	EmbeddingAPIKeyRef string
}

// Enabled reports whether every endpoint the pipeline needs is configured.
// Partial configuration (say a Qdrant URL with no object store) is refused
// at parse time rather than reported here, so Enabled is a plain all-set
// check.
func (k Knowledge) Enabled() bool {
	return k.QdrantURL != "" && k.MinIOEndpoint != "" && k.EmbeddingBaseURL != ""
}

// knowledgeToYAML renders the section for Save/Marshal. A disabled section
// is omitted entirely: writing four empty strings would claim a knowledge
// deployment that does not exist.
func knowledgeToYAML(k Knowledge) *knowledgeYAML {
	if !k.Enabled() {
		return nil
	}
	return &knowledgeYAML{
		QdrantURL:     k.QdrantURL,
		Collection:    k.Collection,
		MinIOEndpoint: k.MinIOEndpoint,
		MinIOUser:     k.MinIOUser,
		MinIOPassword: k.MinIOPassword,
		MinIOBucket:   k.MinIOBucket,
		MinIORegion:   k.MinIORegion,
		Embedding: embeddingYAML{
			BaseURL:   k.EmbeddingBaseURL,
			Model:     k.EmbeddingModel,
			Dim:       k.EmbeddingDim,
			APIKeyRef: k.EmbeddingAPIKeyRef,
		},
	}
}

// parseKnowledge normalizes the section. Omission is off; a *partial*
// section is an error, because the failure mode of letting it through is an
// upload path that accepts bytes it cannot index.
func parseKnowledge(y *knowledgeYAML) (Knowledge, error) {
	k := Knowledge{
		Collection:   DefaultCollection,
		MinIOBucket:  "tas-knowledge",
		MinIORegion:  "us-east-1",
		EmbeddingDim: 64,
	}
	if y != nil {
		k.QdrantURL = y.QdrantURL
		if y.Collection != "" {
			k.Collection = y.Collection
		}
		k.MinIOEndpoint = y.MinIOEndpoint
		k.MinIOUser = y.MinIOUser
		k.MinIOPassword = y.MinIOPassword
		if y.MinIOBucket != "" {
			k.MinIOBucket = y.MinIOBucket
		}
		if y.MinIORegion != "" {
			k.MinIORegion = y.MinIORegion
		}
		k.EmbeddingBaseURL = y.Embedding.BaseURL
		k.EmbeddingModel = y.Embedding.Model
		if y.Embedding.Dim > 0 {
			k.EmbeddingDim = y.Embedding.Dim
		}
		k.EmbeddingAPIKeyRef = y.Embedding.APIKeyRef
	}
	if v := os.Getenv(envKnowledgeQdrantURL); v != "" {
		k.QdrantURL = v
	}
	if v := os.Getenv(envKnowledgeCollect); v != "" {
		k.Collection = v
	}
	if v := os.Getenv(envKnowledgeMinIOEP); v != "" {
		k.MinIOEndpoint = v
	}
	if v := os.Getenv(envKnowledgeMinIOUser); v != "" {
		k.MinIOUser = v
	}
	if v := os.Getenv(envKnowledgeMinIOPass); v != "" {
		k.MinIOPassword = v
	}
	if v := os.Getenv(envKnowledgeMinIOBuck); v != "" {
		k.MinIOBucket = v
	}
	if v := os.Getenv(envKnowledgeMinIOReg); v != "" {
		k.MinIORegion = v
	}
	if v := os.Getenv(envEmbeddingBaseURL); v != "" {
		k.EmbeddingBaseURL = v
	}
	if v := os.Getenv(envEmbeddingModel); v != "" {
		k.EmbeddingModel = v
	}
	if v := os.Getenv(envEmbeddingDim); v != "" {
		dim, err := strconv.Atoi(v)
		if err != nil || dim <= 0 {
			return Knowledge{}, fmt.Errorf("EMBEDDING_DIM %q is not a positive integer", v)
		}
		k.EmbeddingDim = dim
	}
	if v := os.Getenv(envEmbeddingAPIKeyRef); v != "" {
		k.EmbeddingAPIKeyRef = v
	}

	if !k.Enabled() {
		return Knowledge{}, nil
	}
	if strings.TrimSpace(k.QdrantURL) == "" || strings.TrimSpace(k.EmbeddingBaseURL) == "" {
		return Knowledge{}, fmt.Errorf("knowledge: qdrant_url and embedding base_url are both required when knowledge is enabled")
	}
	if k.EmbeddingModel == "" {
		return Knowledge{}, fmt.Errorf("knowledge: embedding.model is required when knowledge is enabled")
	}
	if k.MinIOUser == "" || k.MinIOPassword == "" {
		return Knowledge{}, fmt.Errorf("knowledge: minio_user and minio_password are required when knowledge is enabled")
	}
	if k.MinIOBucket == "" {
		return Knowledge{}, fmt.Errorf("knowledge: minio_bucket is required when knowledge is enabled")
	}
	return k, nil
}
