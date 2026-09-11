package executionv1

const KnowledgePath = "/internal/v1/knowledge"
const MaxKnowledgeTextBytes = 1024 * 1024
const MaxKnowledgeRequestBytes = 2 * 1024 * 1024

type KnowledgeSecrets struct {
	QdrantAPIKey    string `json:"qdrant_api_key"`
	EmbeddingAPIKey string `json:"embedding_api_key"`
}

// KnowledgeRequest is a Control-owner-authorized synchronous import into the
// resource selected by this immutable Manifest. No caller-selected namespace.
type KnowledgeRequest struct {
	TenantID             string           `json:"tenant_id"`
	ManifestRef          string           `json:"manifest_ref"`
	ManifestDigest       string           `json:"manifest_digest"`
	DeploymentRevisionID string           `json:"deployment_revision_id"`
	Resource             string           `json:"resource"`
	Operation            string           `json:"operation"`
	Name                 string           `json:"name"`
	Text                 string           `json:"text"`
	Credentials          KnowledgeSecrets `json:"credentials"`
}
type KnowledgeResponse struct {
	Documents int `json:"documents"`
}
