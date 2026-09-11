package executionv1

const ArtifactPath = "/internal/v1/artifacts"

// Transport bounds are distinct from the published per-backend MaxBytes.
const MaxArtifactBytes = 16 * 1024 * 1024
const MaxArtifactRequestBytes = 24 * 1024 * 1024

type ArtifactSecrets struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
}

// ArtifactRequest is accepted only from the authenticated Control workload.
// Control authorizes the owner and resolves these secrets from the published
// Profile; Worker independently checks the Run and immutable Manifest identity.
type ArtifactRequest struct {
	TenantID             string          `json:"tenant_id"`
	RunID                string          `json:"run_id"`
	ManifestRef          string          `json:"manifest_ref"`
	ManifestDigest       string          `json:"manifest_digest"`
	DeploymentRevisionID string          `json:"deployment_revision_id"`
	Operation            string          `json:"operation"`
	Name                 string          `json:"name"`
	Version              *int            `json:"version,omitempty"`
	Content              []byte          `json:"content_base64,omitempty"`
	MimeType             string          `json:"mime_type,omitempty"`
	Credentials          ArtifactSecrets `json:"credentials"`
}
type ArtifactResponse struct {
	Name      string `json:"name"`
	Version   int    `json:"version"`
	Ref       string `json:"ref"`
	MimeType  string `json:"mime_type"`
	SizeBytes int    `json:"size_bytes"`
	SHA256    string `json:"sha256"`
	Content   []byte `json:"content_base64,omitempty"`
}
