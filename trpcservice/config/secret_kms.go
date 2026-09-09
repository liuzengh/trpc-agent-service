package config

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"
)

// KMSResolver resolves secrets over HTTP from a KMS sidecar or Vault agent.
// Contract: GET {endpoint}/v1/secrets/{ref} with a bearer token; a 200
// response body is the plaintext secret. Anything else is an error (the
// CachedResolver wrapper serves staleness through short outages).
type KMSResolver struct {
	endpoint string
	token    string
	client   *http.Client
}

// NewKMSResolver creates the resolver. The KMS token itself is resolved
// through the bootstrap (file) resolver — the KMS credential cannot come
// from the KMS.
func NewKMSResolver(endpoint, token string) (*KMSResolver, error) {
	if endpoint == "" || token == "" {
		return nil, fmt.Errorf("kms: endpoint and token are required")
	}
	return &KMSResolver{
		endpoint: strings.TrimRight(endpoint, "/"),
		token:    token,
		client:   &http.Client{Timeout: 5 * time.Second},
	}, nil
}

// secretRefPattern is the shape a KMS secret reference may take: slash
// separated segments of the characters a KMS or Vault path uses. The ref is
// interpolated into the request path, so anything else is refused rather than
// escaped — a ".." segment or a query/fragment would let whoever can set a
// binding make the platform request an arbitrary KMS path.
var secretRefPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*(/[A-Za-z0-9][A-Za-z0-9._-]*)*$`)

// Resolve implements SecretResolver.
func (k *KMSResolver) Resolve(ctx context.Context, ref string) (string, error) {
	if !secretRefPattern.MatchString(ref) {
		return "", fmt.Errorf("kms: invalid secret ref %q", ref)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		k.endpoint+"/v1/secrets/"+ref, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+k.token)
	resp, err := k.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("kms resolve %q: %w", ref, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("kms resolve %q: status %d", ref, resp.StatusCode)
	}
	body, err := io.ReadAll(http.MaxBytesReader(nil, resp.Body, 64<<10))
	if err != nil {
		return "", err
	}
	v := strings.TrimSpace(string(body))
	if v == "" {
		return "", fmt.Errorf("kms: empty secret for %q", ref)
	}
	return v, nil
}
