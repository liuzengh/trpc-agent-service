// Package vault resolves exact scoped Secret versions through HashiCorp Vault
// KV v2. Vault policy is an additional authorization boundary; callers still
// have to supply the service's full local Scope on every resolution.
package vault

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

const defaultMaximumBytes int64 = 64 << 10

// TokenSource obtains a Vault transport token only for one outbound request.
// It is intentionally separate from Provider to avoid a recursive unscoped
// SecretRef lookup for Vault's own bootstrap credential.
type TokenSource interface {
	Token(context.Context) ([]byte, error)
}

type Config struct {
	Endpoint          string
	Namespace         string
	HTTPClient        *http.Client
	TokenSource       TokenSource
	MaximumBytes      int64
	AllowInsecureHTTP bool // test/dev only; production endpoints must be HTTPS.
}

type Provider struct {
	endpoint  *url.URL
	namespace string
	client    *http.Client
	tokens    TokenSource
	maximum   int64
}

func New(config Config) (*Provider, error) {
	endpoint, err := url.Parse(strings.TrimSpace(config.Endpoint))
	if err != nil || endpoint.Scheme == "" || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" ||
		(endpoint.Scheme != "https" && !(config.AllowInsecureHTTP && endpoint.Scheme == "http")) || strings.ContainsAny(config.Namespace, "\r\n\x00") {
		return nil, runtime.ErrCapabilityUnsupported
	}
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{Timeout: 20 * time.Second}
	}
	// A redirect can forward X-Vault-Token to an unintended endpoint. Vault
	// clients must use their final canonical endpoint, never discover it while
	// carrying an authentication token.
	client := *config.HTTPClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if config.TokenSource == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	if config.MaximumBytes <= 0 {
		config.MaximumBytes = defaultMaximumBytes
	}
	return &Provider{endpoint: endpoint, namespace: config.Namespace, client: &client, tokens: config.TokenSource, maximum: config.MaximumBytes}, nil
}

func (p *Provider) Resolve(ctx context.Context, scope secrets.Scope, ref secrets.SecretRef) (secrets.SecretValue, error) {
	if p == nil || p.endpoint == nil || p.client == nil || p.tokens == nil || p.maximum < 1 {
		return secrets.SecretValue{}, runtime.ErrCapabilityUnsupported
	}
	if err := secrets.ValidateRequest(scope, ref); err != nil {
		return secrets.SecretValue{}, err
	}
	mount, secretPath, err := parseRef(ref.Ref)
	if err != nil {
		return secrets.SecretValue{}, err
	}
	token, err := p.tokens.Token(ctx)
	if err != nil || len(token) == 0 || int64(len(token)) > p.maximum || bytes.IndexAny(token, "\r\n\x00") >= 0 {
		clear(token)
		return secrets.SecretValue{}, runtime.ErrBackendUnavailable
	}
	defer clear(token)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.endpoint.String()+"/v1/"+url.PathEscape(mount)+"/data/"+escapePath(secretPath), nil)
	if err != nil {
		return secrets.SecretValue{}, runtime.ErrInvariantViolation
	}
	req.Header.Set("X-Vault-Token", string(token))
	if p.namespace != "" {
		req.Header.Set("X-Vault-Namespace", p.namespace)
	}
	response, err := p.client.Do(req)
	if err != nil {
		return secrets.SecretValue{}, runtime.ErrBackendUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return secrets.SecretValue{}, runtime.ErrNotFound
	}
	if response.StatusCode == http.StatusForbidden || response.StatusCode == http.StatusUnauthorized {
		return secrets.SecretValue{}, runtime.ErrTenantScope
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return secrets.SecretValue{}, runtime.ErrBackendUnavailable
	}
	var payload struct {
		Data struct {
			Data     map[string]string `json:"data"`
			Metadata struct {
				Version int64 `json:"version"`
			} `json:"metadata"`
		} `json:"data"`
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, p.maximum+4096))
	if err := decoder.Decode(&payload); err != nil {
		return secrets.SecretValue{}, runtime.ErrBackendUnavailable
	}
	value, ok := payload.Data.Data["value"]
	if !ok || value == "" || int64(len(value)) > p.maximum || payload.Data.Metadata.Version != ref.Version {
		return secrets.SecretValue{}, runtime.ErrVersionMismatch
	}
	return secrets.SecretValue{Bytes: []byte(value), Version: payload.Data.Metadata.Version}, nil
}

func parseRef(raw string) (mount, secretPath string, err error) {
	lower := strings.ToLower(raw)
	if strings.Contains(raw, "/../") || strings.HasSuffix(raw, "/..") || strings.Contains(raw, "/./") || strings.HasSuffix(raw, "/.") || strings.Contains(lower, "%2e") {
		return "", "", runtime.ErrCapabilityUnsupported
	}
	parsed, parseErr := url.Parse(raw)
	if parseErr != nil || parsed.Scheme != "vault" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", "", runtime.ErrCapabilityUnsupported
	}
	if !validSegment(parsed.Host) {
		return "", "", runtime.ErrCapabilityUnsupported
	}
	secretPath = strings.TrimPrefix(parsed.EscapedPath(), "/")
	if secretPath == "" || path.Clean(secretPath) != secretPath {
		return "", "", runtime.ErrCapabilityUnsupported
	}
	for _, segment := range strings.Split(secretPath, "/") {
		if !validSegment(segment) {
			return "", "", runtime.ErrCapabilityUnsupported
		}
	}
	return parsed.Host, secretPath, nil
}

func validSegment(value string) bool {
	if value == "" || len(value) > 255 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_' || char == '-' || char == '.') {
			return false
		}
	}
	return true
}

func escapePath(value string) string {
	parts := strings.Split(value, "/")
	for index := range parts {
		parts[index] = url.PathEscape(parts[index])
	}
	return strings.Join(parts, "/")
}

var _ secrets.Provider = (*Provider)(nil)
