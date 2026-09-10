package secret

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	BackendEnvironment = "env"
	BackendFile        = "file"
	BackendVault       = "vault"
)

// ScopedEnvironmentKey is also the key format used by Kubernetes Secret
// volumes. The value is never logged; only this stable lookup name is shared.
func ScopedEnvironmentKey(scope tenant.Scope, ref tenant.SecretRef) string {
	parts := []string{
		"TRPC_AGENT_SERVICE_SECRET",
		hex.EncodeToString([]byte(scope.TenantID)),
		hex.EncodeToString([]byte(scope.AppID)),
		hex.EncodeToString([]byte(ref.Name)),
		hex.EncodeToString([]byte(ref.Version)),
	}
	return strings.Join(parts, "_")
}

// EnvironmentProvider is retained for local development and tests.
type EnvironmentProvider struct {
	Getenv func(string) string
}

func (p EnvironmentProvider) ResolveSecret(ctx context.Context, scope tenant.Scope, ref tenant.SecretRef) (string, error) {
	if err := validateLookup(ctx, scope, ref); err != nil {
		return "", err
	}
	if p.Getenv == nil {
		return "", errors.New("environment reader is required")
	}
	value := p.Getenv(ScopedEnvironmentKey(scope, ref))
	if value == "" {
		return "", errors.New("scoped secret is not configured")
	}
	return value, nil
}

// FileProvider reads projected Kubernetes Secret files. The directory is
// trusted deployment configuration; the scoped key is still validated as a
// single filename to prevent traversal.
type FileProvider struct {
	directory string
	readFile  func(string) ([]byte, error)
}

func NewFileProvider(directory string) (*FileProvider, error) {
	directory = strings.TrimSpace(directory)
	if directory == "" {
		return nil, errors.New("secret directory is required")
	}
	return &FileProvider{directory: filepath.Clean(directory), readFile: os.ReadFile}, nil
}

func (p *FileProvider) ResolveSecret(ctx context.Context, scope tenant.Scope, ref tenant.SecretRef) (string, error) {
	if p == nil {
		return "", errors.New("file secret provider is nil")
	}
	if err := validateLookup(ctx, scope, ref); err != nil {
		return "", err
	}
	key := ScopedEnvironmentKey(scope, ref)
	path := filepath.Join(p.directory, key)
	if filepath.Dir(path) != p.directory {
		return "", errors.New("secret key escaped secret directory")
	}
	value, err := p.readFile(path)
	if err != nil {
		return "", errors.New("projected secret is not configured")
	}
	if len(value) == 0 {
		return "", errors.New("projected secret is empty")
	}
	return string(value), nil
}

// VaultProvider resolves a Vault KV v2 value at mount/data/<scoped-key>.
// The token is supplied by deployment configuration and never included in an
// error or request URL.
type VaultProvider struct {
	baseURL   string
	mount     string
	token     string
	namespace string
	client    *http.Client
}

func NewVaultProvider(baseURL, mount, token, namespace string, client *http.Client) (*VaultProvider, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return nil, errors.New("vault address must be an https URL")
	}
	if strings.TrimSpace(mount) == "" || strings.TrimSpace(token) == "" {
		return nil, errors.New("vault mount and token are required")
	}
	if client == nil {
		client = http.DefaultClient
	}
	return &VaultProvider{
		baseURL: strings.TrimRight(parsed.String(), "/"),
		mount:   strings.Trim(mount, "/"), token: token,
		namespace: strings.TrimSpace(namespace), client: client,
	}, nil
}

func (p *VaultProvider) ResolveSecret(ctx context.Context, scope tenant.Scope, ref tenant.SecretRef) (string, error) {
	if p == nil || p.client == nil {
		return "", errors.New("vault secret provider is nil")
	}
	if err := validateLookup(ctx, scope, ref); err != nil {
		return "", err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	path := p.baseURL + "/v1/" + p.mount + "/data/" + url.PathEscape(ScopedEnvironmentKey(scope, ref))
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, path, nil)
	if err != nil {
		return "", errors.New("create vault request failed")
	}
	request.Header.Set("X-Vault-Token", p.token)
	if p.namespace != "" {
		request.Header.Set("X-Vault-Namespace", p.namespace)
	}
	response, err := p.client.Do(request)
	if err != nil {
		return "", errors.New("vault secret lookup failed")
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return "", errors.New("vault secret lookup failed")
	}
	var payload struct {
		Data struct {
			Data  map[string]json.RawMessage `json:"data"`
			Value json.RawMessage            `json:"value"`
		} `json:"data"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&payload); err != nil {
		return "", errors.New("vault secret response is invalid")
	}
	var value string
	if raw := payload.Data.Data["value"]; len(raw) != 0 {
		if err := json.Unmarshal(raw, &value); err != nil {
			return "", errors.New("vault secret value is invalid")
		}
	} else if len(payload.Data.Value) != 0 {
		if err := json.Unmarshal(payload.Data.Value, &value); err != nil {
			return "", errors.New("vault secret value is invalid")
		}
	}
	if value == "" {
		return "", errors.New("vault secret is not configured")
	}
	return value, nil
}

// NewConfiguredProvider selects the runtime backend without changing any
// resolver or SecretProvider consumer. env is the development default;
// production deployments should use file (Kubernetes Secret volume) or vault.
func NewConfiguredProvider(getenv func(string) string) (SecretProvider, error) {
	if getenv == nil {
		return nil, errors.New("environment reader is required")
	}
	switch backend := strings.ToLower(strings.TrimSpace(getenv("TRPC_AGENT_SERVICE_SECRET_BACKEND"))); backend {
	case "", BackendEnvironment:
		return EnvironmentProvider{Getenv: getenv}, nil
	case BackendFile:
		directory := getenv("TRPC_AGENT_SERVICE_SECRET_DIR")
		if directory == "" {
			directory = "/var/run/secrets/trpc-agent-service"
		}
		return NewFileProvider(directory)
	case BackendVault:
		return NewVaultProvider(
			getenv("TRPC_AGENT_SERVICE_VAULT_ADDR"),
			getenv("TRPC_AGENT_SERVICE_VAULT_MOUNT"),
			getenv("TRPC_AGENT_SERVICE_VAULT_TOKEN"),
			getenv("TRPC_AGENT_SERVICE_VAULT_NAMESPACE"),
			http.DefaultClient,
		)
	default:
		return nil, fmt.Errorf("unsupported secret backend %q", backend)
	}
}

func validateLookup(ctx context.Context, scope tenant.Scope, ref tenant.SecretRef) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := scope.Validate(); err != nil {
		return err
	}
	return ref.Validate()
}
