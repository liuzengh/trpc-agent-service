package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// runtimeSecrets holds in-process secrets registered through the admin API
// (e.g. custom model API keys). They are referenced from configuration by
// environment-variable-style names, keeping credentials out of YAML, logs and
// the admin output, while still allowing dynamic tenants to be provisioned
// without restarting the process.
var runtimeSecrets sync.Map // name -> string

// SecretResolver keeps provider-specific secret management outside the config
// and business packages. Implementations must return the secret only to the
// caller; references and errors must remain safe to log.
type SecretResolver interface {
	Resolve(reference string) (string, error)
}

type environmentResolver struct{}

func (environmentResolver) Resolve(reference string) (string, error) {
	if strings.HasPrefix(reference, "secret://") {
		return "", errors.New("external secret resolver is not configured")
	}
	if !envReference.MatchString(reference) {
		return "", errors.New("invalid secret reference")
	}
	if value, ok := lookupRuntimeSecret(reference); ok && value != "" {
		return value, nil
	}
	value, ok := os.LookupEnv(reference)
	if !ok || value == "" {
		return "", errors.New("secret reference is not set")
	}
	return value, nil
}

// FileResolver is the deliberately small local/demo adapter for an external
// Secret Manager contract. Only references of the form secret://file/name are
// accepted and paths are confined below Root. Production deployments should
// replace it with a provider-backed implementation; the application still
// handles the same reference and fail-closed semantics.
type FileResolver struct {
	Root string
}

func NewFileResolver(root string) (SecretResolver, error) {
	if root == "" {
		return nil, errors.New("secret file resolver root is required")
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, errors.New("secret file resolver root is invalid")
	}
	return fileResolver{root: root}, nil
}

type fileResolver struct{ root string }

func (r fileResolver) Resolve(reference string) (string, error) {
	if strings.HasPrefix(reference, "secret://") {
		body := strings.TrimPrefix(reference, "secret://")
		provider, pathAndVersion, ok := strings.Cut(body, "/")
		if !ok || provider != "file" {
			return "", errors.New("external secret provider is unavailable")
		}
		pathPart := strings.SplitN(pathAndVersion, "#", 2)[0]
		if pathPart == "" || filepath.IsAbs(pathPart) || strings.Contains(pathPart, "..") {
			return "", errors.New("invalid secret file reference")
		}
		root := filepath.Clean(r.root)
		path := filepath.Join(root, filepath.Clean(pathPart))
		if path != root && !strings.HasPrefix(path, root+string(os.PathSeparator)) {
			return "", errors.New("invalid secret file path")
		}
		data, err := os.ReadFile(path)
		if err != nil || len(data) == 0 {
			return "", errors.New("secret file is unavailable")
		}
		return strings.TrimRight(string(data), "\r\n"), nil
	}
	return environmentResolver{}.Resolve(reference)
}

var resolverMu sync.RWMutex
var secretResolver SecretResolver = environmentResolver{}

// SetSecretResolver installs the process-wide resolver used by Secret and
// returns the previous resolver so tests or embedding hosts can restore it.
func SetSecretResolver(next SecretResolver) (previous SecretResolver) {
	resolverMu.Lock()
	defer resolverMu.Unlock()
	previous = secretResolver
	if next == nil {
		secretResolver = environmentResolver{}
	} else {
		secretResolver = next
	}
	return previous
}

// ConfigureSecretResolver selects the deployment adapter declared by a
// loaded configuration. External providers are intentionally not guessed: an
// embedding host must install one with SetSecretResolver before resolving a
// secret:// reference, otherwise resolution fails closed.
func ConfigureSecretResolver(provider, fileRoot string) error {
	switch provider {
	case "", "env":
		SetSecretResolver(nil)
		return nil
	case "file":
		resolver, err := NewFileResolver(fileRoot)
		if err != nil {
			return err
		}
		SetSecretResolver(resolver)
		return nil
	case "external":
		SetSecretResolver(nil)
		return nil
	default:
		return errors.New("unsupported secret resolver provider")
	}
}

// RegisterSecret stores a secret under an environment-variable-style name.
// Re-registering the same name replaces the value; an empty value clears it.
func RegisterSecret(name, value string) {
	if value == "" {
		runtimeSecrets.Delete(name)
		return
	}
	runtimeSecrets.Store(name, value)
}

// lookupRuntimeSecret resolves a secret from the in-process registry.
func lookupRuntimeSecret(name string) (string, bool) {
	value, ok := runtimeSecrets.Load(name)
	if !ok {
		return "", false
	}
	secret, ok := value.(string)
	return secret, ok
}
