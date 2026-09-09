package config

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// SecretResolver resolves a secret reference into its plaintext value. The DB
// stores only references (token_ref, aeskey_ref); plaintext is fetched at
// runtime and must never enter any log or trace. ctx is for implementations
// that make network calls (e.g. KMS); local implementations may ignore it.
type SecretResolver interface {
	Resolve(ctx context.Context, ref string) (string, error)
}

// FileResolver is the local-development implementation: ref is a file name
// under SecretsDir and the file content (trimmed) is the plaintext secret.
// Production deployments use a KMS implementation of the same interface.
type FileResolver struct {
	dir string
}

// NewFileResolver creates a file-based resolver reading secrets from dir.
func NewFileResolver(dir string) *FileResolver {
	return &FileResolver{dir: dir}
}

// Resolve implements SecretResolver.
// ref must be a bare file name; path traversal (../) is rejected.
func (r *FileResolver) Resolve(_ context.Context, ref string) (string, error) {
	if ref == "" || ref != filepath.Base(ref) {
		return "", fmt.Errorf("invalid secret ref %q", ref)
	}
	//nolint:gosec // G304: ref is validated as a bare file name above (no traversal)
	data, err := os.ReadFile(filepath.Join(r.dir, ref))
	if err != nil {
		return "", fmt.Errorf("resolve secret %q: %w", ref, err)
	}
	return strings.TrimSpace(string(data)), nil
}
