// Package filesystem resolves exact scoped Secret versions from a read-only
// directory such as a Kubernetes CSI Secret Store mount.
package filesystem

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

const defaultMaximumBytes int64 = 64 << 10

type Provider struct {
	root    string
	maximum int64
	mu      sync.Mutex
	digests map[string][sha256.Size]byte
}

func New(root string, maximumBytes int64) (*Provider, error) {
	if root == "" {
		return nil, runtime.ErrInvalidEnvelope
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, runtime.ErrInvalidEnvelope
	}
	info, err := os.Stat(absolute)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0o022 != 0 {
		return nil, runtime.ErrCapabilityUnsupported
	}
	if maximumBytes <= 0 {
		maximumBytes = defaultMaximumBytes
	}
	return &Provider{root: filepath.Clean(absolute), maximum: maximumBytes, digests: make(map[string][sha256.Size]byte)}, nil
}

// StableFilename derives a non-reversible filename from the complete
// authorization coordinate. Ref text is never interpreted as a path.
func StableFilename(scope secrets.Scope, ref secrets.SecretRef) (string, error) {
	if err := secrets.ValidateRequest(scope, ref); err != nil {
		return "", err
	}
	hash := sha256.New()
	for _, value := range []string{scope.TenantID, scope.Subject, string(scope.Purpose), scope.ResourceID,
		encodeVersion(scope.ResourceVersion), ref.Ref, encodeVersion(ref.Version)} {
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(value)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write([]byte(value))
	}
	return "s1_" + base64.RawURLEncoding.EncodeToString(hash.Sum(nil)) + ".secret", nil
}

func (p *Provider) Resolve(ctx context.Context, scope secrets.Scope, ref secrets.SecretRef) (secrets.SecretValue, error) {
	if p == nil || p.root == "" || p.maximum <= 0 {
		return secrets.SecretValue{}, runtime.ErrCapabilityUnsupported
	}
	if err := ctx.Err(); err != nil {
		return secrets.SecretValue{}, err
	}
	name, err := StableFilename(scope, ref)
	if err != nil {
		return secrets.SecretValue{}, err
	}
	path := filepath.Join(p.root, name)
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return secrets.SecretValue{}, runtime.ErrNotFound
	}
	if err != nil {
		return secrets.SecretValue{}, runtime.ErrBackendUnavailable
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() < 1 || info.Size() > p.maximum {
		return secrets.SecretValue{}, runtime.ErrCapabilityUnsupported
	}
	value, err := io.ReadAll(io.LimitReader(file, p.maximum+1))
	if err != nil {
		return secrets.SecretValue{}, runtime.ErrBackendUnavailable
	}
	if err := ctx.Err(); err != nil {
		clear(value)
		return secrets.SecretValue{}, err
	}
	if len(value) == 0 || int64(len(value)) > p.maximum {
		clear(value)
		return secrets.SecretValue{}, runtime.ErrCapabilityUnsupported
	}
	digest := sha256.Sum256(value)
	p.mu.Lock()
	known, exists := p.digests[name]
	if !exists {
		p.digests[name] = digest
	}
	p.mu.Unlock()
	if exists && known != digest {
		clear(value)
		return secrets.SecretValue{}, runtime.ErrVersionMismatch
	}
	return secrets.SecretValue{Bytes: value, Version: ref.Version}, nil
}

func (p *Provider) Probe(ctx context.Context, scope secrets.Scope, ref secrets.SecretRef) error {
	value, err := p.Resolve(ctx, scope, ref)
	clear(value.Bytes)
	return err
}

// ProbeRoot verifies that the projected SecretProvider root is still a
// readable, private directory. Individual scoped files remain lazy and are
// checked on every Resolve; this probe only covers the process-wide mount.
func (p *Provider) ProbeRoot(ctx context.Context) error {
	if p == nil || p.root == "" {
		return runtime.ErrCapabilityUnsupported
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := os.Stat(p.root)
	if err != nil {
		return runtime.ErrBackendUnavailable
	}
	if !info.IsDir() || info.Mode().Perm()&0o022 != 0 {
		return runtime.ErrCapabilityUnsupported
	}
	return nil
}

func encodeVersion(value int64) string {
	var bytes [8]byte
	binary.BigEndian.PutUint64(bytes[:], uint64(value))
	return string(bytes[:])
}

var _ secrets.Provider = (*Provider)(nil)
