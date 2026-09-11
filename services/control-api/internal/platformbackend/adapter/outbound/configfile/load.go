// Package configfile loads non-secret backend metadata from platform-owned files.
package configfile

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/platformbackend/domain"
)

var ErrConfig = errors.New("invalid platform backend catalog configuration")

const MaxBytes = 1024 * 1024

// FileEntry is a strict platform configuration DTO, not a public registration API.
type FileEntry struct {
	ID        string        `json:"id"`
	Revision  uint64        `json:"revision"`
	Label     string        `json:"label"`
	Kind      domain.Kind   `json:"kind"`
	Roles     []domain.Role `json:"roles"`
	Enabled   bool          `json:"enabled"`
	TenantIDs []string      `json:"tenant_ids"`
}
type Document struct {
	Version  string      `json:"version"`
	Backends []FileEntry `json:"backends"`
}

// Load binds each replica to exact file bytes. An absent pair gives an empty catalog;
// partial configuration is an error. No file paths or parser data appear in errors.
func Load(path, expected string) (*domain.Catalog, error) {
	if path == "" && expected == "" {
		return domain.NewCatalog(nil)
	}
	b, err := readPinned(path, expected, false)
	if err != nil {
		return nil, err
	}
	return Decode(b)
}
func readPinned(path, expected string, private bool) ([]byte, error) {
	if path == "" || len(expected) != 64 {
		return nil, ErrConfig
	}
	rawDigest, err := hex.DecodeString(expected)
	if err != nil || hex.EncodeToString(rawDigest) != expected {
		return nil, ErrConfig
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, ErrConfig
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > MaxBytes || (private && info.Mode().Perm()&0077 != 0) {
		return nil, ErrConfig
	}
	b, err := io.ReadAll(io.LimitReader(f, MaxBytes+1))
	if err != nil || len(b) > MaxBytes || !utf8.Valid(b) {
		return nil, ErrConfig
	}
	sum := sha256.Sum256(b)
	if !bytes.Equal(sum[:], rawDigest) {
		return nil, ErrConfig
	}
	return b, nil
}
func Decode(b []byte) (*domain.Catalog, error) {
	if len(b) > MaxBytes || !utf8.Valid(b) {
		return nil, ErrConfig
	}
	// Token traversal rejects duplicate keys and null at every depth before decoding.
	d := json.NewDecoder(bytes.NewReader(b))
	if err := uniqueValue(d, 0); err != nil {
		return nil, ErrConfig
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, ErrConfig
	}
	var doc Document
	d = json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(&doc); err != nil || doc.Version != "v1" || doc.Backends == nil {
		return nil, ErrConfig
	}
	es := make([]domain.Entry, 0, len(doc.Backends))
	for _, e := range doc.Backends {
		es = append(es, domain.Entry{ID: e.ID, Revision: e.Revision, Label: e.Label, Kind: e.Kind, Roles: e.Roles, Enabled: e.Enabled, TenantIDs: e.TenantIDs})
	}
	c, err := domain.NewCatalog(es)
	if err != nil {
		return nil, ErrConfig
	}
	return c, nil
}
func uniqueValue(d *json.Decoder, depth int) error {
	if depth > 8 {
		return ErrConfig
	}
	t, err := d.Token()
	if err != nil || t == nil {
		return ErrConfig
	}
	delim, ok := t.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			t, err := d.Token()
			if err != nil {
				return ErrConfig
			}
			k, ok := t.(string)
			if !ok || seen[k] || !allowedKey(k) {
				return ErrConfig
			}
			seen[k] = true
			if err := uniqueValue(d, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := uniqueValue(d, depth+1); err != nil {
				return err
			}
		}
	default:
		return ErrConfig
	}
	_, err = d.Token()
	return err
}

func allowedKey(k string) bool {
	switch k {
	case "version", "backends", "id", "revision", "label", "kind", "roles", "enabled", "tenant_ids":
		return true
	}
	return false
}
