// Package secret provides a unified credential store. It holds small,
// sensitive values (model API keys, IM channel credentials) referenced by
// other domains via an opaque key, so plaintext credentials never live in the
// domain tables.
//
// Backends:
//   - MemStore: in-memory, plaintext — dev/test only.
//   - MySQLStore: AES-256-GCM encrypted at rest; the master key is never
//     persisted, only a sha256-derived AES key is kept in memory.
package secret

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned when a secret key does not exist.
var ErrNotFound = errors.New("secret: not found")

// Secret is the non-sensitive view of a stored secret (used for listings).
// The value itself is never exposed through this type.
type Secret struct {
	Key       string    `json:"key"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Store is the credential-store contract. List returns only metadata, never
// plaintext values.
type Store interface {
	Put(ctx context.Context, key, value string) error
	Get(ctx context.Context, key string) (string, error)
	List(ctx context.Context) ([]Secret, error)
	Delete(ctx context.Context, key string) error
}
