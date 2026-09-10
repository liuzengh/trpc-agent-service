// Package storageerrors contains error identities shared by storage layers.
// It has no database or runtime dependencies, so low-level primitives and
// tenant-scoped adapters can preserve compatibility without importing each
// other's implementation packages.
package storageerrors

import "errors"

// ErrPostgres is the stable redacted category for PostgreSQL-backed storage
// failures. The historical error text is part of the compatibility surface.
var ErrPostgres = errors.New("postgres storage error")
