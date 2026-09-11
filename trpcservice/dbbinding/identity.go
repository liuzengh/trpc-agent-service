// Package dbbinding derives a credential-free identity for a PostgreSQL
// database binding. Durable queue records persist this opaque value so a
// strict Session transaction is never asked to update Inbox/Outbox tables in
// another database during a rolling configuration change.
package dbbinding

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const identityVersion = "postgres-binding-v1"

// Provider exposes the opaque binding identity without revealing connection
// credentials. PostgreSQL Queue and Session implementations satisfy it.
type Provider interface {
	DatabaseIdentity() string
}

// FromPool returns an opaque identity for the pool's configured server,
// database and search path. Credentials and the database role are excluded:
// production may intentionally use separate least-privilege roles for Queue
// and Session while still targeting the same transaction boundary.
func FromPool(pool *pgxpool.Pool) string {
	if pool == nil {
		return ""
	}
	return FromConnConfig(pool.Config().ConnConfig)
}

// FromConnConfig is the deterministic form used by FromPool and tests.
func FromConnConfig(cfg *pgx.ConnConfig) string {
	if cfg == nil {
		return ""
	}
	searchPath := ""
	if cfg.RuntimeParams != nil {
		searchPath = cfg.RuntimeParams["search_path"]
	}
	canonical := strings.Join([]string{
		identityVersion,
		strings.ToLower(strings.TrimSpace(cfg.Host)),
		strconv.FormatUint(uint64(cfg.Port), 10),
		strings.TrimSpace(cfg.Database),
		strings.TrimSpace(searchPath),
	}, "\x00")
	digest := sha256.Sum256([]byte(canonical))
	return identityVersion + ":" + hex.EncodeToString(digest[:])
}
