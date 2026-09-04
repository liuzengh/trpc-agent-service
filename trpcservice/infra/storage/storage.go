// Package storage provides tenant-scoped Session and Memory storage on top of
// tRPC-Agent-Go backends. The tenant id maps to the framework's AppName
// dimension, which is the framework's native isolation boundary.
package storage

// Backend selects a storage backend implementation.
type Backend string

const (
	// BackendInMemory is the in-process backend, for tests and local dev.
	BackendInMemory Backend = "inmemory"
	// BackendMySQL is the MySQL-backed session store.
	BackendMySQL Backend = "mysql"
	// BackendRedis is the Redis-backed session/memory store.
	BackendRedis Backend = "redis"
)

// SessionConfig holds session backend settings.
type SessionConfig struct {
	Backend  Backend
	MySQLDSN string
	RedisURL string
}

// MemoryConfig holds memory backend settings.
type MemoryConfig struct {
	Backend  Backend
	RedisURL string
}
