package storagebundle

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	goredis "github.com/redis/go-redis/v9"
)

// probeReleaseTimeout bounds giving a probe connection back. It runs on a
// context detached from the build's, because the build may have failed by
// timing out and cleanup that inherited that deadline would be dead on
// arrival — which for PostgreSQL means not returning the advisory lock.
const probeReleaseTimeout = 5 * time.Second

// acquireBuildLockSQL takes the session-level advisory lock that serializes
// first builds against one target.
//
// Session level rather than transaction level, and released by closing the
// connection rather than by pg_advisory_unlock: the lock has to outlive any
// transaction the upstream constructor runs, and there is exactly one thing
// that reliably ends a session, whatever state the connection is in.
const acquireBuildLockSQL = "SELECT pg_advisory_lock($1)"

// buildLockNamespace prefixes everything hashed into an advisory lock key.
//
// PostgreSQL has one 64-bit advisory lock space per database, shared by every
// application that connects to it. The prefix makes a collision with another
// user of that space — including this project's own control-plane migration
// lock — a collision between two SHA-256 outputs rather than between two
// small hand-picked constants.
const buildLockNamespace = "storagebundle/session-build\x00"

// advisoryLockKey derives the lock two cooperating processes must agree on
// before either creates the upstream tables.
//
// What goes into it is exactly what decides which tables get created: the
// server and database being connected to, the schema, and the table prefix.
// What must not go into it is the tenant or the profile id — two tenants whose
// profiles point at the same database, schema and prefix create the same
// tables, and keying the lock by profile would let them race each other while
// each held a lock nobody else wanted.
//
// The key is a truncated SHA-256 rather than a structured integer because the
// space is flat and 64 bits wide: any encoding that reserved bits for meaning
// would narrow it for no benefit, since nothing ever reads a key back.
func advisoryLockKey(target, schema, tablePrefix string) int64 {
	sum := sha256.Sum256([]byte(
		buildLockNamespace + target + "\x00" + schema + "\x00" + tablePrefix,
	))
	return int64(binary.BigEndian.Uint64(sum[:8]))
}

// parsePostgresTarget returns the identity of what a DSN points at: host, port
// and database, and nothing that could be a credential.
//
// The result is hashed into a lock key and never logged, but it is built to be
// safe if it ever were: the user name and password are deliberately left out,
// because two processes connecting to one database as different users still
// create the same tables and still have to serialize.
func parsePostgresTarget(dsn string) (string, error) {
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		// Discarded by the caller, which reports a fixed message instead: this
		// error quotes the string it could not parse.
		return "", err
	}
	return fmt.Sprintf("%s\x00%d\x00%s", config.Host, config.Port, config.Database), nil
}

// checkRedisURL reports whether a URL is one go-redis can use. The error is
// discarded by the caller for the reason parsePostgresTarget's is.
func checkRedisURL(url string) error {
	_, err := goredis.ParseURL(url)
	return err
}

// probePostgres connects, checks the target answers, and takes the build lock.
//
// The returned release ends the session, which is what gives the lock back. It
// is the only owner of that connection: everything that fails after the
// connect closes it here rather than returning it to a caller that has no way
// to know it exists.
func probePostgres(ctx context.Context, dsn string, lockKey int64) (func() error, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	release := func() error {
		closeCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx), probeReleaseTimeout)
		defer cancel()
		return conn.Close(closeCtx)
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, errors.Join(err, release())
	}
	// Blocking rather than pg_try_advisory_lock: a contended first build should
	// wait for the process that is creating the tables, not fail because
	// another worker got there first. The wait is bounded by the build context,
	// which cancels the query.
	if _, err := conn.Exec(ctx, acquireBuildLockSQL, lockKey); err != nil {
		return nil, errors.Join(err, release())
	}
	return release, nil
}

// probeRedis checks that the target answers.
//
// It holds nothing: Redis has no schema for two builds to create at once, so
// the client exists only to ask the question and is released as soon as the
// build is done with it.
func probeRedis(ctx context.Context, url string) (func() error, error) {
	options, err := goredis.ParseURL(url)
	if err != nil {
		// Replaced rather than wrapped: this error quotes the URL.
		return nil, errors.New("redis url is not usable")
	}
	client := goredis.NewClient(options)
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, errors.Join(err, client.Close())
	}
	return client.Close, nil
}
