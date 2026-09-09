# PostgreSQL migrations

This directory is the versioned source of the P0-05 PostgreSQL schema. The
migration runner is in `trpcservice/storage/postgres` and uses the repository's
already pinned `github.com/jackc/pgx/v5` dependency.

## Running locally

Use only a disposable local or CI PostgreSQL database. The application does not
run migrations automatically. The test helper requires an explicit
`TEST_DATABASE_URL` and refuses an empty value:

```bash
TEST_DATABASE_URL='postgres://user:password@127.0.0.1:5432/trpc_test?sslmode=disable' \
  ./scripts/test-postgres-migrations.sh
```

Do not place this URL in source control or logs. The helper redacts the value
from its own output and does not read production configuration.

Each migration has a paired `.up.sql` and `.down.sql` file. The runner stores
version, name, checksum, and applied time in `schema_migration`, rejects a
changed applied file, and serializes migration work with a PostgreSQL advisory
lock. Each version runs in its own transaction. The initial migration creates
all 15 business tables and their tenant-qualified constraints in one transaction.

`Down` is disabled by default and is intended for disposable test databases
only. A caller must explicitly set `AllowDestructiveDown: true` in
`PostgresConfig`; production code should never set it. Production changes must
be reviewed separately, backed up and restore-tested first, and normally
reversed with a compatible forward migration or backup recovery rather than a
destructive down migration. P0-05 does not provide a production execution
entrypoint, RLS, business Repository implementations, or backup automation.

The package loads migrations from an `fs.FS`; callers can use `os.DirFS` for a
checkout or a test filesystem. It intentionally does not embed the repository
root from a Go package located below it. Migration work acquires one dedicated
pool connection and keeps the advisory lock, metadata reads, DDL transactions,
and unlock on that same connection. Lock and statement timeouts default to 30
seconds and 10 minutes. The applied-version set is checked against the local
source, so missing or unknown migration files fail closed.
