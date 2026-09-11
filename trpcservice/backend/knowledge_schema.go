package backend

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// PreparePostgresKnowledge creates one dimension-specific pgvector table. It
// belongs in the independent migration process and is never called by the
// business runtime.
func PreparePostgresKnowledge(ctx context.Context, tx pgx.Tx, table string, dimension int) error {
	if tx == nil || !safeIdentifier(table) || dimension <= 0 || dimension > 65535 {
		return errors.New("knowledge schema configuration is invalid")
	}
	if _, err := tx.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS vector`); err != nil {
		return errors.New("prepare pgvector extension")
	}
	tableSQL := pgx.Identifier{table}.Sanitize()
	ddl := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			tenant_id text NOT NULL,
			app_name text NOT NULL,
			document_id text NOT NULL,
			name text NOT NULL DEFAULT '',
			content text NOT NULL,
			content_sha256 text NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
			metadata jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
			embedding vector(%d) NOT NULL,
			created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
			updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
			deleted_at timestamptz,
			PRIMARY KEY (tenant_id, app_name, document_id)
		)`, tableSQL, dimension)
	if _, err := tx.Exec(ctx, ddl); err != nil {
		return errors.New("prepare knowledge vector table")
	}
	// These columns make the vector table safe as a migration target: an old
	// session epoch or lower summary sequence can never silently replace newer
	// content, and a rollback can identify exactly which migration wrote it.
	if _, err := tx.Exec(ctx, fmt.Sprintf(`
		ALTER TABLE %s
			ADD COLUMN IF NOT EXISTS source_session_epoch text NOT NULL DEFAULT '',
			ADD COLUMN IF NOT EXISTS source_through_sequence bigint NOT NULL DEFAULT 0,
			ADD COLUMN IF NOT EXISTS source_summary_version bigint NOT NULL DEFAULT 0,
			ADD COLUMN IF NOT EXISTS embedding_model text NOT NULL DEFAULT '',
			ADD COLUMN IF NOT EXISTS migration_id text NOT NULL DEFAULT '',
			ADD COLUMN IF NOT EXISTS source_hash text NOT NULL DEFAULT '',
			ADD COLUMN IF NOT EXISTS target_hash text NOT NULL DEFAULT ''`, tableSQL)); err != nil {
		return errors.New("prepare knowledge vector migration metadata")
	}
	index := pgx.Identifier{table + "_tenant_app_active_idx"}.Sanitize()
	if _, err := tx.Exec(ctx, fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s
		(tenant_id, app_name, updated_at DESC) WHERE deleted_at IS NULL`, index, tableSQL)); err != nil {
		return errors.New("prepare knowledge vector scope index")
	}
	vectorIndex := pgx.Identifier{table + "_embedding_hnsw_idx"}.Sanitize()
	if _, err := tx.Exec(ctx, fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s
		USING hnsw (embedding vector_cosine_ops) WHERE deleted_at IS NULL`, vectorIndex, tableSQL)); err != nil {
		return errors.New("prepare knowledge vector similarity index")
	}
	return nil
}
