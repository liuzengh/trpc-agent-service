package recovery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

// tenantTables is the exact expected set of tenant-scoped business tables of
// the current release. It mirrors the array installed by migration
// 000010_p2_01_row_level_security; the integration suite asserts both stay
// in sync. schema_migration is part of the expected catalog but its DATA is
// never written into an archive: migrations themselves are the schema
// authority, the archive carries business facts only.
var tenantTables = []string{
	"tenant", "agent_app", "channel_binding", "user_identity",
	"session", "session_event", "message_dedup", "memory", "summary",
	"artifact", "audit_log", "outbox_message", "dead_letter",
	"agent_release", "tenant_config_version", "coordination_epoch",
	"session_lease", "execution_result", "job_queue",
	"channel_binding_audit", "vector_projection_task",
	"vector_rebuild_run", "tenant_config_rollout", "tenant_config_operation",
	"capacity_reservation", "capacity_budget",
}

// migrationCatalogTable is the migration directory table; present in the
// database, exempt from RLS, owner-only and excluded from archives.
const migrationCatalogTable = "schema_migration"

// ArchiveTables returns the ordered list of business tables whose TABLE DATA
// an archive is allowed to contain.
func ArchiveTables() []string {
	out := make([]string, len(tenantTables))
	copy(out, tenantTables)
	return out
}

// expectedCatalogTables is the full expected table set of one release schema.
func expectedCatalogTables() map[string]struct{} {
	set := make(map[string]struct{}, len(tenantTables)+1)
	for _, t := range tenantTables {
		set[t] = struct{}{}
	}
	set[migrationCatalogTable] = struct{}{}
	return set
}

// catalogSchemaInfo captures the non-sensitive structural identity of one
// schema: its table set and a fingerprint over the sorted column metadata.
type catalogSchemaInfo struct {
	Schema      string
	Fingerprint string
}

// readCatalogTables resolves the current schema and verifies the table set
// matches the expected release set exactly: unknown tables and missing
// tables both fail closed (unknown catalog objects must never be dumped or
// restored over silently).
func readCatalogTables(ctx context.Context, conn pgxQuery) (string, error) {
	var schema string
	if err := conn.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		return "", classifyQueryError("catalog schema", err)
	}
	if strings.TrimSpace(schema) == "" {
		return "", fmt.Errorf("%w: current schema is empty", ErrForbiddenState)
	}
	rows, err := conn.Query(ctx, `SELECT tablename FROM pg_tables WHERE schemaname = $1`, schema)
	if err != nil {
		return "", classifyQueryError("catalog tables", err)
	}
	defer rows.Close()
	found := map[string]struct{}{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return "", classifyQueryError("catalog tables scan", err)
		}
		found[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return "", classifyQueryError("catalog tables iterate", err)
	}
	expected := expectedCatalogTables()
	var unknown, missing []string
	for name := range found {
		if _, ok := expected[name]; !ok {
			unknown = append(unknown, name)
		}
	}
	for name := range expected {
		if _, ok := found[name]; !ok {
			missing = append(missing, name)
		}
	}
	if len(unknown) > 0 || len(missing) > 0 {
		sort.Strings(unknown)
		sort.Strings(missing)
		return "", fmt.Errorf("%w: catalog drift unknown_tables=%d missing_tables=%d", ErrForbiddenState, len(unknown), len(missing))
	}
	return schema, nil
}

// readSchemaFingerprint hashes the structural column metadata of the schema.
// It contains table/column names and types only: no rows, no identifiers,
// no tenant data.
func readSchemaFingerprint(ctx context.Context, conn pgxQuery, schema string) (string, error) {
	rows, err := conn.Query(ctx, `SELECT table_name, ordinal_position, column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_schema = $1
		ORDER BY table_name, ordinal_position`, schema)
	if err != nil {
		return "", classifyQueryError("schema fingerprint", err)
	}
	defer rows.Close()
	hash := sha256.New()
	for rows.Next() {
		var table, column, dataType, nullable string
		var ordinal int
		if err := rows.Scan(&table, &ordinal, &column, &dataType, &nullable); err != nil {
			return "", classifyQueryError("schema fingerprint scan", err)
		}
		fmt.Fprintf(hash, "%s|%d|%s|%s|%s\n", table, ordinal, column, dataType, nullable)
	}
	if err := rows.Err(); err != nil {
		return "", classifyQueryError("schema fingerprint iterate", err)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// readRowCounts returns the row count of every archive table from one
// connection/transaction so the counts and the pg_dump archive are taken
// from the same PostgreSQL snapshot.
func readRowCounts(ctx context.Context, conn pgxQuery, schema string, tables []string) (map[string]int64, error) {
	counts := make(map[string]int64, len(tables))
	for _, table := range tables {
		var count int64
		// Table names come from the fixed catalog constant, never from user
		// input; identifiers are sanitized through pgx.Identifier.
		query := fmt.Sprintf(`SELECT count(*) FROM %s`, pgx.Identifier{schema, table}.Sanitize())
		if err := conn.QueryRow(ctx, query).Scan(&count); err != nil {
			return nil, classifyQueryError("row count", err)
		}
		counts[table] = count
	}
	return counts, nil
}

// requirePrivilegedRole enforces the offline restore/backup role rule: the
// operator connection must bypass row level security (superuser or BYPASSRLS)
// because FORCE RLS on every tenant table would otherwise reject snapshot
// reads and data-only COPY for multi-tenant rows. This is the explicitly
// bounded high-privilege offline path; runtime roles stay NOBYPASSRLS.
func requirePrivilegedRole(ctx context.Context, conn pgxQuery, forbidRole string) error {
	var name string
	var super, bypass bool
	if err := conn.QueryRow(ctx, `SELECT current_user, r.rolsuper, r.rolbypassrls
		FROM pg_roles r WHERE r.rolname = current_user`).Scan(&name, &super, &bypass); err != nil {
		return classifyQueryError("role check", err)
	}
	if forbidRole != "" && name == forbidRole {
		return fmt.Errorf("%w: runtime business role must not run recovery operations", ErrForbiddenState)
	}
	if !super && !bypass {
		return fmt.Errorf("%w: recovery operator role lacks the required FORCE-RLS exemption", ErrForbiddenState)
	}
	return nil
}

// pgxQuery is the shared query surface of *pgxpool.Pool, pgxpool.Conn,
// pgx.Tx and pgxpool.Pool conn transactions.
type pgxQuery interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// classifyQueryError reduces database failures to stable sentinel errors
// without embedding raw backend error text.
func classifyQueryError(step string, err error) error {
	if err == nil {
		return nil
	}
	switch {
	case ctxErr(err):
		return fmt.Errorf("%w: %s", ErrTimeoutOrCancelled, step)
	default:
		return fmt.Errorf("%w: %s", ErrDependencyUnavailable, step)
	}
}

func ctxErr(err error) bool {
	type canceller interface{ Unwrap() error }
	for e := err; e != nil; {
		if e == context.Canceled || e == context.DeadlineExceeded {
			return true
		}
		u, ok := e.(canceller)
		if !ok {
			return false
		}
		e = u.Unwrap()
	}
	return false
}
