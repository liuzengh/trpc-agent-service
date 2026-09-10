// Package mysql provides the MySQL connection, transaction, error, and
// serialization primitives shared by the control-plane repositories.
// Domain packages own their SQL and use this package only for the portable
// database/sql boundary.
package mysql

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	driver "github.com/go-sql-driver/mysql"
)

// ErrStorage is the stable error category returned for unexpected database
// failures. Driver diagnostics, DSNs, and server metadata never cross this
// boundary.
var ErrStorage = errors.New("mysql storage error")

// Options configures a database/sql pool opened by Open.
type Options struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

// Open creates and pings a MySQL database/sql pool. MySQL timestamps are
// decoded as time.Time and the session uses UTC regardless of server defaults.
func Open(ctx context.Context, dsn string, options Options) (*sql.DB, error) {
	if ctx == nil {
		return nil, ErrStorage
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dsn = normalizeDSN(dsn)
	if dsn == "" {
		return nil, ErrStorage
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, ErrStorage
	}
	if options.MaxOpenConns > 0 {
		db.SetMaxOpenConns(options.MaxOpenConns)
	}
	if options.MaxIdleConns > 0 {
		db.SetMaxIdleConns(options.MaxIdleConns)
	}
	if options.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(options.ConnMaxLifetime)
	}
	if options.ConnMaxIdleTime > 0 {
		db.SetConnMaxIdleTime(options.ConnMaxIdleTime)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, MapError(ctx, err, ErrStorage, ErrStorage, ErrStorage, ErrStorage)
	}
	return db, nil
}

// Ping is used by readiness probes and does not disclose driver errors.
func Ping(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return ErrStorage
	}
	if ctx == nil {
		return ErrStorage
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := db.PingContext(ctx); err != nil {
		return MapError(ctx, err, ErrStorage, ErrStorage, ErrStorage, ErrStorage)
	}
	return nil
}

// normalizeDSN makes the timestamp and session-time-zone behavior explicit.
// Callers may still provide additional driver parameters, including
// multiStatements when using an external migration tool.
func normalizeDSN(dsn string) string {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return ""
	}
	parts := strings.SplitN(dsn, "?", 2)
	base := parts[0]
	parameters := make([]string, 0, 3)
	if len(parts) == 2 {
		for _, parameter := range strings.Split(parts[1], "&") {
			key, _, _ := strings.Cut(parameter, "=")
			switch strings.ToLower(strings.TrimSpace(key)) {
			case "parsetime", "loc", "time_zone":
				// These values are part of the storage contract and are
				// replaced below even when a caller supplies a conflicting value.
				continue
			default:
				if strings.TrimSpace(parameter) != "" {
					parameters = append(parameters, parameter)
				}
			}
		}
	}
	// loc controls Go's time.Time decoding only. time_zone is a driver system
	// variable and is encoded as a SQL literal so every pooled connection uses
	// the UTC MySQL session timezone as well.
	parameters = append(parameters, "parseTime=true", "loc=UTC", "time_zone=%27%2B00%3A00%27")
	return base + "?" + strings.Join(parameters, "&")
}

// Queryer is the shared read/write surface implemented by sql.DB, sql.Tx,
// and sql.Conn.
type Queryer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// RowScanner is implemented by sql.Row and sql.Rows.
type RowScanner interface{ Scan(...any) error }

// Begin starts an explicit READ COMMITTED transaction. PostgreSQL control
// plane repositories use the same level, so switching drivers does not alter
// snapshot or lock semantics.
func Begin(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	if db == nil {
		return nil, ErrStorage
	}
	if ctx == nil {
		return nil, ErrStorage
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, MapError(ctx, err, ErrStorage, ErrStorage, ErrStorage, ErrStorage)
	}
	return tx, nil
}

// BeginConn pins a transaction to the supplied connection. It is used for
// MySQL named locks because GET_LOCK is connection-scoped.
func BeginConn(ctx context.Context, conn *sql.Conn) (*sql.Tx, error) {
	if conn == nil {
		return nil, ErrStorage
	}
	if ctx == nil {
		return nil, ErrStorage
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, MapError(ctx, err, ErrStorage, ErrStorage, ErrStorage, ErrStorage)
	}
	return tx, nil
}

// AcquireLock obtains a named MySQL lock on a pinned connection.
func AcquireLock(ctx context.Context, conn *sql.Conn, name string, timeoutSeconds int) error {
	if conn == nil || strings.TrimSpace(name) == "" {
		return ErrStorage
	}
	if ctx == nil {
		return ErrStorage
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var acquired sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", name, timeoutSeconds).Scan(&acquired); err != nil {
		return MapError(ctx, err, ErrStorage, ErrStorage, ErrStorage, ErrStorage)
	}
	if !acquired.Valid || acquired.Int64 != 1 {
		return ErrStorage
	}
	return nil
}

// ReleaseLock explicitly releases a named MySQL lock on its pinned
// connection. It is safe to call during deferred cleanup.
func ReleaseLock(ctx context.Context, conn *sql.Conn, name string) error {
	if conn == nil || strings.TrimSpace(name) == "" {
		return ErrStorage
	}
	if ctx == nil {
		return ErrStorage
	}
	var released sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", name).Scan(&released); err != nil {
		return MapError(ctx, err, ErrStorage, ErrStorage, ErrStorage, ErrStorage)
	}
	if !released.Valid || released.Int64 != 1 {
		return ErrStorage
	}
	return nil
}

// CurrentUser returns the authenticated MySQL account identity without
// exposing driver diagnostics. Bootstrap uses it to prove that migration and
// application connections are not the same account.
func CurrentUser(ctx context.Context, db *sql.DB) (string, error) {
	if db == nil || ctx == nil {
		return "", ErrStorage
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	var user string
	if err := db.QueryRowContext(ctx, "SELECT CURRENT_USER()").Scan(&user); err != nil {
		return "", MapError(ctx, err, ErrStorage, ErrStorage, ErrStorage, ErrStorage)
	}
	if strings.TrimSpace(user) == "" {
		return "", ErrStorage
	}
	return user, nil
}

func currentGranteeIdentity(user string) (string, error) {
	separator := strings.LastIndexByte(user, '@')
	if user == "" || separator < 0 || separator == len(user)-1 {
		return "", ErrStorage
	}
	escape := func(value string) string {
		return "'" + strings.ReplaceAll(value, "'", "''") + "'"
	}
	return escape(user[:separator]) + "@" + escape(user[separator+1:]), nil
}

// verifyNoDirectRoutinePrivileges checks the grants visible to the current
// account without requiring SELECT access to mysql.procs_priv. SHOW GRANTS is
// always available for the authenticated account and is the only portable way
// for the restricted application connection to detect a direct routine grant.
func verifyNoDirectRoutinePrivileges(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, "SHOW GRANTS")
	if err != nil {
		return MapError(ctx, err, ErrStorage, ErrStorage, ErrStorage, ErrStorage)
	}
	defer func() { _ = rows.Close() }()
	var grant string
	for rows.Next() {
		if err := rows.Scan(&grant); err != nil {
			return MapError(ctx, err, ErrStorage, ErrStorage, ErrStorage, ErrStorage)
		}
		normalized := strings.ToUpper(grant)
		if strings.Contains(normalized, "GRANT PROXY ON ") ||
			strings.Contains(normalized, " ON PROCEDURE ") ||
			strings.Contains(normalized, " ON FUNCTION ") {
			return ErrStorage
		}
	}
	if err := rows.Err(); err != nil {
		return MapError(ctx, err, ErrStorage, ErrStorage, ErrStorage, ErrStorage)
	}
	return nil
}

// CurrentDatabase returns the selected MySQL schema for the current session.
// Bootstrap compares the migration and application sessions so a successful
// migration can never be mistaken for readiness of a different database.
func CurrentDatabase(ctx context.Context, db *sql.DB) (string, error) {
	if db == nil || ctx == nil {
		return "", ErrStorage
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	var database sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&database); err != nil {
		return "", MapError(ctx, err, ErrStorage, ErrStorage, ErrStorage, ErrStorage)
	}
	if !database.Valid || strings.TrimSpace(database.String) == "" {
		return "", ErrStorage
	}
	return database.String, nil
}

// VerifyApplicationPrivileges fails closed when the connected MySQL account
// does not exactly match the control-plane table-level DML allowlist.
// Migrations run through a separate account; the application account is not
// allowed any global/schema/column grant, routine grant, role grant, grant
// option, or table outside the selected control-plane database.
func VerifyApplicationPrivileges(ctx context.Context, db *sql.DB) error {
	if db == nil || ctx == nil {
		return ErrStorage
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := CurrentDatabase(ctx, db); err != nil {
		return err
	}
	currentUser, err := CurrentUser(ctx, db)
	if err != nil {
		return err
	}
	currentGrantee, err := currentGranteeIdentity(currentUser)
	if err != nil {
		return err
	}
	var forbidden int
	if err := db.QueryRowContext(ctx, `WITH effective_grantees (grantee) AS (
			SELECT ?
			UNION
			SELECT CONCAT(CHAR(39), role_name, CHAR(39), '@', CHAR(39), role_host, CHAR(39))
			FROM information_schema.enabled_roles
		), allowed_tables (table_name) AS (
			SELECT 'tenant' UNION ALL
			SELECT 'model_profile' UNION ALL
			SELECT 'agent_app' UNION ALL
			SELECT 'agent_app_revision' UNION ALL
			SELECT 'agent_app_revision_tool' UNION ALL
			SELECT 'backend_profile' UNION ALL
			SELECT 'backend_profile_binding' UNION ALL
			SELECT 'channel_binding' UNION ALL
			SELECT 'tenant_status_change_outbox' UNION ALL
			SELECT 'model_profile_change_outbox' UNION ALL
			SELECT 'backend_profile_change_outbox' UNION ALL
			SELECT 'agent_app_change_outbox' UNION ALL
			SELECT 'channel_binding_change_outbox' UNION ALL
			SELECT 'tenant_configuration_outbox'
		), required_privilege_types (privilege_type) AS (
			SELECT 'SELECT' UNION ALL
			SELECT 'INSERT' UNION ALL
			SELECT 'UPDATE' UNION ALL
			SELECT 'DELETE'
		), required_privileges (table_name, privilege_type) AS (
			SELECT allowed_tables.table_name, required_privilege_types.privilege_type
			FROM allowed_tables CROSS JOIN required_privilege_types
		), effective_table_privileges (table_schema, table_name, privilege_type, is_grantable) AS (
			SELECT table_schema, table_name, privilege_type, is_grantable
			FROM information_schema.table_privileges
			WHERE grantee IN (SELECT grantee FROM effective_grantees)
			UNION ALL
			SELECT table_schema, table_name, privilege_type, is_grantable
			FROM information_schema.role_table_grants
			WHERE CONCAT(CHAR(39), grantee, CHAR(39), '@', CHAR(39), grantee_host, CHAR(39)) IN (SELECT grantee FROM effective_grantees)
		)
		SELECT COUNT(*)
		FROM (
			SELECT privilege_type FROM information_schema.user_privileges
			WHERE grantee IN (SELECT grantee FROM effective_grantees)
			  AND (privilege_type <> 'USAGE' OR is_grantable <> 'NO')
			UNION ALL
			SELECT privilege_type FROM information_schema.schema_privileges
			WHERE grantee IN (SELECT grantee FROM effective_grantees)
			UNION ALL
			SELECT privilege_type FROM effective_table_privileges
			WHERE table_schema <> DATABASE()
			   OR table_name NOT IN (SELECT table_name FROM allowed_tables)
			   OR privilege_type NOT IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE')
			   OR is_grantable <> 'NO'
			UNION ALL
			SELECT privilege_type FROM information_schema.column_privileges
			WHERE grantee IN (SELECT grantee FROM effective_grantees)
			UNION ALL
			SELECT privilege_type FROM information_schema.role_column_grants
			WHERE CONCAT(CHAR(39), grantee, CHAR(39), '@', CHAR(39), grantee_host, CHAR(39)) IN (SELECT grantee FROM effective_grantees)
			UNION ALL
			SELECT role_name FROM information_schema.applicable_roles
			UNION ALL
			SELECT rp.table_name
			FROM required_privileges AS rp
			WHERE NOT EXISTS (
				SELECT 1
				FROM effective_table_privileges AS etp
				WHERE etp.table_schema = DATABASE()
				  AND etp.table_name = rp.table_name
				  AND etp.privilege_type = rp.privilege_type
				  AND etp.is_grantable = 'NO'
			)
			UNION ALL
			SELECT privilege_type FROM information_schema.role_routine_grants
			WHERE CONCAT(CHAR(39), grantee, CHAR(39), '@', CHAR(39), grantee_host, CHAR(39)) IN (SELECT grantee FROM effective_grantees)
		) AS violations`, currentGrantee).Scan(&forbidden); err != nil {
		return MapError(ctx, err, ErrStorage, ErrStorage, ErrStorage, ErrStorage)
	}
	if forbidden != 0 {
		return ErrStorage
	}
	return verifyNoDirectRoutinePrivileges(ctx, db)
}

// Rollback makes a best-effort rollback for a transaction that did not
// commit.
func Rollback(tx *sql.Tx) {
	if tx != nil {
		_ = tx.Rollback()
	}
}

// Commit preserves cancellation and error-redaction behavior.
func Commit(ctx context.Context, tx *sql.Tx) error {
	if tx == nil {
		return ErrStorage
	}
	if ctx == nil {
		Rollback(tx)
		return ErrStorage
	}
	if err := ctx.Err(); err != nil {
		Rollback(tx)
		return err
	}
	if err := tx.Commit(); err != nil {
		return MapError(ctx, err, ErrStorage, ErrStorage, ErrStorage, ErrStorage)
	}
	return nil
}

// MapError maps portable SQL and MySQL errors to caller-provided domain
// categories while retaining context cancellation.
func MapError(ctx context.Context, err error, notFound, duplicate, conflict, invalid error) error {
	if err == nil {
		return nil
	}
	if ctx != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, sql.ErrNoRows) {
		return notFound
	}
	var mysqlErr *driver.MySQLError
	if errors.As(err, &mysqlErr) {
		switch mysqlErr.Number {
		case 1062:
			return duplicate
		case 1205, 1213:
			return conflict
		case 1048, 1264, 1366, 1406, 1451, 1452, 3819:
			return invalid
		}
	}
	return ErrStorage
}

// AsUTC normalizes persisted timestamps at the storage boundary.
func AsUTC(value time.Time) time.Time { return value.UTC() }

// NullableInt converts a nullable SQL integer to an owned optional value.
func NullableInt(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	copy := value.Int64
	return &copy
}

// NullableString converts a nullable SQL string to an owned optional value.
func NullableString(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	copy := value.String
	return &copy
}

// NullableText represents an optional persisted string argument.
func NullableText(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

// MonotonicNow produces a UTC timestamp that never predates an existing
// persisted timestamp.
func MonotonicNow(previous time.Time) time.Time {
	now := time.Now().UTC()
	if now.Before(previous) {
		return previous
	}
	return now
}
