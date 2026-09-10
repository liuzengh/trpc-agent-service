package migrations

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	storage "github.com/XnLemon/trpc-agent-service/trpcservice/storage/mysql"
	driver "github.com/go-sql-driver/mysql"
)

// MySQLFiles is the ordered, immutable MySQL control-plane migration set.
// Runtime-storage migrations remain PostgreSQL-owned until their own adapter
// contract is introduced.
//
//go:embed mysql/000*_*.sql
var MySQLFiles embed.FS

const mysqlMigrationLock = "trpc-agent-service/control-plane-migrations"

type mysqlMigrationFile struct {
	version      int
	name, digest string
	statements   []string
}

type mysqlMigrationHistory struct {
	name, digest, status, errorText string
	statementIndex                  int
}

// ApplyMySQL runs MySQL control-plane migrations under a connection-scoped
// advisory lock. MySQL DDL implicitly commits, so each statement is
// checkpointed and failed versions remain recoverable on restart.
func ApplyMySQL(ctx context.Context, db *sql.DB) error {
	if ctx == nil || db == nil {
		return ErrMigration
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	files, err := orderedMySQLFiles()
	if err != nil {
		return err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return ErrMigration
	}
	defer func() { _ = conn.Close() }()
	if err := storage.AcquireLock(ctx, conn, mysqlMigrationLock, 30); err != nil {
		return ErrMigration
	}
	defer func() {
		_ = storage.ReleaseLock(context.Background(), conn, mysqlMigrationLock)
	}()
	if err := ensureMySQLHistory(ctx, conn); err != nil {
		return err
	}
	history, err := readMySQLHistory(ctx, conn)
	if err != nil {
		return err
	}
	if err := validateMySQLHistory(history, files); err != nil {
		return err
	}
	if err := applyMySQLFiles(ctx, conn, history, files); err != nil {
		return err
	}
	return nil
}

func applyMySQLFiles(ctx context.Context, conn *sql.Conn, history map[int]mysqlMigrationHistory, files []mysqlMigrationFile) error {
	for _, migration := range files {
		if err := applyOneMySQLMigration(ctx, conn, history, migration); err != nil {
			return err
		}
	}
	return nil
}

func applyOneMySQLMigration(ctx context.Context, conn *sql.Conn, history map[int]mysqlMigrationHistory, migration mysqlMigrationFile) error {
	entry, exists := history[migration.version]
	if exists && entry.digest != migration.digest {
		return fmt.Errorf("%w: version %d digest mismatch", ErrInvalidHistory, migration.version)
	}
	if exists && entry.status == "applied" {
		return nil
	}
	if !exists {
		if migration.version != nextMySQLVersion(history) {
			return fmt.Errorf("%w: version %d is not the next migration", ErrInvalidHistory, migration.version)
		}
		entry = mysqlMigrationHistory{name: migration.name, digest: migration.digest, status: "applying"}
		if err := insertMySQLHistory(ctx, conn, migration, entry); err != nil {
			return err
		}
		history[migration.version] = entry
	} else if entry.status != "applying" && entry.status != "failed" {
		return fmt.Errorf("%w: version %d has unknown status", ErrInvalidHistory, migration.version)
	}
	if entry.statementIndex < 0 || entry.statementIndex > len(migration.statements) {
		return fmt.Errorf("%w: version %d checkpoint out of range", ErrInvalidHistory, migration.version)
	}
	for index := entry.statementIndex; index < len(migration.statements); index++ {
		if err := applyMySQLStatement(ctx, conn, history, migration, index); err != nil {
			return err
		}
	}
	if err := updateMySQLCheckpoint(ctx, conn, migration.version, len(migration.statements), "applied", ""); err != nil {
		return fmt.Errorf("%w: record %s", ErrMigration, migration.name)
	}
	history[migration.version] = mysqlMigrationHistory{name: migration.name, digest: migration.digest, status: "applied", statementIndex: len(migration.statements)}
	return nil
}

func applyMySQLStatement(ctx context.Context, conn *sql.Conn, history map[int]mysqlMigrationHistory, migration mysqlMigrationFile, index int) error {
	if _, execErr := conn.ExecContext(ctx, migration.statements[index]); execErr != nil && !isMySQLAlreadyApplied(execErr) {
		_ = updateMySQLFailure(ctx, conn, migration.version, index)
		return fmt.Errorf("%w: %s", ErrMigration, migration.name)
	}
	if err := updateMySQLCheckpoint(ctx, conn, migration.version, index+1, "applying", ""); err != nil {
		return fmt.Errorf("%w: checkpoint %s", ErrMigration, migration.name)
	}
	history[migration.version] = mysqlMigrationHistory{name: migration.name, digest: migration.digest, status: "applying", statementIndex: index + 1}
	return nil
}

// VerifyMySQL checks MySQL migration history without mutating the database.
func VerifyMySQL(ctx context.Context, db *sql.DB) error {
	if ctx == nil || db == nil {
		return ErrMigration
	}
	files, err := orderedMySQLFiles()
	if err != nil {
		return err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return ErrMigration
	}
	defer func() { _ = conn.Close() }()
	history, err := readMySQLHistory(ctx, conn)
	if err != nil {
		return err
	}
	if len(history) != len(files) {
		return ErrInvalidHistory
	}
	for _, migration := range files {
		entry, ok := history[migration.version]
		if !ok || entry.status != "applied" || entry.digest != migration.digest || entry.statementIndex != len(migration.statements) {
			return ErrInvalidHistory
		}
	}
	return verifyMySQLSchema(ctx, conn)
}

type mysqlSchemaTable struct {
	name string
}

var requiredMySQLTables = []mysqlSchemaTable{
	{name: "schema_migrations"},
	{name: "tenant"},
	{name: "model_profile"},
	{name: "agent_app"},
	{name: "agent_app_revision"},
	{name: "agent_app_revision_tool"},
	{name: "backend_profile"},
	{name: "backend_profile_binding"},
	{name: "channel_binding"},
	{name: "tenant_status_change_outbox"},
	{name: "model_profile_change_outbox"},
	{name: "backend_profile_change_outbox"},
	{name: "agent_app_change_outbox"},
	{name: "channel_binding_change_outbox"},
	{name: "tenant_configuration_outbox"},
}

type mysqlSchemaIndex struct {
	table, name string
	unique      bool
	columns     []string
}

var requiredMySQLIndexes = []mysqlSchemaIndex{
	{table: "tenant", name: "tenant_key_idx", unique: true, columns: []string{"tenant_key"}},
	{table: "model_profile", name: "model_profile_key_idx", unique: true, columns: []string{"tenant_id", "profile_key"}},
	{table: "agent_app", name: "agent_app_key_idx", unique: true, columns: []string{"tenant_id", "app_key"}},
	{table: "agent_app", name: "agent_app_canary_revision_idx", unique: false, columns: []string{"tenant_id", "app_id", "canary_revision"}},
	{table: "backend_profile", name: "backend_profile_key_idx", unique: true, columns: []string{"tenant_id", "profile_key"}},
	{table: "channel_binding", name: "channel_binding_key_idx", unique: true, columns: []string{"tenant_id", "binding_key"}},
	{table: "channel_binding", name: "channel_binding_active_account_idx", unique: true, columns: []string{"channel", "active_provider_account_id"}},
	{table: "channel_binding", name: "channel_binding_candidate_idx", unique: false, columns: []string{"channel", "public_route_key_digest", "status"}},
}

type mysqlSchemaTrigger struct {
	name, table, event, timing string
	actionFragments            []string
	actionStatement            string
}

var requiredMySQLTriggers = []mysqlSchemaTrigger{
	{name: "agent_app_revision_guard_ins", table: "agent_app", event: "INSERT", timing: "BEFORE", actionFragments: []string{"new.current_revision", "from agent_app_revision", "new.status", "agent app current revision must be published"}},
	{name: "agent_app_revision_guard_upd", table: "agent_app", event: "UPDATE", timing: "BEFORE", actionFragments: []string{"new.current_revision", "from agent_app_revision", "new.status", "agent app current revision must be published"}},
	{name: "agent_app_canary_guard_upd", table: "agent_app", event: "UPDATE", timing: "BEFORE", actionFragments: []string{"new.canary_revision", "new.current_revision", "from agent_app_revision", "agent app canary revision must be published"}},
	{name: "agent_revision_immutable_upd", table: "agent_app_revision", event: "UPDATE", timing: "BEFORE", actionFragments: []string{"new.tenant_id <> old.tenant_id", "old.state = 'published'", "published agent app revision is immutable"}},
	{name: "agent_revision_immutable_del", table: "agent_app_revision", event: "DELETE", timing: "BEFORE", actionFragments: []string{"old.state = 'published'", "published agent app revision is immutable"}},
	{name: "agent_revision_tool_guard_ins", table: "agent_app_revision_tool", event: "INSERT", timing: "BEFORE", actionFragments: []string{"select state into revision_state", "revision_state = 'published'", "published agent app tool authorization is immutable"}},
	{name: "agent_revision_tool_guard_upd", table: "agent_app_revision_tool", event: "UPDATE", timing: "BEFORE", actionFragments: []string{"select state into revision_state", "revision_state = 'published'", "published agent app tool authorization is immutable"}},
	{name: "agent_revision_tool_guard_del", table: "agent_app_revision_tool", event: "DELETE", timing: "BEFORE", actionFragments: []string{"select state into revision_state", "revision_state = 'published'", "published agent app tool authorization is immutable"}},
	{name: "tenant_identity_immutable_upd", table: "tenant", event: "UPDATE", timing: "BEFORE", actionFragments: []string{"new.tenant_id <> old.tenant_id", "new.tenant_key <> old.tenant_key", "tenant identity is immutable"}},
	{name: "model_profile_identity_immutable_upd", table: "model_profile", event: "UPDATE", timing: "BEFORE", actionFragments: []string{"new.tenant_id <> old.tenant_id", "new.profile_id <> old.profile_id", "new.profile_key <> old.profile_key", "model profile identity is immutable"}},
	{name: "agent_app_identity_immutable_upd", table: "agent_app", event: "UPDATE", timing: "BEFORE", actionFragments: []string{"new.tenant_id <> old.tenant_id", "new.app_id <> old.app_id", "new.app_key <> old.app_key", "agent app identity is immutable"}},
	{name: "backend_profile_insert_guard", table: "backend_profile", event: "INSERT", timing: "BEFORE", actionFragments: []string{"new.status <> 'disabled'", "backend profile must be created disabled"}},
	{name: "backend_profile_lifecycle_guard", table: "backend_profile", event: "UPDATE", timing: "BEFORE", actionFragments: []string{"new.status <> 'disabled'", "not exists", "capability = 'session'", "non-disabled backend profile requires a binding"}},
	{name: "backend_profile_identity_guard", table: "backend_profile", event: "UPDATE", timing: "BEFORE", actionFragments: []string{"new.tenant_id <> old.tenant_id", "new.profile_id <> old.profile_id", "new.profile_key <> old.profile_key", "backend profile identity is immutable"}},
	{name: "backend_binding_identity_guard", table: "backend_profile_binding", event: "UPDATE", timing: "BEFORE", actionFragments: []string{"new.tenant_id <> old.tenant_id", "new.profile_id <> old.profile_id", "new.capability <> old.capability", "backend profile binding identity is immutable"}},
	{name: "backend_binding_delete_guard", table: "backend_profile_binding", event: "DELETE", timing: "AFTER", actionFragments: []string{"profile_status", "not exists", "capability = 'session'", "non-disabled backend profile requires a binding"}},
	{name: "channel_binding_identity_guard", table: "channel_binding", event: "UPDATE", timing: "BEFORE", actionFragments: []string{"new.tenant_id <> old.tenant_id", "new.binding_id <> old.binding_id", "new.binding_key <> old.binding_key", "new.channel <> old.channel", "channel binding identity is immutable"}},
}

func verifyMySQLSchema(ctx context.Context, conn *sql.Conn) error {
	rows, err := conn.QueryContext(ctx, `SELECT table_name, engine, table_collation
		FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, mysqlTableArgs()...)
	if err != nil {
		return ErrInvalidHistory
	}
	defer func() { _ = rows.Close() }()
	foundTables := make(map[string]bool, len(requiredMySQLTables))
	for rows.Next() {
		var name, engine, collation string
		if err := rows.Scan(&name, &engine, &collation); err != nil {
			return ErrInvalidHistory
		}
		if engine != "InnoDB" || collation != "utf8mb4_bin" {
			return ErrInvalidHistory
		}
		foundTables[name] = true
	}
	if err := rows.Err(); err != nil {
		return ErrInvalidHistory
	}
	for _, table := range requiredMySQLTables {
		if !foundTables[table.name] {
			return ErrInvalidHistory
		}
	}
	if err := verifyMySQLIndexes(ctx, conn); err != nil {
		return err
	}
	return verifyMySQLTriggers(ctx, conn)
}

func verifyMySQLIndexes(ctx context.Context, conn *sql.Conn) error {
	rows, err := conn.QueryContext(ctx, `SELECT table_name, index_name, non_unique, seq_in_index, column_name, sub_part
		FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND
		((table_name = ? AND index_name = ?) OR (table_name = ? AND index_name = ?) OR
		 (table_name = ? AND index_name = ?) OR (table_name = ? AND index_name = ?) OR
		 (table_name = ? AND index_name = ?) OR (table_name = ? AND index_name = ?) OR
		 (table_name = ? AND index_name = ?) OR (table_name = ? AND index_name = ?))`, mysqlIndexArgs()...)
	if err != nil {
		return ErrInvalidHistory
	}
	defer func() { _ = rows.Close() }()
	type foundIndex struct {
		unique  bool
		columns map[int]string
	}
	found := make(map[string]foundIndex, len(requiredMySQLIndexes))
	for rows.Next() {
		var table, name string
		var nonUnique, sequence int
		var column string
		var subPart sql.NullInt64
		if err := rows.Scan(&table, &name, &nonUnique, &sequence, &column, &subPart); err != nil {
			return ErrInvalidHistory
		}
		if subPart.Valid || sequence < 1 || (nonUnique != 0 && nonUnique != 1) {
			return ErrInvalidHistory
		}
		key := table + "/" + name
		entry := found[key]
		if entry.columns == nil {
			entry.columns = make(map[int]string)
			entry.unique = nonUnique == 0
		}
		if entry.unique != (nonUnique == 0) || entry.columns[sequence] != "" {
			return ErrInvalidHistory
		}
		entry.columns[sequence] = column
		found[key] = entry
	}
	if err := rows.Err(); err != nil {
		return ErrInvalidHistory
	}
	for _, index := range requiredMySQLIndexes {
		key := index.table + "/" + index.name
		entry, ok := found[key]
		if !ok || entry.unique != index.unique || len(entry.columns) != len(index.columns) {
			return ErrInvalidHistory
		}
		for sequence, column := range index.columns {
			if entry.columns[sequence+1] != column {
				return ErrInvalidHistory
			}
		}
	}
	return nil
}

func verifyMySQLTriggers(ctx context.Context, conn *sql.Conn) error {
	rows, err := conn.QueryContext(ctx, `SELECT trigger_name, event_manipulation, event_object_table,
		action_timing, action_statement
		FROM information_schema.triggers
		WHERE trigger_schema = DATABASE() AND trigger_name IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, mysqlTriggerArgs()...)
	if err != nil {
		return ErrInvalidHistory
	}
	defer func() { _ = rows.Close() }()
	found := make(map[string]mysqlSchemaTrigger, len(requiredMySQLTriggers))
	for rows.Next() {
		var trigger mysqlSchemaTrigger
		var actionStatement string
		if err := rows.Scan(&trigger.name, &trigger.event, &trigger.table, &trigger.timing, &actionStatement); err != nil {
			return ErrInvalidHistory
		}
		trigger.actionStatement = actionStatement
		found[trigger.name] = trigger
	}
	if err := rows.Err(); err != nil {
		return ErrInvalidHistory
	}
	for _, required := range requiredMySQLTriggers {
		foundTrigger, ok := found[required.name]
		if !ok || foundTrigger.table != required.table || foundTrigger.event != required.event || foundTrigger.timing != required.timing || !mysqlTriggerActionMatches(foundTrigger.actionStatement, required.actionFragments) {
			return ErrInvalidHistory
		}
	}
	return nil
}

func mysqlTriggerActionMatches(action string, required []string) bool {
	action = strings.Join(strings.Fields(strings.ToLower(action)), " ")
	for _, fragment := range required {
		fragment = strings.Join(strings.Fields(strings.ToLower(fragment)), " ")
		if fragment == "" || !strings.Contains(action, fragment) {
			return false
		}
	}
	return true
}

func mysqlTableArgs() []any {
	args := make([]any, 0, len(requiredMySQLTables))
	for _, table := range requiredMySQLTables {
		args = append(args, table.name)
	}
	return args
}

func mysqlIndexArgs() []any {
	args := make([]any, 0, len(requiredMySQLIndexes)*2)
	for _, index := range requiredMySQLIndexes {
		args = append(args, index.table, index.name)
	}
	return args
}

func mysqlTriggerArgs() []any {
	args := make([]any, 0, len(requiredMySQLTriggers))
	for _, trigger := range requiredMySQLTriggers {
		args = append(args, trigger.name)
	}
	return args
}

func orderedMySQLFiles() ([]mysqlMigrationFile, error) {
	entries, err := fs.Glob(MySQLFiles, "mysql/000*_*.sql")
	if err != nil {
		return nil, fmt.Errorf("%w: list MySQL files", ErrMigration)
	}
	files := make([]mysqlMigrationFile, 0, len(entries))
	for _, name := range entries {
		base := filepath.Base(name)
		parts := strings.SplitN(base, "_", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("%w: invalid MySQL file %s", ErrInvalidHistory, base)
		}
		version, err := strconv.Atoi(parts[0])
		if err != nil || version < 1 {
			return nil, fmt.Errorf("%w: invalid MySQL version %s", ErrInvalidHistory, base)
		}
		contents, err := fs.ReadFile(MySQLFiles, name)
		if err != nil {
			return nil, fmt.Errorf("%w: read MySQL %s", ErrMigration, base)
		}
		statements := splitMySQLStatements(string(contents))
		if len(statements) == 0 {
			return nil, fmt.Errorf("%w: empty MySQL migration %s", ErrInvalidHistory, base)
		}
		files = append(files, mysqlMigrationFile{version: version, name: base, digest: fmt.Sprintf("%x", sha256.Sum256(contents)), statements: statements})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].version < files[j].version })
	for i := 1; i < len(files); i++ {
		if files[i-1].version == files[i].version {
			return nil, fmt.Errorf("%w: duplicate MySQL version %d", ErrInvalidHistory, files[i].version)
		}
	}
	return files, nil
}

func ensureMySQLHistory(ctx context.Context, conn *sql.Conn) error {
	_, err := conn.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INT NOT NULL,
    name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    statement_index INT NOT NULL DEFAULT 0,
    error_text VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT '',
    applied_at DATETIME(6) NULL,
    PRIMARY KEY (version)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin`)
	if err != nil {
		return fmt.Errorf("%w: MySQL history table", ErrMigration)
	}
	return nil
}

func readMySQLHistory(ctx context.Context, conn *sql.Conn) (map[int]mysqlMigrationHistory, error) {
	rows, err := conn.QueryContext(ctx, "SELECT version, name, sha256, status, statement_index, error_text FROM schema_migrations ORDER BY version")
	if err != nil {
		return nil, fmt.Errorf("%w: read MySQL history", ErrInvalidHistory)
	}
	defer func() { _ = rows.Close() }()
	history := map[int]mysqlMigrationHistory{}
	for rows.Next() {
		var version, index int
		var entry mysqlMigrationHistory
		if err := rows.Scan(&version, &entry.name, &entry.digest, &entry.status, &index, &entry.errorText); err != nil {
			return nil, fmt.Errorf("%w: scan MySQL history", ErrInvalidHistory)
		}
		entry.statementIndex = index
		history[version] = entry
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate MySQL history", ErrInvalidHistory)
	}
	return history, nil
}

func validateMySQLHistory(history map[int]mysqlMigrationHistory, files []mysqlMigrationFile) error {
	if len(files) == 0 {
		return fmt.Errorf("%w: no embedded MySQL migrations", ErrInvalidHistory)
	}
	maxVersion := files[len(files)-1].version
	for version, entry := range history {
		if version < 1 || version > maxVersion || (entry.status != "applied" && entry.status != "applying" && entry.status != "failed") {
			return fmt.Errorf("%w: unknown MySQL history version %d", ErrInvalidHistory, version)
		}
	}
	return nil
}

func nextMySQLVersion(history map[int]mysqlMigrationHistory) int {
	max := 0
	for version := range history {
		if version > max {
			max = version
		}
	}
	return max + 1
}

func insertMySQLHistory(ctx context.Context, conn *sql.Conn, migration mysqlMigrationFile, entry mysqlMigrationHistory) error {
	_, err := conn.ExecContext(ctx, `INSERT INTO schema_migrations(version, name, sha256, status, statement_index, error_text) VALUES (?, ?, ?, ?, ?, ?)`, migration.version, migration.name, migration.digest, entry.status, entry.statementIndex, entry.errorText)
	if err != nil {
		return fmt.Errorf("%w: record %s", ErrMigration, migration.name)
	}
	return nil
}

func updateMySQLCheckpoint(ctx context.Context, conn *sql.Conn, version, statementIndex int, status, errorText string) error {
	var appliedAt any
	if status == "applied" {
		appliedAt = time.Now().UTC()
	}
	_, err := conn.ExecContext(ctx, `UPDATE schema_migrations SET status = ?, statement_index = ?, error_text = ?, applied_at = ? WHERE version = ?`, status, statementIndex, errorText, appliedAt, version)
	return err
}

func updateMySQLFailure(ctx context.Context, conn *sql.Conn, version, statementIndex int) error {
	_, err := conn.ExecContext(ctx, `UPDATE schema_migrations SET status = 'failed', statement_index = ?, error_text = ? WHERE version = ?`, statementIndex, "statement failed", version)
	return err
}

func isMySQLAlreadyApplied(err error) bool {
	var mysqlErr *driver.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	switch mysqlErr.Number {
	case 1022, 1050, 1060, 1061, 1359, 1826:
		return true
	default:
		return false
	}
}

// splitMySQLStatements separates ordinary DDL statements while respecting
// quoted strings, identifiers, SQL comments, and compound trigger bodies. The
// runner still executes one statement at a time rather than enabling
// multiStatements on the DSN.
//
//nolint:gocyclo // The scanner keeps quote, comment, and compound-body state in one pass.
func splitMySQLStatements(script string) []string {
	var statements []string
	start := 0
	compoundDepth := 0
	skipCompoundKeyword := false
	for i := 0; i < len(script); {
		if end, ok := skipMySQLComment(script, i); ok {
			i = end
			continue
		}
		if end, ok := skipMySQLQuote(script, i); ok {
			i = end
			continue
		}
		if isMySQLIdentifierStart(script[i]) {
			end := i + 1
			for end < len(script) && isMySQLIdentifierPart(script[end]) {
				end++
			}
			word := strings.ToUpper(script[i:end])
			if skipCompoundKeyword {
				skipCompoundKeyword = false
			} else {
				switch word {
				case "BEGIN":
					compoundDepth++
				case "IF", "CASE", "LOOP", "WHILE", "REPEAT":
					// IF NOT EXISTS and similar top-level DDL clauses are
					// not compound blocks. Only count control-flow keywords
					// after a compound statement has started.
					if compoundDepth > 0 {
						compoundDepth++
					}
				case "END":
					compoundDepth--
					if compoundDepth < 0 {
						compoundDepth = 0
					}
					next, nextEnd := nextMySQLWord(script, end)
					if next == "IF" || next == "CASE" || next == "LOOP" || next == "WHILE" || next == "REPEAT" {
						// Do not count the qualifier in END IF/CASE/LOOP/
						// WHILE/REPEAT as a new opening block.
						skipCompoundKeyword = true
						_ = nextEnd
					}
				}
			}
			i = end
			continue
		}
		if script[i] == ';' {
			if compoundDepth == 0 {
				if statement := strings.TrimSpace(script[start:i]); statement != "" {
					statements = append(statements, statement)
				}
				start = i + 1
			}
			i++
			continue
		}
		i++
	}
	if statement := strings.TrimSpace(script[start:]); statement != "" {
		statements = append(statements, statement)
	}
	return statements
}

func isMySQLIdentifierStart(ch byte) bool {
	return ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch == '_'
}

func isMySQLIdentifierPart(ch byte) bool {
	return isMySQLIdentifierStart(ch) || ch >= '0' && ch <= '9' || ch == '$'
}

func nextMySQLWord(script string, start int) (string, int) {
	for start < len(script) {
		if script[start] == ' ' || script[start] == '\t' || script[start] == '\n' || script[start] == '\r' {
			start++
			continue
		}
		if !isMySQLIdentifierStart(script[start]) {
			return "", start
		}
		end := start + 1
		for end < len(script) && isMySQLIdentifierPart(script[end]) {
			end++
		}
		return strings.ToUpper(script[start:end]), end
	}
	return "", start
}

func skipMySQLComment(script string, start int) (int, bool) {
	if start >= len(script) || script[start] == '\n' {
		return start, false
	}
	if script[start] == '#' || (script[start] == '-' && start+2 < len(script) && script[start+1] == '-' && isMySQLCommentWhitespace(script[start+2])) {
		for end := start + 1; end < len(script); end++ {
			if script[end] == '\n' {
				return end + 1, true
			}
		}
		return len(script), true
	}
	if script[start] != '/' || start+1 >= len(script) || script[start+1] != '*' {
		return start, false
	}
	for end := start + 2; end+1 < len(script); end++ {
		if script[end] == '*' && script[end+1] == '/' {
			return end + 2, true
		}
	}
	return len(script), true
}

func isMySQLCommentWhitespace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func skipMySQLQuote(script string, start int) (int, bool) {
	if start >= len(script) || (script[start] != '\'' && script[start] != '"' && script[start] != '`') {
		return start, false
	}
	quote := script[start]
	for end := start + 1; end < len(script); end++ {
		switch script[end] {
		case '\\':
			end++
		case quote:
			if end+1 < len(script) && script[end+1] == quote {
				end++
				continue
			}
			return end + 1, true
		}
	}
	return len(script), true
}
