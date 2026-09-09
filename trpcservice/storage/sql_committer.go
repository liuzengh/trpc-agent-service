package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/persistence"
	"trpc.group/trpc-go/trpc-agent-go/session"
	mysqlstorage "trpc.group/trpc-go/trpc-agent-go/storage/mysql"
	postgresstorage "trpc.group/trpc-go/trpc-agent-go/storage/postgres"
)

type sqlDialect string

const (
	dialectPostgres sqlDialect = "postgres"
	dialectMySQL    sqlDialect = "mysql"
)

type sqlTurnCommitter struct {
	dialect        sqlDialect
	schema         string
	prefix         string
	stateTable     string
	eventTable     string
	trackTable     string
	summaryTable   string
	appStateTable  string
	userStateTable string
	memoryTable    string
	headTable      string
	commitTable    string
	skipDBInit     bool
	transaction    func(context.Context, func(*sql.Tx) error) error
	exec           func(context.Context, string, ...any) (sql.Result, error)
}

type sqlColumnContract struct {
	table     string
	column    string
	dataType  string
	precision *int64
}

func newPostgresCommitter(client postgresstorage.Client, schema, prefix string, skipDBInit bool) *sqlTurnCommitter {
	return &sqlTurnCommitter{
		dialect: dialectPostgres, schema: schema, prefix: prefix, skipDBInit: skipDBInit,
		stateTable: quotePostgres(schema, prefix+"session_states"), eventTable: quotePostgres(schema, prefix+"session_events"),
		trackTable: quotePostgres(schema, prefix+"session_track_events"), summaryTable: quotePostgres(schema, prefix+"session_summaries"),
		appStateTable: quotePostgres(schema, prefix+"app_states"), userStateTable: quotePostgres(schema, prefix+"user_states"), memoryTable: quotePostgres(schema, prefix+"memories"),
		headTable: quotePostgres(schema, prefix+"platform_session_heads"), commitTable: quotePostgres(schema, prefix+"platform_turn_commits"),
		transaction: func(ctx context.Context, fn func(*sql.Tx) error) error { return client.Transaction(ctx, fn) },
		exec:        client.ExecContext,
	}
}

func newMySQLCommitter(client mysqlstorage.Client, prefix string, skipDBInit bool) *sqlTurnCommitter {
	return &sqlTurnCommitter{
		dialect: dialectMySQL, prefix: prefix, skipDBInit: skipDBInit,
		stateTable: quoteMySQL(prefix + "session_states"), eventTable: quoteMySQL(prefix + "session_events"),
		trackTable: quoteMySQL(prefix + "session_track_events"), summaryTable: quoteMySQL(prefix + "session_summaries"),
		appStateTable: quoteMySQL(prefix + "app_states"), userStateTable: quoteMySQL(prefix + "user_states"), memoryTable: quoteMySQL(prefix + "memories"),
		headTable: quoteMySQL(prefix + "platform_session_heads"), commitTable: quoteMySQL(prefix + "platform_turn_commits"),
		transaction: func(ctx context.Context, fn func(*sql.Tx) error) error { return client.Transaction(ctx, fn) },
		exec:        client.Exec,
	}
}

func quotePostgres(schema, name string) string { return `"` + schema + `"."` + name + `"` }
func quoteMySQL(name string) string            { return "`" + name + "`" }

func (c *sqlTurnCommitter) Ready(ctx context.Context) error {
	if !c.skipDBInit {
		for _, statement := range c.platformDDL() {
			if _, err := c.exec(ctx, statement); err != nil {
				return persistence.ErrSchemaIncompatible
			}
		}
	}
	err := c.transaction(ctx, func(tx *sql.Tx) error {
		keyColumn := "key"
		if c.dialect == dialectMySQL {
			keyColumn = "`key`"
		}
		for _, query := range []string{
			fmt.Sprintf("SELECT id, app_name, user_id, session_id, state, created_at, updated_at, expires_at, deleted_at FROM %s WHERE 1=0", c.stateTable),
			fmt.Sprintf("SELECT id, app_name, user_id, session_id, event, created_at, updated_at, expires_at, deleted_at FROM %s WHERE 1=0", c.eventTable),
			fmt.Sprintf("SELECT id, app_name, user_id, session_id, track, event, created_at, updated_at, expires_at, deleted_at FROM %s WHERE 1=0", c.trackTable),
			fmt.Sprintf("SELECT id, app_name, user_id, session_id, filter_key, summary, updated_at, expires_at, deleted_at FROM %s WHERE 1=0", c.summaryTable),
			fmt.Sprintf("SELECT id, app_name, %s, value, created_at, updated_at, expires_at, deleted_at FROM %s WHERE 1=0", keyColumn, c.appStateTable),
			fmt.Sprintf("SELECT id, app_name, user_id, %s, value, created_at, updated_at, expires_at, deleted_at FROM %s WHERE 1=0", keyColumn, c.userStateTable),
			fmt.Sprintf("SELECT memory_id, app_name, user_id, memory_data, created_at, updated_at, deleted_at FROM %s WHERE 1=0", c.memoryTable),
			fmt.Sprintf("SELECT session_coord, last_committed_seq, backend_fingerprint, created_at, updated_at FROM %s WHERE 1=0", c.headTable),
			fmt.Sprintf("SELECT task_id, tenant_id, agent_app_id, storage_profile_id, backend_kind, session_coord, session_seq, payload_digest, envelope_digest, prepared_at, committed_at FROM %s WHERE 1=0", c.commitTable),
		} {
			rows, queryErr := tx.QueryContext(ctx, query)
			if queryErr != nil {
				return queryErr
			}
			if closeErr := rows.Close(); closeErr != nil {
				return closeErr
			}
		}
		return c.validateSchemaContracts(ctx, tx)
	})
	if err != nil {
		return persistence.ErrSchemaIncompatible
	}
	return nil
}

func (c *sqlTurnCommitter) rawTable(base string) string { return c.prefix + base }

func normalizeIndexDefinition(value string) string {
	value = strings.ToLower(strings.ReplaceAll(value, `"`, ""))
	return strings.Join(strings.Fields(value), "")
}

func (c *sqlTurnCommitter) validateSchemaContracts(ctx context.Context, tx *sql.Tx) error {
	if c.dialect == dialectPostgres {
		if err := c.validateColumnContracts(ctx, tx, []sqlColumnContract{
			{c.rawTable("session_states"), "state", "jsonb", nil},
			{c.rawTable("session_events"), "event", "jsonb", nil},
			{c.rawTable("memories"), "memory_data", "jsonb", nil},
			{c.rawTable("platform_session_heads"), "last_committed_seq", "bigint", nil},
			{c.rawTable("platform_turn_commits"), "session_seq", "bigint", nil},
		}); err != nil {
			return err
		}
		checks := []struct {
			table     string
			columns   string
			predicate string
		}{
			{c.rawTable("session_states"), "(app_name,user_id,session_id)", "deleted_atisnull"},
			{c.rawTable("memories"), "(memory_id)", ""},
			{c.rawTable("platform_session_heads"), "(session_coord)", ""},
			{c.rawTable("platform_turn_commits"), "(task_id)", ""},
			{c.rawTable("platform_turn_commits"), "(session_coord,session_seq)", ""},
		}
		for _, check := range checks {
			ok, err := postgresHasUniqueContract(ctx, tx, c.schema, check.table, check.columns, check.predicate)
			if err != nil || !ok {
				return persistence.ErrSchemaIncompatible
			}
		}
		return nil
	}
	precision6 := int64(6)
	if err := c.validateColumnContracts(ctx, tx, []sqlColumnContract{
		{c.rawTable("session_states"), "state", "json", nil},
		{c.rawTable("session_states"), "created_at", "timestamp", &precision6},
		{c.rawTable("session_events"), "event", "json", nil},
		{c.rawTable("session_events"), "created_at", "timestamp", &precision6},
		{c.rawTable("memories"), "memory_data", "json", nil},
		{c.rawTable("platform_session_heads"), "last_committed_seq", "bigint", nil},
		{c.rawTable("platform_session_heads"), "created_at", "timestamp", &precision6},
		{c.rawTable("platform_turn_commits"), "session_seq", "bigint", nil},
		{c.rawTable("platform_turn_commits"), "prepared_at", "timestamp", &precision6},
	}); err != nil {
		return err
	}
	for _, table := range []string{
		c.rawTable("session_states"), c.rawTable("session_events"), c.rawTable("session_track_events"),
		c.rawTable("session_summaries"), c.rawTable("app_states"), c.rawTable("user_states"),
		c.rawTable("memories"), c.rawTable("platform_session_heads"), c.rawTable("platform_turn_commits"),
	} {
		var engine, collation string
		if err := tx.QueryRowContext(ctx, `SELECT engine, table_collation FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name=?`, table).Scan(&engine, &collation); err != nil || !strings.EqualFold(engine, "InnoDB") || !strings.HasPrefix(strings.ToLower(collation), "utf8mb4_") {
			return persistence.ErrSchemaIncompatible
		}
	}
	checks := []struct {
		table   string
		columns string
	}{
		{c.rawTable("session_states"), "app_name,user_id,session_id,deleted_at"},
		{c.rawTable("memories"), "app_name,user_id,memory_id"},
		{c.rawTable("platform_session_heads"), "session_coord"},
		{c.rawTable("platform_turn_commits"), "task_id"},
		{c.rawTable("platform_turn_commits"), "session_coord,session_seq"},
	}
	for _, check := range checks {
		ok, err := mysqlHasUniqueContract(ctx, tx, check.table, check.columns)
		if err != nil || !ok {
			return persistence.ErrSchemaIncompatible
		}
	}
	return nil
}

func (c *sqlTurnCommitter) validateColumnContracts(ctx context.Context, tx *sql.Tx, contracts []sqlColumnContract) error {
	for _, contract := range contracts {
		query := `SELECT data_type, datetime_precision FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2 AND column_name=$3`
		args := []any{c.schema, contract.table, contract.column}
		if c.dialect == dialectMySQL {
			query = `SELECT data_type, datetime_precision FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=? AND column_name=?`
			args = []any{contract.table, contract.column}
		}
		var dataType string
		var precision sql.NullInt64
		if err := tx.QueryRowContext(ctx, query, args...).Scan(&dataType, &precision); err != nil || !strings.EqualFold(dataType, contract.dataType) {
			return persistence.ErrSchemaIncompatible
		}
		if contract.precision != nil && (!precision.Valid || precision.Int64 != *contract.precision) {
			return persistence.ErrSchemaIncompatible
		}
	}
	return nil
}

func postgresHasUniqueContract(ctx context.Context, tx *sql.Tx, schema, table, columns, predicate string) (bool, error) {
	rows, err := tx.QueryContext(ctx, `SELECT ix.indisunique, pg_get_indexdef(i.oid)
FROM pg_class t JOIN pg_namespace n ON n.oid=t.relnamespace
JOIN pg_index ix ON t.oid=ix.indrelid JOIN pg_class i ON i.oid=ix.indexrelid
WHERE n.nspname=$1 AND t.relname=$2`, schema, table)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var unique bool
		var definition string
		if err := rows.Scan(&unique, &definition); err != nil {
			return false, err
		}
		normalized := normalizeIndexDefinition(definition)
		if unique && strings.Contains(normalized, columns) && (predicate == "" || strings.Contains(normalized, predicate)) {
			return true, nil
		}
	}
	return false, rows.Err()
}

func mysqlHasUniqueContract(ctx context.Context, tx *sql.Tx, table, columns string) (bool, error) {
	rows, err := tx.QueryContext(ctx, `SELECT non_unique, GROUP_CONCAT(column_name ORDER BY seq_in_index SEPARATOR ',')
FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name=? GROUP BY index_name,non_unique`, table)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var nonUnique int
		var actual string
		if err := rows.Scan(&nonUnique, &actual); err != nil {
			return false, err
		}
		if nonUnique == 0 && strings.EqualFold(actual, columns) {
			return true, nil
		}
	}
	return false, rows.Err()
}

func (c *sqlTurnCommitter) platformDDL() []string {
	if c.dialect == dialectPostgres {
		return []string{
			fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
session_coord VARCHAR(64) PRIMARY KEY, last_committed_seq BIGINT NOT NULL DEFAULT 0,
backend_fingerprint VARCHAR(64) NOT NULL, created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL)`, c.headTable),
			fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
task_id VARCHAR(512) PRIMARY KEY, tenant_id VARCHAR(128) NOT NULL, agent_app_id VARCHAR(128) NOT NULL,
storage_profile_id VARCHAR(128) NOT NULL, backend_kind VARCHAR(16) NOT NULL, session_coord VARCHAR(64) NOT NULL,
session_seq BIGINT NOT NULL, payload_digest VARCHAR(64) NOT NULL, envelope_digest VARCHAR(64) NOT NULL,
prepared_at TIMESTAMP NOT NULL, committed_at TIMESTAMP NOT NULL,
UNIQUE(session_coord, session_seq))`, c.commitTable),
		}
	}
	return []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
session_coord VARCHAR(64) NOT NULL PRIMARY KEY, last_committed_seq BIGINT NOT NULL DEFAULT 0,
backend_fingerprint VARCHAR(64) NOT NULL, created_at TIMESTAMP(6) NOT NULL, updated_at TIMESTAMP(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`, c.headTable),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
task_id VARCHAR(512) NOT NULL PRIMARY KEY, tenant_id VARCHAR(128) NOT NULL, agent_app_id VARCHAR(128) NOT NULL,
storage_profile_id VARCHAR(128) NOT NULL, backend_kind VARCHAR(16) NOT NULL, session_coord VARCHAR(64) NOT NULL,
session_seq BIGINT NOT NULL, payload_digest VARCHAR(64) NOT NULL, envelope_digest VARCHAR(64) NOT NULL,
prepared_at TIMESTAMP(6) NOT NULL, committed_at TIMESTAMP(6) NOT NULL,
UNIQUE KEY unique_session_seq (session_coord, session_seq)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`, c.commitTable),
	}
}

func (c *sqlTurnCommitter) Commit(ctx context.Context, envelope persistence.Envelope) error {
	if err := envelope.Validate(); err != nil {
		return err
	}
	fingerprint, err := envelope.BackendFingerprint.Digest()
	if err != nil {
		return persistence.ErrInvalidEnvelope
	}
	err = c.transaction(ctx, func(tx *sql.Tx) error {
		lastCommitted, err := c.lockSessionHead(ctx, tx, envelope, fingerprint)
		if err != nil {
			return err
		}
		found, err := c.checkReceipt(ctx, tx, envelope)
		if err != nil || found {
			return err
		}
		if lastCommitted+1 != envelope.SessionSeq {
			return persistence.ErrSessionSequenceConflict
		}
		if err := c.writeSessionState(ctx, tx, envelope); err != nil {
			return err
		}
		for index, current := range envelope.TurnCommit.Events {
			raw, marshalErr := json.Marshal(current)
			if marshalErr != nil {
				return persistence.ErrInvalidEnvelope
			}
			createdAt := envelope.PreparedAt.Add(time.Duration(index+1) * time.Microsecond)
			if c.dialect == dialectPostgres {
				_, err = tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (app_name,user_id,session_id,event,created_at,updated_at,expires_at,deleted_at) VALUES ($1,$2,$3,$4,$5,$5,NULL,NULL)`, c.eventTable), envelope.TurnCommit.AppName, envelope.TurnCommit.UserID, envelope.TurnCommit.SessionID, string(raw), createdAt)
			} else {
				_, err = tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (app_name,user_id,session_id,event,created_at,updated_at,expires_at,deleted_at) VALUES (?,?,?,?,?,?,NULL,NULL)`, c.eventTable), envelope.TurnCommit.AppName, envelope.TurnCommit.UserID, envelope.TurnCommit.SessionID, string(raw), createdAt, createdAt)
			}
			if err != nil {
				return err
			}
		}
		if err := c.insertReceipt(ctx, tx, envelope); err != nil {
			return err
		}
		if c.dialect == dialectPostgres {
			_, err = tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET last_committed_seq=$1,updated_at=$2 WHERE session_coord=$3`, c.headTable), envelope.SessionSeq, envelope.PreparedAt, envelope.SessionCoord)
		} else {
			_, err = tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET last_committed_seq=?,updated_at=? WHERE session_coord=?`, c.headTable), envelope.SessionSeq, envelope.PreparedAt, envelope.SessionCoord)
		}
		return err
	})
	if err == nil || errors.Is(err, persistence.ErrCommitDigestConflict) || errors.Is(err, persistence.ErrSessionSequenceConflict) || errors.Is(err, persistence.ErrInvalidEnvelope) || errors.Is(err, persistence.ErrSchemaIncompatible) {
		return err
	}
	return persistence.ErrBackendUnavailable
}

func (c *sqlTurnCommitter) lockSessionHead(ctx context.Context, tx *sql.Tx, envelope persistence.Envelope, fingerprint string) (int64, error) {
	if c.dialect == dialectPostgres {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (session_coord,last_committed_seq,backend_fingerprint,created_at,updated_at) VALUES ($1,0,$2,$3,$3) ON CONFLICT (session_coord) DO NOTHING`, c.headTable), envelope.SessionCoord, fingerprint, envelope.PreparedAt); err != nil {
			return 0, err
		}
	} else {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (session_coord,last_committed_seq,backend_fingerprint,created_at,updated_at) VALUES (?,0,?,?,?) ON DUPLICATE KEY UPDATE session_coord=session_coord`, c.headTable), envelope.SessionCoord, fingerprint, envelope.PreparedAt, envelope.PreparedAt); err != nil {
			return 0, err
		}
	}
	query := fmt.Sprintf(`SELECT last_committed_seq,backend_fingerprint FROM %s WHERE session_coord=$1 FOR UPDATE`, c.headTable)
	if c.dialect == dialectMySQL {
		query = fmt.Sprintf(`SELECT last_committed_seq,backend_fingerprint FROM %s WHERE session_coord=? FOR UPDATE`, c.headTable)
	}
	var last int64
	var storedFingerprint string
	if err := tx.QueryRowContext(ctx, query, envelope.SessionCoord).Scan(&last, &storedFingerprint); err != nil {
		return 0, err
	}
	if storedFingerprint != fingerprint {
		return 0, persistence.ErrCommitDigestConflict
	}
	return last, nil
}

type commitReceipt struct {
	taskID, tenantID, agentAppID, profileID, backendKind, sessionCoord string
	sessionSeq                                                         int64
	payloadDigest, envelopeDigest                                      string
}

func (c *sqlTurnCommitter) checkReceipt(ctx context.Context, tx *sql.Tx, envelope persistence.Envelope) (bool, error) {
	query := fmt.Sprintf(`SELECT task_id,tenant_id,agent_app_id,storage_profile_id,backend_kind,session_coord,session_seq,payload_digest,envelope_digest FROM %s WHERE task_id=$1`, c.commitTable)
	if c.dialect == dialectMySQL {
		query = fmt.Sprintf(`SELECT task_id,tenant_id,agent_app_id,storage_profile_id,backend_kind,session_coord,session_seq,payload_digest,envelope_digest FROM %s WHERE task_id=?`, c.commitTable)
	}
	var got commitReceipt
	err := tx.QueryRowContext(ctx, query, envelope.TaskID).Scan(&got.taskID, &got.tenantID, &got.agentAppID, &got.profileID, &got.backendKind, &got.sessionCoord, &got.sessionSeq, &got.payloadDigest, &got.envelopeDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	want := commitReceipt{envelope.TaskID, envelope.TenantID, envelope.AgentAppID, envelope.StorageProfileID, string(envelope.BackendKind), envelope.SessionCoord, envelope.SessionSeq, envelope.PayloadDigest, envelope.EnvelopeDigest}
	if got != want {
		return false, persistence.ErrCommitDigestConflict
	}
	return true, nil
}

type persistedSessionState struct {
	ID        string           `json:"id"`
	State     session.StateMap `json:"state"`
	CreatedAt time.Time        `json:"createdAt"`
	UpdatedAt time.Time        `json:"updatedAt"`
}

func (c *sqlTurnCommitter) writeSessionState(ctx context.Context, tx *sql.Tx, envelope persistence.Envelope) error {
	state := persistedSessionState{ID: envelope.TurnCommit.SessionID, State: envelope.TurnCommit.FinalState, CreatedAt: envelope.PreparedAt, UpdatedAt: envelope.PreparedAt}
	raw, err := json.Marshal(state)
	if err != nil {
		return persistence.ErrInvalidEnvelope
	}
	if c.dialect == dialectPostgres {
		_, err = tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (app_name,user_id,session_id,state,created_at,updated_at,expires_at,deleted_at) VALUES ($1,$2,$3,$4,$5,$5,NULL,NULL) ON CONFLICT (app_name,user_id,session_id) WHERE deleted_at IS NULL DO UPDATE SET state=EXCLUDED.state,updated_at=EXCLUDED.updated_at,expires_at=NULL`, c.stateTable), envelope.TurnCommit.AppName, envelope.TurnCommit.UserID, envelope.TurnCommit.SessionID, string(raw), envelope.PreparedAt)
		return err
	}
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`SELECT id FROM %s WHERE app_name=? AND user_id=? AND session_id=? AND deleted_at IS NULL ORDER BY id FOR UPDATE`, c.stateTable), envelope.TurnCommit.AppName, envelope.TurnCommit.UserID, envelope.TurnCommit.SessionID)
	if err != nil {
		return err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(ids) > 1 {
		return persistence.ErrSchemaIncompatible
	}
	if len(ids) == 0 {
		_, err = tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (app_name,user_id,session_id,state,created_at,updated_at,expires_at,deleted_at) VALUES (?,?,?,?,?,?,NULL,NULL)`, c.stateTable), envelope.TurnCommit.AppName, envelope.TurnCommit.UserID, envelope.TurnCommit.SessionID, string(raw), envelope.PreparedAt, envelope.PreparedAt)
	} else {
		_, err = tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET state=?,updated_at=?,expires_at=NULL WHERE id=?`, c.stateTable), string(raw), envelope.PreparedAt, ids[0])
	}
	return err
}

func (c *sqlTurnCommitter) insertReceipt(ctx context.Context, tx *sql.Tx, envelope persistence.Envelope) error {
	args := []any{envelope.TaskID, envelope.TenantID, envelope.AgentAppID, envelope.StorageProfileID, string(envelope.BackendKind), envelope.SessionCoord, envelope.SessionSeq, envelope.PayloadDigest, envelope.EnvelopeDigest, envelope.PreparedAt, envelope.PreparedAt}
	if c.dialect == dialectPostgres {
		_, err := tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (task_id,tenant_id,agent_app_id,storage_profile_id,backend_kind,session_coord,session_seq,payload_digest,envelope_digest,prepared_at,committed_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, c.commitTable), args...)
		return err
	}
	_, err := tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (task_id,tenant_id,agent_app_id,storage_profile_id,backend_kind,session_coord,session_seq,payload_digest,envelope_digest,prepared_at,committed_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)`, c.commitTable), args...)
	return err
}

var _ persistence.Committer = (*sqlTurnCommitter)(nil)
