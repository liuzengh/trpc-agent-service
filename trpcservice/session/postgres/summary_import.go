package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// SummaryImporter writes existing Session summaries to the PostgreSQL Session
// schema selected by one immutable execution configuration.
type SummaryImporter struct {
	pool  *pgxpool.Pool
	table string
}

// NewSummaryImporter creates an importer for the PostgreSQL Session backend
// resolved by exec. The caller must Close the returned importer.
func (r *SessionResolver) NewSummaryImporter(
	ctx context.Context,
	exec worker.Execution,
) (*SummaryImporter, error) {
	if r == nil || r.secrets == nil {
		return nil, errors.New("postgres session resolver is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := r.ResolveSession(ctx, exec); err != nil {
		return nil, fmt.Errorf("resolve postgres session service: %w", err)
	}
	ref := exec.Config.BackendConfig.Session
	if ref.Kind != tenant.BackendSQL || ref.Provider != "postgres" {
		return nil, fmt.Errorf("session backend %q must use postgres provider", ref.Name)
	}
	schema, err := sessionSchema(ref)
	if err != nil {
		return nil, err
	}
	dsn, err := r.resolveSessionDSN(ctx, exec)
	if err != nil {
		return nil, err
	}
	if err := ensureSchema(ctx, dsn, schema); err != nil {
		return nil, err
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect postgres summary importer: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres summary importer: %w", err)
	}
	return &SummaryImporter{
		pool:  pool,
		table: pgx.Identifier{schema, "session_summaries"}.Sanitize(),
	}, nil
}

// ReplaceSessionSummaries replaces all active summaries for key while
// preserving their text, topic list, update time, and boundary metadata.
func (i *SummaryImporter) ReplaceSessionSummaries(
	ctx context.Context,
	key session.Key,
	summaries map[string]*session.Summary,
) (err error) {
	if i == nil || i.pool == nil || i.table == "" {
		return errors.New("postgres summary importer is not initialized")
	}
	if err := key.CheckSessionKey(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	tx, err := i.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin replace session summaries: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(context.Background()); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			err = errors.Join(err, fmt.Errorf("rollback replace session summaries: %w", rollbackErr))
		}
	}()
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		"DELETE FROM %s WHERE app_name = $1 AND user_id = $2 AND session_id = $3",
		i.table,
	), key.AppName, key.UserID, key.SessionID); err != nil {
		return fmt.Errorf("delete target session summaries: %w", err)
	}
	for filterKey, summary := range summaries {
		if summary == nil {
			continue
		}
		value, err := json.Marshal(summary)
		if err != nil {
			return fmt.Errorf("marshal summary %q: %w", filterKey, err)
		}
		query := fmt.Sprintf(
			"INSERT INTO %s (app_name, user_id, session_id, filter_key, summary, updated_at, expires_at, deleted_at) "+
				"VALUES ($1, $2, $3, $4, $5, $6, NULL, NULL) "+
				"ON CONFLICT (app_name, user_id, session_id, filter_key) WHERE deleted_at IS NULL "+
				"DO UPDATE SET summary = EXCLUDED.summary, updated_at = EXCLUDED.updated_at, expires_at = EXCLUDED.expires_at",
			i.table,
		)
		if _, err := tx.Exec(ctx, query,
			key.AppName, key.UserID, key.SessionID, filterKey, value, time.Now().UTC(),
		); err != nil {
			return fmt.Errorf("upsert target summary %q: %w", filterKey, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit replace session summaries: %w", err)
	}
	return nil
}

// GetSessionSummaries returns all active summaries stored for key.
func (i *SummaryImporter) GetSessionSummaries(
	ctx context.Context,
	key session.Key,
) (map[string]*session.Summary, error) {
	if i == nil || i.pool == nil || i.table == "" {
		return nil, errors.New("postgres summary importer is not initialized")
	}
	if err := key.CheckSessionKey(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rows, err := i.pool.Query(ctx, fmt.Sprintf(
		"SELECT filter_key, summary FROM %s WHERE app_name = $1 AND user_id = $2 AND session_id = $3 AND deleted_at IS NULL ORDER BY filter_key",
		i.table,
	), key.AppName, key.UserID, key.SessionID)
	if err != nil {
		return nil, fmt.Errorf("read session summaries: %w", err)
	}
	defer rows.Close()
	var result map[string]*session.Summary
	for rows.Next() {
		var filterKey string
		var encoded []byte
		if err := rows.Scan(&filterKey, &encoded); err != nil {
			return nil, fmt.Errorf("scan session summary: %w", err)
		}
		var value *session.Summary
		if err := json.Unmarshal(encoded, &value); err != nil {
			return nil, fmt.Errorf("unmarshal session summary %q: %w", filterKey, err)
		}
		if result == nil {
			result = make(map[string]*session.Summary)
		}
		result[filterKey] = value
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate session summaries: %w", err)
	}
	return result, nil
}

// Close releases the database resources owned by SummaryImporter.
func (i *SummaryImporter) Close() {
	if i != nil && i.pool != nil {
		i.pool.Close()
		i.pool = nil
	}
}
