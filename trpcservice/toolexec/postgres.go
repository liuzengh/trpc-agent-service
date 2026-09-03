package toolexec

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type PostgresJournal struct{ db *sql.DB }

func NewPostgresJournal(db *sql.DB) (*PostgresJournal, error) {
	if db == nil {
		return nil, errors.New("tool execution database is nil")
	}
	return &PostgresJournal{db: db}, nil
}

func (j *PostgresJournal) Start(ctx context.Context, execution Execution) (StartResult, error) {
	if err := validate(&execution); err != nil {
		return StartResult{}, err
	}
	result, err := j.db.ExecContext(ctx, `
INSERT INTO tool_execution(
    execution_id,tenant_id,request_id,revision_id,tool_call_id,
    tool_name,arguments_hash,status
) VALUES ($1,$2,$3,$4,$5,$6,$7,'running')
ON CONFLICT (request_id,tool_call_id) DO NOTHING`,
		execution.ID, execution.TenantID, execution.RequestID, execution.RevisionID,
		execution.ToolCallID, execution.ToolName, execution.ArgumentsHash,
	)
	if err != nil {
		return StartResult{}, fmt.Errorf("start tool execution: %w", err)
	}
	rows, _ := result.RowsAffected()
	stored, err := j.get(ctx, execution.RequestID, execution.ToolCallID)
	if err != nil {
		return StartResult{}, err
	}
	return StartResult{Execution: stored, Existing: rows == 0}, nil
}

func (j *PostgresJournal) Complete(
	ctx context.Context,
	executionID string,
	status string,
	resultHash string,
	errorType string,
) error {
	result, err := j.db.ExecContext(ctx, `
UPDATE tool_execution SET status=$2,result_hash=NULLIF($3,''),
    error_type=NULLIF($4,''),completed_at=now()
WHERE execution_id=$1 AND status='running'`, executionID, status, resultHash, errorType)
	if err != nil {
		return fmt.Errorf("complete tool execution: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows != 1 {
		return errors.New("tool execution completion conflict")
	}
	return nil
}

func (j *PostgresJournal) get(
	ctx context.Context,
	requestID string,
	toolCallID string,
) (Execution, error) {
	var execution Execution
	var completedAt sql.NullTime
	err := j.db.QueryRowContext(ctx, `
SELECT execution_id,tenant_id,request_id,revision_id,tool_call_id,tool_name,
       arguments_hash,status,COALESCE(result_hash,''),COALESCE(error_type,''),
       started_at,completed_at
FROM tool_execution WHERE request_id=$1 AND tool_call_id=$2`, requestID, toolCallID).Scan(
		&execution.ID, &execution.TenantID, &execution.RequestID, &execution.RevisionID,
		&execution.ToolCallID, &execution.ToolName, &execution.ArgumentsHash,
		&execution.Status, &execution.ResultHash, &execution.ErrorType,
		&execution.StartedAt, &completedAt,
	)
	if err != nil {
		return Execution{}, err
	}
	if completedAt.Valid {
		execution.CompletedAt = completedAt.Time
	}
	return execution, nil
}

func (j *PostgresJournal) Ready(ctx context.Context) error { return j.db.PingContext(ctx) }
func (j *PostgresJournal) Close() error                    { return nil }

var _ Journal = (*PostgresJournal)(nil)
