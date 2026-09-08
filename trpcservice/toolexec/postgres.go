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
	if !sameExecution(stored, execution) {
		return StartResult{}, ErrConflict
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
	if !validOutcome(status) {
		return ErrConflict
	}
	result, err := j.db.ExecContext(ctx, `
UPDATE tool_execution SET status=$2,result_hash=NULLIF($3,''),
    error_type=NULLIF($4,''),completed_at=now()
WHERE execution_id=$1 AND (status IN ('running','unknown') OR
    (status=$2 AND COALESCE(result_hash,'')=$3 AND COALESCE(error_type,'')=$4))`, executionID, status, resultHash, errorType)
	if err != nil {
		return fmt.Errorf("complete tool execution: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows != 1 {
		return ErrConflict
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
       started_at,completed_at,COALESCE(operation_id,'')
FROM tool_execution WHERE request_id=$1 AND tool_call_id=$2`, requestID, toolCallID).Scan(
		&execution.ID, &execution.TenantID, &execution.RequestID, &execution.RevisionID,
		&execution.ToolCallID, &execution.ToolName, &execution.ArgumentsHash,
		&execution.Status, &execution.ResultHash, &execution.ErrorType,
		&execution.StartedAt, &completedAt, &execution.OperationID,
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

func (j *PostgresJournal) ListByRequest(ctx context.Context, tenantID, requestID string) ([]Execution, error) {
	rows, err := j.db.QueryContext(ctx, `
SELECT execution_id,tenant_id,request_id,revision_id,tool_call_id,tool_name,
       arguments_hash,status,COALESCE(result_hash,''),COALESCE(error_type,''),
       started_at,completed_at,COALESCE(operation_id,'') FROM tool_execution
WHERE tenant_id=$1 AND request_id=$2 ORDER BY tool_call_id`, tenantID, requestID)
	if err != nil {
		return nil, fmt.Errorf("read tool outcomes: %w", err)
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(rows)
	result := make([]Execution, 0)
	for rows.Next() {
		var item Execution
		var completedAt sql.NullTime
		if err := rows.Scan(&item.ID, &item.TenantID, &item.RequestID, &item.RevisionID,
			&item.ToolCallID, &item.ToolName, &item.ArgumentsHash, &item.Status, &item.ResultHash,
			&item.ErrorType, &item.StartedAt, &completedAt, &item.OperationID); err != nil {
			return nil, err
		}
		if completedAt.Valid {
			item.CompletedAt = completedAt.Time
		}
		result = append(result, item)
	}
	return result, rows.Err()
}
func (j *PostgresJournal) Close() error { return nil }

func (j *PostgresJournal) Get(ctx context.Context, tenantID, id string) (Execution, error) {
	var requestID, callID string
	err := j.db.QueryRowContext(ctx, `SELECT request_id,tool_call_id FROM tool_execution WHERE tenant_id=$1 AND execution_id=$2`, tenantID, id).Scan(&requestID, &callID)
	if errors.Is(err, sql.ErrNoRows) {
		return Execution{}, ErrNotFound
	}
	if err != nil {
		return Execution{}, err
	}
	return j.get(ctx, requestID, callID)
}

func (j *PostgresJournal) LinkOperation(ctx context.Context, tenantID, id, operationID string) error {
	if operationID == "" {
		return ErrConflict
	}
	result, err := j.db.ExecContext(ctx, `UPDATE tool_execution SET operation_id=$3
WHERE tenant_id=$1 AND execution_id=$2 AND (operation_id IS NULL OR operation_id=$3)`, tenantID, id, operationID)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return ErrConflict
	}
	return nil
}

func (j *PostgresJournal) ResolveOperation(ctx context.Context, tenantID, operationID, status, resultHash, errorType string) error {
	if operationID == "" || !validOutcome(status) {
		return ErrConflict
	}
	_, err := j.db.ExecContext(ctx, `UPDATE tool_execution SET status=$3,result_hash=NULLIF($4,''),error_type=NULLIF($5,''),completed_at=now()
WHERE tenant_id=$1 AND operation_id=$2 AND status IN ('running','unknown')`, tenantID, operationID, status, resultHash, errorType)
	return err
}

var _ Journal = (*PostgresJournal)(nil)
