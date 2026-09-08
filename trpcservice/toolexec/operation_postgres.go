package toolexec

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

type PostgresOperations struct{ db *sql.DB }

func NewPostgresOperations(db *sql.DB) *PostgresOperations { return &PostgresOperations{db: db} }

const operationColumns = `operation_id,tenant_id,app_id,user_id,tool_name,business_key_hash,input_hash,status,result,error_type,created_at,updated_at`

func scanOperation(row interface{ Scan(...any) error }) (Operation, error) {
	var op Operation
	var result []byte
	err := row.Scan(&op.ID, &op.TenantID, &op.AppID, &op.UserID, &op.ToolName, &op.BusinessKeyHash, &op.InputHash, &op.Status, &result, &op.ErrorType, &op.CreatedAt, &op.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Operation{}, ErrNotFound
	}
	if len(result) > 0 { // canonicalize JSONB's whitespace/key order for hashes.
		op.Result, err = canonicalJSON(result)
	}
	return op, err
}
func (s *PostgresOperations) Reserve(ctx context.Context, op Operation) (Operation, bool, error) {
	if !validOperation(op) {
		return Operation{}, false, ErrConflict
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO tool_operation(operation_id,tenant_id,app_id,user_id,tool_name,business_key_hash,input_hash,status)
VALUES($1,$2,$3,$4,$5,$6,$7,'running') ON CONFLICT DO NOTHING`, op.ID, op.TenantID, op.AppID, op.UserID, op.ToolName, op.BusinessKeyHash, op.InputHash)
	if err != nil {
		return Operation{}, false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return Operation{}, false, err
	}
	stored, err := s.Get(ctx, op.TenantID, op.ID)
	if err != nil {
		return Operation{}, false, err
	}
	if !sameOperation(stored, op) {
		return Operation{}, false, ErrOperationConflict
	}
	return stored, rows == 1, nil
}
func (s *PostgresOperations) Get(ctx context.Context, tenantID, id string) (Operation, error) {
	return scanOperation(s.db.QueryRowContext(ctx, `SELECT `+operationColumns+` FROM tool_operation WHERE tenant_id=$1 AND operation_id=$2`, tenantID, id))
}
func (s *PostgresOperations) List(ctx context.Context, tenantID, status, after string, limit int) ([]Operation, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+operationColumns+` FROM tool_operation WHERE tenant_id=$1 AND ($2='' OR status=$2) AND operation_id>$3 ORDER BY operation_id LIMIT $4`, tenantID, status, after, limit)
	if err != nil {
		return nil, err
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(rows)
	result := []Operation{}
	for rows.Next() {
		op, err := scanOperation(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, op)
	}
	return result, rows.Err()
}
func (s *PostgresOperations) Finish(ctx context.Context, tenantID, id, status string, result json.RawMessage, errorType string) (Operation, error) {
	if !validOutcome(status) {
		return Operation{}, ErrConflict
	}
	var encoded any
	if len(result) > 0 {
		encoded = string(result)
	}
	_, err := s.db.ExecContext(ctx, `UPDATE tool_operation SET status=$3,result=$4::jsonb,error_type=$5,updated_at=now() WHERE tenant_id=$1 AND operation_id=$2 AND status IN ('running','unknown')`, tenantID, id, status, encoded, errorType)
	if err != nil {
		return Operation{}, err
	}
	return s.Get(ctx, tenantID, id)
}
func (s *PostgresOperations) Ready(ctx context.Context) error { return s.db.PingContext(ctx) }
