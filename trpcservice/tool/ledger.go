package tool

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type ExecutionStatus string

const (
	ExecutionRunning        ExecutionStatus = "running"
	ExecutionCompleted      ExecutionStatus = "completed"
	ExecutionFailed         ExecutionStatus = "failed"
	ExecutionOutcomeUnknown ExecutionStatus = "outcome_unknown"
)

type ExecutionRecord struct {
	TenantID, RequestID, ToolCallID, ToolName string
	ArgumentsHash, IdempotencyKey, TraceID    string
	Status                                    ExecutionStatus
	ResultHash, ErrorType                     string
}

type ExecutionStore interface {
	Begin(context.Context, ExecutionRecord) (ExecutionDecision, error)
	Complete(context.Context, string, string, string, any) error
	Fail(context.Context, string, string, string, string, ExecutionStatus) error
}

type ExecutionDecision struct {
	Created bool
	Status  ExecutionStatus
	Result  any
}

// SQLExecutionStore is the durable tool side-effect ledger. A unique
// tenant/request/call key makes retries observe the previous invocation.
type SQLExecutionStore struct {
	DB            *sql.DB
	Now           func() time.Time
	EncryptionKey []byte
}

func (store *SQLExecutionStore) Begin(ctx context.Context, record ExecutionRecord) (ExecutionDecision, error) {
	if store == nil || store.DB == nil || ctx == nil || record.TenantID == "" || record.RequestID == "" || record.ToolCallID == "" || record.ToolName == "" {
		return ExecutionDecision{}, errors.New("tool: execution ledger request is invalid")
	}
	now := time.Now
	if store.Now != nil {
		now = store.Now
	}
	result, err := store.DB.ExecContext(ctx, `INSERT INTO tool_executions (tenant_id,request_id,tool_call_id,tool_name,arguments_hash,idempotency_key,status,trace_id,started_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$9) ON CONFLICT (tenant_id,request_id,tool_call_id) DO NOTHING`, record.TenantID, record.RequestID, record.ToolCallID, record.ToolName, record.ArgumentsHash, record.IdempotencyKey, ExecutionRunning, record.TraceID, now().UTC())
	if err != nil {
		return ExecutionDecision{}, err
	}
	created, err := result.RowsAffected()
	if err != nil {
		return ExecutionDecision{}, err
	}
	var status ExecutionStatus
	var argumentsHash, toolName string
	var ciphertext []byte
	if err := store.DB.QueryRowContext(ctx, `SELECT tool_name,arguments_hash,status,result_ciphertext FROM tool_executions WHERE tenant_id=$1 AND request_id=$2 AND tool_call_id=$3`, record.TenantID, record.RequestID, record.ToolCallID).Scan(&toolName, &argumentsHash, &status, &ciphertext); err != nil {
		return ExecutionDecision{}, err
	}
	if toolName != record.ToolName || argumentsHash != record.ArgumentsHash {
		return ExecutionDecision{}, errors.New("tool: execution ledger call identity conflict")
	}
	decision := ExecutionDecision{Created: created == 1, Status: status}
	if status == ExecutionCompleted {
		if len(ciphertext) == 0 || len(store.EncryptionKey) == 0 {
			return ExecutionDecision{}, errors.New("tool: completed execution result is unavailable")
		}
		plain, err := decryptResult(store.EncryptionKey, ciphertext)
		if err != nil {
			return ExecutionDecision{}, errors.New("tool: completed execution result is unavailable")
		}
		if err := json.Unmarshal(plain, &decision.Result); err != nil {
			return ExecutionDecision{}, errors.New("tool: completed execution result is invalid")
		}
	}
	return decision, nil
}

func (store *SQLExecutionStore) Complete(ctx context.Context, tenantID, requestID, callID string, result any) error {
	if len(store.EncryptionKey) == 0 {
		return errors.New("tool: result encryption key is unavailable")
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return errors.New("tool: execution result cannot be encoded")
	}
	ciphertext, err := encryptResult(store.EncryptionKey, encoded)
	if err != nil {
		return errors.New("tool: execution result cannot be encrypted")
	}
	hash := hashBytes(encoded)
	_, err = store.DB.ExecContext(ctx, `UPDATE tool_executions SET status=$4,result_hash=$5,result_ciphertext=$6,error_type=NULL,completed_at=NOW(),updated_at=NOW() WHERE tenant_id=$1 AND request_id=$2 AND tool_call_id=$3 AND status <> 'completed'`, tenantID, requestID, callID, ExecutionCompleted, hash, ciphertext)
	return err
}

func (store *SQLExecutionStore) Fail(ctx context.Context, tenantID, requestID, callID, errorType string, status ExecutionStatus) error {
	if status != ExecutionOutcomeUnknown {
		status = ExecutionFailed
	}
	return store.update(ctx, tenantID, requestID, callID, status, "", errorType)
}

func (store *SQLExecutionStore) update(ctx context.Context, tenantID, requestID, callID string, status ExecutionStatus, resultHash, errorType string) error {
	if store == nil || store.DB == nil || ctx == nil || tenantID == "" || requestID == "" || callID == "" {
		return errors.New("tool: execution ledger update is invalid")
	}
	_, err := store.DB.ExecContext(ctx, `UPDATE tool_executions SET status=$4,result_hash=NULLIF($5,''),error_type=NULLIF($6,''),completed_at=CASE WHEN $4 IN ('failed','outcome_unknown') THEN NOW() ELSE completed_at END,updated_at=NOW() WHERE tenant_id=$1 AND request_id=$2 AND tool_call_id=$3`, tenantID, requestID, callID, status, resultHash, errorType)
	return err
}

func encryptResult(key, plain []byte) ([]byte, error) {
	block, err := aes.NewCipher(normalizeKey(key))
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, plain, nil), nil
}

func decryptResult(key, ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(normalizeKey(key))
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(ciphertext) < gcm.NonceSize() {
		return nil, errors.New("short ciphertext")
	}
	nonce, payload := ciphertext[:gcm.NonceSize()], ciphertext[gcm.NonceSize():]
	return gcm.Open(nil, nonce, payload, nil)
}

func normalizeKey(key []byte) []byte {
	digest := sha256.Sum256(key)
	return digest[:]
}

func hashBytes(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func executionCallID(toolName string, ordinal uint64) string {
	return fmt.Sprintf("%s:%d", toolName, ordinal)
}
