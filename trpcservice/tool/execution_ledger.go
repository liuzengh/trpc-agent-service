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
	"io"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/dbscope"
)

// ExecutionStatus is the durable outcome of one side-effecting tool intent.
// outcome_unknown is terminal for automatic retries because the remote side
// effect may already have happened even though its response was lost.
type ExecutionStatus string

const (
	ExecutionRunning        ExecutionStatus = "running"
	ExecutionCompleted      ExecutionStatus = "completed"
	ExecutionFailed         ExecutionStatus = "failed"
	ExecutionOutcomeUnknown ExecutionStatus = "outcome_unknown"
)

var (
	ErrToolExecutionInProgress = errors.New("tool execution is already in progress")
	ErrToolOutcomeUnknown      = errors.New("tool execution outcome is unknown")
)

type ExecutionRequest struct {
	TenantID   string
	RequestID  string
	ToolCallID string
	ToolName   string
	Arguments  []byte
	TraceID    string
	LeaseTTL   time.Duration
}

type ExecutionDecision struct {
	Created        bool
	Status         ExecutionStatus
	IdempotencyKey string
	Result         any
}

// ExecutionRecord is the non-sensitive operational view of one real tool
// call. Arguments and results are deliberately excluded from this projection.
type ExecutionRecord struct {
	RequestID   string          `json:"request_id"`
	ToolCallID  string          `json:"tool_call_id"`
	ToolName    string          `json:"tool_name"`
	Status      ExecutionStatus `json:"status"`
	TraceID     string          `json:"trace_id"`
	ErrorType   string          `json:"error_type,omitempty"`
	StartedAt   time.Time       `json:"started_at"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
}

type ExecutionLister interface {
	ListExecutions(context.Context, string, string) ([]ExecutionRecord, error)
}

// ExecutionLedger is the framework-callback seam for durable tool idempotency.
// Implementations must make Begin atomic across Worker replicas.
type ExecutionLedger interface {
	Begin(context.Context, ExecutionRequest) (ExecutionDecision, error)
	Complete(context.Context, string, string, any) error
	Fail(context.Context, string, string, string) error
	MarkOutcomeUnknown(context.Context, string, string, string) error
}

// PostgresExecutionLedger stores only encrypted tool results. Raw arguments are
// never persisted; their digest participates in the stable idempotency key.
type PostgresExecutionLedger struct {
	database *sql.DB
	key      [32]byte
	now      func() time.Time
}

type memoryExecution struct {
	request        ExecutionRequest
	status         ExecutionStatus
	idempotencyKey string
	result         any
	leaseUntil     time.Time
}

// MemoryExecutionLedger is a deterministic test/local implementation with the
// same retry semantics as the PostgreSQL ledger.
type MemoryExecutionLedger struct {
	mu      sync.Mutex
	entries map[string]memoryExecution
	now     func() time.Time
}

func NewMemoryExecutionLedger() *MemoryExecutionLedger {
	return &MemoryExecutionLedger{entries: make(map[string]memoryExecution), now: time.Now}
}

func (s *MemoryExecutionLedger) Begin(ctx context.Context, request ExecutionRequest) (ExecutionDecision, error) {
	if err := ctx.Err(); err != nil {
		return ExecutionDecision{}, err
	}
	if err := validateExecutionRequest(request); err != nil {
		return ExecutionDecision{}, err
	}
	request.ToolCallID = normalizedToolCallID(request)
	argumentsHash := digestBytes(request.Arguments)
	key := digestString(strings.Join([]string{request.TenantID, request.RequestID, request.ToolName, argumentsHash}, "\x00"))
	now := s.now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	if current, ok := s.entries[request.TenantID+"\x00"+key]; ok {
		if current.status == ExecutionRunning && !current.leaseUntil.After(now) {
			current.status = ExecutionOutcomeUnknown
			s.entries[request.TenantID+"\x00"+key] = current
		}
		if current.status == ExecutionFailed {
			current.request = request
			current.status = ExecutionRunning
			current.leaseUntil = now.Add(request.LeaseTTL)
			current.result = nil
			s.entries[request.TenantID+"\x00"+key] = current
			return ExecutionDecision{Created: true, Status: ExecutionRunning, IdempotencyKey: key}, nil
		}
		return ExecutionDecision{Status: current.status, IdempotencyKey: key, Result: current.result}, nil
	}
	s.entries[request.TenantID+"\x00"+key] = memoryExecution{
		request: request, status: ExecutionRunning, idempotencyKey: key, leaseUntil: now.Add(request.LeaseTTL),
	}
	return ExecutionDecision{Created: true, Status: ExecutionRunning, IdempotencyKey: key}, nil
}

func (s *MemoryExecutionLedger) Complete(ctx context.Context, tenantID, key string, result any) error {
	return s.finish(ctx, tenantID, key, ExecutionCompleted, result)
}

func (s *MemoryExecutionLedger) Fail(ctx context.Context, tenantID, key, _ string) error {
	return s.finish(ctx, tenantID, key, ExecutionFailed, nil)
}

func (s *MemoryExecutionLedger) MarkOutcomeUnknown(ctx context.Context, tenantID, key, _ string) error {
	return s.finish(ctx, tenantID, key, ExecutionOutcomeUnknown, nil)
}

func (s *MemoryExecutionLedger) finish(ctx context.Context, tenantID, key string, status ExecutionStatus, result any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	mapKey := tenantID + "\x00" + key
	current, ok := s.entries[mapKey]
	if !ok || current.status != ExecutionRunning {
		return ErrToolOutcomeUnknown
	}
	current.status = status
	current.result = result
	current.leaseUntil = time.Time{}
	s.entries[mapKey] = current
	return nil
}

func NewPostgresExecutionLedger(database *sql.DB, keyMaterial []byte) (*PostgresExecutionLedger, error) {
	if database == nil {
		return nil, errors.New("tool execution database is required")
	}
	if len(keyMaterial) < 32 {
		return nil, errors.New("tool execution key material must be at least 32 bytes")
	}
	// Domain separation prevents the caller's platform secret from being used
	// directly as an AES key when the same root material has another purpose.
	key := sha256.Sum256(append([]byte("trpc-agent-service/tool-result/v1\x00"), keyMaterial...))
	return &PostgresExecutionLedger{database: database, key: key, now: time.Now}, nil
}

func (s *PostgresExecutionLedger) ListExecutions(ctx context.Context, tenantID, requestID string) ([]ExecutionRecord, error) {
	tenantID = strings.TrimSpace(tenantID)
	requestID = strings.TrimSpace(requestID)
	if tenantID == "" || requestID == "" {
		return nil, errors.New("tool execution tenant and request IDs are required")
	}
	tx, err := dbscope.BeginTenantTransaction(ctx, s.database, tenantID)
	if err != nil {
		return nil, fmt.Errorf("begin tool execution list: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `
SELECT request_id, tool_call_id, tool_name, status, trace_id, error_type, started_at, completed_at
FROM tool_executions
WHERE tenant_id = $1 AND request_id = $2
ORDER BY started_at, tool_call_id`, tenantID, requestID)
	if err != nil {
		return nil, fmt.Errorf("list tool executions: %w", err)
	}
	defer rows.Close()
	records := make([]ExecutionRecord, 0)
	for rows.Next() {
		var record ExecutionRecord
		var completedAt sql.NullTime
		if err := rows.Scan(
			&record.RequestID, &record.ToolCallID, &record.ToolName, &record.Status,
			&record.TraceID, &record.ErrorType, &record.StartedAt, &completedAt,
		); err != nil {
			return nil, fmt.Errorf("scan tool execution: %w", err)
		}
		if completedAt.Valid {
			completed := completedAt.Time
			record.CompletedAt = &completed
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tool executions: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit tool execution list: %w", err)
	}
	return records, nil
}

func (s *PostgresExecutionLedger) Begin(ctx context.Context, request ExecutionRequest) (ExecutionDecision, error) {
	if err := validateExecutionRequest(request); err != nil {
		return ExecutionDecision{}, err
	}
	request.ToolCallID = normalizedToolCallID(request)
	argumentsHash := digestBytes(request.Arguments)
	idempotencyKey := digestString(strings.Join([]string{
		request.TenantID,
		request.RequestID,
		request.ToolName,
		argumentsHash,
	}, "\x00"))
	now := s.now().UTC()
	leaseUntil := now.Add(request.LeaseTTL)

	tx, err := dbscope.BeginTenantTransaction(ctx, s.database, request.TenantID)
	if err != nil {
		return ExecutionDecision{}, fmt.Errorf("begin tool execution transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", idempotencyKey); err != nil {
		return ExecutionDecision{}, fmt.Errorf("lock tool execution: %w", err)
	}

	decision, found, err := s.readDecision(ctx, tx, request.TenantID, idempotencyKey, now)
	if err != nil {
		return ExecutionDecision{}, err
	}
	if found {
		switch decision.Status {
		case ExecutionCompleted, ExecutionOutcomeUnknown, ExecutionRunning:
			if err := tx.Commit(); err != nil {
				return ExecutionDecision{}, fmt.Errorf("commit tool execution read: %w", err)
			}
			return decision, nil
		case ExecutionFailed:
			if _, err := tx.ExecContext(ctx, `
UPDATE tool_executions
SET request_id = $3, tool_call_id = $4, tool_name = $5, arguments_hash = $6,
    status = 'running', result_hash = NULL, result_ciphertext = NULL,
    error_type = '', trace_id = $7, lease_until = $8,
    completed_at = NULL, started_at = $9, updated_at = $9
WHERE tenant_id = $1 AND idempotency_key = $2`,
				request.TenantID, idempotencyKey, request.RequestID, request.ToolCallID,
				request.ToolName, argumentsHash, request.TraceID, leaseUntil, now); err != nil {
				return ExecutionDecision{}, fmt.Errorf("restart failed tool execution: %w", err)
			}
			if err := tx.Commit(); err != nil {
				return ExecutionDecision{}, fmt.Errorf("commit restarted tool execution: %w", err)
			}
			return ExecutionDecision{Created: true, Status: ExecutionRunning, IdempotencyKey: idempotencyKey}, nil
		default:
			return ExecutionDecision{}, fmt.Errorf("unsupported tool execution status %q", decision.Status)
		}
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO tool_executions (
    tenant_id, request_id, tool_call_id, tool_name, arguments_hash,
    idempotency_key, status, trace_id, lease_until, started_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, 'running', $7, $8, $9, $9)`,
		request.TenantID, request.RequestID, request.ToolCallID, request.ToolName,
		argumentsHash, idempotencyKey, request.TraceID, leaseUntil, now); err != nil {
		return ExecutionDecision{}, fmt.Errorf("insert tool execution: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return ExecutionDecision{}, fmt.Errorf("commit tool execution: %w", err)
	}
	return ExecutionDecision{Created: true, Status: ExecutionRunning, IdempotencyKey: idempotencyKey}, nil
}

func (s *PostgresExecutionLedger) readDecision(ctx context.Context, tx *sql.Tx, tenantID, idempotencyKey string, now time.Time) (ExecutionDecision, bool, error) {
	var status ExecutionStatus
	var ciphertext []byte
	var leaseUntil sql.NullTime
	err := tx.QueryRowContext(ctx, `
SELECT status, result_ciphertext, lease_until
FROM tool_executions
WHERE tenant_id = $1 AND idempotency_key = $2
FOR UPDATE`, tenantID, idempotencyKey).Scan(&status, &ciphertext, &leaseUntil)
	if errors.Is(err, sql.ErrNoRows) {
		return ExecutionDecision{}, false, nil
	}
	if err != nil {
		return ExecutionDecision{}, false, fmt.Errorf("read tool execution: %w", err)
	}
	decision := ExecutionDecision{Status: status, IdempotencyKey: idempotencyKey}
	if status == ExecutionRunning && (!leaseUntil.Valid || !leaseUntil.Time.After(now)) {
		if _, err := tx.ExecContext(ctx, `
UPDATE tool_executions
SET status = 'outcome_unknown', error_type = 'execution_lease_expired',
    lease_until = NULL, completed_at = $3, updated_at = $3
WHERE tenant_id = $1 AND idempotency_key = $2 AND status = 'running'`, tenantID, idempotencyKey, now); err != nil {
			return ExecutionDecision{}, false, fmt.Errorf("mark expired tool execution unknown: %w", err)
		}
		decision.Status = ExecutionOutcomeUnknown
	}
	if decision.Status == ExecutionCompleted {
		if len(ciphertext) == 0 {
			return ExecutionDecision{}, false, errors.New("completed tool result is unavailable")
		}
		plain, err := decryptToolResult(s.key, ciphertext)
		if err != nil {
			return ExecutionDecision{}, false, fmt.Errorf("decrypt completed tool result: %w", err)
		}
		if err := json.Unmarshal(plain, &decision.Result); err != nil {
			return ExecutionDecision{}, false, fmt.Errorf("decode completed tool result: %w", err)
		}
	}
	return decision, true, nil
}

func (s *PostgresExecutionLedger) Complete(ctx context.Context, tenantID, idempotencyKey string, result any) error {
	encoded, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("encode tool result: %w", err)
	}
	ciphertext, err := encryptToolResult(s.key, encoded)
	if err != nil {
		return fmt.Errorf("encrypt tool result: %w", err)
	}
	return s.finish(ctx, tenantID, idempotencyKey, ExecutionCompleted, "", digestBytes(encoded), ciphertext)
}

func (s *PostgresExecutionLedger) Fail(ctx context.Context, tenantID, idempotencyKey, errorType string) error {
	return s.finish(ctx, tenantID, idempotencyKey, ExecutionFailed, errorType, "", nil)
}

func (s *PostgresExecutionLedger) MarkOutcomeUnknown(ctx context.Context, tenantID, idempotencyKey, errorType string) error {
	return s.finish(ctx, tenantID, idempotencyKey, ExecutionOutcomeUnknown, errorType, "", nil)
}

func (s *PostgresExecutionLedger) finish(ctx context.Context, tenantID, idempotencyKey string, status ExecutionStatus, errorType, resultHash string, ciphertext []byte) error {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(idempotencyKey) == "" {
		return errors.New("tool execution identity is required")
	}
	now := s.now().UTC()
	tx, err := dbscope.BeginTenantTransaction(ctx, s.database, tenantID)
	if err != nil {
		return fmt.Errorf("begin tool execution completion: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `
UPDATE tool_executions
SET status = $3, result_hash = NULLIF($4, ''), result_ciphertext = $5,
    error_type = $6, lease_until = NULL, completed_at = $7, updated_at = $7
WHERE tenant_id = $1 AND idempotency_key = $2 AND status = 'running'`,
		tenantID, idempotencyKey, status, resultHash, ciphertext, strings.TrimSpace(errorType), now)
	if err != nil {
		return fmt.Errorf("finish tool execution: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect tool execution completion: %w", err)
	}
	if rows != 1 {
		return ErrToolOutcomeUnknown
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tool execution completion: %w", err)
	}
	return nil
}

func validateExecutionRequest(request ExecutionRequest) error {
	if strings.TrimSpace(request.TenantID) == "" || strings.TrimSpace(request.RequestID) == "" ||
		strings.TrimSpace(request.ToolName) == "" ||
		strings.TrimSpace(request.TraceID) == "" {
		return errors.New("tool execution request identity is incomplete")
	}
	if request.LeaseTTL <= 0 {
		return errors.New("tool execution lease TTL must be positive")
	}
	return nil
}

func normalizedToolCallID(request ExecutionRequest) string {
	if callID := strings.TrimSpace(request.ToolCallID); callID != "" {
		return callID
	}
	return "implicit-" + digestString(request.ToolName + "\x00" + string(request.Arguments))[:24]
}

func encryptToolResult(key [32]byte, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

func decryptToolResult(key [32]byte, ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(ciphertext) < gcm.NonceSize() {
		return nil, errors.New("tool result ciphertext is truncated")
	}
	nonce := ciphertext[:gcm.NonceSize()]
	return gcm.Open(nil, nonce, ciphertext[gcm.NonceSize():], nil)
}

func digestBytes(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func digestString(value string) string { return digestBytes([]byte(value)) }
