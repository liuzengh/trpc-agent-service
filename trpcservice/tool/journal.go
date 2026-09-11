package tool

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	tasmysql "github.com/liuzengh/trpc-agent-service/trpcservice/storage/mysql"
)

// The ledger (migrations/0003_tooling.sql). Every tool call writes an intent
// row *before* the callable runs and an outcome row after it, each its own
// short transaction — never inside the execution's commit, because the whole
// point is that the record exists even if the process dies before the
// execution ever gets to commit.
//
// The recovery rule the approved plan states, encoded here: an unresolved
// call from an older fence with a side effect blocks the execution for a
// human ("完成记录恢复不重放副作用"); the same row for a read is abandoned
// and the execution may re-run, because a read cannot have changed anything.

// ErrAlreadyResolved means a human (or a previous request) already disposed
// of this call. Resolving twice is not an error a caller should retry — the
// state they wanted is the state that exists — but it is not a silent
// success either, because the two requests may disagree.
var ErrAlreadyResolved = errors.New("tool: this call already has a resolution")

// CallID derives the stable logical key of one call: it is unique within an
// execution *attempt*, so a fresh claim (higher fence) writes fresh rows
// while the dead attempt's rows stay exactly where the recovery check can
// find them.
func CallID(executionID string, fence uint64, seq int) string {
	return fmt.Sprintf("%s:%d:%d", executionID, fence, seq)
}

// CallIntent is everything known before the callable runs.
type CallIntent struct {
	TenantID    string
	ExecutionID string
	SessionPK   int64
	CallSeq     int
	ToolID      int64
	ToolName    string
	ToolVersion uint32
	ToolKind    string
	SideEffect  string
	Idempotent  bool
	ArgsHash    string
	ArgsMasked  json.RawMessage
	TraceID     string
	WorkerID    string
	Fence       uint64
}

// Call is one ledger row as read back for recovery or operator review.
type Call struct {
	CallID          string
	TenantID        string
	ExecutionID     string
	SessionPK       int64
	CallSeq         int
	ToolID          int64
	ToolName        string
	ToolVersion     uint32
	ToolKind        string
	SideEffect      string
	Idempotent      bool
	ArgumentsHash   string
	ArgumentsMasked json.RawMessage
	Status          string
	ErrorType       string
	Detail          string
	LatencyMS       int
	Attempts        int
	TraceID         string
	WorkerID        string
	FencingToken    uint64
	Resolution      string
	ResolvedBy      string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// Journal writes and reads the tool_calls ledger.
type Journal struct {
	db *controlplane.DB
}

// NewJournal wires the ledger to the control-plane database.
func NewJournal(db *controlplane.DB) *Journal { return &Journal{db: db} }

// Begin records the intent. It is idempotent for a retry of the same logical
// call (same call id): the existing row is reused if — and only if — its
// argument hash matches, so a bug that reuses a call id for different
// arguments fails loudly instead of erasing evidence.
func (j *Journal) Begin(ctx context.Context, in CallIntent) error {
	scope, err := j.db.Scope(in.TenantID)
	if err != nil {
		return err
	}
	callID := CallID(in.ExecutionID, in.Fence, in.CallSeq)
	idempotent := 0
	if in.Idempotent {
		idempotent = 1
	}
	var toolID any
	if in.ToolID > 0 {
		toolID = in.ToolID
	}
	_, err = scope.Exec(ctx, `
		INSERT INTO tool_calls
			(call_id, tenant_id, execution_id, session_pk, call_seq, tool_id, tool_name,
			 tool_version, tool_kind, side_effect, idempotent, arguments_hash, arguments_masked,
			 status, trace_id, worker_id, fencing_token)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'running', ?, ?, ?)`,
		callID, in.TenantID, in.ExecutionID, in.SessionPK, in.CallSeq, toolID, in.ToolName,
		in.ToolVersion, in.ToolKind, in.SideEffect, idempotent, in.ArgsHash, nullableBytes(in.ArgsMasked),
		in.TraceID, in.WorkerID, in.Fence)
	if err == nil {
		return nil
	}
	if !tasmysql.IsDuplicateKey(err) {
		return fmt.Errorf("tool: journal begin: %w", err)
	}
	existing, err := j.Get(ctx, in.TenantID, callID)
	if err != nil {
		return err
	}
	if existing.ArgumentsHash != in.ArgsHash {
		return fmt.Errorf("tool: journal: call %s already exists with different arguments", callID)
	}
	return nil
}

// Get reads one row by call id.
func (j *Journal) Get(ctx context.Context, tenantID, callID string) (Call, error) {
	scope, err := j.db.Scope(tenantID)
	if err != nil {
		return Call{}, err
	}
	row, err := scope.QueryRow(ctx, `
		SELECT `+callColumns+`
		FROM tool_calls WHERE tenant_id = ? AND call_id = ?`, tenantID, callID)
	if err != nil {
		return Call{}, err
	}
	return scanCall(row)
}

const callColumns = `call_id, tenant_id, execution_id, session_pk, call_seq, tool_id, tool_name,
	tool_version, tool_kind, side_effect, idempotent, arguments_hash, arguments_masked,
	status, error_type, detail, latency_ms, attempts, trace_id, worker_id, fencing_token,
	resolution, resolved_by, created_at, updated_at`

// Finish closes the intent row with what actually happened. resultBytes is
// the size of the serialized result; a failure records why.
func (j *Journal) Finish(
	ctx context.Context,
	tenantID, callID string,
	outcome Outcome,
	errorType, detail string,
	resultBytes, latencyMS, attempts int,
) error {
	scope, err := j.db.Scope(tenantID)
	if err != nil {
		return err
	}
	res, err := scope.Exec(ctx, `
		UPDATE tool_calls
		SET status = ?, error_type = ?, detail = ?, result_bytes = ?, latency_ms = ?, attempts = ?
		WHERE tenant_id = ? AND call_id = ?`,
		outcome.String(), truncate(errorType, 64), truncate(detail, 512),
		resultBytes, latencyMS, attempts, tenantID, callID)
	if err != nil {
		return fmt.Errorf("tool: journal finish: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("tool: journal: no call %s to finish", callID)
	}
	return nil
}

// Reject closes a row the platform refused before the callable ran. It is a
// distinct status from 'failed' because the two answer different questions:
// failed means the tool ran and did not succeed, rejected means the platform
// stopped it — an audit of a suspicious session looks for the second.
func (j *Journal) Reject(ctx context.Context, tenantID, callID, errorType, detail string) error {
	scope, err := j.db.Scope(tenantID)
	if err != nil {
		return err
	}
	res, err := scope.Exec(ctx, `
		UPDATE tool_calls SET status = 'rejected', error_type = ?, detail = ?
		WHERE tenant_id = ? AND call_id = ? AND status = 'running'`,
		truncate(errorType, 64), truncate(detail, 512), tenantID, callID)
	if err != nil {
		return fmt.Errorf("tool: journal reject: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("tool: journal: no running call %s to reject", callID)
	}
	return nil
}

// RecordAttempt writes one physical attempt row. The governor wires it to
// the HTTP tool's attempt hook, so the ledger shows each time the network
// was actually touched — the evidence for "安全重试计数受限".
func (j *Journal) RecordAttempt(
	ctx context.Context,
	tenantID, callID string,
	attemptNo int,
	workerID string,
	outcome Outcome,
	httpStatus int,
	errorType, detail string,
) error {
	scope, err := j.db.Scope(tenantID)
	if err != nil {
		return err
	}
	_, err = scope.Exec(ctx, `
		INSERT INTO tool_call_attempts
			(call_id, tenant_id, attempt_no, worker_id, status, http_status, error_type, detail, finished_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(6))
		ON DUPLICATE KEY UPDATE
			status = VALUES(status), http_status = VALUES(http_status),
			error_type = VALUES(error_type), detail = VALUES(detail), finished_at = UTC_TIMESTAMP(6)`,
		callID, tenantID, attemptNo, workerID, outcome.String(), httpStatus,
		truncate(errorType, 64), truncate(detail, 512))
	if err != nil {
		return fmt.Errorf("tool: journal attempt: %w", err)
	}
	return nil
}

// StaleUnresolved returns calls from *older* fences of this execution that
// never reached a conclusion. These are the rows the recovery check decides
// on: the current attempt's own rows are live and are not stale.
func (j *Journal) StaleUnresolved(ctx context.Context, tenantID, executionID string, currentFence uint64) ([]Call, error) {
	scope, err := j.db.Scope(tenantID)
	if err != nil {
		return nil, err
	}
	rows, err := scope.Query(ctx, `
		SELECT `+callColumns+`
		FROM tool_calls
		WHERE tenant_id = ? AND execution_id = ? AND resolution IS NULL
		  AND status IN ('running', 'unknown') AND fencing_token < ?
		ORDER BY call_seq`, tenantID, executionID, currentFence)
	if err != nil {
		return nil, fmt.Errorf("tool: stale calls: %w", err)
	}
	defer rows.Close()
	return collectCalls(rows)
}

// Abandon marks a stale row as conclusively not-happened. Only used for
// calls that cannot have had an effect; a write goes to a human instead.
func (j *Journal) Abandon(ctx context.Context, tenantID, callID, reason string) error {
	scope, err := j.db.Scope(tenantID)
	if err != nil {
		return err
	}
	_, err = scope.Exec(ctx, `
		UPDATE tool_calls SET status = 'failed', error_type = 'abandoned', detail = ?
		WHERE tenant_id = ? AND call_id = ? AND status IN ('running', 'unknown') AND resolution IS NULL`,
		truncate(reason, 512), tenantID, callID)
	if err != nil {
		return fmt.Errorf("tool: abandon call: %w", err)
	}
	return nil
}

// UnresolvedCount counts rows a human still has to dispose of: running or
// unknown, not yet resolved. A blocked session is unblocked only when this
// hits zero.
func (j *Journal) UnresolvedCount(ctx context.Context, tenantID, executionID string) (int, error) {
	scope, err := j.db.Scope(tenantID)
	if err != nil {
		return 0, err
	}
	var n int
	row, err := scope.QueryRow(ctx, `
		SELECT COUNT(*) FROM tool_calls
		WHERE tenant_id = ? AND execution_id = ? AND resolution IS NULL
		  AND status IN ('running', 'unknown')`, tenantID, executionID)
	if err != nil {
		return 0, err
	}
	if err := row.Scan(&n); err != nil {
		return 0, fmt.Errorf("tool: count unresolved calls: %w", err)
	}
	return n, nil
}

// ResolutionCounts reports how many unresolved rows an execution has and how
// many of the already-resolved ones were confirmed. The session-unblock
// decision reads both: any confirmed call means the work partly happened, so
// the conservative disposition is confirmation, never a re-run.
func (j *Journal) ResolutionCounts(ctx context.Context, tenantID, executionID string) (confirmed, unresolved int, err error) {
	scope, err := j.db.Scope(tenantID)
	if err != nil {
		return 0, 0, err
	}
	rows, err := scope.Query(ctx, `
		SELECT resolution, COUNT(*) FROM tool_calls
		WHERE tenant_id = ? AND execution_id = ?
		GROUP BY resolution`, tenantID, executionID)
	if err != nil {
		return 0, 0, fmt.Errorf("tool: resolution counts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var res sql.NullString
		var n int
		if err := rows.Scan(&res, &n); err != nil {
			return 0, 0, fmt.Errorf("tool: scan resolution count: %w", err)
		}
		switch {
		case !res.Valid:
			unresolved += n
		case res.String == "confirmed":
			confirmed += n
		}
	}
	return confirmed, unresolved, rows.Err()
}

// Resolve disposes of one call. resolution is 'confirmed' (the side effect
// happened) or 'cancelled' (it did not).
func (j *Journal) Resolve(ctx context.Context, tenantID, callID, resolution, by string) (Call, error) {
	if resolution != "confirmed" && resolution != "cancelled" {
		return Call{}, fmt.Errorf("tool: resolution %q must be confirmed or cancelled", resolution)
	}
	scope, err := j.db.Scope(tenantID)
	if err != nil {
		return Call{}, err
	}
	res, err := scope.Exec(ctx, `
		UPDATE tool_calls SET resolution = ?, resolved_by = ?, resolved_at = UTC_TIMESTAMP(6)
		WHERE tenant_id = ? AND call_id = ? AND resolution IS NULL`,
		resolution, truncate(by, 128), tenantID, callID)
	if err != nil {
		return Call{}, fmt.Errorf("tool: resolve call: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		existing, err := j.Get(ctx, tenantID, callID)
		if errors.Is(err, controlplane.ErrNotFound) {
			return Call{}, controlplane.ErrNotFound
		}
		if err != nil {
			return Call{}, err
		}
		if existing.Resolution != "" {
			return existing, ErrAlreadyResolved
		}
		return existing, fmt.Errorf("tool: call %s cannot be resolved from status %q", callID, existing.Status)
	}
	return j.Get(ctx, tenantID, callID)
}

// CallFilter narrows an operator listing.
type CallFilter struct {
	Status         string
	ExecutionID    string
	SessionPK      int64
	UnresolvedOnly bool
	Limit          int
}

// List returns ledger rows for operator review, newest first.
func (j *Journal) List(ctx context.Context, tenantID string, f CallFilter) ([]Call, error) {
	scope, err := j.db.Scope(tenantID)
	if err != nil {
		return nil, err
	}
	var (
		where []string
		args  []any
	)
	where = append(where, "tenant_id = ?")
	args = append(args, tenantID)
	if f.Status != "" {
		where = append(where, "status = ?")
		args = append(args, f.Status)
	}
	if f.ExecutionID != "" {
		where = append(where, "execution_id = ?")
		args = append(args, f.ExecutionID)
	}
	if f.SessionPK > 0 {
		where = append(where, "session_pk = ?")
		args = append(args, f.SessionPK)
	}
	if f.UnresolvedOnly {
		where = append(where, "resolution IS NULL", "status IN ('running', 'unknown')")
	}
	limit := f.Limit
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	args = append(args, limit)
	rows, err := scope.Query(ctx, `
		SELECT `+callColumns+`
		FROM tool_calls
		WHERE `+strings.Join(where, " AND ")+`
		ORDER BY updated_at DESC, call_seq DESC
		LIMIT ?`, args...)
	if err != nil {
		return nil, fmt.Errorf("tool: list calls: %w", err)
	}
	defer rows.Close()
	return collectCalls(rows)
}

// BlockedSessions lists sessions parked for human review, with the head
// execution a resolve action would target.
func (j *Journal) BlockedSessions(ctx context.Context, tenantID string, limit int) ([]BlockedSession, error) {
	scope, err := j.db.Scope(tenantID)
	if err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := scope.Query(ctx, `
		SELECT s.session_pk, s.blocked_reason, im.execution_id, im.in_seq, s.updated_at
		FROM sessions s
		LEFT JOIN inbox_messages im
		  ON im.tenant_id = s.tenant_id AND im.session_pk = s.session_pk AND im.in_seq = s.head_seq
		WHERE s.tenant_id = ? AND s.blocked_reason IS NOT NULL
		ORDER BY s.updated_at DESC
		LIMIT ?`, tenantID, limit)
	if err != nil {
		return nil, fmt.Errorf("tool: list blocked sessions: %w", err)
	}
	defer rows.Close()
	var out []BlockedSession
	for rows.Next() {
		var b BlockedSession
		var execID sql.NullString
		var inSeq sql.NullInt64
		if err := rows.Scan(&b.SessionPK, &b.BlockedReason, &execID, &inSeq, &b.UpdatedAt); err != nil {
			return nil, fmt.Errorf("tool: scan blocked session: %w", err)
		}
		b.ExecutionID = execID.String
		b.InSeq = uint32(inSeq.Int64)
		out = append(out, b)
	}
	return out, rows.Err()
}

// BlockedSession is one session waiting for a human decision.
type BlockedSession struct {
	SessionPK     int64
	BlockedReason string
	ExecutionID   string
	InSeq         uint32
	UpdatedAt     time.Time
}

func scanCall(row *sql.Row) (Call, error) {
	var c Call
	err := scanCallInto(row.Scan, &c)
	if errors.Is(err, sql.ErrNoRows) {
		return Call{}, controlplane.ErrNotFound
	}
	return c, err
}

func collectCalls(rows *sql.Rows) ([]Call, error) {
	var out []Call
	for rows.Next() {
		var c Call
		if err := scanCallInto(rows.Scan, &c); err != nil {
			return nil, fmt.Errorf("tool: scan call: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func scanCallInto(scan func(...any) error, c *Call) error {
	var (
		toolID      sql.NullInt64
		masked      sql.NullString
		resolution  sql.NullString
		resolvedBy  sql.NullString
		fencing     uint64
		toolVersion uint32
	)
	if err := scan(&c.CallID, &c.TenantID, &c.ExecutionID, &c.SessionPK, &c.CallSeq, &toolID,
		&c.ToolName, &toolVersion, &c.ToolKind, &c.SideEffect, &c.Idempotent, &c.ArgumentsHash,
		&masked, &c.Status, &c.ErrorType, &c.Detail, &c.LatencyMS, &c.Attempts,
		&c.TraceID, &c.WorkerID, &fencing, &resolution, &resolvedBy, &c.CreatedAt, &c.UpdatedAt); err != nil {
		return err
	}
	c.ToolID = toolID.Int64
	c.ToolVersion = toolVersion
	c.FencingToken = fencing
	if masked.Valid {
		c.ArgumentsMasked = json.RawMessage(masked.String)
	}
	c.Resolution = resolution.String
	c.ResolvedBy = resolvedBy.String
	return nil
}

// MaskArguments returns the operator-facing copy of arguments: same shape,
// but any value under a secret-looking key is replaced. The ledger must be
// reviewable without becoming a second place credentials live.
func MaskArguments(raw []byte) json.RawMessage {
	const maxMasked = 8 << 10
	if len(raw) == 0 {
		return nil
	}
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil
	}
	masked := maskValue(doc, "")
	out, err := json.Marshal(masked)
	if err != nil {
		return nil
	}
	if len(out) > maxMasked {
		return json.RawMessage(`{"_":"arguments omitted: masked copy exceeds 8 KiB"}`)
	}
	return out
}

var secretishKeys = []string{"token", "secret", "password", "passwd", "apikey", "api_key", "authorization", "credential"}

func maskValue(v any, key string) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, child := range t {
			out[k] = maskValue(child, k)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, child := range t {
			out[i] = maskValue(child, key)
		}
		return out
	default:
		if key != "" && isSecretish(key) {
			return "***"
		}
		return v
	}
}

func isSecretish(key string) bool {
	lower := strings.ToLower(key)
	for _, k := range secretishKeys {
		if strings.Contains(lower, k) {
			return true
		}
	}
	return false
}

// HashArguments is the canonical digest of an argument payload. It is
// computed over the exact bytes the model produced: the ledger's job is to
// prove "these were the arguments", and re-marshaling would let a map
// iteration order change the answer.
func HashArguments(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func nullableBytes(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	return []byte(raw)
}
