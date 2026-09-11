// Package toolapproval persists exact human-approved tool actions in Worker
// storage. It deliberately supports only test.ticket.status.update in V1.
package toolapproval

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	approvalv1 "github.com/liuzengh/trpc-agent-service/api/runtime/approval/v1"
)

var (
	ErrInvalid  = errors.New("invalid tool approval")
	ErrConflict = errors.New("tool approval conflict")
	ErrNotFound = errors.New("tool approval not found")
	ErrDenied   = errors.New("tool approval denied")
	ErrUnknown  = errors.New("tool execution outcome unknown")
)

var ticketIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)

type Proposal struct {
	TenantID, RunID, AttemptID, InvocationID, ToolCallID string
	NodeID, ToolName, ToolResource, Capability           string
	Arguments                                            []byte
	TTL                                                  time.Duration
}

type ticketArguments struct {
	TicketID string `json:"ticket_id"`
	Status   string `json:"status"`
}

type stored struct {
	Operation approvalv1.Operation
	Arguments []byte
	Result    []byte
}

type Store struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) (*Store, error) {
	if pool == nil {
		return nil, ErrInvalid
	}
	return &Store{pool: pool}, nil
}

func digest(b []byte) string {
	h := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(h[:])
}

func normalizeArguments(raw []byte) ([]byte, ticketArguments, error) {
	var value ticketArguments
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&value); err != nil {
		return nil, value, ErrInvalid
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return nil, value, ErrInvalid
	}
	if !ticketIDPattern.MatchString(value.TicketID) {
		return nil, value, ErrInvalid
	}
	switch value.Status {
	case "open", "in_progress", "resolved", "closed":
	default:
		return nil, value, ErrInvalid
	}
	canonical, _ := json.Marshal(value)
	return canonical, value, nil
}

func stableID(p Proposal, argsDigest string) string {
	b, _ := json.Marshal([]string{p.TenantID, p.RunID, p.AttemptID, p.InvocationID, p.ToolCallID, p.ToolName, argsDigest})
	h := sha256.Sum256(b)
	return "tap_" + hex.EncodeToString(h[:])
}

func (s *Store) Propose(ctx context.Context, p Proposal) (approvalv1.Operation, error) {
	if s == nil || s.pool == nil || p.Capability != approvalv1.CapabilityTestTicketStatusUpdate || p.TTL < time.Second || p.TTL > 30*time.Minute {
		return approvalv1.Operation{}, ErrInvalid
	}
	for _, v := range []string{p.TenantID, p.RunID, p.AttemptID, p.InvocationID, p.ToolCallID, p.NodeID, p.ToolName, p.ToolResource} {
		if strings.TrimSpace(v) == "" || len(v) > 256 {
			return approvalv1.Operation{}, ErrInvalid
		}
	}
	canonical, args, err := normalizeArguments(p.Arguments)
	if err != nil {
		return approvalv1.Operation{}, err
	}
	argsDigest := digest(canonical)
	id := stableID(p, argsDigest)
	summary := fmt.Sprintf("将测试工单 %s 的状态修改为 %s", args.TicketID, args.Status)
	_, err = s.pool.Exec(ctx, `INSERT INTO tool_approval_operations
	 (tenant_id,operation_id,run_id,attempt_id,invocation_id,tool_call_id,node_id,tool_name,tool_resource,capability,target,parameter_summary,arguments_digest,arguments_json,status,expires_at)
	 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,'PENDING',$15)
	 ON CONFLICT(tenant_id,operation_id) DO NOTHING`, p.TenantID, id, p.RunID, p.AttemptID, p.InvocationID, p.ToolCallID, p.NodeID, p.ToolName, p.ToolResource, p.Capability, args.TicketID, summary, argsDigest, canonical, time.Now().UTC().Add(p.TTL))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return approvalv1.Operation{}, fmt.Errorf("%w: %s", ErrConflict, pgErr.ConstraintName)
		}
		return approvalv1.Operation{}, err
	}
	got, err := s.get(ctx, p.TenantID, id)
	if err != nil {
		return approvalv1.Operation{}, err
	}
	storedCanonical, _, normalizeErr := normalizeArguments(got.Arguments)
	if normalizeErr != nil || !bytes.Equal(storedCanonical, canonical) || got.Operation.ArgumentsDigest != argsDigest || got.Operation.AttemptID != p.AttemptID || got.Operation.ToolName != p.ToolName {
		return approvalv1.Operation{}, ErrConflict
	}
	return got.Operation, nil
}

const selectColumns = `operation_id,tenant_id,run_id,attempt_id,node_id,tool_name,tool_resource,capability,target,parameter_summary,arguments_digest,status,requested_at,expires_at,decided_by,decided_at,decision_reason,execution_started_at,execution_finished_at,result_summary,result_digest,arguments_json,COALESCE(result_json,'null'::jsonb)`

type scanner interface{ Scan(...any) error }

func scan(row scanner) (stored, error) {
	var v stored
	err := row.Scan(&v.Operation.OperationID, &v.Operation.TenantID, &v.Operation.RunID, &v.Operation.AttemptID, &v.Operation.NodeID, &v.Operation.ToolName, &v.Operation.ToolResource, &v.Operation.Capability, &v.Operation.Target, &v.Operation.ParameterSummary, &v.Operation.ArgumentsDigest, &v.Operation.Status, &v.Operation.RequestedAt, &v.Operation.ExpiresAt, &v.Operation.DecidedBy, &v.Operation.DecidedAt, &v.Operation.DecisionReason, &v.Operation.ExecutionStartedAt, &v.Operation.ExecutionFinishedAt, &v.Operation.ResultSummary, &v.Operation.ResultDigest, &v.Arguments, &v.Result)
	if errors.Is(err, pgx.ErrNoRows) {
		return stored{}, ErrNotFound
	}
	return v, err
}
func (s *Store) get(ctx context.Context, tenant, id string) (stored, error) {
	return scan(s.pool.QueryRow(ctx, `SELECT `+selectColumns+` FROM tool_approval_operations WHERE tenant_id=$1 AND operation_id=$2`, tenant, id))
}
func (s *Store) Get(ctx context.Context, tenant, id string) (approvalv1.Operation, error) {
	v, e := s.get(ctx, tenant, id)
	return v.Operation, e
}

func (s *Store) List(ctx context.Context, tenant string, offset, limit int) (approvalv1.Page, error) {
	if tenant == "" || offset < 0 || limit < 1 || limit > approvalv1.MaxPageSize {
		return approvalv1.Page{}, ErrInvalid
	}
	_, _ = s.pool.Exec(ctx, `UPDATE tool_approval_operations SET status='EXPIRED' WHERE tenant_id=$1 AND status='PENDING' AND expires_at<=clock_timestamp()`, tenant)
	var total int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM tool_approval_operations WHERE tenant_id=$1`, tenant).Scan(&total); err != nil {
		return approvalv1.Page{}, err
	}
	rows, err := s.pool.Query(ctx, `SELECT `+selectColumns+` FROM tool_approval_operations WHERE tenant_id=$1 ORDER BY requested_at DESC,operation_id DESC OFFSET $2 LIMIT $3`, tenant, offset, limit)
	if err != nil {
		return approvalv1.Page{}, err
	}
	defer rows.Close()
	page := approvalv1.Page{Operations: []approvalv1.Operation{}, Offset: offset, Limit: limit, Total: total}
	for rows.Next() {
		v, e := scan(rows)
		if e != nil {
			return approvalv1.Page{}, e
		}
		page.Operations = append(page.Operations, v.Operation)
	}
	return page, rows.Err()
}

func (s *Store) Decide(ctx context.Context, tenant, id, actor, action, reason, expectedDigest string) (approvalv1.DecisionResponse, error) {
	if tenant == "" || id == "" || actor == "" || !approvalv1.ValidDigest(expectedDigest) || len(reason) > 500 || (action != "approve" && action != "reject") {
		return approvalv1.DecisionResponse{}, ErrInvalid
	}
	status := approvalv1.StatusApproved
	if action == "reject" {
		status = approvalv1.StatusRejected
	}
	tag, err := s.pool.Exec(ctx, `UPDATE tool_approval_operations SET status=$1,decided_by=$2,decided_at=clock_timestamp(),decision_reason=$3
	 WHERE tenant_id=$4 AND operation_id=$5 AND arguments_digest=$6 AND status='PENDING' AND expires_at>clock_timestamp()`, status, actor, reason, tenant, id, expectedDigest)
	if err != nil {
		return approvalv1.DecisionResponse{}, err
	}
	got, e := s.get(ctx, tenant, id)
	if e != nil {
		return approvalv1.DecisionResponse{}, e
	}
	if got.Operation.ArgumentsDigest != expectedDigest {
		return approvalv1.DecisionResponse{}, ErrConflict
	}
	if tag.RowsAffected() == 1 {
		return approvalv1.DecisionResponse{Operation: got.Operation, Outcome: "DECIDED"}, nil
	}
	if got.Operation.Status == status {
		return approvalv1.DecisionResponse{Operation: got.Operation, Outcome: "REPLAY"}, nil
	}
	if got.Operation.Status == approvalv1.StatusPending && !got.Operation.ExpiresAt.After(time.Now()) {
		_, _ = s.pool.Exec(ctx, `UPDATE tool_approval_operations SET status='EXPIRED' WHERE tenant_id=$1 AND operation_id=$2 AND status='PENDING'`, tenant, id)
	}
	return approvalv1.DecisionResponse{}, ErrConflict
}

func (s *Store) AwaitAndBegin(ctx context.Context, tenant, id string, poll time.Duration) (approvalv1.Operation, []byte, error) {
	if poll <= 0 {
		poll = 200 * time.Millisecond
	}
	timer := time.NewTicker(poll)
	defer timer.Stop()
	for {
		v, err := s.get(ctx, tenant, id)
		if err != nil {
			return approvalv1.Operation{}, nil, err
		}
		switch v.Operation.Status {
		case approvalv1.StatusPending:
			if !v.Operation.ExpiresAt.After(time.Now()) {
				_, _ = s.pool.Exec(context.WithoutCancel(ctx), `UPDATE tool_approval_operations SET status='EXPIRED' WHERE tenant_id=$1 AND operation_id=$2 AND status='PENDING'`, tenant, id)
				return v.Operation, nil, ErrDenied
			}
		case approvalv1.StatusApproved:
			tag, e := s.pool.Exec(ctx, `UPDATE tool_approval_operations SET status='EXECUTING',execution_started_at=clock_timestamp() WHERE tenant_id=$1 AND operation_id=$2 AND status='APPROVED'`, tenant, id)
			if e != nil {
				return approvalv1.Operation{}, nil, e
			}
			if tag.RowsAffected() == 1 {
				v.Operation.Status = approvalv1.StatusExecuting
				return v.Operation, nil, nil
			}
		case approvalv1.StatusRejected, approvalv1.StatusExpired:
			return v.Operation, nil, ErrDenied
		case approvalv1.StatusSucceeded:
			return v.Operation, append([]byte(nil), v.Result...), nil
		case approvalv1.StatusExecuting, approvalv1.StatusUnknown:
			return v.Operation, nil, ErrUnknown
		}
		select {
		case <-ctx.Done():
			// The waiting SDK invocation is the only continuation that may consume
			// this approval. Once it has gone away, make the request terminal so a
			// late click cannot authorize a future invocation.
			expireCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
			_, _ = s.pool.Exec(expireCtx, `UPDATE tool_approval_operations SET status='EXPIRED',result_summary='Approval waiter ended before a decision' WHERE tenant_id=$1 AND operation_id=$2 AND status='PENDING'`, tenant, id)
			cancel()
			return approvalv1.Operation{}, nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *Store) Finish(ctx context.Context, tenant, id string, result []byte, callErr error) error {
	status := approvalv1.StatusSucceeded
	summary := "测试工单状态修改完成"
	resultDigest := ""
	var value any
	if callErr != nil {
		status = approvalv1.StatusUnknown
		summary = "工具返回结果未知"
	} else if len(result) > 32768 || !json.Valid(result) {
		status = approvalv1.StatusUnknown
		summary = "工具结果未能可靠持久化"
	} else {
		resultDigest = digest(result)
		if err := json.Unmarshal(result, &value); err != nil {
			return ErrInvalid
		}
	}
	tag, err := s.pool.Exec(ctx, `UPDATE tool_approval_operations SET status=$1,execution_finished_at=clock_timestamp(),result_summary=$2,result_digest=$3,result_json=$4 WHERE tenant_id=$5 AND operation_id=$6 AND status='EXECUTING'`, status, summary, resultDigest, value, tenant, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	return nil
}

// ReconcileInterrupted fences approval continuations owned by a restarted
// Worker identity. Scheduled excludes the resulting UNKNOWN operations, so it
// cannot recover by re-running the entire Agent turn and repeating earlier
// side effects. An operator must reconcile the run explicitly in a later flow.
func (s *Store) ReconcileInterrupted(ctx context.Context, workerID string) error {
	if strings.TrimSpace(workerID) == "" || len(workerID) > 256 {
		return ErrInvalid
	}
	_, err := s.pool.Exec(ctx, `WITH interrupted AS (
	 UPDATE tool_approval_operations o
	 SET status='UNKNOWN',execution_finished_at=clock_timestamp(),
	     result_summary=CASE WHEN o.status='PENDING'
	       THEN 'Worker restarted while waiting for approval; execution did not resume'
	       ELSE 'Worker restarted before a durable tool result was recorded' END
	 FROM execution_attempts a
	 WHERE a.tenant_id=o.tenant_id AND a.attempt_id=o.attempt_id
	   AND a.worker_id=$1 AND o.status IN ('PENDING','APPROVED','EXECUTING')
	 RETURNING o.tenant_id,o.run_id
	)
	UPDATE execution_runs r
	SET wait_reason='TOOL_APPROVAL_RECONCILIATION_REQUIRED',retry_at=NULL
	WHERE EXISTS (SELECT 1 FROM interrupted i WHERE i.tenant_id=r.tenant_id AND i.run_id=r.run_id)
	  AND r.status IN ('RUNNING','RETRY_WAIT')`, workerID)
	return err
}
