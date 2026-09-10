package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
)

// HandoffStore is a tenant-bound PostgreSQL execution-audit handoff adapter.
// The database functions own the reserve/finalize state fence.
type HandoffStore struct {
	db       *sql.DB
	tenantID string
}

var _ audit.HandoffStore = (*HandoffStore)(nil)

// NewHandoffStore creates a PostgreSQL-backed handoff store.
func NewHandoffStore(db *sql.DB, tenantID string) (*HandoffStore, error) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" || hasControl(tenantID) {
		return nil, audit.ErrInvalid
	}
	return &HandoffStore{db: db, tenantID: tenantID}, nil
}

func (s *HandoffStore) check(ctx context.Context, tenantID string) error {
	if ctx == nil {
		return audit.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.db == nil {
		return ErrStorage
	}
	if tenantID != s.tenantID {
		return audit.ErrTenantScope
	}
	return nil
}

// Reserve stores a pending handoff.
func (s *HandoffStore) Reserve(ctx context.Context, value audit.ExecutionHandoff) (audit.ExecutionHandoff, error) {
	if err := s.check(ctx, value.TenantID); err != nil {
		return audit.ExecutionHandoff{}, err
	}
	if value.HandoffID == "" || value.RequestID == "" || value.State != audit.HandoffPending {
		return audit.ExecutionHandoff{}, audit.ErrInvalid
	}
	tx, err := begin(ctx, s.db)
	if err != nil {
		return audit.ExecutionHandoff{}, err
	}
	defer rollback(tx)
	var out audit.ExecutionHandoff
	err = tx.QueryRowContext(ctx, `SELECT tenant_id,handoff_id,request_id,trace_id,event_id,state,result,error_type,latency_ms,created_at,updated_at FROM public.reserve_execution_audit_handoff($1,$2,$3,$4,$5,$6)`, s.tenantID, value.HandoffID, value.RequestID, value.TraceID, value.EventID, nullableTime(value.CreatedAt)).Scan(handoffArgs(&out)...)
	if err != nil {
		return audit.ExecutionHandoff{}, mapDBError(ctx, err, audit.ErrHandoffNotFound, audit.ErrHandoffConflict, audit.ErrHandoffConflict, audit.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return audit.ExecutionHandoff{}, err
	}
	return out.Clone(), nil
}

// Finalize marks a handoff complete.
func (s *HandoffStore) Finalize(ctx context.Context, value audit.ExecutionHandoff) (audit.ExecutionHandoff, error) {
	if err := s.check(ctx, value.TenantID); err != nil {
		return audit.ExecutionHandoff{}, err
	}
	if value.HandoffID == "" || value.State != audit.HandoffFinalized {
		return audit.ExecutionHandoff{}, audit.ErrInvalid
	}
	tx, err := begin(ctx, s.db)
	if err != nil {
		return audit.ExecutionHandoff{}, err
	}
	defer rollback(tx)
	var out audit.ExecutionHandoff
	err = tx.QueryRowContext(ctx, `SELECT tenant_id,handoff_id,request_id,trace_id,event_id,state,result,error_type,latency_ms,created_at,updated_at FROM public.finalize_execution_audit_handoff($1,$2,$3,$4,$5,$6)`, s.tenantID, value.HandoffID, string(value.Result), nullable(value.ErrorType), value.LatencyMS, nullableTime(value.UpdatedAt)).Scan(handoffArgs(&out)...)
	if err != nil {
		return audit.ExecutionHandoff{}, mapDBError(ctx, err, audit.ErrHandoffNotFound, audit.ErrHandoffConflict, audit.ErrHandoffConflict, audit.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return audit.ExecutionHandoff{}, err
	}
	return out.Clone(), nil
}

// Get retrieves a handoff within the configured tenant scope.
func (s *HandoffStore) Get(ctx context.Context, tenantID, handoffID string) (audit.ExecutionHandoff, error) {
	if err := s.check(ctx, tenantID); err != nil {
		return audit.ExecutionHandoff{}, err
	}
	if strings.TrimSpace(handoffID) == "" {
		return audit.ExecutionHandoff{}, audit.ErrInvalid
	}
	var out audit.ExecutionHandoff
	err := s.db.QueryRowContext(ctx, `SELECT tenant_id,handoff_id,request_id,trace_id,event_id,state,result,error_type,latency_ms,created_at,updated_at FROM public.execution_audit_handoff WHERE tenant_id=$1 AND handoff_id=$2`, s.tenantID, handoffID).Scan(handoffArgs(&out)...)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return audit.ExecutionHandoff{}, audit.ErrHandoffNotFound
		}
		return audit.ExecutionHandoff{}, mapDBError(ctx, err, audit.ErrHandoffNotFound, audit.ErrHandoffConflict, audit.ErrHandoffConflict, audit.ErrInvalid)
	}
	return out.Clone(), nil
}

func handoffArgs(out *audit.ExecutionHandoff) []any {
	return []any{&out.TenantID, &out.HandoffID, &out.RequestID, &out.TraceID, &out.EventID, scanState{out: out}, scanResult{out: out}, scanError{out: out}, scanLatency{out: out}, &out.CreatedAt, &out.UpdatedAt}
}

type scanState struct{ out *audit.ExecutionHandoff }

func (s scanState) Scan(src any) error {
	var v sql.NullString
	if err := v.Scan(src); err != nil {
		return err
	}
	s.out.State = audit.HandoffState(v.String)
	return nil
}

type scanResult struct{ out *audit.ExecutionHandoff }

func (s scanResult) Scan(src any) error {
	var v sql.NullString
	if err := v.Scan(src); err != nil {
		return err
	}
	s.out.Result = audit.ExecutionResult(v.String)
	return nil
}

type scanError struct{ out *audit.ExecutionHandoff }

func (s scanError) Scan(src any) error {
	var v sql.NullString
	if err := v.Scan(src); err != nil {
		return err
	}
	s.out.ErrorType = v.String
	return nil
}

type scanLatency struct{ out *audit.ExecutionHandoff }

func (s scanLatency) Scan(src any) error {
	var v sql.NullInt64
	if err := v.Scan(src); err != nil {
		return err
	}
	if v.Valid {
		x := v.Int64
		s.out.LatencyMS = &x
	}
	return nil
}
func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}
