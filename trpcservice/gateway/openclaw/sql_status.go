package openclaw

import (
	"context"
	"database/sql"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

// CancellationStore persists cancellation intent so a missed transient
// notification is still observed by the Worker that owns the request.
type CancellationStore interface {
	Requested(context.Context, string, string) bool
}

// SQLStatusStore is the production cross-node status and cancellation store.
type SQLStatusStore struct {
	DB  *sql.DB
	Now func() time.Time
}

var _ StatusStore = (*SQLStatusStore)(nil)
var _ Canceler = (*SQLStatusStore)(nil)
var _ CancellationStore = (*SQLStatusStore)(nil)

// Publish projects a run event into one tenant-scoped status row. The event
// publisher contract cannot return errors, so a bounded write failure is left
// for the durable Inbox/Event facts and operational telemetry to expose.
func (store *SQLStatusStore) Publish(event gateway.RunEvent) {
	if store == nil || store.DB == nil || event.TenantID == "" || event.BindingID == "" || event.RequestID == "" || event.Type == "" {
		return
	}
	// Token deltas and tool progress stay on Redis Pub/Sub; persisting each one
	// would turn model streaming into PostgreSQL write amplification. Only
	// queryable lifecycle transitions belong in the shared status projection.
	switch event.Type {
	case "run.accepted", "run.started", "run.approval_required", "run.retrying", "message.completed", "run.completed", "run.canceled", "run.error":
	default:
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	now := store.now().UTC()
	_, _ = store.DB.ExecContext(ctx, `INSERT INTO run_statuses
		(tenant_id,binding_id,request_id,session_id,trace_id,status,reply,error,worker_id,created_at,updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,NULLIF($7,''),NULLIF($8,''),NULLIF($9,''),$10,$10)
		ON CONFLICT (tenant_id,request_id) DO UPDATE SET
		binding_id=EXCLUDED.binding_id,session_id=EXCLUDED.session_id,trace_id=EXCLUDED.trace_id,
		status=EXCLUDED.status,reply=EXCLUDED.reply,error=EXCLUDED.error,
		worker_id=COALESCE(EXCLUDED.worker_id,run_statuses.worker_id),updated_at=EXCLUDED.updated_at
		WHERE run_statuses.status NOT IN ('run.completed','run.canceled','run.error')
		   OR EXCLUDED.status IN ('run.completed','run.canceled','run.error')`,
		event.TenantID, event.BindingID, event.RequestID, event.SessionID, event.TraceID,
		event.Type, event.Message, event.Error, event.WorkerID, now)
}

// Get reads request status from the shared PostgreSQL projection.
func (store *SQLStatusStore) Get(ctx context.Context, tenantID, requestID string) (RunStatus, bool) {
	if store == nil || store.DB == nil || ctx == nil || tenantID == "" || requestID == "" {
		return RunStatus{}, false
	}
	var status RunStatus
	var reply, runError, workerID sql.NullString
	var sessionID, traceID string
	err := store.DB.QueryRowContext(ctx, `SELECT tenant_id,binding_id,request_id,session_id,COALESCE(trace_id,''),status,reply,error,worker_id,cancel_requested,updated_at
		FROM run_statuses WHERE tenant_id=$1 AND request_id=$2`, tenantID, requestID).Scan(
		&status.TenantID, &status.BindingID, &status.RequestID, &sessionID, &traceID, &status.Type,
		&reply, &runError, &workerID, &status.CancelRequested, &status.UpdatedAt)
	if err != nil {
		return RunStatus{}, false
	}
	_ = sessionID
	_ = traceID
	status.Reply, status.Error, status.WorkerID = reply.String, runError.String, workerID.String
	return status, true
}

// Cancel atomically records intent only for a known non-terminal request.
func (store *SQLStatusStore) Cancel(tenantID, requestID string) bool {
	if store == nil || store.DB == nil || tenantID == "" || requestID == "" {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result, err := store.DB.ExecContext(ctx, `UPDATE run_statuses SET cancel_requested=TRUE,updated_at=$3
		WHERE tenant_id=$1 AND request_id=$2 AND status NOT IN ('run.completed','run.canceled','run.error')`, tenantID, requestID, store.now().UTC())
	if err != nil {
		return false
	}
	affected, err := result.RowsAffected()
	return err == nil && affected == 1
}

// Requested is the durable fallback checked before and during execution.
func (store *SQLStatusStore) Requested(ctx context.Context, tenantID, requestID string) bool {
	if store == nil || store.DB == nil || ctx == nil || tenantID == "" || requestID == "" {
		return false
	}
	var requested bool
	err := store.DB.QueryRowContext(ctx, `SELECT cancel_requested FROM run_statuses WHERE tenant_id=$1 AND request_id=$2`, tenantID, requestID).Scan(&requested)
	return err == nil && requested
}

func (store *SQLStatusStore) now() time.Time {
	if store != nil && store.Now != nil {
		return store.Now()
	}
	return time.Now()
}
