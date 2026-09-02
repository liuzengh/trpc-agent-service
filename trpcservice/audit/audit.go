// Package audit records governance decisions and per-request accounting to
// MySQL asynchronously, so audit writes never block the hot message path.
package audit

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// Decision values for Entry.Decision. The first three mirror the audit_logs
// schema comment (allow/deny/approve); executed/failed cover tool/agent runs.
const (
	DecisionAllow    = "allow"
	DecisionDeny     = "deny"
	DecisionApprove  = "approve"
	DecisionExecuted = "executed"
	DecisionFailed   = "failed"
)

// Entry is one audit record, mirroring the audit_logs table. It carries the
// full README-required surface: tenant, channel, user, session, agent, tool,
// decision, latency, error, cost, and trace id.
type Entry struct {
	TenantID  string
	Channel   string
	UserID    string
	SessionID string
	AgentName string
	ToolName  string
	Decision  string
	Latency   time.Duration
	ErrorType string
	Cost      float64
	TraceID   string
}

// Recorder accepts audit entries without blocking the caller, plus
// synchronous best-effort usage metering.
type Recorder interface {
	Record(e Entry)
	RecordUsage(ctx context.Context, e UsageEntry) error
	Close() error
}

// Log is one persisted audit row for the read side (admin list). It mirrors
// the audit_logs columns in snake_case JSON for direct front-end rendering.
type Log struct {
	AuditID   string    `json:"audit_id"`
	TenantID  string    `json:"tenant_id"`
	Channel   string    `json:"channel"`
	UserID    string    `json:"user_id"`
	SessionID string    `json:"session_id"`
	AgentName string    `json:"agent_name,omitempty"`
	ToolName  string    `json:"tool_name,omitempty"`
	Decision  string    `json:"decision,omitempty"`
	LatencyMS int64     `json:"latency_ms,omitempty"`
	ErrorType string    `json:"error_type,omitempty"`
	Cost      float64   `json:"cost,omitempty"`
	TraceID   string    `json:"trace_id"`
	CreatedAt time.Time `json:"created_at"`
}

// toLog maps an in-memory entry plus its storage key onto a Log row.
func (e Entry) toLog(auditID string, createdAt time.Time) Log {
	return Log{
		AuditID:   auditID,
		TenantID:  e.TenantID,
		Channel:   e.Channel,
		UserID:    e.UserID,
		SessionID: e.SessionID,
		AgentName: e.AgentName,
		ToolName:  e.ToolName,
		Decision:  e.Decision,
		LatencyMS: e.Latency.Milliseconds(),
		ErrorType: e.ErrorType,
		Cost:      e.Cost,
		TraceID:   e.TraceID,
		CreatedAt: createdAt,
	}
}

const (
	defaultBuffer     = 1024
	defaultBatch      = 100
	defaultFlushEvery = time.Second
)

// MySQLRecorder batches entries in memory and flushes them to audit_logs in
// transactions, driven by a background goroutine.
type MySQLRecorder struct {
	db         *sql.DB
	ch         chan Entry
	done       chan struct{}
	closeOnce  sync.Once
	wg         sync.WaitGroup
	dropped    int64
	batch      int
	flushEvery time.Duration
}

// NewMySQL starts a recorder flushing to the given DB.
func NewMySQL(db *sql.DB) *MySQLRecorder {
	r := &MySQLRecorder{
		db:         db,
		ch:         make(chan Entry, defaultBuffer),
		done:       make(chan struct{}),
		batch:      defaultBatch,
		flushEvery: defaultFlushEvery,
	}
	r.wg.Add(1)
	go r.loop()
	return r
}

// Record enqueues an entry without blocking. When the buffer is full the entry
// is dropped and counted; audit is best-effort, never on the hot path.
func (r *MySQLRecorder) Record(e Entry) {
	select {
	case r.ch <- e:
	default:
		atomic.AddInt64(&r.dropped, 1)
	}
}

// Dropped reports how many entries were dropped due to a full buffer.
func (r *MySQLRecorder) Dropped() int64 {
	return atomic.LoadInt64(&r.dropped)
}

const listSQL = `SELECT audit_id, tenant_id, channel, user_id, session_id,
	COALESCE(agent_name, ''), COALESCE(tool_name, ''), COALESCE(decision, ''),
	COALESCE(latency_ms, 0), COALESCE(error_type, ''), CAST(COALESCE(cost, 0) AS DOUBLE),
	trace_id, created_at
FROM audit_logs
WHERE (? = '' OR tenant_id = ?)
ORDER BY id DESC
LIMIT ?`

// List returns the most recent audit rows, newest first, optionally scoped to
// one tenant ("" = all tenants). It is the read side backing the admin page.
func (r *MySQLRecorder) List(ctx context.Context, tenantID string, limit int) ([]Log, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := r.db.QueryContext(ctx, listSQL, tenantID, tenantID, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]Log, 0, 32)
	for rows.Next() {
		var lg Log
		if err := rows.Scan(&lg.AuditID, &lg.TenantID, &lg.Channel, &lg.UserID,
			&lg.SessionID, &lg.AgentName, &lg.ToolName, &lg.Decision,
			&lg.LatencyMS, &lg.ErrorType, &lg.Cost, &lg.TraceID, &lg.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, lg)
	}
	return out, rows.Err()
}

// Close drains and flushes all pending entries, then stops the goroutine.
func (r *MySQLRecorder) Close() error {
	r.closeOnce.Do(func() {
		close(r.done)
		r.wg.Wait()
	})
	return nil
}

func (r *MySQLRecorder) loop() {
	defer r.wg.Done()
	batch := make([]Entry, 0, r.batch)
	ticker := time.NewTicker(r.flushEvery)
	defer ticker.Stop()

	for {
		select {
		case e := <-r.ch:
			batch = append(batch, e)
			if len(batch) >= r.batch {
				r.flush(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				r.flush(batch)
				batch = batch[:0]
			}
		case <-r.done:
			for {
				select {
				case e := <-r.ch:
					batch = append(batch, e)
				default:
					r.flush(batch)
					return
				}
			}
		}
	}
}

const insertSQL = `INSERT INTO audit_logs
(audit_id, tenant_id, channel, user_id, session_id, agent_name, tool_name, decision, latency_ms, error_type, cost, trace_id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

// flush writes one batch in a single transaction; a failed row rolls back the
// whole batch (audit is best-effort, so we log and drop rather than retry).
func (r *MySQLRecorder) flush(batch []Entry) {
	if len(batch) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		slog.Error("audit: begin tx", "err", err)
		return
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		slog.Error("audit: prepare", "err", err)
		return
	}
	defer func() { _ = stmt.Close() }()

	for _, e := range batch {
		if _, err := stmt.ExecContext(ctx,
			uuid.NewString(), e.TenantID, e.Channel, e.UserID, e.SessionID,
			nullString(e.AgentName), nullString(e.ToolName), nullString(e.Decision),
			nullInt(e.Latency), nullString(e.ErrorType), nullFloat(e.Cost), e.TraceID,
		); err != nil {
			slog.Error("audit: insert", "err", err)
			return // roll back the whole batch
		}
	}
	if err := tx.Commit(); err != nil {
		slog.Error("audit: commit", "err", err)
	}
}

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullInt(d time.Duration) any {
	ms := d.Milliseconds()
	if ms == 0 {
		return nil
	}
	return ms
}

func nullFloat(f float64) any {
	if f == 0 {
		return nil
	}
	return f
}
