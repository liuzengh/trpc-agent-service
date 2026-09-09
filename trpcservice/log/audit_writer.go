package log

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// AuditWriterConfig controls asynchronous audit persistence.
type AuditWriterConfig struct {
	BufferSize    int
	BatchSize     int
	FlushInterval time.Duration
}

// MySQLAuditWriter batches audit rows without blocking request execution.
type MySQLAuditWriter struct {
	db       *sql.DB
	redactor Redactor
	config   AuditWriterConfig
	input    chan AuditRecord

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	writeMu   sync.RWMutex
	closed    atomic.Bool
	dropped   atomic.Uint64
	closeOnce sync.Once
	closeErr  error
}

// NewMySQLAuditWriter starts the background batch writer.
func NewMySQLAuditWriter(
	db *sql.DB,
	redactor Redactor,
	config AuditWriterConfig,
) (*MySQLAuditWriter, error) {
	if db == nil {
		return nil, errors.New("audit database is required")
	}
	if redactor == nil {
		redactor = NewRedactor()
	}
	if config.BufferSize <= 0 {
		config.BufferSize = 1024
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 50
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	writer := &MySQLAuditWriter{
		db:       db,
		redactor: redactor,
		config:   config,
		input:    make(chan AuditRecord, config.BufferSize),
		ctx:      ctx,
		cancel:   cancel,
		done:     make(chan struct{}),
	}
	go writer.run()
	return writer, nil
}

// Write implements AuditWriter. A full buffer drops rather than blocking the
// business path; Dropped exposes the loss for monitoring.
func (w *MySQLAuditWriter) Write(_ context.Context, record AuditRecord) {
	w.writeMu.RLock()
	defer w.writeMu.RUnlock()
	if w.closed.Load() {
		w.dropped.Add(1)
		return
	}
	record = w.redactRecord(record)
	if record.TS.IsZero() {
		record.TS = time.Now()
	}
	select {
	case w.input <- record:
	default:
		w.dropped.Add(1)
	}
}

// Dropped returns records rejected because the writer was full or closed.
func (w *MySQLAuditWriter) Dropped() uint64 {
	return w.dropped.Load()
}

// Close drains accepted records and waits for the worker.
func (w *MySQLAuditWriter) Close() error {
	w.closeOnce.Do(func() {
		w.writeMu.Lock()
		w.closed.Store(true)
		w.writeMu.Unlock()
		w.cancel()
		<-w.done
	})
	return w.closeErr
}

func (w *MySQLAuditWriter) run() {
	defer close(w.done)
	ticker := time.NewTicker(w.config.FlushInterval)
	defer ticker.Stop()
	batch := make([]AuditRecord, 0, w.config.BatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := w.flush(ctx, batch)
		cancel()
		if err != nil {
			w.closeErr = errors.Join(w.closeErr, err)
		}
		batch = batch[:0]
	}
	for {
		select {
		case record := <-w.input:
			batch = append(batch, record)
			if len(batch) >= w.config.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-w.ctx.Done():
			for {
				select {
				case record := <-w.input:
					batch = append(batch, record)
				default:
					flush()
					return
				}
			}
		}
	}
}

func (w *MySQLAuditWriter) flush(ctx context.Context, records []AuditRecord) error {
	const columns = `(tenant_id, channel, user_id, session_id, agent_name,
		tool_name, decision, latency_ms, error_type, cost, trace_id, request_id, ts)`
	placeholders := make([]string, 0, len(records))
	args := make([]any, 0, len(records)*13)
	for _, record := range records {
		placeholders = append(placeholders, "(?,?,?,?,?,?,?,?,?,?,?,?,?)")
		args = append(args,
			record.TenantID,
			record.Channel,
			record.UserID,
			record.SessionID,
			record.AgentName,
			record.ToolName,
			record.Decision,
			record.Latency.Milliseconds(),
			record.ErrorType,
			record.Cost,
			record.TraceID,
			record.RequestID,
			record.TS,
		)
	}
	query := `INSERT INTO audit_log ` + columns + ` VALUES ` + strings.Join(placeholders, ",")
	if _, err := w.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("insert audit batch: %w", err)
	}
	return nil
}

func (w *MySQLAuditWriter) redactRecord(record AuditRecord) AuditRecord {
	record.TenantID = w.redactor.Redact(record.TenantID)
	record.Channel = w.redactor.Redact(record.Channel)
	record.UserID = w.redactor.Redact(record.UserID)
	record.SessionID = w.redactor.Redact(record.SessionID)
	record.AgentName = w.redactor.Redact(record.AgentName)
	record.ToolName = w.redactor.Redact(record.ToolName)
	record.Decision = w.redactor.Redact(record.Decision)
	record.ErrorType = w.redactor.Redact(record.ErrorType)
	record.TraceID = w.redactor.Redact(record.TraceID)
	record.RequestID = w.redactor.Redact(record.RequestID)
	return record
}
