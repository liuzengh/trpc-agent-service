package log

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/cyl6/trpc-agent-service/trpcservice/observability"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrAuditConflict   = errors.New("audit record conflicts with an existing audit id")
	ErrAuditDurability = errors.New("audit record could not be durably persisted")
)

// AuditRecord is the redacted, hash-linked representation returned by the
// cross-node audit reader.
type AuditRecord struct {
	AuditID      string
	TenantID     string
	Sequence     int64
	Payload      json.RawMessage
	ContentHash  string
	PreviousHash string
	RecordHash   string
	CreatedAt    time.Time
}

// PostgresSink writes an append-only hash chain and uses one 0600 JSONL spool
// per tenant when PostgreSQL is temporarily unavailable. A spool append is
// considered successful only after fsync.
type PostgresSink struct {
	pool      *pgxpool.Pool
	spoolRoot string
	mu        sync.Mutex
}

// ReplayChecker lets a worker distinguish a successful audit commit from the
// narrow case where a strict session commit succeeded but audit persistence
// failed before the worker could save its result. Non-durable test sinks do
// not implement this interface and therefore do not duplicate replay output.
type ReplayChecker interface {
	AuditExists(context.Context, string) (bool, error)
}

func NewPostgresSink(pool *pgxpool.Pool, spoolRoot string) (*PostgresSink, error) {
	if pool == nil {
		return nil, errors.New("audit postgres pool is nil")
	}
	return &PostgresSink{pool: pool, spoolRoot: spoolRoot}, nil
}

func (s *PostgresSink) Ready(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return ErrAuditDurability
	}
	return s.pool.Ping(ctx)
}

func (s *PostgresSink) Write(entry Entry) (err error) {
	return s.WriteContext(context.Background(), entry)
}

func (s *PostgresSink) WriteContext(ctx context.Context, entry Entry) (err error) {
	ctx, finish := observability.StartStorage(ctx, "audit.append", "postgres", entry.TenantID, entry.ConfigRevision)
	defer func() { finish(err) }()
	if s == nil || s.pool == nil {
		return fmt.Errorf("%w: postgres sink is unavailable", ErrAuditDurability)
	}
	// Router applies tenant-specific patterns before calling the SQL sink. Keep
	// the sink safe for direct callers as well, so a recovery spool can never
	// become an accidental raw-content log.
	redactor := NewRedactor(nil, nil)
	entry.Reason = redactor.Clean(entry.Reason)
	entry.ErrorType = redactor.Clean(entry.ErrorType)
	entry.AuditID = StableAuditID(entry)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writeDB(ctxOrBackground(ctx), entry); err == nil {
		return nil
	} else if errors.Is(err, ErrAuditConflict) {
		return err
	} else if spoolErr := s.appendSpool(entry); spoolErr == nil {
		return nil
	} else {
		return fmt.Errorf("%w: database and spool persistence failed: %v", ErrAuditDurability, spoolErr)
	}
}

func (s *PostgresSink) AuditExists(ctx context.Context, auditID string) (exists bool, err error) {
	ctx, finish := observability.StartStorage(ctx, "audit.exists", "postgres", "", "")
	defer func() { finish(err) }()
	if s == nil || s.pool == nil || auditID == "" {
		return false, ErrAuditDurability
	}
	if queryErr := s.pool.QueryRow(ctxOrBackground(ctx), `SELECT EXISTS(SELECT 1 FROM audit_records WHERE audit_id = $1)`, auditID).Scan(&exists); queryErr != nil {
		err = queryErr
		return false, queryErr
	}
	return exists, nil
}

func (s *PostgresSink) writeDB(ctx context.Context, entry Entry) error {
	if ctx == nil {
		ctx = context.Background()
	}
	payload, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal audit record: %w", err)
	}
	contentHash := canonicalJSONHash(payload)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin audit transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var existingHash string
	var existingPayload []byte
	err = tx.QueryRow(ctx, `
		SELECT content_sha256, payload
		FROM audit_records WHERE audit_id = $1`, entry.AuditID).Scan(&existingHash, &existingPayload)
	if err == nil {
		if existingHash != contentHash || !jsonEqual(existingPayload, payload) {
			return ErrAuditConflict
		}
		return tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("check audit idempotency: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO audit_tenant_heads (tenant_id)
		VALUES ($1) ON CONFLICT (tenant_id) DO NOTHING`, entry.TenantID); err != nil {
		return fmt.Errorf("ensure audit tenant head: %w", err)
	}
	var sequence int64
	var previousHash string
	if err := tx.QueryRow(ctx, `
		SELECT last_sequence, last_hash
		FROM audit_tenant_heads WHERE tenant_id = $1 FOR UPDATE`, entry.TenantID).
		Scan(&sequence, &previousHash); err != nil {
		return fmt.Errorf("lock audit tenant head: %w", err)
	}
	sequence++
	recordHash := auditRecordHash(entry.TenantID, sequence, previousHash, contentHash)
	if _, err := tx.Exec(ctx, `
		INSERT INTO audit_records
			(audit_id, tenant_id, sequence, payload, content_sha256, previous_hash, record_hash)
		VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7)`,
		entry.AuditID, entry.TenantID, sequence, payload, contentHash, previousHash, recordHash); err != nil {
		return fmt.Errorf("insert audit record: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE audit_tenant_heads
		SET last_sequence = $2, last_hash = $3, updated_at = clock_timestamp()
		WHERE tenant_id = $1`, entry.TenantID, sequence, recordHash); err != nil {
		return fmt.Errorf("advance audit tenant head: %w", err)
	}
	return tx.Commit(ctx)
}

func auditRecordHash(tenant string, sequence int64, previousHash, contentHash string) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x1f%d\x1f%s\x1f%s", tenant, sequence, previousHash, contentHash)))
	return hex.EncodeToString(digest[:])
}

func jsonEqual(left, right []byte) bool {
	var a, b any
	return json.Unmarshal(left, &a) == nil && json.Unmarshal(right, &b) == nil && reflect.DeepEqual(a, b)
}

func canonicalJSONHash(payload []byte) string {
	var value any
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return ContentHash(string(payload))
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return ContentHash(string(payload))
	}
	return ContentHash(string(canonical))
}

func (s *PostgresSink) spoolPath(tenantID string) string {
	if s.spoolRoot == "" || tenantID == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(tenantID))
	return filepath.Join(s.spoolRoot, hex.EncodeToString(digest[:])+".jsonl")
}

func (s *PostgresSink) appendSpool(entry Entry) error {
	path := s.spoolPath(entry.TenantID)
	if path == "" {
		return errors.New("audit spool path is not configured")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create audit spool directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open audit spool: %w", err)
	}
	defer file.Close()
	if err := file.Chmod(0o600); err != nil {
		return fmt.Errorf("set audit spool permissions: %w", err)
	}
	if err := json.NewEncoder(file).Encode(entry); err != nil {
		return fmt.Errorf("write audit spool: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync audit spool: %w", err)
	}
	return nil
}

// Drain retries every complete line. Successful lines are removed only after
// the PostgreSQL transaction commits; malformed lines are isolated so one bad
// record cannot block the rest of the spool.
func (s *PostgresSink) Drain(ctx context.Context) (err error) {
	ctx, finish := observability.StartStorage(ctx, "audit.drain", "postgres", "", "")
	defer func() { finish(err) }()
	if s == nil || s.spoolRoot == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.spoolRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var result error
	for _, item := range entries {
		if item.IsDir() || filepath.Ext(item.Name()) != ".jsonl" {
			continue
		}
		if err := s.drainFile(ctx, filepath.Join(s.spoolRoot, item.Name())); err != nil {
			result = errors.Join(result, err)
		}
	}
	return result
}

// StartDrain keeps retrying the local spool while the process is alive. The
// initial startup drain is still performed by the caller before serving
// traffic; this loop closes the recovery gap where PostgreSQL becomes
// available again after startup. The returned function is idempotent and
// should be called before closing the sink.
func (s *PostgresSink) StartDrain(ctx context.Context, interval time.Duration) func() {
	if s == nil || s.spoolRoot == "" {
		return func() {}
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	loopCtx, cancel := context.WithCancel(ctxOrBackground(ctx))
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = s.Drain(loopCtx)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-loopCtx.Done():
				return
			case <-ticker.C:
				_ = s.Drain(loopCtx)
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func (s *PostgresSink) drainFile(ctx context.Context, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	quarantinePath := path + ".corrupt"
	var remaining [][]byte
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 4<<20)
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		var entry Entry
		if err := json.Unmarshal(line, &entry); err != nil || entry.TenantID == "" {
			if quarantineErr := appendQuarantine(quarantinePath, line); quarantineErr != nil {
				return errors.Join(fmt.Errorf("quarantine corrupt audit spool: %w", err), quarantineErr)
			}
			continue
		}
		entry.AuditID = StableAuditID(entry)
		if err := s.writeDB(ctxOrBackground(ctx), entry); err != nil {
			remaining = append(remaining, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if len(remaining) == 0 {
		return os.Remove(path)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".audit-spool-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	for _, line := range remaining {
		if _, err := tmp.Write(append(line, '\n')); err != nil {
			tmp.Close()
			return err
		}
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func appendQuarantine(path string, line []byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := file.Chmod(0o600); err != nil {
		return err
	}
	if _, err := file.Write(append(line, '\n')); err != nil {
		return err
	}
	return file.Sync()
}

func (s *PostgresSink) ReadTenant(ctx context.Context, tenantID string, afterSequence int64, limit int) (result []AuditRecord, err error) {
	ctx, finish := observability.StartStorage(ctx, "audit.read", "postgres", tenantID, "")
	defer func() { finish(err) }()
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	rows, err := s.pool.Query(ctxOrBackground(ctx), `
		SELECT audit_id, tenant_id, sequence, payload, content_sha256, previous_hash, record_hash, created_at
		FROM audit_records WHERE tenant_id = $1 AND sequence > $2
		ORDER BY sequence LIMIT $3`, tenantID, afterSequence, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result = make([]AuditRecord, 0)
	for rows.Next() {
		var item AuditRecord
		if err := rows.Scan(&item.AuditID, &item.TenantID, &item.Sequence, &item.Payload,
			&item.ContentHash, &item.PreviousHash, &item.RecordHash, &item.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

// VerifyTenantChain validates both payload hashes and predecessor links, so a
// reader on a different node can prove it observed a complete audit prefix.
func (s *PostgresSink) VerifyTenantChain(ctx context.Context, tenantID string) (err error) {
	ctx, finish := observability.StartStorage(ctx, "audit.verify", "postgres", tenantID, "")
	defer func() { finish(err) }()
	previous := ""
	var after, expected int64 = 0, 1
	for {
		records, err := s.ReadTenant(ctx, tenantID, after, 1000)
		if err != nil {
			return err
		}
		for _, record := range records {
			if record.Sequence != expected {
				return fmt.Errorf("audit sequence gap at sequence %d, want %d", record.Sequence, expected)
			}
			if canonicalJSONHash(record.Payload) != record.ContentHash {
				return fmt.Errorf("audit payload hash mismatch at sequence %d", record.Sequence)
			}
			if record.PreviousHash != previous || auditRecordHash(tenantID, record.Sequence, previous, record.ContentHash) != record.RecordHash {
				return fmt.Errorf("audit chain mismatch at sequence %d", record.Sequence)
			}
			previous = record.RecordHash
			after = record.Sequence
			expected++
		}
		if len(records) < 1000 {
			return nil
		}
	}
}

func (s *PostgresSink) String() string { return strings.TrimSpace(s.spoolRoot) }

func ctxOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

var _ Sink = (*PostgresSink)(nil)
var _ io.Closer = (*PostgresSink)(nil)

func (s *PostgresSink) Close() error {
	return s.Drain(context.Background())
}
