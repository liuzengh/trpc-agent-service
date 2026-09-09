// Package audit persists tenant-scoped, redacted governance decisions.
package audit

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"

	servicelog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type Record struct {
	TenantID, Channel, UserID, SessionID, AgentName, ToolName, Decision, ErrorType, TraceID, RequestID, EventID string
	Latency                                                                                                     time.Duration
	CostMicros                                                                                                  int64
	ConfigVersion, PolicyVersion                                                                                tenant.ConfigVersion
	Details                                                                                                     map[string]any
	CreatedAt                                                                                                   time.Time
}
type Store interface {
	Append(context.Context, Record) error
}
type RetentionStore interface {
	Prune(context.Context, string, time.Time) (int64, error)
}

func PruneTenant(ctx context.Context, store RetentionStore, tenantID string, policy tenant.AuditPolicy, now time.Time) (int64, error) {
	if store == nil || tenantID == "" || policy.RetentionDays <= 0 {
		return 0, nil
	}
	return store.Prune(ctx, tenantID, now.UTC().Add(-time.Duration(policy.RetentionDays)*24*time.Hour))
}

type MemoryStore struct {
	mu       sync.Mutex
	records  []Record
	redactor *servicelog.Redactor
}

func NewMemoryStore(redactor *servicelog.Redactor) *MemoryStore {
	if redactor == nil {
		redactor = servicelog.NewRedactor(nil, nil)
	}
	return &MemoryStore{redactor: redactor}
}
func (store *MemoryStore) Append(_ context.Context, record Record) error {
	if record.TenantID == "" || record.Decision == "" || record.TraceID == "" {
		return errors.New("audit: tenant, decision, and trace are required")
	}
	record.ErrorType = store.redactor.RedactString(record.ErrorType)
	record.Details = store.redactor.RedactMap(record.Details)
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	store.mu.Lock()
	for _, existing := range store.records {
		if existing.TenantID == record.TenantID && auditID(existing) == auditID(record) {
			store.mu.Unlock()
			return nil
		}
	}
	store.records = append(store.records, record)
	store.mu.Unlock()
	return nil
}
func (store *MemoryStore) Records(tenantID string) []Record {
	store.mu.Lock()
	defer store.mu.Unlock()
	var result []Record
	for _, record := range store.records {
		if record.TenantID == tenantID {
			copy := record
			copy.Details = store.redactor.RedactMap(record.Details)
			result = append(result, copy)
		}
	}
	return result
}
func (store *MemoryStore) Prune(_ context.Context, tenantID string, before time.Time) (int64, error) {
	if tenantID == "" {
		return 0, errors.New("audit: tenant is required")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	kept := store.records[:0]
	var deleted int64
	for _, record := range store.records {
		if record.TenantID == tenantID && record.CreatedAt.Before(before) {
			deleted++
			continue
		}
		kept = append(kept, record)
	}
	store.records = kept
	return deleted, nil
}

type SQLStore struct {
	DB       *sql.DB
	Redactor *servicelog.Redactor
	Now      func() time.Time
}

func (store *SQLStore) Append(ctx context.Context, record Record) error {
	if store == nil || store.DB == nil {
		return errors.New("audit: nil SQL database")
	}
	if record.TenantID == "" || record.Decision == "" || record.TraceID == "" {
		return errors.New("audit: tenant, decision, and trace are required")
	}
	redactor := store.Redactor
	if redactor == nil {
		redactor = servicelog.NewRedactor(nil, nil)
	}
	details := redactor.RedactMap(record.Details)
	payload, err := json.Marshal(details)
	if err != nil {
		return err
	}
	created := record.CreatedAt
	if created.IsZero() {
		created = store.now().UTC()
	}
	_, err = store.DB.ExecContext(ctx, `INSERT INTO audit_logs (tenant_id,audit_id,channel,user_id,session_id,agent_name,tool_name,decision,latency_ms,error_type,cost_micros,config_version,policy_version,trace_id,request_id,event_id,details_json,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18) ON CONFLICT (tenant_id,audit_id) DO NOTHING`, record.TenantID, auditID(record), record.Channel, record.UserID, record.SessionID, record.AgentName, record.ToolName, record.Decision, record.Latency.Milliseconds(), redactor.RedactString(record.ErrorType), record.CostMicros, record.ConfigVersion, record.PolicyVersion, record.TraceID, record.RequestID, record.EventID, payload, created)
	return err
}
func (store *SQLStore) Prune(ctx context.Context, tenantID string, before time.Time) (int64, error) {
	if store == nil || store.DB == nil || tenantID == "" {
		return 0, errors.New("audit: SQL database and tenant are required")
	}
	result, err := store.DB.ExecContext(ctx, `DELETE FROM audit_logs WHERE tenant_id=$1 AND created_at<$2`, tenantID, before.UTC())
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}
func (store *SQLStore) now() time.Time {
	if store.Now != nil {
		return store.Now()
	}
	return time.Now()
}
func auditID(record Record) string {
	digest := sha256.Sum256([]byte(record.TraceID + "\x00" + record.RequestID + "\x00" + record.Decision))
	return hex.EncodeToString(digest[:])
}

var _ Store = (*MemoryStore)(nil)
var _ Store = (*SQLStore)(nil)
