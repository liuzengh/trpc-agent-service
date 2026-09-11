package background

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
)

type WatermarkKey struct{ TenantID, AppID, UserID, SessionID, JobType string }
type Watermarks interface {
	Read(context.Context, WatermarkKey) (time.Time, bool, error)
	Advance(context.Context, WatermarkKey, time.Time) (time.Time, error)
}

func watermarkKey(job Job, payload SessionJobPayload) (WatermarkKey, error) {
	tenantID, appID, err := runtimecontext.ParseStorageScope(payload.StorageScope)
	if err != nil || tenantID != job.TenantID || appID != job.AppID {
		return WatermarkKey{}, errors.New("background Session scope does not match job owner")
	}
	key := WatermarkKey{tenantID, appID, payload.UserID, payload.SessionID, job.Type}
	return key, key.validate()
}
func (k WatermarkKey) validate() error {
	_, err := runtimecontext.NewScope(k.TenantID, k.AppID, "progress", "job", "background")
	if err != nil || k.UserID == "" || k.SessionID == "" || len(k.UserID) > 512 || len(k.SessionID) > 512 || (k.JobType != JobMemoryExtract && k.JobType != JobSummary) {
		return errors.New("invalid background watermark scope")
	}
	return nil
}

func NewWatermarks(repo controlplane.Repository) Watermarks {
	if sqlRepo, ok := repo.(interface{ SQLDB() *sql.DB }); ok {
		return &postgresWatermarks{db: sqlRepo.SQLDB()}
	}
	return &memoryWatermarks{values: make(map[WatermarkKey]time.Time)}
}

type memoryWatermarks struct {
	mu     sync.Mutex
	values map[WatermarkKey]time.Time
}

func (m *memoryWatermarks) Read(ctx context.Context, key WatermarkKey) (time.Time, bool, error) {
	if err := key.validate(); err != nil {
		return time.Time{}, false, err
	}
	if err := ctx.Err(); err != nil {
		return time.Time{}, false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	value, ok := m.values[key]
	return value, ok, nil
}
func (m *memoryWatermarks) Advance(ctx context.Context, key WatermarkKey, value time.Time) (time.Time, error) {
	if err := validateWatermark(ctx, key, value); err != nil {
		return time.Time{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if value.After(m.values[key]) {
		m.values[key] = value.UTC()
	}
	return m.values[key], nil
}

func validateWatermark(ctx context.Context, key WatermarkKey, value time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := key.validate(); err != nil {
		return err
	}
	if value.IsZero() || value.UnixNano() < 0 || !time.Unix(0, value.UnixNano()).Equal(value) {
		return errors.New("invalid background event watermark")
	}
	return nil
}

type postgresWatermarks struct{ db *sql.DB }

func (p *postgresWatermarks) Read(ctx context.Context, key WatermarkKey) (time.Time, bool, error) {
	if err := key.validate(); err != nil {
		return time.Time{}, false, err
	}
	var ns int64
	err := p.db.QueryRowContext(ctx, `SELECT watermark_ns FROM background_watermark WHERE tenant_id=$1 AND app_id=$2 AND user_id=$3 AND session_id=$4 AND job_type=$5`, key.TenantID, key.AppID, key.UserID, key.SessionID, key.JobType).Scan(&ns)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, errors.New("background watermark read unavailable")
	}
	return time.Unix(0, ns).UTC(), true, nil
}
func (p *postgresWatermarks) Advance(ctx context.Context, key WatermarkKey, value time.Time) (time.Time, error) {
	if err := validateWatermark(ctx, key, value); err != nil {
		return time.Time{}, err
	}
	var ns int64
	err := p.db.QueryRowContext(ctx, `INSERT INTO background_watermark(tenant_id,app_id,user_id,session_id,job_type,watermark_ns) VALUES($1,$2,$3,$4,$5,$6)
ON CONFLICT(tenant_id,app_id,user_id,session_id,job_type) DO UPDATE SET watermark_ns=GREATEST(background_watermark.watermark_ns,EXCLUDED.watermark_ns),updated_at=now()
RETURNING watermark_ns`, key.TenantID, key.AppID, key.UserID, key.SessionID, key.JobType, value.UnixNano()).Scan(&ns)
	if err != nil {
		return time.Time{}, errors.New("background watermark advance unavailable")
	}
	return time.Unix(0, ns).UTC(), nil
}
