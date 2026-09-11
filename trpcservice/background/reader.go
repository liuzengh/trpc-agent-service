package background

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"
)

type JobFilter struct {
	TenantID, AppID, RequestID, BeforeID string
	BeforeTime                           time.Time
	Limit                                int
}

// JobView intentionally excludes payload, dedupe keys and raw provider errors.
type JobView struct {
	ID           string    `json:"job_id"`
	AppID        string    `json:"app_id"`
	RevisionID   string    `json:"revision_id"`
	RequestID    string    `json:"request_id,omitempty"`
	Type         string    `json:"type"`
	Status       string    `json:"status"`
	AttemptCount int       `json:"attempt_count"`
	MaxAttempts  int       `json:"max_attempts"`
	HasError     bool      `json:"has_error"`
	CreatedAt    time.Time `json:"created_at"`
	CompletedAt  time.Time `json:"completed_at,omitempty"`
}
type Reader interface {
	ListJobs(context.Context, JobFilter) ([]JobView, error)
}

func jobView(job Job) JobView {
	var payload struct {
		SourceRequestID string `json:"source_request_id"`
	}
	_ = json.Unmarshal(job.Payload, &payload)
	return JobView{ID: job.ID, AppID: job.AppID, RevisionID: job.RevisionID, RequestID: payload.SourceRequestID, Type: job.Type, Status: job.Status, AttemptCount: job.AttemptCount, MaxAttempts: job.MaxAttempts, HasError: job.LastError != "", CreatedAt: job.CreatedAt, CompletedAt: job.CompletedAt}
}
func validFilter(f JobFilter) bool { return f.TenantID != "" && f.Limit > 0 && f.Limit <= 100 }
func (r *MemoryRepository) ListJobs(ctx context.Context, f JobFilter) ([]JobView, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !validFilter(f) {
		return nil, errors.New("invalid job filter")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	items := []JobView{}
	for _, entry := range r.jobs {
		job := entry.job
		v := jobView(job)
		if job.TenantID != f.TenantID || f.AppID != "" && job.AppID != f.AppID || f.RequestID != "" && v.RequestID != f.RequestID {
			continue
		}
		if !f.BeforeTime.IsZero() && (job.CreatedAt.After(f.BeforeTime) || job.CreatedAt.Equal(f.BeforeTime) && job.ID >= f.BeforeID) {
			continue
		}
		items = append(items, v)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].CreatedAt.Equal(items[j].CreatedAt) {
			return items[i].ID > items[j].ID
		}
		return items[i].CreatedAt.After(items[j].CreatedAt)
	})
	if len(items) > f.Limit {
		items = items[:f.Limit]
	}
	return items, nil
}
func (r *PostgresRepository) ListJobs(ctx context.Context, f JobFilter) ([]JobView, error) {
	if !validFilter(f) {
		return nil, errors.New("invalid job filter")
	}
	var before any
	if !f.BeforeTime.IsZero() {
		before = f.BeforeTime
	}
	rows, err := r.db.QueryContext(ctx, `SELECT job_id,app_id,revision_id,COALESCE(payload->>'source_request_id',''),job_type,status,attempt_count,max_attempts,COALESCE(last_error,'')<>'',created_at,COALESCE(completed_at,'0001-01-01'::timestamptz) FROM background_job WHERE tenant_id=$1 AND ($2='' OR app_id=$2) AND ($3='' OR payload->>'source_request_id'=$3) AND ($4::timestamptz IS NULL OR (created_at,job_id)<($4,$5)) ORDER BY created_at DESC,job_id DESC LIMIT $6`, f.TenantID, f.AppID, f.RequestID, before, f.BeforeID, f.Limit)
	if err != nil {
		return nil, errors.New("background job query unavailable")
	}
	defer func() { _ = rows.Close() }()
	items := []JobView{}
	for rows.Next() {
		var v JobView
		if err = rows.Scan(&v.ID, &v.AppID, &v.RevisionID, &v.RequestID, &v.Type, &v.Status, &v.AttemptCount, &v.MaxAttempts, &v.HasError, &v.CreatedAt, &v.CompletedAt); err != nil {
			return nil, errors.New("background job query unavailable")
		}
		items = append(items, v)
	}
	if rows.Err() != nil {
		return nil, errors.New("background job query unavailable")
	}
	return items, nil
}
