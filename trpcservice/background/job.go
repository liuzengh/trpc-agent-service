// Package background provides a durable PostgreSQL/in-memory job queue.
package background

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	JobSummary         = "session_summary"
	JobMemoryExtract   = "memory_extract"
	JobKnowledgeUpsert = "knowledge_upsert"
	JobKnowledgeDelete = "knowledge_delete"
	JobMemoryBackfill  = "memory_backfill"
	JobMemoryVerify    = "memory_verify"
	JobSessionBackfill = "session_backfill"
	JobSessionVerify   = "session_verify"
)

var (
	ErrNoJob       = errors.New("no background job available")
	ErrClosed      = errors.New("background job repository is closed")
	ErrJobNotFound = errors.New("background job not found")
	ErrJobConflict = errors.New("background job state conflict")
)

type EnqueueRequest struct {
	TenantID    string
	AppID       string
	RevisionID  string
	Type        string
	DedupeKey   string
	Payload     json.RawMessage
	MaxAttempts int
	TraceParent string
}

type Job struct {
	ID           string
	TenantID     string
	AppID        string
	RevisionID   string
	Type         string
	DedupeKey    string
	Payload      json.RawMessage
	Status       string
	AttemptCount int
	MaxAttempts  int
	TraceParent  string
	CreatedAt    time.Time
	LastError    string
	CompletedAt  time.Time
}

type EnqueueResult struct {
	Job       Job
	Duplicate bool
}

type Repository interface {
	Enqueue(ctx context.Context, request EnqueueRequest) (EnqueueResult, error)
	Claim(ctx context.Context, workerID string, lease time.Duration) (Job, error)
	Complete(ctx context.Context, jobID string, workerID string) error
	Fail(ctx context.Context, job Job, workerID string, retryAt time.Time, cause error) error
	Get(ctx context.Context, tenantID string, jobID string) (Job, error)
	Retry(ctx context.Context, tenantID string, jobID string) error
	Ready(ctx context.Context) error
	Close() error
}

func StableJobID(tenantID string, jobType string, dedupeKey string) string {
	digest := sha256.Sum256([]byte(tenantID + "\x00" + jobType + "\x00" + dedupeKey))
	return "job_" + hex.EncodeToString(digest[:16])
}

func validateEnqueue(request *EnqueueRequest) error {
	if request == nil {
		return errors.New("background job request is required")
	}
	request.TenantID = strings.TrimSpace(request.TenantID)
	request.AppID = strings.TrimSpace(request.AppID)
	request.RevisionID = strings.TrimSpace(request.RevisionID)
	request.Type = strings.TrimSpace(request.Type)
	request.DedupeKey = strings.TrimSpace(request.DedupeKey)
	if request.TenantID == "" || request.AppID == "" || request.RevisionID == "" ||
		request.Type == "" || request.DedupeKey == "" || !json.Valid(request.Payload) {
		return fmt.Errorf("background job identity and JSON payload are required")
	}
	if request.MaxAttempts <= 0 {
		request.MaxAttempts = 5
	}
	return nil
}
