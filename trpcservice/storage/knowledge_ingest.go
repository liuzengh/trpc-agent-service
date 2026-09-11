package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/dbscope"
)

var ErrKnowledgeIngestActive = errors.New("knowledge document is already being indexed")

type KnowledgeIngestRequest struct {
	TenantID, AppCode, DocumentID string
	Name, Filename, ContentType   string
	Data                          []byte
	ChunkSize, Overlap            int
	Metadata                      map[string]string
	ProfileID                     string
	Backend                       config.BackendConfig
}

type KnowledgeIngestJob struct {
	ID                            string
	TenantID, AppCode, DocumentID string
	Name, Filename, ContentType   string
	Data                          []byte
	ChunkSize, Overlap            int
	Metadata                      map[string]string
	ProfileID                     string
	Backend                       config.BackendConfig
	Attempts                      int
}

type KnowledgeDocumentSource struct {
	TenantID, AppCode, DocumentID string
	Name, Filename, ContentType   string
	Data                          []byte
	ChunkSize, Overlap            int
	Metadata                      map[string]string
	CanonicalDocuments            []byte
}

type KnowledgeSourceStore interface {
	ListKnowledgeDocumentSources(context.Context, string, string) ([]KnowledgeDocumentSource, error)
	SaveKnowledgeDocumentSnapshot(context.Context, string, string, string, []byte) error
}

type KnowledgeIngestQueue interface {
	EnqueueKnowledgeIngest(context.Context, KnowledgeIngestRequest) (string, error)
	CancelKnowledgeIngest(context.Context, string, string, string) error
	ClaimKnowledgeIngest(context.Context, string, time.Duration) (KnowledgeIngestJob, bool, error)
	CompleteKnowledgeIngest(context.Context, KnowledgeIngestCompletion) error
	FailKnowledgeIngest(context.Context, string, string, string, int) (bool, error)
}

type KnowledgeIngestCompletion struct {
	JobID, Owner                  string
	TenantID, AppCode, DocumentID string
	TotalChunks                   int
}

func (q *PostgresKnowledgeIngestQueue) CancelKnowledgeIngest(ctx context.Context, tenantID, appCode, documentID string) error {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(appCode) == "" || strings.TrimSpace(documentID) == "" {
		return errors.New("knowledge ingest tenant, application, and document are required")
	}
	tx, err := dbscope.BeginTenantTransaction(ctx, q.database, tenantID)
	if err != nil {
		return fmt.Errorf("begin knowledge ingest cancel transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM knowledge_ingest_jobs WHERE tenant_id=$1 AND app_code=$2 AND document_id=$3`,
		tenantID, appCode, documentID,
	); err != nil {
		return fmt.Errorf("cancel knowledge ingest: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit knowledge ingest cancellation: %w", err)
	}
	return nil
}

type PostgresKnowledgeIngestQueue struct{ database *sql.DB }

func NewPostgresKnowledgeIngestQueue(database *sql.DB) (*PostgresKnowledgeIngestQueue, error) {
	if database == nil {
		return nil, errors.New("knowledge ingest database is required")
	}
	return &PostgresKnowledgeIngestQueue{database: database}, nil
}

func (q *PostgresKnowledgeIngestQueue) EnqueueKnowledgeIngest(ctx context.Context, request KnowledgeIngestRequest) (string, error) {
	if err := validateKnowledgeIngestRequest(request); err != nil {
		return "", err
	}
	metadata, err := json.Marshal(request.Metadata)
	if err != nil {
		return "", fmt.Errorf("encode knowledge ingest metadata: %w", err)
	}
	jobID := uuid.NewString()
	tx, err := dbscope.BeginTenantTransaction(ctx, q.database, request.TenantID)
	if err != nil {
		return "", fmt.Errorf("begin knowledge ingest transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `
INSERT INTO knowledge_ingest_jobs (
    id, tenant_id, app_code, document_id, name, filename, content_type,
    source_data, chunk_size, overlap, metadata, backend_profile_id, backend_driver, backend_connection_ref
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,$13,$14)
ON CONFLICT (tenant_id, app_code, document_id) DO UPDATE
SET id=EXCLUDED.id, name=EXCLUDED.name, filename=EXCLUDED.filename,
    content_type=EXCLUDED.content_type, source_data=EXCLUDED.source_data,
    chunk_size=EXCLUDED.chunk_size, overlap=EXCLUDED.overlap, metadata=EXCLUDED.metadata,
    backend_profile_id=EXCLUDED.backend_profile_id,
    backend_driver=EXCLUDED.backend_driver, backend_connection_ref=EXCLUDED.backend_connection_ref,
    status='queued', attempts=0, available_at=NOW(), lease_owner='', lease_until=NULL,
    last_error='', created_at=NOW(), updated_at=NOW()
WHERE knowledge_ingest_jobs.status='failed'`,
		jobID, request.TenantID, request.AppCode, request.DocumentID, request.Name, request.Filename,
		request.ContentType, request.Data, request.ChunkSize, request.Overlap, string(metadata),
		strings.TrimSpace(request.ProfileID), request.Backend.Driver, request.Backend.ConnectionRef)
	if err != nil {
		return "", fmt.Errorf("enqueue knowledge ingest: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return "", fmt.Errorf("read knowledge ingest enqueue result: %w", err)
	}
	if rows != 1 {
		return "", ErrKnowledgeIngestActive
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO knowledge_documents (
    tenant_id, app_code, document_id, name, status, total_chunks, metadata
) VALUES ($1,$2,$3,$4,'indexing',0,$5::jsonb)
ON CONFLICT (tenant_id, app_code, document_id) DO UPDATE
SET name=EXCLUDED.name, status='indexing', total_chunks=0,
    metadata=EXCLUDED.metadata, updated_at=NOW()`,
		request.TenantID, request.AppCode, request.DocumentID, request.Name, string(metadata)); err != nil {
		return "", fmt.Errorf("create indexing knowledge document: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO knowledge_document_sources (
    tenant_id, app_code, document_id, name, filename, content_type,
    source_data, chunk_size, overlap, metadata, updated_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,NOW())
ON CONFLICT (tenant_id, app_code, document_id) DO UPDATE
SET name=EXCLUDED.name, filename=EXCLUDED.filename, content_type=EXCLUDED.content_type,
    source_data=EXCLUDED.source_data, chunk_size=EXCLUDED.chunk_size,
    overlap=EXCLUDED.overlap, metadata=EXCLUDED.metadata,
    canonical_documents='[]'::jsonb, updated_at=NOW()`,
		request.TenantID, request.AppCode, request.DocumentID, request.Name, request.Filename,
		request.ContentType, request.Data, request.ChunkSize, request.Overlap, string(metadata)); err != nil {
		return "", fmt.Errorf("persist canonical knowledge source: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("commit knowledge ingest: %w", err)
	}
	return jobID, nil
}

func (q *PostgresKnowledgeIngestQueue) ClaimKnowledgeIngest(ctx context.Context, owner string, lease time.Duration) (KnowledgeIngestJob, bool, error) {
	if strings.TrimSpace(owner) == "" || lease <= 0 {
		return KnowledgeIngestJob{}, false, errors.New("knowledge ingest owner and positive lease are required")
	}
	row := q.database.QueryRowContext(ctx, `
WITH candidate AS (
    SELECT id FROM knowledge_ingest_jobs
    WHERE (status='queued' AND available_at <= NOW())
       OR (status='running' AND lease_until < NOW())
    ORDER BY available_at, created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE knowledge_ingest_jobs AS job
SET status='running', attempts=job.attempts+1, lease_owner=$1,
    lease_until=NOW()+($2 * INTERVAL '1 millisecond'), updated_at=NOW()
FROM candidate
WHERE job.id=candidate.id
RETURNING job.id, job.tenant_id, job.app_code, job.document_id, job.name, job.filename,
          job.content_type, job.source_data, job.chunk_size, job.overlap, job.metadata,
          job.backend_profile_id, job.backend_driver, job.backend_connection_ref, job.attempts`,
		owner, lease.Milliseconds())
	var job KnowledgeIngestJob
	var metadata []byte
	if err := row.Scan(&job.ID, &job.TenantID, &job.AppCode, &job.DocumentID, &job.Name, &job.Filename,
		&job.ContentType, &job.Data, &job.ChunkSize, &job.Overlap, &metadata,
		&job.ProfileID, &job.Backend.Driver, &job.Backend.ConnectionRef, &job.Attempts); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return KnowledgeIngestJob{}, false, nil
		}
		return KnowledgeIngestJob{}, false, fmt.Errorf("claim knowledge ingest: %w", err)
	}
	if err := json.Unmarshal(metadata, &job.Metadata); err != nil {
		return KnowledgeIngestJob{}, false, fmt.Errorf("decode knowledge ingest metadata: %w", err)
	}
	return job, true, nil
}

func (q *PostgresKnowledgeIngestQueue) CompleteKnowledgeIngest(ctx context.Context, completion KnowledgeIngestCompletion) error {
	if strings.TrimSpace(completion.JobID) == "" || strings.TrimSpace(completion.Owner) == "" ||
		strings.TrimSpace(completion.TenantID) == "" || strings.TrimSpace(completion.AppCode) == "" || strings.TrimSpace(completion.DocumentID) == "" || completion.TotalChunks < 0 {
		return errors.New("knowledge ingest job and owner are required")
	}
	tx, err := dbscope.BeginTenantTransaction(ctx, q.database, completion.TenantID)
	if err != nil {
		return fmt.Errorf("begin knowledge ingest completion transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `
UPDATE knowledge_documents AS document
SET status='ready', total_chunks=$6, updated_at=NOW()
FROM knowledge_ingest_jobs AS job
WHERE job.id=$1 AND job.status='running' AND job.lease_owner=$2 AND job.lease_until >= NOW()
  AND job.tenant_id=$3 AND job.app_code=$4 AND job.document_id=$5
  AND document.tenant_id=job.tenant_id AND document.app_code=job.app_code AND document.document_id=job.document_id`,
		completion.JobID, completion.Owner, completion.TenantID, completion.AppCode, completion.DocumentID, completion.TotalChunks)
	if err != nil {
		return fmt.Errorf("complete knowledge document projection: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		return fmt.Errorf("complete knowledge ingest: lease ownership lost")
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM knowledge_ingest_jobs WHERE id=$1`, completion.JobID); err != nil {
		return fmt.Errorf("complete knowledge ingest: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit knowledge ingest completion: %w", err)
	}
	return nil
}

func (q *PostgresKnowledgeIngestQueue) FailKnowledgeIngest(ctx context.Context, jobID, owner, failure string, maxAttempts int) (bool, error) {
	if maxAttempts <= 0 {
		return false, errors.New("knowledge ingest max attempts must be positive")
	}
	tx, err := q.database.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin failed knowledge ingest transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var tenantID, appCode, documentID, state string
	err = tx.QueryRowContext(ctx, `
UPDATE knowledge_ingest_jobs
SET status=CASE WHEN attempts >= $4 THEN 'failed' ELSE 'queued' END,
    source_data=CASE WHEN attempts >= $4 THEN ''::bytea ELSE source_data END,
    available_at=CASE WHEN attempts >= $4 THEN available_at ELSE NOW()+(attempts * INTERVAL '5 seconds') END,
    lease_owner='', lease_until=NULL, last_error=$3, updated_at=NOW()
WHERE id=$1 AND status='running' AND lease_owner=$2
RETURNING tenant_id, app_code, document_id, status`, jobID, owner, truncateIngestError(failure), maxAttempts).
		Scan(&tenantID, &appCode, &documentID, &state)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, fmt.Errorf("fail knowledge ingest: lease ownership lost")
		}
		return false, fmt.Errorf("fail knowledge ingest: %w", err)
	}
	terminal := state == "failed"
	if terminal {
		if _, err := tx.ExecContext(ctx, `SELECT set_config('app.tenant_id', $1, true)`, tenantID); err != nil {
			return false, fmt.Errorf("scope failed knowledge ingest tenant: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE knowledge_documents SET status='failed', updated_at=NOW()
WHERE tenant_id=$1 AND app_code=$2 AND document_id=$3 AND status='indexing'`, tenantID, appCode, documentID); err != nil {
			return false, fmt.Errorf("mark knowledge document failed: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit failed knowledge ingest: %w", err)
	}
	return terminal, nil
}

func validateKnowledgeIngestRequest(request KnowledgeIngestRequest) error {
	if strings.TrimSpace(request.TenantID) == "" || strings.TrimSpace(request.AppCode) == "" || strings.TrimSpace(request.DocumentID) == "" ||
		strings.TrimSpace(request.Name) == "" || strings.TrimSpace(request.Filename) == "" || len(request.Data) == 0 {
		return errors.New("knowledge ingest tenant, application, document, name, filename, and data are required")
	}
	if request.ChunkSize < 0 || request.Overlap < 0 || (request.ChunkSize > 0 && request.Overlap >= request.ChunkSize) {
		return errors.New("knowledge ingest chunk size and overlap are invalid")
	}
	switch strings.ToLower(strings.TrimSpace(request.Backend.Driver)) {
	case "pgvector":
	case "qdrant", "elasticsearch":
		if strings.TrimSpace(request.Backend.ConnectionRef) == "" {
			return fmt.Errorf("%s knowledge backend requires connection_ref", request.Backend.Driver)
		}
	default:
		return fmt.Errorf("unsupported knowledge backend %q", request.Backend.Driver)
	}
	return nil
}

func (q *PostgresKnowledgeIngestQueue) ListKnowledgeDocumentSources(ctx context.Context, tenantID, appCode string) ([]KnowledgeDocumentSource, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(appCode) == "" {
		return nil, errors.New("knowledge source list requires tenant and application")
	}
	tx, err := dbscope.BeginTenantTransaction(ctx, q.database, tenantID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `
SELECT tenant_id, app_code, document_id, name, filename, content_type,
       source_data, chunk_size, overlap, metadata, canonical_documents
FROM knowledge_document_sources
WHERE tenant_id=$1 AND app_code=$2
ORDER BY document_id`, tenantID, appCode)
	if err != nil {
		return nil, fmt.Errorf("list canonical knowledge sources: %w", err)
	}
	defer rows.Close()
	result := make([]KnowledgeDocumentSource, 0)
	for rows.Next() {
		var current KnowledgeDocumentSource
		var metadata []byte
		if err := rows.Scan(&current.TenantID, &current.AppCode, &current.DocumentID, &current.Name,
			&current.Filename, &current.ContentType, &current.Data, &current.ChunkSize, &current.Overlap, &metadata, &current.CanonicalDocuments); err != nil {
			return nil, fmt.Errorf("scan canonical knowledge source: %w", err)
		}
		if err := json.Unmarshal(metadata, &current.Metadata); err != nil {
			return nil, fmt.Errorf("decode canonical knowledge source metadata: %w", err)
		}
		result = append(result, current)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

func (q *PostgresKnowledgeIngestQueue) SaveKnowledgeDocumentSnapshot(ctx context.Context, tenantID, appCode, documentID string, encoded []byte) error {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(appCode) == "" || strings.TrimSpace(documentID) == "" || len(encoded) == 0 {
		return errors.New("knowledge snapshot tenant, application, document, and payload are required")
	}
	var raw any
	if err := json.Unmarshal(encoded, &raw); err != nil {
		return fmt.Errorf("validate knowledge snapshot JSON: %w", err)
	}
	tx, err := dbscope.BeginTenantTransaction(ctx, q.database, tenantID)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `
UPDATE knowledge_document_sources
SET canonical_documents=$4::jsonb, updated_at=NOW()
WHERE tenant_id=$1 AND app_code=$2 AND document_id=$3`, tenantID, appCode, documentID, string(encoded))
	if err != nil {
		return fmt.Errorf("persist canonical knowledge snapshot: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil || affected != 1 {
		return fmt.Errorf("persist canonical knowledge snapshot: source not found")
	}
	return tx.Commit()
}

func truncateIngestError(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 512 {
		return value[:512]
	}
	return value
}

var _ KnowledgeIngestQueue = (*PostgresKnowledgeIngestQueue)(nil)
var _ KnowledgeSourceStore = (*PostgresKnowledgeIngestQueue)(nil)
