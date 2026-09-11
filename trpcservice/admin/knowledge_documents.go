package admin

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

var errKnowledgeDocumentsUnavailable = errors.New("知识资料管理不可用，请检查 PostgreSQL 和 schema 33")

type KnowledgeDocumentView struct {
	TenantID   string         `json:"tenant_id"`
	AppID      string         `json:"app_id"`
	ID         string         `json:"document_id"`
	Name       string         `json:"name"`
	RevisionID string         `json:"revision_id"`
	Checksum   string         `json:"checksum"`
	Bytes      int64          `json:"bytes"`
	Metadata   map[string]any `json:"metadata"`
	Status     string         `json:"status"`
	JobID      string         `json:"job_id,omitempty"`
	Chunks     int            `json:"chunks"`
	Version    int64          `json:"version"`
	CreatedBy  string         `json:"created_by"`
	UpdatedBy  string         `json:"updated_by"`
	CreatedAt  time.Time      `json:"created_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
}

type KnowledgeDocumentStore struct{ db *sql.DB }

func NewKnowledgeDocumentStore(repository any) *KnowledgeDocumentStore {
	provider, ok := repository.(interface{ SQLDB() *sql.DB })
	if !ok || provider.SQLDB() == nil {
		return nil
	}
	return &KnowledgeDocumentStore{db: provider.SQLDB()}
}

type knowledgeExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *KnowledgeDocumentStore) sql(ctx context.Context) knowledgeExecutor {
	if tx := database.Transaction(ctx, s.db); tx != nil {
		return tx
	}
	return s.db
}

func (s *KnowledgeDocumentStore) SaveRequest(ctx context.Context, in KnowledgeDocumentInput, jobID, actor string, chunks int) error {
	if s == nil {
		return errKnowledgeDocumentsUnavailable
	}
	metadata, err := json.Marshal(in.Metadata)
	if err != nil || len(metadata) > 64<<10 {
		return invalidf("knowledge document metadata is invalid or too large")
	}
	digest := sha256.Sum256([]byte(in.Content))
	in.Name = strings.TrimSpace(in.Name)
	if in.Name == "" {
		in.Name = in.DocumentID
	}
	state := "pending"
	if jobID == "" {
		state = "ready"
	} else {
		var jobStatus string
		if err := s.sql(ctx).QueryRowContext(ctx, `SELECT status FROM background_job WHERE tenant_id=$1 AND job_id=$2`, in.TenantID, jobID).Scan(&jobStatus); err != nil {
			return errKnowledgeDocumentsUnavailable
		}
		switch jobStatus {
		case "completed":
			state = "ready"
		case "dead":
			state = "failed"
		}
	}
	_, err = s.sql(ctx).ExecContext(ctx, `
INSERT INTO knowledge_document(
  tenant_id,app_id,document_id,name,revision_id,content_sha256,content_bytes,
  metadata,operation,job_id,chunks,state,created_by,updated_by
) VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,'upsert',$9,$10,$11,$12,$12)
ON CONFLICT(tenant_id,app_id,document_id) DO UPDATE SET
  name=EXCLUDED.name,revision_id=EXCLUDED.revision_id,
  content_sha256=EXCLUDED.content_sha256,content_bytes=EXCLUDED.content_bytes,
  metadata=EXCLUDED.metadata,operation='upsert',job_id=EXCLUDED.job_id,
  chunks=EXCLUDED.chunks,state=EXCLUDED.state,version=knowledge_document.version+1,
  updated_by=EXCLUDED.updated_by,updated_at=now()`,
		in.TenantID, in.AppID, in.DocumentID, in.Name, in.RevisionID,
		hex.EncodeToString(digest[:]), len([]byte(in.Content)), string(metadata), jobID,
		chunks, state, actor,
	)
	if err != nil {
		return errKnowledgeDocumentsUnavailable
	}
	return nil
}

func (s *KnowledgeDocumentStore) SaveDelete(ctx context.Context, tenant, app, revision, document, jobID, actor string) error {
	if s == nil {
		return errKnowledgeDocumentsUnavailable
	}
	if jobID == "" {
		result, err := s.sql(ctx).ExecContext(ctx, `DELETE FROM knowledge_document WHERE tenant_id=$1 AND app_id=$2 AND document_id=$3`, tenant, app, document)
		if err != nil {
			return errKnowledgeDocumentsUnavailable
		}
		rows, _ := result.RowsAffected()
		if rows != 1 {
			return controlplane.ErrNotFound
		}
		return nil
	}
	result, err := s.sql(ctx).ExecContext(ctx, `UPDATE knowledge_document SET revision_id=$4,operation='delete',job_id=$5,state='deleting',version=version+1,updated_by=$6,updated_at=now() WHERE tenant_id=$1 AND app_id=$2 AND document_id=$3`, tenant, app, document, revision, jobID, actor)
	if err != nil {
		return errKnowledgeDocumentsUnavailable
	}
	rows, _ := result.RowsAffected()
	if rows != 1 {
		return controlplane.ErrNotFound
	}
	return nil
}

func (s *KnowledgeDocumentStore) List(ctx context.Context, tenant, app, after string) ([]KnowledgeDocumentView, string, error) {
	out := []KnowledgeDocumentView{}
	if s == nil {
		return out, "", errKnowledgeDocumentsUnavailable
	}
	var before any
	beforeID := ""
	if after != "" {
		parts := strings.SplitN(after, "|", 2)
		if len(parts) != 2 {
			return nil, "", invalidf("invalid knowledge document cursor")
		}
		parsed, err := time.Parse(time.RFC3339Nano, parts[0])
		if err != nil || !identifierPattern.MatchString(parts[1]) {
			return nil, "", invalidf("invalid knowledge document cursor")
		}
		before, beforeID = parsed, parts[1]
	}
	rows, err := s.sql(ctx).QueryContext(ctx, `
SELECT d.tenant_id,d.app_id,d.document_id,d.name,d.revision_id,d.content_sha256,
       d.content_bytes,d.metadata,d.state,
       d.job_id,d.chunks,d.version,d.created_by,d.updated_by,d.created_at,d.updated_at
FROM knowledge_document d
WHERE d.tenant_id=$1 AND ($2='' OR d.app_id=$2)
  AND ($3::timestamptz IS NULL OR (d.updated_at,d.document_id)<($3,$4))
ORDER BY d.updated_at DESC,d.document_id DESC LIMIT 101`, tenant, app, before, beforeID)
	if err != nil {
		return nil, "", errKnowledgeDocumentsUnavailable
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var item KnowledgeDocumentView
		var metadata []byte
		if err := rows.Scan(&item.TenantID, &item.AppID, &item.ID, &item.Name, &item.RevisionID, &item.Checksum, &item.Bytes, &metadata, &item.Status, &item.JobID, &item.Chunks, &item.Version, &item.CreatedBy, &item.UpdatedBy, &item.CreatedAt, &item.UpdatedAt); err != nil || json.Unmarshal(metadata, &item.Metadata) != nil {
			return nil, "", errKnowledgeDocumentsUnavailable
		}
		if len(out) == 100 {
			last := out[len(out)-1]
			return out, last.UpdatedAt.UTC().Format(time.RFC3339Nano) + "|" + last.ID, nil
		}
		out = append(out, item)
	}
	if rows.Err() != nil {
		return nil, "", errKnowledgeDocumentsUnavailable
	}
	return out, "", nil
}

func (h *Handler) handleKnowledgeManagement(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/admin/knowledge/documents/list" {
		var in struct {
			TenantID string `json:"tenant_id"`
			AppID    string `json:"app_id"`
			After    string `json:"after"`
		}
		if !decodeAdmin(w, r, &in) || !h.require(w, r, in.TenantID, PermissionRead) {
			return
		}
		if len(in.After) > 256 || (in.AppID != "" && !identifierPattern.MatchString(in.AppID)) {
			h.writeResult(w, 0, nil, invalidf("invalid knowledge document query"))
			return
		}
		items, next, err := h.service.knowledgeDocuments.List(r.Context(), in.TenantID, in.AppID, in.After)
		h.writeResult(w, http.StatusOK, map[string]any{"items": items, "next": next, "enabled": h.service.knowledgeDocuments != nil}, err)
		return
	}
	var in struct {
		TenantID string `json:"tenant_id"`
		APIKey   string `json:"api_key"`
	}
	if !decodeAdmin(w, r, &in) || !h.require(w, r, in.TenantID, PermissionWrite) {
		return
	}
	key := strings.TrimSpace(in.APIKey)
	if key == "" || len(key) > 16384 || strings.ContainsAny(key, "\r\n\x00") || h.service.credentialVault == nil {
		h.writeResult(w, 0, nil, invalidf("Embedding API Key 无效或加密存储未启用"))
		return
	}
	var ref string
	err := h.service.consoleStore.Transaction(r.Context(), func(ctx context.Context) error {
		var err error
		ref, err = h.service.credentialVault.Put(ctx, in.TenantID, []string{secret.Embedding}, key)
		if err != nil {
			return err
		}
		return h.service.record(ctx, in.TenantID, "admin_embedding_credential_created", nil)
	})
	in.APIKey = ""
	if err != nil {
		h.writeResult(w, 0, nil, err)
		return
	}
	h.writeResult(w, http.StatusCreated, map[string]string{"reference": ref}, nil)
}
