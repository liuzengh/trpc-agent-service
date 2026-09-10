package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/dbscope"
)

// PostgresPlatformStore persists platform-owned Knowledge management
// projections. Framework VectorStore data is deliberately not queried here.
type PostgresPlatformStore struct{ database *sql.DB }

func NewPostgresPlatformStore(database *sql.DB) (*PostgresPlatformStore, error) {
	if database == nil {
		return nil, errors.New("platform data database is required")
	}
	return &PostgresPlatformStore{database: database}, nil
}

func (s *PostgresPlatformStore) beginTenantTransaction(ctx context.Context, tenantID string) (*sql.Tx, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, errors.New("tenant_id is required for platform data transaction")
	}
	transaction, err := dbscope.BeginTenantTransaction(ctx, s.database, tenantID)
	if err != nil {
		return nil, fmt.Errorf("begin platform data transaction: %w", err)
	}
	return transaction, nil
}

func (s *PostgresPlatformStore) ListKnowledgeDocuments(ctx context.Context, tenantID, appCode string) ([]KnowledgeDocument, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(appCode) == "" {
		return nil, errors.New("tenant_id and app_code are required")
	}
	transaction, err := s.beginTenantTransaction(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = transaction.Rollback() }()

	rows, err := transaction.QueryContext(ctx, `
SELECT tenant_id, app_code, document_id, name, status, total_chunks, metadata, updated_at
FROM knowledge_documents
WHERE tenant_id=$1 AND app_code=$2
ORDER BY updated_at DESC, document_id`, tenantID, appCode)
	if err != nil {
		return nil, fmt.Errorf("list knowledge documents: %w", err)
	}
	defer rows.Close()

	documents := make([]KnowledgeDocument, 0)
	for rows.Next() {
		var doc KnowledgeDocument
		var metadataJSON []byte
		if err := rows.Scan(&doc.TenantID, &doc.AppCode, &doc.DocumentID, &doc.Name, &doc.Status, &doc.TotalChunks, &metadataJSON, &doc.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan knowledge document: %w", err)
		}
		if err := json.Unmarshal(metadataJSON, &doc.Metadata); err != nil {
			return nil, fmt.Errorf("decode document metadata: %w", err)
		}
		documents = append(documents, doc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate knowledge documents: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return nil, fmt.Errorf("commit list knowledge documents: %w", err)
	}
	return documents, nil
}

func (s *PostgresPlatformStore) DeleteKnowledgeDocument(ctx context.Context, tenantID, appCode, documentID string) error {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(appCode) == "" || strings.TrimSpace(documentID) == "" {
		return errors.New("tenant_id, app_code, and document_id are required")
	}
	transaction, err := s.beginTenantTransaction(ctx, tenantID)
	if err != nil {
		return err
	}
	defer func() { _ = transaction.Rollback() }()

	if _, err := transaction.ExecContext(ctx, `
DELETE FROM knowledge_documents WHERE tenant_id=$1 AND app_code=$2 AND document_id=$3`,
		tenantID, appCode, documentID); err != nil {
		return fmt.Errorf("delete knowledge document: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit delete knowledge: %w", err)
	}
	return nil
}

var _ KnowledgeProjectionStore = (*PostgresPlatformStore)(nil)
