// Package knowledgestore holds the infra-backed knowledge components: the
// MySQL metadata store (knowledge_mysql.go) and the Milvus vector-store
// factory (milvus.go), keeping the domain layer free of network clients.
package knowledgestore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/knowledge"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/sqlutil"
)

// mysqlStore persists KB metadata in the knowledge_bases / knowledge_documents
// tables (006_knowledge.sql).
type mysqlStore struct {
	db *sql.DB
}

// NewMySQLManager returns a manager persisting metadata to MySQL.
func NewMySQLManager(db *sql.DB, vsf knowledge.VectorStoreFactory, embf knowledge.EmbedderFactory) *knowledge.Manager {
	return knowledge.NewManagerWithStore(&mysqlStore{db: db}, vsf, embf)
}

const kbCols = "kb_id, tenant_id, name, embedding_model, collection_name, dimension"

func (s *mysqlStore) CreateKB(ctx context.Context, kb *knowledge.KnowledgeBase) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO knowledge_bases (kb_id, tenant_id, name, embedding_model, collection_name, dimension)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		kb.ID, kb.TenantID, kb.Name, kb.EmbeddingEndpointID, kb.CollectionName, kb.Dimension)
	if sqlutil.IsDuplicate(err) {
		return fmt.Errorf("knowledge: kb %q already exists", kb.ID)
	}
	return err
}

func (s *mysqlStore) GetKB(ctx context.Context, id string) (*knowledge.KnowledgeBase, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+kbCols+` FROM knowledge_bases WHERE kb_id = ? AND is_deleted = 0`, id)
	kb, err := scanKB(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, knowledge.ErrKBNotFound
	}
	return kb, err
}

func (s *mysqlStore) ListKBs(ctx context.Context, tenantID string) ([]*knowledge.KnowledgeBase, error) {
	q := `SELECT ` + kbCols + ` FROM knowledge_bases WHERE is_deleted = 0`
	args := []any{}
	if tenantID != "" {
		q += ` AND tenant_id = ?`
		args = append(args, tenantID)
	}
	q += ` ORDER BY created_at`
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]*knowledge.KnowledgeBase, 0)
	for rows.Next() {
		kb, err := scanKB(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, kb)
	}
	return out, rows.Err()
}

func (s *mysqlStore) DeleteKB(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE knowledge_bases SET is_deleted = 1 WHERE kb_id = ? AND is_deleted = 0`, id)
	if err != nil {
		return err
	}
	return sqlutil.RowsAffected(res, knowledge.ErrKBNotFound, id)
}

func (s *mysqlStore) CreateDocument(ctx context.Context, doc *knowledge.Document) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO knowledge_documents (doc_id, kb_id, title, source_uri, status, error)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		doc.ID, doc.KBID, sqlutil.Null(doc.Title), sqlutil.Null(doc.SourceURI), doc.Status, sqlutil.Null(doc.Error))
	if sqlutil.IsDuplicate(err) {
		return fmt.Errorf("knowledge: document %q already exists", doc.ID)
	}
	return err
}

func (s *mysqlStore) UpdateDocument(ctx context.Context, doc *knowledge.Document) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE knowledge_documents
		 SET title = ?, source_uri = ?, chunk_count = ?, status = ?, error = ?
		 WHERE doc_id = ?`,
		sqlutil.Null(doc.Title), sqlutil.Null(doc.SourceURI), doc.ChunkCount, doc.Status, sqlutil.Null(doc.Error), doc.ID)
	if err != nil {
		return err
	}
	return sqlutil.RowsAffected(res, knowledge.ErrDocNotFound, doc.ID)
}

func (s *mysqlStore) GetDocument(ctx context.Context, id string) (*knowledge.Document, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT doc_id, kb_id, title, source_uri, chunk_count, status, error
		 FROM knowledge_documents WHERE doc_id = ?`, id)
	doc, err := scanDoc(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, knowledge.ErrDocNotFound
	}
	return doc, err
}

func (s *mysqlStore) ListDocuments(ctx context.Context, kbID string) ([]*knowledge.Document, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT doc_id, kb_id, title, source_uri, chunk_count, status, error
		 FROM knowledge_documents WHERE kb_id = ? ORDER BY created_at`, kbID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]*knowledge.Document, 0)
	for rows.Next() {
		doc, err := scanDoc(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, doc)
	}
	return out, rows.Err()
}

func scanKB(sc sqlutil.RowScanner) (*knowledge.KnowledgeBase, error) {
	var kb knowledge.KnowledgeBase
	if err := sc.Scan(&kb.ID, &kb.TenantID, &kb.Name, &kb.EmbeddingEndpointID, &kb.CollectionName, &kb.Dimension); err != nil {
		return nil, err
	}
	if kb.Dimension <= 0 {
		kb.Dimension = knowledge.DefaultDimension
	}
	return &kb, nil
}

func scanDoc(sc sqlutil.RowScanner) (*knowledge.Document, error) {
	var (
		doc       knowledge.Document
		title     sql.NullString
		sourceURI sql.NullString
		errMsg    sql.NullString
	)
	if err := sc.Scan(&doc.ID, &doc.KBID, &title, &sourceURI, &doc.ChunkCount, &doc.Status, &errMsg); err != nil {
		return nil, err
	}
	doc.Title = title.String
	doc.SourceURI = sourceURI.String
	doc.Error = errMsg.String
	return &doc, nil
}
