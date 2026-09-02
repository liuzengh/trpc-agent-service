package knowledge

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	gosql "github.com/go-sql-driver/mysql"

	fwk "trpc.group/trpc-go/trpc-agent-go/knowledge"
)

// mysqlStore persists KB metadata in the knowledge_bases / knowledge_documents
// tables (006_knowledge.sql). Vectors never pass through here.
type mysqlStore struct {
	db *sql.DB
}

// NewMySQLManager returns a manager persisting metadata to MySQL.
func NewMySQLManager(db *sql.DB, vsf VectorStoreFactory, embf EmbedderFactory) *Manager {
	return &Manager{store: &mysqlStore{db: db}, vsf: vsf, embf: embf, inst: make(map[string]*fwk.BuiltinKnowledge)}
}

const kbCols = "kb_id, tenant_id, name, embedding_model, collection_name, dimension"

func (s *mysqlStore) CreateKB(ctx context.Context, kb *KnowledgeBase) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO knowledge_bases (kb_id, tenant_id, name, embedding_model, collection_name, dimension)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		kb.ID, kb.TenantID, kb.Name, kb.EmbeddingEndpointID, kb.CollectionName, kb.Dimension)
	if isDuplicateSQL(err) {
		return fmt.Errorf("knowledge: kb %q already exists", kb.ID)
	}
	return err
}

func (s *mysqlStore) GetKB(ctx context.Context, id string) (*KnowledgeBase, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+kbCols+` FROM knowledge_bases WHERE kb_id = ? AND is_deleted = 0`, id)
	kb, err := scanKB(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrKBNotFound
	}
	return kb, err
}

func (s *mysqlStore) ListKBs(ctx context.Context, tenantID string) ([]*KnowledgeBase, error) {
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

	out := make([]*KnowledgeBase, 0)
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
	return rowsAffectedNotFound(res, ErrKBNotFound, id)
}

func (s *mysqlStore) CreateDocument(ctx context.Context, doc *Document) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO knowledge_documents (doc_id, kb_id, title, source_uri, status, error)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		doc.ID, doc.KBID, nullString(doc.Title), nullString(doc.SourceURI), doc.Status, nullString(doc.Error))
	if isDuplicateSQL(err) {
		return fmt.Errorf("knowledge: document %q already exists", doc.ID)
	}
	return err
}

func (s *mysqlStore) UpdateDocument(ctx context.Context, doc *Document) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE knowledge_documents
		 SET title = ?, source_uri = ?, chunk_count = ?, status = ?, error = ?
		 WHERE doc_id = ?`,
		nullString(doc.Title), nullString(doc.SourceURI), doc.ChunkCount, doc.Status, nullString(doc.Error), doc.ID)
	if err != nil {
		return err
	}
	return rowsAffectedNotFound(res, ErrDocNotFound, doc.ID)
}

func (s *mysqlStore) GetDocument(ctx context.Context, id string) (*Document, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT doc_id, kb_id, title, source_uri, chunk_count, status, error
		 FROM knowledge_documents WHERE doc_id = ?`, id)
	doc, err := scanDoc(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrDocNotFound
	}
	return doc, err
}

func (s *mysqlStore) ListDocuments(ctx context.Context, kbID string) ([]*Document, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT doc_id, kb_id, title, source_uri, chunk_count, status, error
		 FROM knowledge_documents WHERE kb_id = ? ORDER BY created_at`, kbID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]*Document, 0)
	for rows.Next() {
		doc, err := scanDoc(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, doc)
	}
	return out, rows.Err()
}

// scanner is satisfied by both *sql.Row and *sql.Rows.
type scanner interface {
	Scan(dest ...any) error
}

func scanKB(sc scanner) (*KnowledgeBase, error) {
	var kb KnowledgeBase
	if err := sc.Scan(&kb.ID, &kb.TenantID, &kb.Name, &kb.EmbeddingEndpointID, &kb.CollectionName, &kb.Dimension); err != nil {
		return nil, err
	}
	if kb.Dimension <= 0 {
		kb.Dimension = DefaultDimension
	}
	return &kb, nil
}

func scanDoc(sc scanner) (*Document, error) {
	var (
		doc       Document
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

func nullString(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func isDuplicateSQL(err error) bool {
	var my *gosql.MySQLError
	return errors.As(err, &my) && my.Number == 1062
}

func rowsAffectedNotFound(res sql.Result, sentinel error, id string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("%w: %q", sentinel, id)
	}
	return nil
}
