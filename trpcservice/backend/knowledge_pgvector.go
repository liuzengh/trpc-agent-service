// Package backend contains production data-plane adapters that add platform
// tenant scoping around the framework SPIs.
package backend

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/cyl6/trpc-agent-service/trpcservice/observability"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
)

const defaultKnowledgeResults = 10

// PostgresKnowledge is a search-only runtime adapter over a table prepared by
// the independent backend migrator. Every query adds tenant and app predicates
// in SQL; model- or caller-provided filters can never remove that scope.
type PostgresKnowledge struct {
	pool      *pgxpool.Pool
	embedder  embedder.Embedder
	tableSQL  string
	tableName string
	tenantID  string
	appName   string
	dimension int
}

// PostgresKnowledgeOptions are immutable for the lifetime of an adapter.
type PostgresKnowledgeOptions struct {
	DSN       string
	Table     string
	TenantID  string
	AppName   string
	Dimension int
	Embedder  embedder.Embedder
}

// NewPostgresKnowledge opens and read-only verifies a pre-migrated vector
// table. It intentionally performs no CREATE/ALTER operation.
func NewPostgresKnowledge(ctx context.Context, opts PostgresKnowledgeOptions) (*PostgresKnowledge, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(opts.DSN) == "" || !safeIdentifier(opts.Table) ||
		opts.TenantID == "" || opts.AppName == "" || opts.Dimension <= 0 || opts.Embedder == nil {
		return nil, errors.New("knowledge backend configuration is invalid")
	}
	pool, err := pgxpool.New(ctx, opts.DSN)
	if err != nil {
		return nil, errors.New("configure knowledge database")
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			pool.Close()
		}
	}()
	if err := pool.Ping(ctx); err != nil {
		return nil, errors.New("connect to knowledge database")
	}
	adapter := &PostgresKnowledge{
		pool: pool, embedder: opts.Embedder, tableName: opts.Table,
		tableSQL: pgx.Identifier{opts.Table}.Sanitize(), tenantID: opts.TenantID,
		appName: opts.AppName, dimension: opts.Dimension,
	}
	if err := adapter.verifySchema(ctx); err != nil {
		return nil, err
	}
	closeOnError = false
	return adapter, nil
}

func (p *PostgresKnowledge) verifySchema(ctx context.Context) error {
	var dimension int
	err := p.pool.QueryRow(ctx, `
		SELECT a.atttypmod
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = current_schema() AND c.relname = $1
		  AND a.attname = 'embedding' AND NOT a.attisdropped`, p.tableName).Scan(&dimension)
	if errors.Is(err, pgx.ErrNoRows) {
		return errors.New("knowledge vector schema is not migrated")
	}
	if err != nil {
		return errors.New("verify knowledge vector schema")
	}
	// pgvector stores vector(n) as typmod n; keep the comparison explicit so a
	// model change cannot query an incompatible embedding space.
	if dimension != p.dimension {
		return errors.New("knowledge vector schema dimension mismatch")
	}
	required := []string{
		"tenant_id", "app_name", "document_id", "name", "content", "content_sha256",
		"metadata", "embedding", "created_at", "updated_at", "deleted_at",
		"source_session_epoch", "source_through_sequence", "source_summary_version",
		"embedding_model", "migration_id", "source_hash", "target_hash",
	}
	var columnCount int
	if err := p.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = current_schema() AND c.relname = $1
		  AND a.attname = ANY($2::text[]) AND NOT a.attisdropped`, p.tableName, required).Scan(&columnCount); err != nil {
		return errors.New("verify knowledge vector columns")
	}
	if columnCount != len(required) {
		return errors.New("knowledge vector schema is incomplete")
	}
	var canSelect bool
	if err := p.pool.QueryRow(ctx, `SELECT has_table_privilege(current_user, $1, 'SELECT')`, p.tableName).Scan(&canSelect); err != nil || !canSelect {
		return errors.New("knowledge vector runtime select privilege is unavailable")
	}
	return nil
}

// Search implements knowledge.Knowledge.
func (p *PostgresKnowledge) Search(ctx context.Context, req *knowledge.SearchRequest) (result *knowledge.SearchResult, err error) {
	ctx, finish := observability.StartStorage(ctx, "knowledge.query", "pgvector", p.tenantID, "")
	defer func() { finish(err) }()
	if req == nil || strings.TrimSpace(req.Query) == "" {
		return nil, errors.New("knowledge query is required")
	}
	vector, err := p.embedder.GetEmbedding(ctx, req.Query)
	if err != nil || len(vector) == 0 {
		return nil, errors.New("embed knowledge query")
	}
	if len(vector) != p.dimension || !finiteVector(vector) {
		return nil, errors.New("knowledge query embedding is invalid")
	}
	limit := req.MaxResults
	if limit <= 0 || limit > 100 {
		limit = defaultKnowledgeResults
	}
	minScore := req.MinScore
	if math.IsNaN(minScore) || math.IsInf(minScore, 0) || minScore < 0 {
		minScore = 0
	}
	query := fmt.Sprintf(`
		SELECT document_id, name, content, metadata, created_at, updated_at,
		       GREATEST(0, 1 - (embedding <=> $1::vector)) AS score
		FROM %s
		WHERE tenant_id = $2 AND app_name = $3 AND deleted_at IS NULL
		  AND GREATEST(0, 1 - (embedding <=> $1::vector)) >= $4
		ORDER BY embedding <=> $1::vector, document_id
		LIMIT $5`, p.tableSQL)
	rows, err := p.pool.Query(ctx, query, vectorLiteral(vector), p.tenantID, p.appName, minScore, limit)
	if err != nil {
		return nil, errors.New("search knowledge vector store")
	}
	defer rows.Close()
	result = &knowledge.SearchResult{}
	for rows.Next() {
		var (
			doc          document.Document
			metadataJSON []byte
			score        float64
		)
		if err := rows.Scan(&doc.ID, &doc.Name, &doc.Content, &metadataJSON, &doc.CreatedAt, &doc.UpdatedAt, &score); err != nil {
			return nil, errors.New("decode knowledge search result")
		}
		if len(metadataJSON) > 0 && json.Unmarshal(metadataJSON, &doc.Metadata) != nil {
			return nil, errors.New("decode knowledge metadata")
		}
		result.Documents = append(result.Documents, &knowledge.Result{Document: &doc, Score: score})
	}
	if rows.Err() != nil {
		return nil, errors.New("iterate knowledge search results")
	}
	if len(result.Documents) > 0 {
		result.Document = result.Documents[0].Document
		result.Score = result.Documents[0].Score
		result.Text = result.Document.Content
	}
	return result, nil
}

// UpsertDocument is used by ingestion and data migration jobs. Scope fields
// are supplied by the adapter, not by document metadata.
func (p *PostgresKnowledge) UpsertDocument(ctx context.Context, doc *document.Document) (err error) {
	ctx, finish := observability.StartStorage(ctx, "knowledge.upsert", "pgvector", p.tenantID, "")
	defer func() { finish(err) }()
	if doc == nil || strings.TrimSpace(doc.ID) == "" || strings.TrimSpace(doc.Content) == "" {
		return errors.New("knowledge document id and content are required")
	}
	text := doc.EmbeddingText
	if text == "" {
		text = doc.Content
	}
	vector, err := p.embedder.GetEmbedding(ctx, text)
	if err != nil || len(vector) != p.dimension || !finiteVector(vector) {
		return errors.New("embed knowledge document")
	}
	metadata, err := json.Marshal(doc.Metadata)
	if err != nil {
		return errors.New("encode knowledge metadata")
	}
	createdAt := doc.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	updatedAt := doc.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = createdAt
	}
	query := fmt.Sprintf(`
		INSERT INTO %s
			(tenant_id, app_name, document_id, name, content, content_sha256, metadata, embedding, created_at, updated_at, deleted_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::vector, $9, $10, NULL)
		ON CONFLICT (tenant_id, app_name, document_id) DO UPDATE
		SET name = EXCLUDED.name, content = EXCLUDED.content, content_sha256 = EXCLUDED.content_sha256, metadata = EXCLUDED.metadata,
		    embedding = EXCLUDED.embedding, updated_at = EXCLUDED.updated_at, deleted_at = NULL`, p.tableSQL)
	contentHash := fmt.Sprintf("%x", sha256.Sum256([]byte(doc.Content)))
	if _, err := p.pool.Exec(ctx, query, p.tenantID, p.appName, doc.ID, doc.Name,
		doc.Content, contentHash, metadata, vectorLiteral(vector), createdAt, updatedAt); err != nil {
		return errors.New("upsert knowledge document")
	}
	return nil
}

// DeleteDocument writes a tombstone so migration reconciliation cannot
// resurrect deleted content.
func (p *PostgresKnowledge) DeleteDocument(ctx context.Context, documentID string) (err error) {
	ctx, finish := observability.StartStorage(ctx, "knowledge.delete", "pgvector", p.tenantID, "")
	defer func() { finish(err) }()
	if strings.TrimSpace(documentID) == "" {
		return errors.New("knowledge document id is required")
	}
	query := fmt.Sprintf(`UPDATE %s SET deleted_at = clock_timestamp(), updated_at = clock_timestamp()
		WHERE tenant_id = $1 AND app_name = $2 AND document_id = $3`, p.tableSQL)
	if _, err := p.pool.Exec(ctx, query, p.tenantID, p.appName, documentID); err != nil {
		return errors.New("delete knowledge document")
	}
	return nil
}

func (p *PostgresKnowledge) Close() error {
	if p != nil && p.pool != nil {
		p.pool.Close()
	}
	return nil
}

func vectorLiteral(vector []float64) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, value := range vector {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(value, 'g', -1, 64))
	}
	b.WriteByte(']')
	return b.String()
}

func finiteVector(vector []float64) bool {
	for _, value := range vector {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return false
		}
	}
	return true
}

func safeIdentifier(value string) bool {
	if value == "" || len(value) > 63 {
		return false
	}
	for i, r := range value {
		if (r >= 'a' && r <= 'z') || r == '_' || (i > 0 && r >= '0' && r <= '9') {
			continue
		}
		return false
	}
	return true
}
