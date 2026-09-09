package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	memtool "trpc.group/trpc-go/trpc-agent-go/memory/tool"
	"trpc.group/trpc-go/trpc-agent-go/session"
	ttool "trpc.group/trpc-go/trpc-agent-go/tool"

	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

// MemoryEmbeddingDimension is the width of memory_embedding.embedding. It must
// match the configured embedder, whose dimension comes from
// TRPC_EMBEDDER_DIMENSION (default 1536, config.Config.EmbedderDim); the table
// DDL declares vector(1536) accordingly. An embedder reporting a different
// dimension is rejected at runtime (warn + ILIKE fallback), never a panic.
const MemoryEmbeddingDimension = 1536

const (
	// memoryEmbedQueueSize bounds pending embedding jobs; a full queue drops
	// the job (the next write of the same memory re-enqueues), so an embedder
	// outage cannot block the request path.
	memoryEmbedQueueSize = 256
	// memoryEmbedTimeout caps one embed + write cycle (embedder call is an
	// external API).
	memoryEmbedTimeout = 30 * time.Second
	// memoryRecallLimit mirrors the keyword-search limit semantics.
	memoryRecallLimit = 10
)

// PGMemoryService implements the framework memory.Service over the
// memory_item table:
//
//   - two levels: app_id set = app-private, NULL = tenant-shared; reads merge
//     both levels, writes default to app-private;
//   - soft delete (deleted_at) backs the "user asks to be forgotten"
//     compliance scenario;
//   - the PG row is the single source of truth: a committed write is visible
//     to every worker (no local cache);
//   - search is keyword ILIKE by default; with an embedder configured it is
//     semantic recall: embedding candidates from memory_embedding (pgvector
//     cosine distance), content read back from memory_item by join. Vectors
//     are produced asynchronously after each write, so semantic recall lags
//     writes by seconds (eventual consistency); any vector-path failure
//     falls back to ILIKE.
type PGMemoryService struct {
	pool    *pgxpool.Pool
	tenants *appTenantResolver

	// embedder, when set, enables the vector path: a background worker turns
	// queued writes into memory_embedding rows and backfills
	// memory_item.embedding_id, and SearchMemories recalls by cosine distance.
	embedder embedder.Embedder
	jobs     chan memoryEmbedJob
	stop     chan struct{}
	wg       sync.WaitGroup
}

// memoryEmbedJob is one queued "embed this memory" request. The job carries
// the content so the worker never re-reads memory_item; a stale job for a
// since-updated memory merely recomputes a vector the next write supersedes.
type memoryEmbedJob struct {
	memoryID string
	tenantID string
	content  string
}

// PGMemoryOption customizes the PG memory service.
type PGMemoryOption func(*PGMemoryService)

// WithMemoryEmbedder enables asynchronous embedding generation and semantic
// (vector) recall. The service starts a background worker; Close stops it.
func WithMemoryEmbedder(e embedder.Embedder) PGMemoryOption {
	return func(svc *PGMemoryService) {
		svc.embedder = e
	}
}

// NewPGMemoryService creates the service on an established pool.
func NewPGMemoryService(pool *pgxpool.Pool, opts ...PGMemoryOption) *PGMemoryService {
	s := &PGMemoryService{pool: pool, tenants: newAppTenantResolver(pool)}
	for _, opt := range opts {
		opt(s)
	}
	if s.embedder != nil {
		s.jobs = make(chan memoryEmbedJob, memoryEmbedQueueSize)
		s.stop = make(chan struct{})
		s.wg.Add(1)
		go s.embedLoop()
	}
	return s
}

// ReadMemories implements memory.Reader: app-private + tenant-shared, active
// rows only, most recently updated first.
func (s *PGMemoryService) ReadMemories(ctx context.Context, userKey memory.UserKey, limit int) ([]*memory.Entry, error) {
	if limit <= 0 {
		limit = 10
	}
	tenantID, err := s.tenants.resolve(ctx, userKey.AppName)
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, content, topics, created_at, updated_at FROM memory_item
		 WHERE tenant_id = $1 AND user_id = $2 AND (app_id = $3 OR app_id IS NULL)
		   AND deleted_at IS NULL
		 ORDER BY updated_at DESC LIMIT $4`,
		tenantID, userKey.UserID, userKey.AppName, limit)
	if err != nil {
		return nil, fmt.Errorf("read memories: %w", err)
	}
	defer rows.Close()
	return scanEntries(rows, userKey)
}

// SearchMemories implements memory.Reader. With an embedder configured and a
// non-empty query it recalls by cosine distance over memory_embedding joined
// back to memory_item (deleted rows are excluded by the join condition); the
// keyword ILIKE match is the fallback when the embedder is unset, the vector
// side errors (e.g. the memory_embedding schema is not deployed yet), or no
// memory has been embedded so far. An empty query degrades to ReadMemories.
func (s *PGMemoryService) SearchMemories(ctx context.Context, userKey memory.UserKey, query string, _ ...memory.SearchOption) ([]*memory.Entry, error) {
	if query == "" {
		return s.ReadMemories(ctx, userKey, 10)
	}
	tenantID, err := s.tenants.resolve(ctx, userKey.AppName)
	if err != nil {
		return nil, err
	}
	if s.embedder != nil {
		entries, err := s.searchByVector(ctx, userKey, tenantID, query)
		switch {
		case err != nil:
			plog.Warnf("memory vector recall failed, falling back to keyword search: %v", err)
		case len(entries) > 0:
			return entries, nil
		}
		// len == 0: no embeddings yet (semantic recall lags writes); the
		// keyword pass below may still find the memory.
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, content, topics, created_at, updated_at FROM memory_item
		 WHERE tenant_id = $1 AND user_id = $2 AND (app_id = $3 OR app_id IS NULL)
		   AND deleted_at IS NULL AND content ILIKE '%' || $4 || '%'
		 ORDER BY updated_at DESC LIMIT 10`,
		tenantID, userKey.UserID, userKey.AppName, query)
	if err != nil {
		return nil, fmt.Errorf("search memories: %w", err)
	}
	defer rows.Close()
	return scanEntries(rows, userKey)
}

// searchByVector embeds the query and returns the nearest active memories in
// the merged scope. The vector literal travels as a text parameter with an
// explicit ::vector cast, so no pgx type registration is needed.
func (s *PGMemoryService) searchByVector(ctx context.Context, userKey memory.UserKey, tenantID, query string) ([]*memory.Entry, error) {
	vec, err := s.embedder.GetEmbedding(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("embed query: %w", err)
	}
	if err := checkEmbeddingDim(vec); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT m.id, m.content, m.topics, m.created_at, m.updated_at
		 FROM memory_item m JOIN memory_embedding e ON e.memory_id = m.id
		 WHERE m.tenant_id = $1 AND m.user_id = $2 AND (m.app_id = $3 OR m.app_id IS NULL)
		   AND m.deleted_at IS NULL
		 ORDER BY e.embedding <=> $4::vector LIMIT $5`,
		tenantID, userKey.UserID, userKey.AppName, vectorLiteral(vec), memoryRecallLimit)
	if err != nil {
		return nil, fmt.Errorf("vector recall: %w", err)
	}
	defer rows.Close()
	return scanEntries(rows, userKey)
}

// AddMemory implements memory.Service: idempotent by (scope, content) — an
// active duplicate just touches updated_at, a matching tombstone is
// reactivated, otherwise a new app-private row is inserted.
func (s *PGMemoryService) AddMemory(ctx context.Context, userKey memory.UserKey, mem string, topics []string, _ ...memory.AddOption) error {
	if mem == "" {
		return errors.New("pg memory: empty content")
	}
	tenantID, err := s.tenants.resolve(ctx, userKey.AppName)
	if err != nil {
		return err
	}
	topicsJSON, err := json.Marshal(topics)
	if err != nil {
		return err
	}

	var memID string
	var embeddingID *string
	err = s.pool.QueryRow(ctx,
		`UPDATE memory_item SET updated_at = now()
		 WHERE tenant_id=$1 AND app_id=$2 AND user_id=$3 AND content=$4 AND deleted_at IS NULL
		 RETURNING id, embedding_id`,
		tenantID, userKey.AppName, userKey.UserID, mem).Scan(&memID, &embeddingID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("add memory (touch): %w", err)
	}
	if memID != "" {
		// Active duplicate: idempotent no-op. Re-enqueue only when the vector
		// was never written (a dropped/failed job self-heals on the next write).
		if embeddingID == nil {
			s.enqueueEmbedding(memID, tenantID, mem)
		}
		return nil
	}
	err = s.pool.QueryRow(ctx,
		`UPDATE memory_item SET deleted_at = NULL, topics = $5, updated_at = now()
		 WHERE tenant_id=$1 AND app_id=$2 AND user_id=$3 AND content=$4 AND deleted_at IS NOT NULL
		 RETURNING id`,
		tenantID, userKey.AppName, userKey.UserID, mem, topicsJSON).Scan(&memID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("add memory (revive): %w", err)
	}
	if memID != "" {
		s.enqueueEmbedding(memID, tenantID, mem) // soft delete removed the vector row
		return nil
	}
	if err := s.pool.QueryRow(ctx,
		`INSERT INTO memory_item (tenant_id, app_id, user_id, content, topics)
		 VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		tenantID, userKey.AppName, userKey.UserID, mem, topicsJSON).Scan(&memID); err != nil {
		return fmt.Errorf("add memory (insert): %w", err)
	}
	s.enqueueEmbedding(memID, tenantID, mem)
	return nil
}

// UpdateMemory implements memory.Service: updates an active row; a matching
// tombstone is reactivated with the new content; a missing row is an error
// (the memory_update tool always targets IDs returned by search/load).
func (s *PGMemoryService) UpdateMemory(ctx context.Context, memoryKey memory.Key, mem string, topics []string, _ ...memory.UpdateOption) error {
	if err := memoryKey.CheckMemoryKey(); err != nil {
		return err
	}
	// The tenant predicate matches the read paths: without it, isolation
	// would rest on "the UUID is unguessable" alone.
	tenantID, err := s.tenants.resolve(ctx, memoryKey.AppName)
	if err != nil {
		return err
	}
	topicsJSON, err := json.Marshal(topics)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE memory_item SET content=$4, topics=$5, updated_at=now()
		 WHERE id=$1 AND user_id=$2 AND tenant_id=$3 AND deleted_at IS NULL`,
		memoryKey.MemoryID, memoryKey.UserID, tenantID, mem, topicsJSON)
	if err != nil {
		return fmt.Errorf("update memory: %w", err)
	}
	if tag.RowsAffected() > 0 {
		s.enqueueEmbeddingForApp(ctx, memoryKey.AppName, memoryKey.MemoryID, mem)
		return nil
	}
	tag, err = s.pool.Exec(ctx,
		`UPDATE memory_item SET content=$4, topics=$5, deleted_at=NULL, updated_at=now()
		 WHERE id=$1 AND user_id=$2 AND tenant_id=$3 AND deleted_at IS NOT NULL`,
		memoryKey.MemoryID, memoryKey.UserID, tenantID, mem, topicsJSON)
	if err != nil {
		return fmt.Errorf("update memory (revive): %w", err)
	}
	if tag.RowsAffected() > 0 {
		s.enqueueEmbeddingForApp(ctx, memoryKey.AppName, memoryKey.MemoryID, mem)
		return nil
	}
	return fmt.Errorf("memory %s not found", memoryKey.MemoryID)
}

// DeleteMemory implements memory.Service: soft delete (compliance scenario).
// The vector row goes with it so a forgotten memory can never be recalled;
// the embedding delete is best-effort (warn only) because the schema may lag
// the binary, and the recall join would exclude the row either way.
func (s *PGMemoryService) DeleteMemory(ctx context.Context, memoryKey memory.Key) error {
	if err := memoryKey.CheckMemoryKey(); err != nil {
		return err
	}
	tenantID, err := s.tenants.resolve(ctx, memoryKey.AppName)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE memory_item SET deleted_at = now()
		 WHERE id=$1 AND user_id=$2 AND tenant_id=$3 AND deleted_at IS NULL`,
		memoryKey.MemoryID, memoryKey.UserID, tenantID)
	if err != nil {
		return fmt.Errorf("delete memory: %w", err)
	}
	// Nothing matched, so the row is another tenant's (or another user's, or
	// already deleted). Deleting the vector anyway would let anyone holding a
	// memory id destroy a recall they were never authorized to touch.
	if tag.RowsAffected() == 0 {
		return nil
	}
	if _, err := s.pool.Exec(ctx,
		`DELETE FROM memory_embedding WHERE memory_id = $1`, memoryKey.MemoryID); err != nil {
		plog.Warnf("delete embedding for memory %s: %v", memoryKey.MemoryID, err)
	}
	return nil
}

// ClearMemories implements memory.Service: soft-deletes all active memories
// of the user within the app scope (tenant-shared rows are untouched — they
// belong to the tenant, not the user's app session).
func (s *PGMemoryService) ClearMemories(ctx context.Context, userKey memory.UserKey) error {
	tenantID, err := s.tenants.resolve(ctx, userKey.AppName)
	if err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx,
		`UPDATE memory_item SET deleted_at = now()
		 WHERE tenant_id=$1 AND app_id=$2 AND user_id=$3 AND deleted_at IS NULL`,
		tenantID, userKey.AppName, userKey.UserID); err != nil {
		return fmt.Errorf("clear memories: %w", err)
	}
	return nil
}

// Tools implements memory.Service: the standard memory tool set. The tools
// resolve the service and app/user identity from the invocation context at
// call time (the runner injects them), so they are service-agnostic.
func (s *PGMemoryService) Tools() []ttool.Tool {
	tools := []ttool.Tool{
		memtool.NewAddTool(),
		memtool.NewSearchTool(),
		memtool.NewLoadTool(),
		memtool.NewUpdateTool(),
		memtool.NewDeleteTool(),
		memtool.NewClearTool(),
	}
	return tools
}

// EnqueueAutoMemoryJob implements memory.Service as a no-op: this backend
// has no automatic memory extraction. The runner tolerates the no-op.
func (s *PGMemoryService) EnqueueAutoMemoryJob(_ context.Context, _ *session.Session) error {
	return nil
}

// Close implements memory.Service: stops the embedding worker and waits for
// it to drain what is still queued. The pool is owned by the caller.
func (s *PGMemoryService) Close() error {
	if s.stop != nil {
		close(s.stop)
		s.wg.Wait()
	}
	return nil
}

// enqueueEmbedding queues a vector job for a written memory. A full queue
// drops the job with a warning — the next write of the same memory
// re-enqueues it, so recall merely keeps lagging (eventual consistency).
func (s *PGMemoryService) enqueueEmbedding(memoryID, tenantID, content string) {
	if s.embedder == nil {
		return
	}
	select {
	case s.jobs <- memoryEmbedJob{memoryID: memoryID, tenantID: tenantID, content: content}:
	default:
		plog.Warnf("memory embedding queue full, dropping job for memory %s (next write retries)", memoryID)
	}
}

// enqueueEmbeddingForApp is enqueueEmbedding for callers that hold only the
// framework key: the tenant is resolved from the app first.
func (s *PGMemoryService) enqueueEmbeddingForApp(ctx context.Context, appID, memoryID, content string) {
	if s.embedder == nil {
		return
	}
	tenantID, err := s.tenants.resolve(ctx, appID)
	if err != nil {
		plog.Warnf("enqueue embedding for memory %s: %v", memoryID, err)
		return
	}
	s.enqueueEmbedding(memoryID, tenantID, content)
}

// embedLoop drains the embedding queue until Close; on stop it empties
// whatever is still queued before exiting.
func (s *PGMemoryService) embedLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.stop:
			for {
				select {
				case job := <-s.jobs:
					s.processEmbedJob(job)
				default:
					return
				}
			}
		case job := <-s.jobs:
			s.processEmbedJob(job)
		}
	}
}

// processEmbedJob embeds one memory, upserts memory_embedding by memory_id
// and backfills memory_item.embedding_id. Every failure degrades to a warn:
// the job must never crash the worker, and a missing memory_embedding schema
// (deploy lag) is survivable — search falls back to keyword matching.
func (s *PGMemoryService) processEmbedJob(job memoryEmbedJob) {
	// Detached context: the request that triggered the write is long gone.
	ctx, cancel := context.WithTimeout(context.Background(), memoryEmbedTimeout)
	defer cancel()

	vec, err := s.embedder.GetEmbedding(ctx, job.content)
	if err != nil {
		plog.Warnf("embed memory %s: %v", job.memoryID, err)
		return
	}
	if err := checkEmbeddingDim(vec); err != nil {
		plog.Warnf("embed memory %s: %v", job.memoryID, err)
		return
	}
	var embedID string
	if err := s.pool.QueryRow(ctx,
		`INSERT INTO memory_embedding (memory_id, tenant_id, embedding)
		 VALUES ($1, $2, $3::vector)
		 ON CONFLICT (memory_id) DO UPDATE SET embedding = EXCLUDED.embedding
		 RETURNING id`,
		job.memoryID, job.tenantID, vectorLiteral(vec)).Scan(&embedID); err != nil {
		plog.Warnf("write embedding for memory %s: %v", job.memoryID, err)
		return
	}
	if _, err := s.pool.Exec(ctx,
		`UPDATE memory_item SET embedding_id = $2 WHERE id = $1`,
		job.memoryID, embedID); err != nil {
		plog.Warnf("backfill embedding_id for memory %s: %v", job.memoryID, err)
	}
}

// checkEmbeddingDim guards the vector(1536) column: a misconfigured embedder
// (dimension ≠ TRPC_EMBEDDER_DIMENSION) is rejected before hitting PG.
func checkEmbeddingDim(vec []float64) error {
	if len(vec) == 0 {
		return errors.New("embedder returned an empty embedding")
	}
	if len(vec) != MemoryEmbeddingDimension {
		return fmt.Errorf("embedding dimension %d does not match memory_embedding vector(%d)",
			len(vec), MemoryEmbeddingDimension)
	}
	return nil
}

// vectorLiteral renders an embedding as the pgvector text literal
// "[v1,v2,...]"; the SQL casts it with ::vector, which keeps the parameter a
// plain text value and needs no pgx type registration.
func vectorLiteral(vec []float64) string {
	var b strings.Builder
	b.Grow(len(vec) * 8)
	b.WriteByte('[')
	for i, v := range vec {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(v, 'g', -1, 64))
	}
	b.WriteByte(']')
	return b.String()
}

func scanEntries(rows pgx.Rows, userKey memory.UserKey) ([]*memory.Entry, error) {
	var out []*memory.Entry
	for rows.Next() {
		var (
			id, content          string
			topicsRaw            []byte
			createdAt, updatedAt time.Time
		)
		if err := rows.Scan(&id, &content, &topicsRaw, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("scan memory: %w", err)
		}
		var topics []string
		if len(topicsRaw) > 0 {
			if err := json.Unmarshal(topicsRaw, &topics); err != nil {
				return nil, fmt.Errorf("decode topics: %w", err)
			}
		}
		out = append(out, &memory.Entry{
			ID: id, AppName: userKey.AppName, UserID: userKey.UserID,
			Memory:    &memory.Memory{Memory: content, Topics: topics},
			CreatedAt: createdAt, UpdatedAt: updatedAt,
		})
	}
	return out, rows.Err()
}
