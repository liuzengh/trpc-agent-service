// Package memory is explicit memory: a user (through a tool) says "remember
// this", and it is stored, indexed and later searched — nothing is extracted
// automatically (the approved plan refuses that for this phase).
//
// The boundaries the plan pins down live here:
//
//   - per user, never per group: a group chat has no personal memory, and a
//     write from one is refused rather than silently scoped to whoever
//     happened to speak;
//   - SQL is authoritative, Qdrant is a rebuildable index: the text a search
//     returns is read from `memory_entries` after the row passed its status
//     check;
//   - indexing is asynchronous: a write answers 'pending', and a search
//     finds it only once the row is 'ready' — the plan says so explicitly
//     ("不承诺写后立即可检索"), and pretending otherwise would need a
//     synchronous embedding call in the tool path.
package memory

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/embedding"
	"github.com/liuzengh/trpc-agent-service/trpcservice/jobs"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/qdrant"
)

// Job kinds.
const (
	KindMemoryIndex   = "memory_index"
	KindMemoryCleanup = "memory_cleanup"
)

// ErrGroupMemory refuses a personal-memory operation in a group chat.
var ErrGroupMemory = errors.New("memory: personal memory is not available in group chats")

// ErrNotFound is the single answer for "no such memory of yours".
var ErrNotFound = errors.New("memory: no such memory")

// MaxMemoryText bounds one memory entry.
const MaxMemoryText = 4000

// Service is the memory store.
type Service struct {
	db       *controlplane.DB
	embedder embedding.Client
	vectors  *qdrant.Client
}

// New wires the service; both stores are required.
func New(db *controlplane.DB, embedder embedding.Client, vectors *qdrant.Client) (*Service, error) {
	if db == nil || embedder == nil || vectors == nil {
		return nil, errors.New("memory: db, embedder and vector store are required")
	}
	return &Service{db: db, embedder: embedder, vectors: vectors}, nil
}

// Entry is one memory row as callers see it.
type Entry struct {
	MemoryID int64
	Text     string
	Status   string
}

// Write stores one memory and enqueues its index. isGroup is a fact of the
// session the write came through, not of the caller's intent.
func (s *Service) Write(ctx context.Context, tenantID, userKey string, isGroup bool, text string) (Entry, error) {
	if isGroup {
		return Entry{}, ErrGroupMemory
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return Entry{}, fmt.Errorf("memory: refusing to store an empty memory")
	}
	if len([]rune(text)) > MaxMemoryText {
		return Entry{}, fmt.Errorf("memory: %d runes exceeds the %d limit", len([]rune(text)), MaxMemoryText)
	}
	scope, err := s.db.Scope(tenantID)
	if err != nil {
		return Entry{}, err
	}
	var id int64
	err = scope.WithTx(ctx, func(tx *controlplane.TxScope) error {
		res, err := tx.Exec(ctx, `
			INSERT INTO memory_entries (tenant_id, user_key, text, status, embedding_model, embedding_dim)
			VALUES (?, ?, ?, 'pending', ?, ?)`,
			tenantID, userKey, text, s.embedder.Model(), s.embedder.Dim())
		if err != nil {
			return fmt.Errorf("memory: insert: %w", err)
		}
		if id, err = res.LastInsertId(); err != nil {
			return fmt.Errorf("memory: id: %w", err)
		}
		return jobs.Enqueue(ctx, scope, tx, tenantID, KindMemoryIndex,
			fmt.Sprintf("mem:%d:index", id), map[string]any{"memory_id": id})
	})
	if err != nil {
		return Entry{}, err
	}
	return Entry{MemoryID: id, Text: text, Status: "pending"}, nil
}

// Delete tombstones and enqueues the index cleanup.
func (s *Service) Delete(ctx context.Context, tenantID, userKey string, memoryID int64) error {
	scope, err := s.db.Scope(tenantID)
	if err != nil {
		return err
	}
	return scope.WithTx(ctx, func(tx *controlplane.TxScope) error {
		res, err := tx.Exec(ctx, `
			UPDATE memory_entries SET status = 'deleting'
			WHERE tenant_id = ? AND user_key = ? AND memory_id = ? AND status IN ('pending', 'ready', 'failed')`,
			tenantID, userKey, memoryID)
		if err != nil {
			return fmt.Errorf("memory: tombstone: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrNotFound
		}
		return jobs.Enqueue(ctx, scope, tx, tenantID, KindMemoryCleanup,
			fmt.Sprintf("mem:%d:cleanup", memoryID), map[string]any{"memory_id": memoryID})
	})
}

// IndexMemory embeds one pending memory and flips it ready. The flip is a
// CAS on status: a delete that landed mid-flight leaves the row 'deleting'
// and this job simply stops (its point, if written, is removed by the
// cleanup that delete enqueued).
func (s *Service) IndexMemory(ctx context.Context, tenantID string, memoryID int64) error {
	scope, err := s.db.Scope(tenantID)
	if err != nil {
		return err
	}
	var (
		text    string
		userKey string
		status  string
		model   string
		dim     int
	)
	row, err := scope.QueryRow(ctx, `
		SELECT text, user_key, status, embedding_model, embedding_dim
		FROM memory_entries WHERE tenant_id = ? AND memory_id = ?`, tenantID, memoryID)
	if err != nil {
		return err
	}
	switch err := row.Scan(&text, &userKey, &status, &model, &dim); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return fmt.Errorf("memory: load: %w", err)
	}
	if status != "pending" {
		return nil
	}
	if model != s.embedder.Model() || dim != s.embedder.Dim() {
		return fmt.Errorf("memory %d was written with embedding %s/%d, this deployment pins %s/%d",
			memoryID, model, dim, s.embedder.Model(), s.embedder.Dim())
	}
	vecs, err := s.embedder.Embed(ctx, []string{text})
	if err != nil {
		return err
	}
	if err := s.vectors.Upsert(ctx, []qdrant.Point{{
		ID:     memoryPointID(tenantID, memoryID, 1),
		Vector: vecs[0],
		Payload: map[string]any{
			"tenant_id": tenantID,
			"user_key":  userKey,
			"memory_id": memoryID,
			"kind":      "memory",
		},
	}}); err != nil {
		return err
	}
	if _, err := scope.Exec(ctx, `
		UPDATE memory_entries SET status = 'ready', error = ''
		WHERE tenant_id = ? AND memory_id = ? AND status = 'pending'`, tenantID, memoryID); err != nil {
		return fmt.Errorf("memory: ready flip: %w", err)
	}
	return nil
}

// CleanupMemory removes the index entry and finalizes the tombstone.
func (s *Service) CleanupMemory(ctx context.Context, tenantID string, memoryID int64) error {
	if err := s.vectors.DeleteByFilter(ctx, qdrant.Filter{Must: []qdrant.Match{
		{Key: "tenant_id", Value: tenantID},
		{Key: "memory_id", Value: memoryID},
	}}); err != nil {
		return err
	}
	scope, err := s.db.Scope(tenantID)
	if err != nil {
		return err
	}
	if _, err := scope.Exec(ctx, `
		UPDATE memory_entries SET status = 'deleted', text = ''
		WHERE tenant_id = ? AND memory_id = ? AND status = 'deleting'`, tenantID, memoryID); err != nil {
		return fmt.Errorf("memory: finalize deletion: %w", err)
	}
	return nil
}

// Found is one search hit: text read from SQL, never from the payload.
type Found struct {
	MemoryID int64
	Text     string
	Score    float32
}

// Search looks up the user's own ready memories. A hit that fails the SQL
// check (deleted, still pending, someone else's) is skipped — which is how
// "异步索引期间返回 pending，不承诺写后立即可检索" is true without a
// promise shipped in a comment.
func (s *Service) Search(ctx context.Context, tenantID, userKey, query string, k int) ([]Found, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, nil
	}
	if k <= 0 {
		k = 4
	}
	vecs, err := s.embedder.Embed(ctx, []string{query})
	if err != nil {
		return nil, fmt.Errorf("memory: embed query: %w", err)
	}
	hits, err := s.vectors.Search(ctx, vecs[0], qdrant.Filter{Must: []qdrant.Match{
		{Key: "tenant_id", Value: tenantID},
		{Key: "user_key", Value: userKey},
		{Key: "kind", Value: "memory"},
	}}, k*2)
	if err != nil {
		return nil, err
	}
	scope, err := s.db.Scope(tenantID)
	if err != nil {
		return nil, err
	}
	out := make([]Found, 0, k)
	for _, hit := range hits {
		if len(out) >= k {
			break
		}
		id, ok := payloadInt(hit.Payload, "memory_id")
		if !ok {
			continue
		}
		var text string
		row, err := scope.QueryRow(ctx, `
			SELECT text FROM memory_entries
			WHERE tenant_id = ? AND user_key = ? AND memory_id = ? AND status = 'ready'`,
			tenantID, userKey, id)
		if err != nil {
			return nil, err
		}
		switch err := row.Scan(&text); {
		case errors.Is(err, sql.ErrNoRows):
			continue
		case err != nil:
			return nil, fmt.Errorf("memory: verify hit: %w", err)
		}
		out = append(out, Found{MemoryID: id, Text: text, Score: hit.Score})
	}
	return out, nil
}

// HandleJob dispatches the memory job kinds.
func (s *Service) HandleJob(ctx context.Context, tenantID, kind string, payload []byte) error {
	var p struct {
		MemoryID int64 `json:"memory_id"`
	}
	if len(payload) == 0 {
		return fmt.Errorf("memory: job payload is empty")
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("memory: decode job payload: %w", err)
	}
	switch kind {
	case KindMemoryIndex:
		return s.IndexMemory(ctx, tenantID, p.MemoryID)
	case KindMemoryCleanup:
		return s.CleanupMemory(ctx, tenantID, p.MemoryID)
	default:
		return fmt.Errorf("memory: unknown job kind %q", kind)
	}
}

func payloadInt(payload map[string]any, key string) (int64, bool) {
	v, ok := payload[key]
	if !ok {
		return 0, false
	}
	switch t := v.(type) {
	case float64:
		return int64(t), true
	case int64:
		return t, true
	case int:
		return int64(t), true
	default:
		return 0, false
	}
}

// memoryPointID is the deterministic vector id for one memory entry. It is a
// local copy of the document-side helper (whose package this one must not
// import): the seed format is namespaced ("mem|") so the two id spaces can
// share a collection without ever colliding.
func memoryPointID(tenantID string, memoryID int64, embeddingVersion int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("mem|%s|%d|%d", tenantID, memoryID, embeddingVersion)))
	h := hex.EncodeToString(sum[:16])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}
