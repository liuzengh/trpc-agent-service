package platform

import (
	"context"
	"errors"
	"sort"
	"time"
)

type Artifact struct {
	ID           string    `json:"id"`
	TenantID     string    `json:"tenant_id"`
	SessionID    string    `json:"session_id"`
	Name         string    `json:"name"`
	ContentRef   string    `json:"content_reference"`
	RequestID    string    `json:"request_id,omitempty"`
	TraceID      string    `json:"trace_id,omitempty"`
	Status       string    `json:"status"`
	CreatedAt    time.Time `json:"created_at"`
	FencingToken uint64    `json:"fencing_token,omitempty"`
}

type KnowledgeRecord struct {
	ID          string    `json:"id"`
	TenantID    string    `json:"tenant_id"`
	AgentAppID  string    `json:"agent_app_id"`
	Source      string    `json:"source"`
	Content     string    `json:"content"`
	IndexStatus string    `json:"index_status"`
	CreatedAt   time.Time `json:"created_at"`
}

type ArtifactStore interface {
	PutArtifact(context.Context, Artifact) (Artifact, error)
	ListArtifacts(context.Context, string, string) ([]Artifact, error)
}

type KnowledgeStore interface {
	PutKnowledge(context.Context, KnowledgeRecord) (KnowledgeRecord, error)
	ListKnowledge(context.Context, string, string) ([]KnowledgeRecord, error)
}

func (s *InMemoryStore) PutArtifact(ctx context.Context, item Artifact) (Artifact, error) {
	if err := ctx.Err(); err != nil {
		return Artifact{}, err
	}
	if item.TenantID == "" || item.SessionID == "" || item.Name == "" || item.ContentRef == "" {
		return Artifact{}, errors.New("invalid Artifact")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stream := storageSessionKey(item.TenantID, item.SessionID)
	if item.FencingToken > 0 && item.FencingToken < s.fences[stream] && !importingMigration(ctx) {
		return Artifact{}, ErrStaleFencingToken
	}
	if item.FencingToken > s.fences[stream] {
		s.fences[stream] = item.FencingToken
	}
	if item.ID == "" {
		item.ID = "artifact-" + stableID(item.TenantID+"\x00"+item.SessionID+"\x00"+item.Name+"\x00"+item.ContentRef)
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = time.Now().UTC()
	}
	if item.Status == "" {
		item.Status = "published"
	}
	s.artifacts[item.TenantID+"\x00"+item.ID] = item
	return item, nil
}

func (s *InMemoryStore) ListArtifacts(ctx context.Context, tenantID, sessionID string) ([]Artifact, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := []Artifact{}
	for _, item := range s.artifacts {
		if item.TenantID == tenantID && (sessionID == "" || item.SessionID == sessionID) {
			items = append(items, item)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.Before(items[j].CreatedAt) })
	return items, nil
}

func (s *InMemoryStore) PutKnowledge(ctx context.Context, item KnowledgeRecord) (KnowledgeRecord, error) {
	if err := ctx.Err(); err != nil {
		return KnowledgeRecord{}, err
	}
	if item.TenantID == "" || item.AgentAppID == "" || item.Source == "" || item.Content == "" {
		return KnowledgeRecord{}, errors.New("invalid Knowledge")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if item.ID == "" {
		item.ID = "knowledge-" + stableID(item.TenantID+"\x00"+item.AgentAppID+"\x00"+item.Source)
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = time.Now().UTC()
	}
	if item.IndexStatus == "" {
		item.IndexStatus = "authoritative"
	}
	s.knowledge[item.TenantID+"\x00"+item.ID] = item
	return item, nil
}

func (s *InMemoryStore) ListKnowledge(ctx context.Context, tenantID, appID string) ([]KnowledgeRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := []KnowledgeRecord{}
	for _, item := range s.knowledge {
		if item.TenantID == tenantID && (appID == "" || item.AgentAppID == appID) {
			items = append(items, item)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.Before(items[j].CreatedAt) })
	return items, nil
}

func (s *SQLStore) PutArtifact(ctx context.Context, item Artifact) (Artifact, error) {
	if item.ID == "" {
		item.ID = "artifact-" + stableID(item.TenantID+"\x00"+item.SessionID+"\x00"+item.Name+"\x00"+item.ContentRef)
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = time.Now().UTC()
	}
	if item.Status == "" {
		item.Status = "published"
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Artifact{}, err
	}
	defer tx.Rollback()
	if err := s.validateFencingToken(ctx, tx, item.TenantID, item.SessionID, item.FencingToken); err != nil {
		return Artifact{}, err
	}
	if _, err := tx.ExecContext(ctx, s.q(`INSERT INTO artifacts(tenant_id,artifact_id,session_id,name,content_reference,request_id,trace_id,status,created_at,fencing_token) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(tenant_id,artifact_id) DO NOTHING`), item.TenantID, item.ID, item.SessionID, item.Name, item.ContentRef, item.RequestID, item.TraceID, item.Status, item.CreatedAt.Format(time.RFC3339Nano), item.FencingToken); err != nil {
		return Artifact{}, err
	}
	return item, tx.Commit()
}

func (s *SQLStore) ListArtifacts(ctx context.Context, tenantID, sessionID string) ([]Artifact, error) {
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT artifact_id,session_id,name,content_reference,request_id,trace_id,status,created_at,fencing_token FROM artifacts WHERE tenant_id=? AND (?='' OR session_id=?) ORDER BY created_at`), tenantID, sessionID, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []Artifact{}
	for rows.Next() {
		var item Artifact
		var created string
		item.TenantID = tenantID
		if err := rows.Scan(&item.ID, &item.SessionID, &item.Name, &item.ContentRef, &item.RequestID, &item.TraceID, &item.Status, &created, &item.FencingToken); err != nil {
			return nil, err
		}
		item.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *SQLStore) PutKnowledge(ctx context.Context, item KnowledgeRecord) (KnowledgeRecord, error) {
	if item.ID == "" {
		item.ID = "knowledge-" + stableID(item.TenantID+"\x00"+item.AgentAppID+"\x00"+item.Source)
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = time.Now().UTC()
	}
	if item.IndexStatus == "" {
		item.IndexStatus = "authoritative"
	}
	_, err := s.db.ExecContext(ctx, s.q(`INSERT INTO knowledge_records(tenant_id,knowledge_id,agent_app_id,source,content,index_status,created_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(tenant_id,knowledge_id) DO UPDATE SET content=excluded.content,index_status=excluded.index_status`), item.TenantID, item.ID, item.AgentAppID, item.Source, item.Content, item.IndexStatus, item.CreatedAt.Format(time.RFC3339Nano))
	return item, err
}

func (s *SQLStore) ListKnowledge(ctx context.Context, tenantID, appID string) ([]KnowledgeRecord, error) {
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT knowledge_id,agent_app_id,source,content,index_status,created_at FROM knowledge_records WHERE tenant_id=? AND (?='' OR agent_app_id=?) ORDER BY created_at`), tenantID, appID, appID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []KnowledgeRecord{}
	for rows.Next() {
		var item KnowledgeRecord
		var created string
		item.TenantID = tenantID
		if err := rows.Scan(&item.ID, &item.AgentAppID, &item.Source, &item.Content, &item.IndexStatus, &created); err != nil {
			return nil, err
		}
		item.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		items = append(items, item)
	}
	return items, rows.Err()
}
