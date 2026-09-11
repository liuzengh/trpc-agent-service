// Package console owns browser sessions, drafts and isolated debugging state.
// It never owns the shared SQL pool or initializes schemas during requests.
package console

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
)

var ErrNotFound = errors.New("console record not found")
var ErrConflict = errors.New("console record changed; reload before saving")
var ErrUnavailable = errors.New("console storage unavailable; check schema and permissions")

type Record struct {
	Kind      string          `json:"-"`
	TenantID  string          `json:"tenant_id"`
	AppID     string          `json:"app_id"`
	ID        string          `json:"id"`
	OwnerID   string          `json:"owner_id"`
	Status    string          `json:"status"`
	Version   int64           `json:"version"`
	Data      json.RawMessage `json:"data"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
	ExpiresAt time.Time       `json:"expires_at"`
}

type Filter struct {
	Kind, TenantID, AppID, OwnerID, Status, Before string
	SessionID                                      string
	BeforeTime                                     time.Time
	AfterTime                                      time.Time
	Ascending                                      bool
	Limit                                          int
	AllTenants                                     bool
}
type Store struct {
	db      *sql.DB
	mu      sync.Mutex
	records map[string]Record
}

type memoryTransactionKey struct{}
type memoryTransaction struct {
	base, work *Store
	parent     *memoryTransaction
}

func (s *Store) transactional(ctx context.Context) *Store {
	value, _ := ctx.Value(memoryTransactionKey{}).(*memoryTransaction)
	for value != nil {
		if value.base == s {
			return value.work
		}
		value = value.parent
	}
	return nil
}

type executor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *Store) executor(ctx context.Context) executor {
	if tx := database.Transaction(ctx, s.db); tx != nil {
		return tx
	}
	return s.db
}
func (s *Store) Transaction(ctx context.Context, fn func(context.Context) error) error {
	if s.db == nil {
		if s.transactional(ctx) != nil {
			return fn(ctx)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		work := &Store{records: make(map[string]Record, len(s.records))}
		for k, v := range s.records {
			work.records[k] = v
		}
		parent, _ := ctx.Value(memoryTransactionKey{}).(*memoryTransaction)
		if err := fn(context.WithValue(ctx, memoryTransactionKey{}, &memoryTransaction{base: s, work: work, parent: parent})); err != nil {
			return err
		}
		s.records = work.records
		return nil
	}
	return database.InTransaction(ctx, s.db, fn)
}
func (s *Store) LockDraft(ctx context.Context, tenant, id string) error {
	if s.db == nil {
		return nil
	}
	var found string
	return mapError(s.executor(ctx).QueryRowContext(ctx, "SELECT record_id FROM agent_draft WHERE tenant_id=$1 AND record_id=$2 FOR UPDATE", tenant, id).Scan(&found))
}

func (s *Store) LockApp(ctx context.Context, tenant, id string) error {
	if s.db == nil {
		return nil
	}
	var found string
	return mapError(s.executor(ctx).QueryRowContext(ctx, "SELECT app_id FROM agent_app WHERE tenant_id=$1 AND app_id=$2 FOR UPDATE", tenant, id).Scan(&found))
}

func (s *Store) LockSession(ctx context.Context, tenant, id string) error {
	if s.db == nil {
		return nil
	}
	var found string
	return mapError(s.executor(ctx).QueryRowContext(ctx, "SELECT record_id FROM debug_session WHERE tenant_id=$1 AND record_id=$2 FOR UPDATE", tenant, id).Scan(&found))
}

func NewStore(repository any) *Store {
	s := &Store{records: map[string]Record{}}
	if provider, ok := repository.(interface{ SQLDB() *sql.DB }); ok {
		s.db = provider.SQLDB()
	}
	return s
}

func table(kind string) string {
	switch kind {
	case "auth":
		return "admin_session"
	case "draft":
		return "agent_draft"
	case "snapshot":
		return "debug_snapshot"
	case "session":
		return "debug_session"
	case "run":
		return "debug_run"
	case "event":
		return "debug_event"
	case "tool":
		return "debug_tool_execution"
	case "approval":
		return "debug_tool_approval"
	case "decision":
		return "debug_approval_decision"
	case "worker":
		return "console_worker"
	}
	return ""
}
func key(kind, tenant, id string) string { return kind + "\x00" + tenant + "\x00" + id }
func clone(r Record) Record              { r.Data = append(json.RawMessage(nil), r.Data...); return r }

const columns = `tenant_id,app_id,record_id,owner_id,status,version,data,created_at,updated_at,expires_at`

type scanner interface{ Scan(...any) error }

func scan(row scanner, kind string) (Record, error) {
	r := Record{Kind: kind}
	err := row.Scan(&r.TenantID, &r.AppID, &r.ID, &r.OwnerID, &r.Status, &r.Version, &r.Data, &r.CreatedAt, &r.UpdatedAt, &r.ExpiresAt)
	return r, mapError(err)
}
func mapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) && pg.Code == "23505" {
		return ErrConflict
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return ErrUnavailable
}

func (s *Store) Get(ctx context.Context, kind, tenant, id string) (Record, error) {
	if target := s.transactional(ctx); target != nil {
		return target.Get(ctx, kind, tenant, id)
	}
	name := table(kind)
	if name == "" {
		return Record{}, ErrNotFound
	}
	if s.db != nil {
		return scan(s.executor(ctx).QueryRowContext(ctx, "SELECT "+columns+" FROM "+name+" WHERE tenant_id=$1 AND record_id=$2 AND expires_at>now()", tenant, id), kind)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[key(kind, tenant, id)]
	if !ok || time.Now().After(r.ExpiresAt) {
		return Record{}, ErrNotFound
	}
	return clone(r), nil
}

func (s *Store) Create(ctx context.Context, r Record) (Record, error) {
	if target := s.transactional(ctx); target != nil {
		return target.Create(ctx, r)
	}
	name := table(r.Kind)
	if name == "" || r.ID == "" || r.OwnerID == "" || len(r.Data) > 1<<20 || !json.Valid(r.Data) {
		return Record{}, ErrUnavailable
	}
	now := time.Now().UTC()
	r.Version = 1
	r.CreatedAt = now
	r.UpdatedAt = now
	if r.ExpiresAt.IsZero() {
		r.ExpiresAt = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	if s.db != nil {
		_, err := s.executor(ctx).ExecContext(ctx, "INSERT INTO "+name+" ("+columns+") VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10)", r.TenantID, r.AppID, r.ID, r.OwnerID, r.Status, r.Version, string(r.Data), r.CreatedAt, r.UpdatedAt, r.ExpiresAt)
		return r, mapError(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(r.Kind, r.TenantID, r.ID)
	if _, ok := s.records[k]; ok {
		return Record{}, ErrConflict
	}
	for id, old := range s.records {
		if old.Kind == "auth" && now.After(old.ExpiresAt) {
			delete(s.records, id)
		}
	}
	if len(s.records) >= 20000 {
		return Record{}, ErrUnavailable
	}
	s.records[k] = clone(r)
	return r, nil
}

func (s *Store) Update(ctx context.Context, r Record, expected int64) (Record, error) {
	if target := s.transactional(ctx); target != nil {
		return target.Update(ctx, r, expected)
	}
	name := table(r.Kind)
	if name == "" || expected < 1 || len(r.Data) > 1<<20 || !json.Valid(r.Data) {
		return Record{}, ErrConflict
	}
	if s.db != nil {
		updated, err := scan(s.executor(ctx).QueryRowContext(ctx, "UPDATE "+name+" SET status=$1,data=$2::jsonb,version=version+1,updated_at=now() WHERE tenant_id=$3 AND record_id=$4 AND owner_id=$5 AND app_id=$6 AND version=$7 AND expires_at>now() RETURNING "+columns, r.Status, string(r.Data), r.TenantID, r.ID, r.OwnerID, r.AppID, expected), r.Kind)
		if errors.Is(err, ErrNotFound) {
			err = ErrConflict
		}
		return updated, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(r.Kind, r.TenantID, r.ID)
	old, ok := s.records[k]
	if !ok || old.Version != expected || old.OwnerID != r.OwnerID || old.AppID != r.AppID || time.Now().After(old.ExpiresAt) {
		return Record{}, ErrConflict
	}
	r.Version = old.Version + 1
	r.CreatedAt = old.CreatedAt
	r.ExpiresAt = old.ExpiresAt
	r.UpdatedAt = time.Now().UTC()
	s.records[k] = clone(r)
	return r, nil
}

func (s *Store) Delete(ctx context.Context, kind, tenant, id string) error {
	if target := s.transactional(ctx); target != nil {
		return target.Delete(ctx, kind, tenant, id)
	}
	name := table(kind)
	if name == "" {
		return ErrNotFound
	}
	if s.db != nil {
		_, err := s.executor(ctx).ExecContext(ctx, "DELETE FROM "+name+" WHERE tenant_id=$1 AND record_id=$2", tenant, id)
		return mapError(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, key(kind, tenant, id))
	return nil
}

func (s *Store) List(ctx context.Context, f Filter) ([]Record, error) {
	if target := s.transactional(ctx); target != nil {
		return target.List(ctx, f)
	}
	name := table(f.Kind)
	if name == "" || f.Limit < 1 || f.Limit > 100 || (!f.AllTenants && f.TenantID == "") {
		return nil, ErrUnavailable
	}
	result := []Record{}
	order := "DESC"
	if f.Ascending {
		order = "ASC"
	}
	if s.db != nil {
		var before any
		var after any
		if !f.AfterTime.IsZero() {
			after = f.AfterTime
		}
		if !f.BeforeTime.IsZero() {
			before = f.BeforeTime
		}
		rows, err := s.executor(ctx).QueryContext(ctx, "SELECT "+columns+" FROM "+name+" WHERE ($1 OR tenant_id=$2) AND ($3='' OR app_id=$3) AND ($4='' OR owner_id=$4) AND ($5='' OR status=$5) AND ($9::timestamptz IS NULL OR (created_at,record_id)<($9,$6)) AND ($10::timestamptz IS NULL OR created_at>=$10) AND ($8='' OR data->>'session_id'=$8) AND expires_at>now() ORDER BY created_at "+order+",record_id "+order+" LIMIT $7", f.AllTenants, f.TenantID, f.AppID, f.OwnerID, f.Status, f.Before, f.Limit, f.SessionID, before, after)
		if err != nil {
			return nil, mapError(err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			r, err := scan(rows, f.Kind)
			if err != nil {
				return nil, err
			}
			result = append(result, r)
		}
		return result, mapError(rows.Err())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.records {
		if !f.AfterTime.IsZero() && r.CreatedAt.Before(f.AfterTime) {
			continue
		}
		if !f.BeforeTime.IsZero() && (r.CreatedAt.After(f.BeforeTime) || r.CreatedAt.Equal(f.BeforeTime) && r.ID >= f.Before) {
			continue
		}
		if f.SessionID != "" {
			var payload struct {
				SessionID string `json:"session_id"`
			}
			if json.Unmarshal(r.Data, &payload) != nil || payload.SessionID != f.SessionID {
				continue
			}
		}
		if r.Kind != f.Kind || (!f.AllTenants && r.TenantID != f.TenantID) || (f.AppID != "" && r.AppID != f.AppID) || (f.OwnerID != "" && r.OwnerID != f.OwnerID) || (f.Status != "" && r.Status != f.Status) || time.Now().After(r.ExpiresAt) {
			continue
		}
		result = append(result, clone(r))
	}
	sort.Slice(result, func(i, j int) bool {
		if !result[i].CreatedAt.Equal(result[j].CreatedAt) {
			if f.Ascending {
				return result[i].CreatedAt.Before(result[j].CreatedAt)
			}
			return result[i].CreatedAt.After(result[j].CreatedAt)
		}
		return result[i].ID > result[j].ID
	})
	if len(result) > f.Limit {
		result = result[:f.Limit]
	}
	return result, nil
}

func (s *Store) Ready(ctx context.Context) error {
	if s.db == nil {
		return nil
	}
	var version int
	err := s.executor(ctx).QueryRowContext(ctx, "SELECT max(version) FROM schema_migration").Scan(&version)
	if err != nil || version < 24 {
		return ErrUnavailable
	}
	var exists bool
	if err := s.executor(ctx).QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM debug_run LIMIT 1)").Scan(&exists); err != nil {
		return ErrUnavailable
	}
	return nil
}

func (s *Store) Cleanup(ctx context.Context, kind string) error {
	if target := s.transactional(ctx); target != nil {
		return target.Cleanup(ctx, kind)
	}
	name := table(kind)
	if name == "" {
		return ErrNotFound
	}
	if s.db != nil {
		_, err := s.executor(ctx).ExecContext(ctx, "DELETE FROM "+name+" WHERE (tenant_id,record_id) IN (SELECT tenant_id,record_id FROM "+name+" WHERE expires_at<now() AND status NOT IN ('running','cancel_requested') LIMIT 200)")
		return mapError(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, r := range s.records {
		if r.Kind == kind && time.Now().After(r.ExpiresAt) {
			delete(s.records, k)
		}
	}
	return nil
}
