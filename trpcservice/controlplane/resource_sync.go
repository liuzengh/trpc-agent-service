package controlplane

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

type ResourceProof struct {
	Epoch          int64             `json:"epoch"`
	Subjects       map[string]string `json:"subjects"`
	Source, Target string
}
type ResourceSync struct {
	Epoch    int64                    `json:"epoch"`
	Subjects map[string]bool          `json:"subjects"`
	Fences   map[string]int64         `json:"fences"`
	Aliases  map[string]string        `json:"aliases"`
	Proofs   map[string]ResourceProof `json:"proofs"`
}

func emptyResourceSync() ResourceSync {
	return ResourceSync{Subjects: map[string]bool{}, Fences: map[string]int64{}, Aliases: map[string]string{}, Proofs: map[string]ResourceProof{}}
}
func ResourceLockName(t, a, r string) string { return "resource:" + t + "/" + a + "/" + r }

type ResourceSyncRepository interface {
	WithResourceSync(context.Context, string, string, string, func(context.Context, *ResourceSync, func() error) error) error
}
type resourceContext struct {
	Key     string
	State   *ResourceSync
	Save    func() error
	Subject string // empty only while holding the exclusive application gate
}
type resourceContextKey struct{}

func ResourceFromContext(ctx context.Context, t, a, r string) (*ResourceSync, func() error, bool) {
	v, ok := ctx.Value(resourceContextKey{}).(resourceContext)
	return v.State, v.Save, ok && v.Key == ResourceLockName(t, a, r)
}
func validResource(resource string) bool { return resource == "session" || resource == "memory" }

func (r *PostgresRepository) WithResourceSync(ctx context.Context, t, a, kind string, fn func(context.Context, *ResourceSync, func() error) error) error {
	if !validResource(kind) {
		return errors.New("invalid synchronized resource")
	}
	if s, save, ok := ResourceFromContext(ctx, t, a, kind); ok {
		if ctx.Value(resourceContextKey{}).(resourceContext).Subject != "" {
			return errors.New("cannot upgrade a subject resource lock to an application lock")
		}
		return fn(ctx, s, save)
	}
	conn, err := r.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(conn)
	key := ResourceLockName(t, a, kind)
	if _, err = conn.ExecContext(ctx, "SELECT pg_advisory_lock(hashtextextended($1,0))", key); err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		var released bool
		if err := conn.QueryRowContext(cleanup, "SELECT pg_advisory_unlock(hashtextextended($1,0))", key).Scan(&released); err != nil || !released {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
	}()
	s := emptyResourceSync()
	var raw []byte
	err = conn.QueryRowContext(ctx, "SELECT state FROM resource_sync WHERE tenant_id=$1 AND app_id=$2 AND resource_type=$3", t, a, kind).Scan(&raw)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil {
		if err = json.Unmarshal(raw, &s); err != nil {
			return err
		}
	}
	save := func() error {
		raw, err := json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `INSERT INTO resource_sync(tenant_id,app_id,resource_type,state) VALUES($1,$2,$3,$4::jsonb) ON CONFLICT(tenant_id,app_id,resource_type) DO UPDATE SET state=EXCLUDED.state,updated_at=now()`, t, a, kind, string(raw))
		return err
	}
	ctx = context.WithValue(ctx, knowledgeConnectionKey{}, scopedKnowledgeConnection{r.db, conn})
	return fn(context.WithValue(ctx, resourceContextKey{}, resourceContext{Key: key, State: &s, Save: save}), &s, save)
}

type resourceMemory struct {
	mu     sync.Mutex
	locks  map[string]chan struct{}
	states map[string][]byte
	gates  map[string]*semaphore.Weighted
}

func (m *resourceMemory) lock(key string) chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.locks == nil {
		m.locks = map[string]chan struct{}{}
	}
	if m.states == nil {
		m.states = map[string][]byte{}
	}
	if m.locks[key] == nil {
		m.locks[key] = make(chan struct{}, 1)
	}
	return m.locks[key]
}
func (r *MemoryRepository) WithResourceSync(ctx context.Context, t, a, kind string, fn func(context.Context, *ResourceSync, func() error) error) error {
	if !validResource(kind) {
		return errors.New("invalid synchronized resource")
	}
	if s, save, ok := ResourceFromContext(ctx, t, a, kind); ok {
		if ctx.Value(resourceContextKey{}).(resourceContext).Subject != "" {
			return errors.New("cannot upgrade a subject resource lock to an application lock")
		}
		return fn(ctx, s, save)
	}
	key := ResourceLockName(t, a, kind)
	gate := r.resourceMemory.gate(key)
	if err := gate.Acquire(ctx, resourceGateCapacity); err != nil {
		return err
	}
	defer gate.Release(resourceGateCapacity)
	r.resourceMemory.mu.Lock()
	raw := append([]byte(nil), r.resourceMemory.states[key]...)
	r.resourceMemory.mu.Unlock()
	s := emptyResourceSync()
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &s); err != nil {
			return err
		}
	}
	save := func() error {
		raw, err := json.Marshal(s)
		if err != nil {
			return err
		}
		r.resourceMemory.mu.Lock()
		r.resourceMemory.states[key] = raw
		r.resourceMemory.mu.Unlock()
		return nil
	}
	return fn(context.WithValue(ctx, resourceContextKey{}, resourceContext{Key: key, State: &s, Save: save}), &s, save)
}
func validResourceProof(s ResourceSync, m BackendMigration) bool {
	p, ok := s.Proofs[m.ID]
	if !ok || p.Epoch != s.Epoch || p.Source != m.SourceBindingID || p.Target != m.TargetBindingID || len(p.Subjects) == 0 || len(p.Subjects) != len(s.Subjects) {
		return false
	}
	for subject := range s.Subjects {
		if p.Subjects[subject] == "" {
			return false
		}
	}
	return true
}

// The proof remains metadata-only. Every write invalidates its epoch; partial
// verification of a declared/observed inventory cannot authorize app cutover.
func RecordResourceProof(s *ResourceSync, m BackendMigration, subject, digest string) {
	p := s.Proofs[m.ID]
	if p.Epoch != s.Epoch || p.Source != m.SourceBindingID || p.Target != m.TargetBindingID || p.Subjects == nil {
		p = ResourceProof{Epoch: s.Epoch, Subjects: map[string]string{}, Source: m.SourceBindingID, Target: m.TargetBindingID}
	}
	p.Subjects[subject] = digest
	s.Proofs[m.ID] = p
}
