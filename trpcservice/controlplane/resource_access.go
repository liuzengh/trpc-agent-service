package controlplane

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"golang.org/x/sync/semaphore"
)

// ResourceAccess holds a shared migration gate and an exclusive subject lock.
// Only registration, fencing and epoch invalidation take the short metadata
// lock; backend I/O never holds that application-wide metadata lock.
type ResourceAccess struct {
	Subject         string
	Write           bool
	FencingToken    int64
	HasFencingToken bool
}

type ResourceAccessRepository interface {
	WithResourceAccess(context.Context, string, string, string, ResourceAccess, func(context.Context) error) error
}

func ResourceAccessHeld(ctx context.Context, t, a, kind, subject string) bool {
	v, ok := ctx.Value(resourceContextKey{}).(resourceContext)
	return ok && v.Key == ResourceLockName(t, a, kind) && (v.Subject == "" || v.Subject == subject)
}

func prepareResourceAccess(s *ResourceSync, access ResourceAccess) (bool, error) {
	if access.HasFencingToken && access.FencingToken < s.Fences[access.Subject] {
		return false, coordination.ErrLeaseLost
	}
	changed := false
	if !s.Subjects[access.Subject] {
		s.Subjects[access.Subject] = true
		s.Epoch++
		changed = true
	}
	if access.HasFencingToken && access.FencingToken > s.Fences[access.Subject] {
		s.Fences[access.Subject] = access.FencingToken
		changed = true
	}
	if access.Write {
		s.Epoch++
		changed = true
	}
	return changed, nil
}

func accessContext(ctx context.Context, key string, access ResourceAccess, s *ResourceSync) context.Context {
	return context.WithValue(ctx, resourceContextKey{}, resourceContext{
		Key: key, State: s, Subject: access.Subject,
		Save: func() error { return errors.New("resource proofs and aliases require the exclusive migration lock") },
	})
}

func resourceSubjectLock(key, subject string) string {
	h := sha256.Sum256([]byte(key + "\x00" + subject))
	return "resource-subject:v1:" + hex.EncodeToString(h[:])
}

func (r *PostgresRepository) WithResourceAccess(ctx context.Context, t, a, kind string, access ResourceAccess, fn func(context.Context) error) (err error) {
	if !validResource(kind) || access.Subject == "" {
		return errors.New("a valid resource subject is required")
	}
	if _, _, held := ResourceFromContext(ctx, t, a, kind); held {
		if !ResourceAccessHeld(ctx, t, a, kind, access.Subject) {
			return errors.New("nested resource access changed subject")
		}
		return fn(ctx)
	}
	conn, err := r.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	key := ResourceLockName(t, a, kind)
	if err := lockResourceConnection(ctx, conn, key, true); err != nil {
		return err
	}
	defer func() { err = errors.Join(err, unlockResourceConnection(conn, key, true)) }()
	subjectKey := resourceSubjectLock(key, access.Subject)
	if err := lockResourceConnection(ctx, conn, subjectKey, false); err != nil {
		return err
	}
	defer func() { err = errors.Join(err, unlockResourceConnection(conn, subjectKey, false)) }()
	state := emptyResourceSync()
	// Serialize read/modify/write of the existing schema-21 JSON document.
	// Merge against the latest persisted state, never a pre-I/O stale copy.
	metadataKey := key + ":metadata:v1"
	if err := lockResourceConnection(ctx, conn, metadataKey, false); err != nil {
		return err
	}
	err = func() (metadataErr error) {
		defer func() { metadataErr = errors.Join(metadataErr, unlockResourceConnection(conn, metadataKey, false)) }()
		var raw []byte
		err := conn.QueryRowContext(ctx, "SELECT state FROM resource_sync WHERE tenant_id=$1 AND app_id=$2 AND resource_type=$3", t, a, kind).Scan(&raw)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &state); err != nil {
				return err
			}
		}
		changed, err := prepareResourceAccess(&state, access)
		if err != nil || !changed {
			return err
		}
		raw, err = json.Marshal(state)
		if err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `INSERT INTO resource_sync(tenant_id,app_id,resource_type,state) VALUES($1,$2,$3,$4::jsonb) ON CONFLICT(tenant_id,app_id,resource_type) DO UPDATE SET state=EXCLUDED.state,updated_at=now()`, t, a, kind, string(raw))
		return err
	}()
	if err != nil {
		return err
	}
	ctx = context.WithValue(ctx, knowledgeConnectionKey{}, scopedKnowledgeConnection{r.db, conn})
	return fn(accessContext(ctx, key, access, &state))
}

func lockResourceConnection(ctx context.Context, conn *sql.Conn, key string, shared bool) error {
	query := "SELECT pg_advisory_lock(hashtextextended($1,0))"
	if shared {
		query = "SELECT pg_advisory_lock_shared(hashtextextended($1,0))"
	}
	_, err := conn.ExecContext(ctx, query, key)
	if err != nil {
		// Cancellation may race with successful acquisition. Never return a
		// connection with an unaccounted session-level advisory lock to the pool.
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	}
	return err
}

func unlockResourceConnection(conn *sql.Conn, key string, shared bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	query := "SELECT pg_advisory_unlock(hashtextextended($1,0))"
	if shared {
		query = "SELECT pg_advisory_unlock_shared(hashtextextended($1,0))"
	}
	var released bool
	if err := conn.QueryRowContext(ctx, query, key).Scan(&released); err != nil || !released {
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		return errors.New("resource lock ownership lost during release")
	}
	return nil
}

const resourceGateCapacity int64 = 1 << 30

func (m *resourceMemory) gate(key string) *semaphore.Weighted {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.gates == nil {
		m.gates = map[string]*semaphore.Weighted{}
	}
	if m.states == nil {
		m.states = map[string][]byte{}
	}
	if m.gates[key] == nil {
		m.gates[key] = semaphore.NewWeighted(resourceGateCapacity)
	}
	return m.gates[key]
}

func (r *MemoryRepository) WithResourceAccess(ctx context.Context, t, a, kind string, access ResourceAccess, fn func(context.Context) error) error {
	if !validResource(kind) || access.Subject == "" {
		return errors.New("a valid resource subject is required")
	}
	if _, _, held := ResourceFromContext(ctx, t, a, kind); held {
		if !ResourceAccessHeld(ctx, t, a, kind, access.Subject) {
			return errors.New("nested resource access changed subject")
		}
		return fn(ctx)
	}
	key := ResourceLockName(t, a, kind)
	gate := r.resourceMemory.gate(key)
	if err := gate.Acquire(ctx, 1); err != nil {
		return err
	}
	defer gate.Release(1)
	lock := r.resourceMemory.lock(resourceSubjectLock(key, access.Subject))
	select {
	case lock <- struct{}{}:
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	defer func() { <-lock }()
	s := emptyResourceSync()
	err := func() error {
		r.resourceMemory.mu.Lock()
		defer r.resourceMemory.mu.Unlock()
		if raw := r.resourceMemory.states[key]; len(raw) > 0 {
			if err := json.Unmarshal(raw, &s); err != nil {
				return err
			}
		}
		changed, err := prepareResourceAccess(&s, access)
		if err != nil || !changed {
			return err
		}
		raw, err := json.Marshal(s)
		if err != nil {
			return err
		}
		r.resourceMemory.states[key] = raw
		return nil
	}()
	if err != nil {
		return err
	}
	return fn(accessContext(ctx, key, access, &s))
}
