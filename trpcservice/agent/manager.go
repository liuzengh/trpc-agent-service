package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cyl6/trpc-agent-service/trpcservice/budget"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/memoryvisibility"
	"github.com/cyl6/trpc-agent-service/trpcservice/sessionturn"
	platformskill "github.com/cyl6/trpc-agent-service/trpcservice/skill"

	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
	agentskill "trpc.group/trpc-go/trpc-agent-go/skill"
)

type Runtime struct {
	Runner                  runner.Runner
	AppNamespace            string
	Session                 session.Service
	TurnSession             sessionturn.TransactionalService
	SessionDatabaseIdentity string
	Memory                  memory.Service
	MemoryReader            memory.Reader
	MemoryIngestor          session.Ingestor
	Artifact                artifact.Service
	Knowledge               knowledge.Knowledge
	backendClosers          []io.Closer
}

func (r *Runtime) close() error {
	if r == nil {
		return nil
	}
	var err error
	if r.Runner != nil {
		err = errors.Join(err, r.Runner.Close())
	}
	for _, closer := range r.backendClosers {
		if closer != nil {
			err = errors.Join(err, closer.Close())
		}
	}
	if r.Session != nil {
		err = errors.Join(err, r.Session.Close())
	}
	return err
}

type handle struct {
	runtime   *Runtime
	refs      int
	lastUsed  time.Time
	evictable bool
}

const defaultRuntimeCacheLimit = 128

// ErrRuntimeCapacity means all cache slots hold in-flight or process-local
// state. Callers may retry after other work releases a persistent runtime.
var ErrRuntimeCapacity = errors.New("runtime cache capacity exhausted")

// Manager caches immutable Runtimes by tenant, revision, and config digest.
// Capacity includes builds in progress. Only idle runtimes with durable state
// can be evicted; process-local Session, Memory and Artifact data stay pinned.
// Persisted task snapshots allow an evicted revision to be rebuilt on demand.
type Manager struct {
	mu         sync.Mutex
	handles    map[string]*handle
	builds     map[string]chan struct{}
	cacheLimit int
	build      func(context.Context, config.TenantConfig) (*Runtime, error)
	closed     bool
}

func NewManager() *Manager {
	return NewManagerWithBudget(nil)
}

func NewManagerWithBudget(ledger budget.Ledger) *Manager {
	return NewManagerWithBudgetAndVisibility(ledger, nil)
}

func NewManagerWithBudgetAndVisibility(ledger budget.Ledger, visibility memoryvisibility.Store) *Manager {
	skills := NewSkillRepository(os.Getenv(platformskill.EnvSkillsRoot))
	return &Manager{
		handles:    make(map[string]*handle),
		builds:     make(map[string]chan struct{}),
		cacheLimit: defaultRuntimeCacheLimit,
		build: func(ctx context.Context, tenant config.TenantConfig) (*Runtime, error) {
			return buildRuntimeWithBudget(ctx, tenant, skills, ledger, visibility)
		},
	}
}

// NewSkillRepository resolves the platform skill repository rooted at root.
// It returns nil (skills disabled) when root is empty or unreadable, so a
// misconfigured repository never blocks tenant Runtime construction.
func NewSkillRepository(root string) agentskill.Repository {
	if strings.TrimSpace(root) == "" {
		return nil
	}
	repo, err := platformskill.Repository(root)
	if err != nil {
		// Repository errors may echo filesystem paths; keep the process log to
		// a stable category.
		log.Printf("skills disabled: repository init failed")
		return nil
	}
	return repo
}

func (m *Manager) Acquire(ctx context.Context, tenant config.TenantConfig) (*Runtime, func(), error) {
	key, err := runtimeCacheKey(tenant)
	if err != nil {
		return nil, nil, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return nil, nil, errors.New("runtime manager is closed")
		}
		if existing := m.handles[key]; existing != nil {
			existing.refs++
			existing.lastUsed = time.Now()
			m.mu.Unlock()
			return existing.runtime, m.releaseFunc(existing), nil
		}
		if pending := m.builds[key]; pending != nil {
			m.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-pending:
				continue
			}
		}
		var retired *Runtime
		if len(m.handles)+len(m.builds) >= m.cacheLimit {
			var oldest *handle
			var oldestKey string
			for candidateKey, candidate := range m.handles {
				if candidate.refs == 0 && candidate.evictable && (oldest == nil || candidate.lastUsed.Before(oldest.lastUsed)) {
					oldest, oldestKey = candidate, candidateKey
				}
			}
			if oldest == nil {
				m.mu.Unlock()
				return nil, nil, ErrRuntimeCapacity
			}
			retired = oldest.runtime
			delete(m.handles, oldestKey)
		}
		done := make(chan struct{})
		m.builds[key] = done
		m.mu.Unlock()

		// Closing and constructing backends must never hold the cache mutex.
		// The reservation includes this close so replacement pools cannot exceed
		// the cache limit while old resources are still being released.
		if retired != nil {
			_ = retired.close()
		}
		built, buildErr := m.build(ctx, tenant)
		m.mu.Lock()
		delete(m.builds, key)
		close(done)
		if buildErr != nil {
			m.mu.Unlock()
			return nil, nil, buildErr
		}
		if m.closed || ctx.Err() != nil {
			m.mu.Unlock()
			_ = built.close()
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
			return nil, nil, errors.New("runtime manager is closed")
		}
		h := &handle{runtime: built, refs: 1, lastUsed: time.Now(), evictable: runtimeStateIsDurable(tenant.Data)}
		m.handles[key] = h
		m.mu.Unlock()
		return built, m.releaseFunc(h), nil
	}
}

// runtimeStateIsDurable is intentionally conservative: an unknown or default
// backend must never cause process-local user data to be silently discarded.
func runtimeStateIsDurable(data config.DataConfig) bool {
	return (data.Session.Type == "redis" || data.Session.Type == "sql") &&
		(data.Memory.Type == "disabled" || data.Memory.Type == "redis" || data.Memory.Type == "external") &&
		data.Artifact.Type == "object" &&
		(data.Knowledge.Type == "" || data.Knowledge.Type == "disabled" || data.Knowledge.Type == "vector")
}

func (m *Manager) releaseFunc(h *handle) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			m.mu.Lock()
			if h.refs > 0 {
				h.refs--
				h.lastUsed = time.Now()
			}
			m.mu.Unlock()
		})
	}
}

func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	all := make([]*Runtime, 0, len(m.handles))
	for _, h := range m.handles {
		all = append(all, h.runtime)
	}
	m.handles = nil
	m.mu.Unlock()
	var result error
	for _, rt := range all {
		result = errors.Join(result, rt.close())
	}
	return result
}

func runtimeCacheKey(tenant config.TenantConfig) (string, error) {
	encoded, err := json.Marshal(tenant)
	if err != nil {
		return "", fmt.Errorf("encode tenant runtime revision: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return tenant.TenantID + "\x1f" + tenant.Version + "\x1f" + hex.EncodeToString(digest[:]), nil
}
