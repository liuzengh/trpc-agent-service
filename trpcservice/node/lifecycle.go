// Package node owns the observable lifecycle of Gateway and Worker processes.
// Kafka remains responsible for Worker assignment and rebalance; this package
// deliberately does not implement a second task scheduler.
package node

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/safego"
)

var (
	ErrUnavailable = errors.New("node is not ready")
	ErrLeaseLost   = errors.New("node lease lost")
)

type State string

const (
	StateReady    State = "ready"
	StateDraining State = "draining"
	StateOffline  State = "offline"
)

// Record is the shared control-plane projection of one running process.
type Record struct {
	NodeID        string    `json:"node_id"`
	BootID        string    `json:"boot_id"`
	Role          string    `json:"role"`
	State         State     `json:"state"`
	BuildVersion  string    `json:"build_version"`
	Inflight      int       `json:"inflight"`
	StartedAt     time.Time `json:"started_at"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	LeaseUntil    time.Time `json:"lease_until"`
}

func (r Record) validate() error {
	if strings.TrimSpace(r.NodeID) == "" || strings.TrimSpace(r.BootID) == "" {
		return errors.New("node and boot IDs are required")
	}
	switch r.Role {
	case "gateway", "channel", "worker", "all":
	default:
		return fmt.Errorf("unsupported node role %q", r.Role)
	}
	if r.State != StateReady && r.State != StateDraining {
		return fmt.Errorf("unsupported persisted node state %q", r.State)
	}
	if r.Inflight < 0 || r.StartedAt.IsZero() || r.LastHeartbeat.IsZero() || r.LeaseUntil.IsZero() {
		return errors.New("node lifecycle fields are incomplete")
	}
	return nil
}

// Lister exposes the cluster-wide node projection to read-only surfaces such
// as the Console. It intentionally cannot mutate node state.
type Lister interface {
	List(context.Context) ([]Record, error)
}

// Store persists the small node projection. It does not own scheduling.
type Store interface {
	Lister
	Register(context.Context, Record) error
	Heartbeat(context.Context, string, string, State, int, time.Time) error
}

type LifecycleConfig struct {
	NodeID            string
	Role              string
	BuildVersion      string
	HeartbeatInterval time.Duration
	OfflineAfter      time.Duration
	Inflight          func() int
}

// Lifecycle hides registration, heartbeat, boot fencing, and drain state
// behind a tiny process-level interface.
type Lifecycle struct {
	store Store
	cfg   LifecycleConfig
	boot  string

	registered atomic.Bool
	draining   atomic.Bool

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

func NewLifecycle(store Store, cfg LifecycleConfig) (*Lifecycle, error) {
	if store == nil {
		return nil, errors.New("node store is required")
	}
	cfg.NodeID = strings.TrimSpace(cfg.NodeID)
	cfg.Role = strings.ToLower(strings.TrimSpace(cfg.Role))
	cfg.BuildVersion = strings.TrimSpace(cfg.BuildVersion)
	if cfg.NodeID == "" || cfg.Role == "" {
		return nil, errors.New("node ID and role are required")
	}
	if cfg.HeartbeatInterval <= 0 || cfg.OfflineAfter <= 0 || cfg.HeartbeatInterval >= cfg.OfflineAfter {
		return nil, errors.New("node heartbeat interval must be shorter than offline window")
	}
	if cfg.Inflight == nil {
		cfg.Inflight = func() int { return 0 }
	}
	if cfg.BuildVersion == "" {
		cfg.BuildVersion = "unknown"
	}
	return &Lifecycle{store: store, cfg: cfg, boot: uuid.NewString(), done: make(chan struct{})}, nil
}

func (l *Lifecycle) Start(ctx context.Context) error {
	if l == nil {
		return errors.New("node lifecycle is required")
	}
	// 防御 nil ctx：心跳 goroutine 依赖 ctx.Done() 退出，nil 会 panic。
	// 调用方应传入服务生命周期 ctx，以便停止时回收心跳。
	if ctx == nil {
		ctx = context.Background()
	}
	l.mu.Lock()
	if l.cancel != nil {
		l.mu.Unlock()
		return errors.New("node lifecycle is already running")
	}
	runCtx, cancel := context.WithCancel(ctx)
	l.cancel = cancel
	l.mu.Unlock()
	if err := l.register(runCtx); err != nil {
		l.mu.Lock()
		l.cancel = nil
		l.mu.Unlock()
		cancel()
		return err
	}
	go func() {
		if panicErr := safego.Run("node lifecycle heartbeat", func() { l.run(runCtx) }); panicErr != nil {
			l.registered.Store(false)
		}
	}()
	return nil
}

func (l *Lifecycle) register(ctx context.Context) error {
	now := time.Now().UTC()
	record := Record{
		NodeID: l.cfg.NodeID, BootID: l.boot, Role: l.cfg.Role, State: StateReady,
		BuildVersion: l.cfg.BuildVersion, Inflight: l.inflight(), StartedAt: now,
		LastHeartbeat: now, LeaseUntil: now.Add(l.cfg.OfflineAfter),
	}
	if err := l.store.Register(ctx, record); err != nil {
		l.registered.Store(false)
		return err
	}
	l.registered.Store(true)
	return nil
}

func (l *Lifecycle) run(ctx context.Context) {
	defer close(l.done)
	ticker := time.NewTicker(l.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if l.draining.Load() {
				continue
			}
			if err := l.heartbeat(ctx, StateReady); err != nil {
				l.registered.Store(false)
			}
		}
	}
}

func (l *Lifecycle) inflight() int {
	value := l.cfg.Inflight()
	if value < 0 {
		return 0
	}
	return value
}

func (l *Lifecycle) heartbeat(ctx context.Context, state State) error {
	err := l.store.Heartbeat(ctx, l.cfg.NodeID, l.boot, state, l.inflight(), time.Now().UTC().Add(l.cfg.OfflineAfter))
	if err != nil {
		l.registered.Store(false)
		return err
	}
	l.registered.Store(true)
	return nil
}

// Ready is suitable for a readiness probe. Draining nodes immediately leave
// service even while an already-admitted delivery is still finishing.
func (l *Lifecycle) Ready(context.Context) error {
	if l != nil && l.registered.Load() && !l.draining.Load() {
		return nil
	}
	return ErrUnavailable
}

// BeginDrain makes the shared node state non-ready before the caller drains
// its active work.
func (l *Lifecycle) BeginDrain(ctx context.Context) error {
	if l == nil {
		return nil
	}
	l.draining.Store(true)
	return l.heartbeat(ctx, StateDraining)
}

func (l *Lifecycle) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	cancel := l.cancel
	l.cancel = nil
	l.mu.Unlock()
	if cancel == nil {
		return nil
	}
	if !l.draining.Load() {
		ctx, finish := context.WithTimeout(context.Background(), time.Second)
		_ = l.BeginDrain(ctx)
		finish()
	}
	cancel()
	<-l.done
	l.registered.Store(false)
	return nil
}
