package control

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
)

type NodeLifecycle struct {
	repository Repository
	config     config.ControlPlaneConfig
	nodeID     string
	role       string
	build      string
	capability []string

	registered atomic.Bool
	ready      atomic.Bool
	draining   atomic.Bool
	inflight   atomic.Int64

	mu     sync.Mutex
	cancel context.CancelFunc
	loops  sync.WaitGroup
	boot   string
}

func NewNodeLifecycle(repository Repository, cfg config.ControlPlaneConfig, nodeID, role, build string, capabilities []string) (*NodeLifecycle, error) {
	if repository == nil {
		return nil, errors.New("control repository is required")
	}
	if cfg.NodeAssignmentEnabled && nodeID == "" {
		return nil, errors.New("NODE_ID is required when node assignment is enabled")
	}
	if nodeID == "" {
		nodeID, _ = identity.RequestID()
		nodeID = "compat-" + nodeID
	}
	if role == "" {
		role = "worker"
	}
	if build == "" {
		build = "unknown"
	}
	if len(capabilities) == 0 {
		capabilities = []string{"governance.v1"}
	}
	return &NodeLifecycle{repository: repository, config: cfg, nodeID: nodeID, role: role, build: build, capability: append([]string(nil), capabilities...)}, nil
}

func (n *NodeLifecycle) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	n.mu.Lock()
	if n.cancel != nil {
		n.mu.Unlock()
		return errors.New("node lifecycle is already running")
	}
	runCtx, cancel := context.WithCancel(ctx)
	n.cancel = cancel
	n.loops.Add(1)
	n.mu.Unlock()
	go func() {
		defer n.loops.Done()
		n.run(runCtx)
	}()
	return nil
}

func (n *NodeLifecycle) run(ctx context.Context) {
	interval := n.config.NodeHeartbeatInterval
	if interval <= 0 {
		interval = config.DefaultNodeHeartbeatInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if !n.registered.Load() && !n.draining.Load() {
			_ = n.register(ctx)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n.registered.Load() && !n.draining.Load() {
				_ = n.heartbeat(ctx, NodeReady)
			}
		}
	}
}

func (n *NodeLifecycle) register(ctx context.Context) error {
	if err := n.repository.Ready(ctx); err != nil {
		n.registered.Store(false)
		n.ready.Store(false)
		return err
	}
	now := time.Now().UTC()
	ttl := n.config.NodeOfflineAfter
	if ttl <= 0 {
		ttl = config.DefaultNodeOfflineAfter
	}
	node := NodeRecord{
		NodeID: n.nodeID, Role: n.role, State: NodeReady, Capacity: 1,
		Inflight: int(n.inflight.Load()), BuildVersion: n.build,
		ProtocolCapabilities: append([]string(nil), n.capability...), BootID: n.bootID(),
		LeaseUntil: now.Add(ttl), StartedAt: now, LastHeartbeat: now,
	}
	if err := n.repository.RegisterNode(ctx, node); err != nil {
		n.registered.Store(false)
		n.ready.Store(false)
		return err
	}
	n.registered.Store(true)
	n.ready.Store(true)
	return nil
}

func (n *NodeLifecycle) heartbeat(ctx context.Context, state NodeState) error {
	now := time.Now().UTC()
	ttl := n.config.NodeOfflineAfter
	if ttl <= 0 {
		ttl = config.DefaultNodeOfflineAfter
	}
	err := n.repository.HeartbeatNode(ctx, n.nodeID, n.bootID(), state, int(n.inflight.Load()), now.Add(ttl))
	if err != nil {
		n.registered.Store(false)
		n.ready.Store(false)
	}
	return err
}

func (n *NodeLifecycle) bootID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.boot != "" {
		return n.boot
	}
	n.boot, _ = identity.RequestID()
	if n.boot == "" {
		n.boot = "boot"
	}
	return n.boot
}

func (n *NodeLifecycle) Ready(context.Context) error {
	if n.ready.Load() && n.registered.Load() && !n.draining.Load() {
		return nil
	}
	return ErrUnavailable
}

func (n *NodeLifecycle) SetInflight(value int) {
	if value < 0 {
		value = 0
	}
	n.inflight.Store(int64(value))
}

func (n *NodeLifecycle) Inflight() int  { return int(n.inflight.Load()) }
func (n *NodeLifecycle) NodeID() string { return n.nodeID }
func (n *NodeLifecycle) BootID() string { return n.bootID() }

func (n *NodeLifecycle) MarkDraining(ctx context.Context) error {
	n.draining.Store(true)
	n.ready.Store(false)
	if !n.registered.Load() {
		return nil
	}
	return n.heartbeat(ctx, NodeDraining)
}

func (n *NodeLifecycle) Close() error {
	n.mu.Lock()
	cancel := n.cancel
	n.cancel = nil
	n.mu.Unlock()
	if cancel != nil {
		_ = n.MarkDraining(context.Background())
		cancel()
	}
	n.loops.Wait()
	n.ready.Store(false)
	n.registered.Store(false)
	return nil
}

var _ interface {
	Ready(context.Context) error
	Start(context.Context) error
	Close() error
} = (*NodeLifecycle)(nil)
