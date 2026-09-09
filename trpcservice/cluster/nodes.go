package cluster

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// ErrNodeIDInUse rejects two live processes sharing one fencing/queue identity.
var ErrNodeIDInUse = errors.New("cluster: node id is already active")

// NodeRegistry publishes liveness and graceful-drain state to PostgreSQL.
// Inbox leases remain the takeover authority; this table makes dead/stuck
// ownership observable without trusting process-local state.
type NodeRegistry struct {
	db       *sql.DB
	nodeID   string
	started  time.Time
	interval time.Duration
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	once     sync.Once
	draining atomic.Bool
}

// NewNodeRegistry writes the initial heartbeat before returning and then
// refreshes it until Close marks the node draining/stopped.
func NewNodeRegistry(parent context.Context, db *sql.DB, nodeID string, interval time.Duration) (*NodeRegistry, error) {
	if parent == nil || db == nil || nodeID == "" {
		return nil, errors.New("cluster: node registry context, database, and node id are required")
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ctx, cancel := context.WithCancel(parent)
	registry := &NodeRegistry{db: db, nodeID: nodeID, started: time.Now().UTC(), interval: interval, ctx: ctx, cancel: cancel, done: make(chan struct{})}
	if err := registry.claim(ctx); err != nil {
		cancel()
		return nil, err
	}
	go registry.run()
	return registry, nil
}

func (registry *NodeRegistry) run() {
	defer close(registry.done)
	ticker := time.NewTicker(registry.interval)
	defer ticker.Stop()
	for {
		select {
		case <-registry.ctx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(registry.ctx, registry.interval)
			_ = registry.heartbeat(ctx)
			cancel()
		}
	}
}

func (registry *NodeRegistry) heartbeat(ctx context.Context) error {
	now := time.Now().UTC()
	result, err := registry.db.ExecContext(ctx, `UPDATE worker_nodes SET last_heartbeat=$3,draining=$4
		WHERE node_id=$1 AND started_at=$2 AND stopped_at IS NULL`, registry.nodeID, registry.started, now, registry.draining.Load())
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return ErrNodeIDInUse
	}
	return nil
}

func (registry *NodeRegistry) claim(ctx context.Context) error {
	now := time.Now().UTC()
	staleBefore := now.Add(-3 * registry.interval)
	result, err := registry.db.ExecContext(ctx, `INSERT INTO worker_nodes (node_id,started_at,last_heartbeat,draining,stopped_at)
		VALUES ($1,$2,$3,FALSE,NULL)
		ON CONFLICT (node_id) DO UPDATE SET started_at=EXCLUDED.started_at,last_heartbeat=EXCLUDED.last_heartbeat,draining=FALSE,stopped_at=NULL
		WHERE worker_nodes.stopped_at IS NOT NULL OR worker_nodes.draining OR worker_nodes.last_heartbeat<$4`, registry.nodeID, registry.started, now, staleBefore)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return ErrNodeIDInUse
	}
	return nil
}

// BeginDrain makes schedulers and operators stop considering this node live
// for new work while the component finishes requests it already owns.
func (registry *NodeRegistry) BeginDrain(ctx context.Context) error {
	if registry == nil {
		return nil
	}
	registry.draining.Store(true)
	_, err := registry.db.ExecContext(ctx, `UPDATE worker_nodes SET draining=TRUE,last_heartbeat=$3 WHERE node_id=$1 AND started_at=$2`, registry.nodeID, registry.started, time.Now().UTC())
	return err
}

// Close stops heartbeats and records graceful drain without deleting history.
func (registry *NodeRegistry) Close(ctx context.Context) error {
	if registry == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("cluster: close context is required")
	}
	registry.once.Do(registry.cancel)
	select {
	case <-registry.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	now := time.Now().UTC()
	_, err := registry.db.ExecContext(ctx, `UPDATE worker_nodes SET draining=TRUE,stopped_at=$3,last_heartbeat=$3 WHERE node_id=$1 AND started_at=$2`, registry.nodeID, registry.started, now)
	return err
}
