package wecomws

import (
	"bytes"
	"context"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

// manager reconciles the channel's bindings against its live connections:
// every resync interval a fresh RoutesByChannel snapshot starts connections
// for new bindings and stops (cancel + drain) connections whose binding
// disappeared or was disabled.
type manager struct {
	parent *Channel

	mu    sync.Mutex
	conns map[string]*botConn // binding id → connection
}

func (m *manager) byBinding(id string) *botConn {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.conns[id]
}

// reconcile converges the connections onto the servable binding set: starts
// connections for new bindings, and stops (cancel + drain) connections whose
// binding disappeared, was disabled, or changed (edited config/webhook_path
// restarts the connection so the new values take effect). A binding whose
// config cannot be parsed is skipped with a warning — one broken row must not
// drag down the other connections.
func (m *manager) reconcile(ctx context.Context, h channels.Handler) error {
	bindings, err := m.parent.routes.RoutesByChannel(ctx, m.parent.Name())
	if err != nil {
		return err
	}
	want := make(map[string]Binding, len(bindings))
	for _, b := range bindings {
		want[b.ID] = b
	}

	m.mu.Lock()
	var stopped []*botConn
	for id, bc := range m.conns {
		b, ok := want[id]
		if ok && b.WebhookPath == bc.binding.WebhookPath && bytes.Equal(b.Config, bc.binding.Config) {
			continue
		}
		bc.cancel()
		delete(m.conns, id)
		stopped = append(stopped, bc)
	}
	m.mu.Unlock()

	// Drain before starting replacements: the platform allows one connection
	// per bot, so an overlap would make the old and new sockets kick each
	// other.
	for _, bc := range stopped {
		<-bc.done
		plog.Infof("wecomws: connection stopped (bot %s, binding %s)", bc.cfg.BotID, bc.binding.ID)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for id, b := range want {
		if _, ok := m.conns[id]; ok {
			continue
		}
		cfg, err := parseBindingConfig(b.Config)
		if err != nil {
			plog.Warnf("wecomws: skipping binding %s (%s): %v", b.ID, b.WebhookPath, err)
			continue
		}
		connCtx, cancel := context.WithCancel(ctx)
		bc := &botConn{
			parent:  m.parent,
			binding: b,
			cfg:     cfg,
			cancel:  cancel,
			done:    make(chan struct{}),
		}
		m.conns[id] = bc
		go bc.run(connCtx, h)
		plog.Infof("wecomws: connection started (bot %s, binding %s, path %s)", cfg.BotID, b.ID, b.WebhookPath)
	}
	return nil
}

// stopAll cancels every connection and waits for all run loops to return —
// Start calls it on shutdown so ctx cancel leaves nothing behind.
func (m *manager) stopAll() {
	m.mu.Lock()
	all := make([]*botConn, 0, len(m.conns))
	for id, bc := range m.conns {
		bc.cancel()
		delete(m.conns, id)
		all = append(all, bc)
	}
	m.mu.Unlock()
	for _, bc := range all {
		<-bc.done
	}
}
