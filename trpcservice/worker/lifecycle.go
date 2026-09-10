package worker

import (
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type LifecycleState string

const (
	LifecycleStarting LifecycleState = "starting"
	LifecycleReady    LifecycleState = "ready"
	LifecycleDraining LifecycleState = "draining"
	LifecycleStopped  LifecycleState = "stopped"
)

// Lifecycle is the process-local readiness and drain authority for one
// Worker. Durable execution ownership remains in the lease/fence stores.
type Lifecycle struct {
	mu    sync.RWMutex
	state LifecycleState
	drain chan struct{}
	once  sync.Once
}

func NewLifecycle() *Lifecycle {
	return &Lifecycle{state: LifecycleStarting, drain: make(chan struct{})}
}

func (l *Lifecycle) MarkReady() error {
	if l == nil {
		return runtime.ErrInvariantViolation
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state != LifecycleStarting {
		return runtime.ErrInvariantViolation
	}
	l.state = LifecycleReady
	return nil
}

func (l *Lifecycle) BeginDrain() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		l.mu.Lock()
		if l.state != LifecycleStopped {
			l.state = LifecycleDraining
		}
		l.mu.Unlock()
		close(l.drain)
	})
}

func (l *Lifecycle) MarkStopped() {
	if l == nil {
		return
	}
	l.BeginDrain()
	l.mu.Lock()
	l.state = LifecycleStopped
	l.mu.Unlock()
}

func (l *Lifecycle) Drain() <-chan struct{} {
	if l == nil {
		return nil
	}
	return l.drain
}

func (l *Lifecycle) State() LifecycleState {
	if l == nil {
		return LifecycleStopped
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.state
}

func (l *Lifecycle) Ready() bool { return l.State() == LifecycleReady }
