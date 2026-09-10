package inmemory

import (
	"context"
	"sync"
)

// contextRWMutex is a fair-enough read/write lock whose wait path observes
// caller cancellation. Repository operations never wait forever on a blocked
// control-plane operation.
type contextRWMutex struct {
	state          sync.Mutex
	readers        int
	writer         bool
	waitingWriters int
	notify         chan struct{}
}

func (m *contextRWMutex) lock(ctx context.Context) error {
	m.state.Lock()
	m.initLocked()
	m.waitingWriters++
	for {
		if err := checkContext(ctx); err != nil {
			m.waitingWriters--
			m.wakeLocked()
			m.state.Unlock()
			return err
		}
		if !m.writer && m.readers == 0 {
			m.writer = true
			m.waitingWriters--
			m.state.Unlock()
			return nil
		}
		wait := m.notify
		m.state.Unlock()
		if err := waitContext(ctx, wait); err != nil {
			m.state.Lock()
			m.waitingWriters--
			m.wakeLocked()
			m.state.Unlock()
			return err
		}
		m.state.Lock()
		m.initLocked()
	}
}

func (m *contextRWMutex) unlock() {
	m.state.Lock()
	if !m.writer {
		m.state.Unlock()
		panic("channels/inmemory: unlock of unlocked contextRWMutex")
	}
	m.writer = false
	m.wakeLocked()
	m.state.Unlock()
}

func (m *contextRWMutex) rlock(ctx context.Context) error {
	m.state.Lock()
	m.initLocked()
	for {
		if err := checkContext(ctx); err != nil {
			m.state.Unlock()
			return err
		}
		if !m.writer && m.waitingWriters == 0 {
			m.readers++
			m.state.Unlock()
			return nil
		}
		wait := m.notify
		m.state.Unlock()
		if err := waitContext(ctx, wait); err != nil {
			return err
		}
		m.state.Lock()
		m.initLocked()
	}
}

func (m *contextRWMutex) runlock() {
	m.state.Lock()
	if m.readers == 0 {
		m.state.Unlock()
		panic("channels/inmemory: runlock of unlocked contextRWMutex")
	}
	m.readers--
	if m.readers == 0 {
		m.wakeLocked()
	}
	m.state.Unlock()
}

func (m *contextRWMutex) initLocked() {
	if m.notify == nil {
		m.notify = make(chan struct{})
	}
}

func (m *contextRWMutex) wakeLocked() {
	close(m.notify)
	m.notify = make(chan struct{})
}

func waitContext(ctx context.Context, wait <-chan struct{}) error {
	if ctx == nil {
		<-wait
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wait:
		return nil
	}
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
