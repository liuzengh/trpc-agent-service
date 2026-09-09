package gateway

import (
	"sync"
	"time"
)

type debounceEntry struct {
	timer *time.Timer
	flush func()
}

// TimerDebouncer uses one resettable timer per session key.
type TimerDebouncer struct {
	window time.Duration

	mu      sync.Mutex
	entries map[string]*debounceEntry
	active  sync.WaitGroup
	closed  bool
}

// NewDebouncer constructs a per-key debouncer.
func NewDebouncer(window time.Duration) *TimerDebouncer {
	if window < 0 {
		window = 0
	}
	return &TimerDebouncer{
		window:  window,
		entries: make(map[string]*debounceEntry),
	}
}

// Schedule replaces the pending callback for a key and resets its window.
func (d *TimerDebouncer) Schedule(key string, flush func()) {
	if key == "" || flush == nil {
		return
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	if previous, ok := d.entries[key]; ok {
		previous.timer.Stop()
	}
	entry := &debounceEntry{flush: flush}
	entry.timer = time.AfterFunc(d.window, func() {
		d.fire(key, entry)
	})
	d.entries[key] = entry
	d.mu.Unlock()
}

func (d *TimerDebouncer) fire(key string, candidate *debounceEntry) {
	d.mu.Lock()
	current, ok := d.entries[key]
	if !ok || current != candidate {
		d.mu.Unlock()
		return
	}
	delete(d.entries, key)
	d.active.Add(1)
	flush := current.flush
	d.mu.Unlock()
	defer d.active.Done()
	flush()
}

// FlushAll stops every timer and synchronously executes each pending callback.
func (d *TimerDebouncer) FlushAll() {
	d.mu.Lock()
	d.closed = true
	callbacks := make([]func(), 0, len(d.entries))
	for key, entry := range d.entries {
		entry.timer.Stop()
		callbacks = append(callbacks, entry.flush)
		delete(d.entries, key)
	}
	d.active.Add(len(callbacks))
	d.mu.Unlock()
	for _, flush := range callbacks {
		func() {
			defer d.active.Done()
			flush()
		}()
	}
	d.active.Wait()
}
