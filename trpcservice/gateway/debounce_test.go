package gateway

import (
	"testing"
	"time"
)

func TestTimerDebouncerCoalescesByKey(t *testing.T) {
	debouncer := NewDebouncer(20 * time.Millisecond)
	called := make(chan int, 3)

	debouncer.Schedule("session-a", func() { called <- 1 })
	debouncer.Schedule("session-a", func() { called <- 2 })
	debouncer.Schedule("session-a", func() { called <- 3 })

	select {
	case got := <-called:
		if got != 3 {
			t.Fatalf("callback value = %d, want latest callback 3", got)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("debounced callback was not invoked")
	}
	select {
	case extra := <-called:
		t.Fatalf("unexpected extra callback %d", extra)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestTimerDebouncerSeparatesKeysAndFlushesAll(t *testing.T) {
	debouncer := NewDebouncer(time.Hour)
	called := make(chan string, 2)
	debouncer.Schedule("session-a", func() { called <- "a" })
	debouncer.Schedule("session-b", func() { called <- "b" })

	debouncer.FlushAll()

	seen := map[string]bool{}
	for range 2 {
		select {
		case key := <-called:
			seen[key] = true
		default:
			t.Fatal("FlushAll() did not synchronously invoke every callback")
		}
	}
	if !seen["a"] || !seen["b"] {
		t.Fatalf("FlushAll() callbacks = %v, want a and b", seen)
	}
}
