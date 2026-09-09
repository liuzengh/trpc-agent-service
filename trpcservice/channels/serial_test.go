package channels

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSessionSerializerMutualExclusion hammers one session from many
// goroutines and fails on any overlap inside the critical section. Run with
// -race to also catch data races on the serializer itself.
func TestSessionSerializerMutualExclusion(t *testing.T) {
	s := newSessionSerializer()
	var inside, overlap, total atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release := s.lock("t1:wecom:user-a")
			defer release()
			if inside.Add(1) != 1 {
				overlap.Add(1)
			}
			time.Sleep(time.Millisecond)
			inside.Add(-1)
			total.Add(1)
		}()
	}
	wg.Wait()
	if overlap.Load() != 0 {
		t.Fatalf("%d overlapping critical sections, want 0", overlap.Load())
	}
	if total.Load() != 64 {
		t.Fatalf("completed = %d, want 64", total.Load())
	}
	s.mu.Lock()
	idle := len(s.locks)
	s.mu.Unlock()
	if idle != 0 {
		t.Fatalf("%d lock entries left after idle, want 0 (refcount cleanup)", idle)
	}
}

// TestSessionSerializerCrossSessionParallel verifies a held lock on one
// session neither blocks another session nor leaks: the same-session lock
// only proceeds after release.
func TestSessionSerializerCrossSessionParallel(t *testing.T) {
	s := newSessionSerializer()
	holdA := s.lock("sess-a")

	otherDone := make(chan struct{})
	go func() {
		release := s.lock("sess-b")
		release()
		close(otherDone)
	}()
	select {
	case <-otherDone:
	case <-time.After(time.Second):
		t.Fatal("a different session must not block behind sess-a")
	}

	sameDone := make(chan struct{})
	go func() {
		release := s.lock("sess-a")
		release()
		close(sameDone)
	}()
	select {
	case <-sameDone:
		t.Fatal("same session must queue behind the held lock")
	case <-time.After(50 * time.Millisecond):
	}

	holdA() // release sess-a
	select {
	case <-sameDone:
	case <-time.After(time.Second):
		t.Fatal("queued same-session lock never acquired after release")
	}

	s.mu.Lock()
	idle := len(s.locks)
	s.mu.Unlock()
	if idle != 0 {
		t.Fatalf("%d lock entries left after idle, want 0", idle)
	}
}
