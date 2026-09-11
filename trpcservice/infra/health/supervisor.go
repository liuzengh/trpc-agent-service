package health

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Supervisor keeps a long-running dependency loop attached across transient
// failures and reports an outage through the health probe.
//
// Why it exists: both the inbound consumer (XREADGROUP) and the IM outbound
// follower (XREAD) return as soon as Redis goes away. Returning on the first
// failure turned a 20 second Redis stop into a permanent outage on the deployed
// stack — publishing recovered, so every new message was accepted and then
// silently never processed, while /healthz kept answering 200.
//
// Reconnecting is only half of it. The loop must also report *recovery*, and the
// signal for that has to come from inside the loop: "the loop is still running"
// is not evidence that Redis is reachable, because a blocking read handed a
// stale pooled connection can sit there for seconds while the server is gone. A
// first version of this type inferred recovery from an attempt that outlived a
// timer, and the fault drill (scripts/faults/redis-outage.ps1) caught it
// immediately: the drill's 25s outage showed /healthz 200 and "reattached" in
// the log while Redis was still down. Superseded design, kept as a warning.
//
// The truthful signal is the one the bus can produce: a read the server
// answered. Loops call Attached for it (RedisBus.SetConsumeReady,
// Gateway.SetReady).
type Supervisor struct {
	// Name identifies the loop in logs, in the probe reason and as the issue
	// key, e.g. "worker: consumer".
	Name string
	// Base is the first retry delay; Max caps the exponential growth.
	Base, Max time.Duration
	// DegradeAfter is the number of consecutive failures after which the node
	// reports degraded on the probe. It is the fallback signal for a dependency
	// that never answered at all.
	DegradeAfter int
	// Grace is how long the dependency may stay silent before a failed attempt
	// is reported as an outage. It is the primary signal: the bus answers about
	// once a second (the 1s blocking read), so "no answer for Grace" is a real
	// outage, while a failure count alone is not — one failed attempt can cost
	// seconds when the pool hands out a connection to a dead server, which is
	// how the deployed stack stayed at /healthz 200 through a 31s Redis stop.
	Grace time.Duration

	mu       sync.Mutex
	failing  bool
	backoff  time.Duration
	failures int
	answered time.Time
}

func (s *Supervisor) base() time.Duration {
	if s.Base > 0 {
		return s.Base
	}
	return time.Second
}

func (s *Supervisor) max() time.Duration {
	if s.Max > 0 {
		return s.Max
	}
	return 30 * time.Second
}

func (s *Supervisor) degradeAfter() int {
	if s.DegradeAfter > 0 {
		return s.DegradeAfter
	}
	return 3
}

func (s *Supervisor) grace() time.Duration {
	if s.Grace > 0 {
		return s.Grace
	}
	return 5 * time.Second
}

// shouldReport decides whether a failed attempt means the dependency is down.
// Either signal is enough: the dependency has been silent for longer than
// Grace, or the loop has failed DegradeAfter times in a row (which covers a
// dependency that never answered).
func (s *Supervisor) shouldReport(failures int) bool {
	if time.Since(s.answered) >= s.grace() {
		return true
	}
	return failures >= s.degradeAfter()
}

// Resolve clears this supervisor's issue.
func (s *Supervisor) Resolve() { Resolve(s.Name) }

// Attached is called by the loop whenever the dependency answered it, which is
// the only honest recovery signal (see the type comment). It clears an outage
// this supervisor reported and resets the backoff so a later failure retries
// quickly again. Safe to call from the loop and cheap enough to call on every
// answered read.
func (s *Supervisor) Attached() {
	s.mu.Lock()
	recovering := s.failing
	s.failing = false
	s.failures = 0
	s.backoff = s.base()
	s.answered = time.Now()
	s.mu.Unlock()

	if recovering {
		slog.Info(s.Name + " reattached")
		s.Resolve()
	}
}

// Run drives loop until it ends cleanly or ctx is cancelled. A nil return from
// loop means it ended cleanly (context cancelled) and is passed through instead
// of being retried.
func (s *Supervisor) Run(ctx context.Context, loop func(context.Context) error) error {
	if ctx.Err() != nil {
		return nil // already shutting down: do not start the loop
	}
	s.mu.Lock()
	s.answered = time.Now() // assume healthy until proven silent
	s.mu.Unlock()

	for {
		err := loop(ctx)
		if err == nil || ctx.Err() != nil {
			s.mu.Lock()
			s.failing = false
			s.mu.Unlock()
			s.Resolve()
			return nil // clean shutdown, not a failure
		}

		s.mu.Lock()
		s.failing = true
		s.failures++
		failures, backoff := s.failures, s.backoff
		if backoff <= 0 {
			backoff = s.base()
		}
		report := s.shouldReport(failures)
		silent := time.Since(s.answered).Round(time.Millisecond)
		if report {
			Report(s.Name, "lost the bus: "+err.Error())
		}
		// Every failed attempt is logged: the count and the silence age are what
		// tell an operator whether this is a blip or an outage, and the backoff
		// bounds the rate to one line per attempt (30s apart at worst).
		slog.Warn(s.Name+" lost the bus, reconnecting",
			"attempt", failures, "silent_for", silent, "in", backoff, "err", err)
		s.mu.Unlock()

		select {
		case <-ctx.Done():
			s.mu.Lock()
			s.failing = false
			s.mu.Unlock()
			s.Resolve()
			return nil
		case <-time.After(backoff):
		}

		s.mu.Lock()
		if s.backoff < s.max() {
			if s.backoff *= 2; s.backoff > s.max() {
				s.backoff = s.max()
			}
		}
		s.mu.Unlock()
	}
}
