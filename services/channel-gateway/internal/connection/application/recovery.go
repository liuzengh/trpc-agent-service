package application

import (
	"time"

	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
)

func (r *accountRecord) terminalReason(a domain.Account, status ClientStatus) Reason {
	if status.Replaced {
		r.block(a.Revision, ReasonReplaced)
		return ReasonReplaced
	}
	if status.Retryable {
		return ReasonRetryable
	}
	r.block(a.Revision, ReasonClient)
	return ReasonClient
}

// restartDue is checked BEFORE acquisition/credential resolution. Source polling
// can wake the actor, but cannot shorten an established backoff or cooldown.
func (r *accountRecord) restartDue(a domain.Account) bool {
	s := r.supervisor
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.retryRevision != a.Revision {
		r.retryRevision = a.Revision
		r.restartAttempts = 0
		r.nextRetry = time.Time{}
		r.cooling = false
	}
	if r.nextRetry.IsZero() {
		return true
	}
	if !time.Now().Before(r.nextRetry) {
		r.nextRetry = time.Time{}
		return true
	}
	phase, reason := PhaseBackoff, ReasonRetryable
	if r.cooling {
		phase, reason = PhaseCooldown, ReasonCooldown
	}
	r.status = Status{AccountID: a.ID, BotID: a.BotID, Revision: a.Revision, Phase: phase, Reason: reason, RestartAttempts: r.restartAttempts, NextRetryAt: r.nextRetry}
	return false
}

// A temporary terminal failure schedules up to MaxRestarts fast restart slots
// (default 1s, 2s, 4s), then one half-open attempt per RestartCooldown (60s).
// READY never resets this count, including brief authentication successes. A
// successful half-open Client may keep running indefinitely; if it later fails
// temporarily, its next attempt still waits the cooldown. A higher revision or
// a new Supervisor process starts a fresh budget; this is not a global limiter.
func (r *accountRecord) scheduleRestart(a domain.Account) {
	s := r.supervisor
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.fatal || r.account != a || r.blockedRevision >= a.Revision {
		return
	}
	// A source outage/removal may pause acquisition, but must not erase a
	// terminal failure already classified by the Client Adapter.
	delay := s.options.RestartCooldown
	r.cooling = r.restartAttempts >= s.options.MaxRestarts
	if !r.cooling {
		r.restartAttempts++
		delay = s.options.RestartBackoff * time.Duration(1<<uint(r.restartAttempts-1))
		delay = min(delay, s.options.RestartCooldown)
	}
	r.nextRetry = time.Now().Add(delay)
}
